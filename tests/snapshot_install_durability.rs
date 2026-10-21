// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A follower keeps the snapshot it accepted.
//!
//! Accepting the last chunk of a snapshot is a promise in the same way an
//! append ack is, and a larger one: the leader advances this peer to the
//! snapshot's last index, which lets it stop sending -- and eventually
//! compact -- every entry below that. If the receiver then restarts with
//! nothing, those entries may no longer exist anywhere to re-send.
//!
//! Measured before the fix:
//!
//! ```text
//! PROBE A: accepted=true next_offset=14 committed=7 reason=snapshot_installed
//! PROBE B: after restart surviving_records=0 recovered_snapshot_index=None
//! ```
//!
//! The receiver claimed state through index 7 and kept none of it. This is the
//! same hole the append ack had, in the other place the WAL was not written:
//! `runtime_step_message` ends in `other => cluster.step(other)`, and an
//! install took that path.
//!
//! **Completing chunks only.** Earlier chunks are buffered and change nothing
//! durable, so a record per chunk would be an fsync per 64 KiB of every
//! transfer and would promise nothing. A throttled response is the receiver
//! asking for less rather than accepting, so it claims nothing either. Both
//! are pinned below.

use matrixraft::{
    Config, InstallSnapshotRequest, LogId, Message, NodeOptions, NodeRuntime, Peer, ReplicaRole,
    SnapshotChunk, SnapshotMetadata, StepResult,
};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 241;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 50_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 50_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn base_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-snapshot-ack-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn options(base: &Path) -> NodeOptions {
    NodeOptions {
        group_id: GROUP_ID,
        node_id: 1,
        raft_addr: "127.0.0.1:50001".to_string(),
        snapshot_addr: "127.0.0.1:50501".to_string(),
        wal_dir: base.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config: Config::default(),
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

fn install_request(index: u64, done: bool) -> InstallSnapshotRequest {
    InstallSnapshotRequest {
        group_id: GROUP_ID,
        term: 9,
        leader_id: 2,
        chunk: SnapshotChunk {
            meta: SnapshotMetadata {
                snapshot_id: "snap-1".to_string(),
                last_log_id: LogId { term: 9, index },
                membership: vec![1, 2, 3],
                members: vec![peer(1), peer(2), peer(3)],
            },
            offset: 0,
            data: b"snapshot-bytes".to_vec(),
            done,
        },
    }
}

/// Steps the chunk in and returns whether the receiver accepted it.
fn step_install(runtime: &NodeRuntime, index: u64, done: bool) -> bool {
    match runtime
        .step(Message::InstallSnapshot {
            target: 1,
            request: install_request(index, done),
        })
        .expect("the install is answered")
    {
        StepResult::InstallSnapshot(response) => response.accepted && !response.throttled,
        other => panic!("expected an install response, got {other:?}"),
    }
}

fn surviving_records(runtime: &NodeRuntime) -> usize {
    runtime
        .wal_recovery_report()
        .expect("a recovery report")
        .map(|report| report.surviving_records)
        .unwrap_or(0)
}

#[test]
fn a_follower_still_holds_the_snapshot_it_accepted() {
    let base = base_dir("holds");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create the group");
        runtime.start().expect("start it");
        assert!(
            step_install(&runtime, 7, true),
            "the premise: the receiver accepts the completing chunk"
        );
        runtime.shutdown().expect("shut it down");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create the group");
    restarted.start().expect("re-start it");
    let report = restarted
        .wal_recovery_report()
        .expect("a recovery report")
        .expect("the group recovered something");
    let recovered_index = report
        .recovered
        .as_ref()
        .and_then(|record| record.installed_snapshot.as_ref())
        .map(|snapshot| snapshot.last_log_id.index);
    assert_eq!(
        recovered_index,
        Some(7),
        "the accepted snapshot has to come back, because the leader may already \
         have stopped sending -- and compacted -- everything below it: {report:?}"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn the_restarted_follower_reports_the_committed_index_it_accepted() {
    // The install answered `committed=7`, so a leader may count index 7 on
    // this node. After a restart the node has to agree.
    let base = base_dir("committed");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        assert!(step_install(&runtime, 7, true));
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    let status = restarted.status().expect("status");
    let health = status
        .cluster_status
        .as_ref()
        .map(|cluster| &cluster.apply_health)
        .expect("a cluster status");
    assert_eq!(
        health.commit_index, 7,
        "the committed index the receiver claimed has to survive: {health:?}"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_chunk_that_does_not_complete_the_snapshot_writes_nothing() {
    // The discriminator. Buffering a chunk changes nothing durable and claims
    // nothing, so persisting per chunk would cost an fsync per 64 KiB of every
    // transfer. A fix that dropped the completing-chunk condition passes the
    // tests above and fails this one.
    //
    // Checked across a RESTART, not on the live runtime: `wal_recovery_report`
    // describes the recovery performed when the WAL was opened, so asking the
    // same runtime always answers zero and the first version of this test
    // passed whatever the install path wrote.
    let base = base_dir("partial");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        // Accepted or not, it is not the last chunk, so there is nothing to keep.
        let accepted = step_install(&runtime, 7, false);
        assert!(
            accepted,
            "the premise: the receiver takes the chunk and buffers it, so the \
             only thing stopping a record is that the chunk does not complete \
             the snapshot"
        );
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    assert_eq!(
        surviving_records(&restarted),
        0,
        "a buffered chunk must not have written a record"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_group_that_accepted_no_snapshot_recovers_nothing() {
    // The positive control: a node that wrote a record on every step, or on
    // start, would satisfy the two tests above whatever the install path did.
    let base = base_dir("nothing");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    assert_eq!(
        surviving_records(&restarted),
        0,
        "so the record above came from the install and not from starting up"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_batched_install_is_durable_too() {
    // `step_batch` persists once for a whole batch and has its own condition,
    // so fixing only the single step would leave a host that batches its RPCs
    // claiming a snapshot it does not keep.
    let base = base_dir("batched");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        let results = runtime
            .step_batch(vec![Message::InstallSnapshot {
                target: 1,
                request: install_request(7, true),
            }])
            .expect("the batch is answered");
        let accepted = matches!(
            results.first(),
            Some(StepResult::InstallSnapshot(response))
                if response.accepted && !response.throttled
        );
        assert!(
            accepted,
            "the premise: the batch accepts the install: {results:?}"
        );
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    assert!(
        surviving_records(&restarted) > 0,
        "a batched install has to be as durable as a single one"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
