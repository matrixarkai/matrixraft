// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A follower keeps the entries it acknowledged.
//!
//! A leader counts a follower's `match_index` toward the commit quorum, so an
//! ack is a promise: the follower is claiming it still holds those entries
//! after a restart. Until now it did not. The WAL was written on exactly two
//! paths -- a proposal, and a granted binding vote -- and an incoming append
//! was neither, so a follower answered success out of memory alone.
//!
//! Measured before the fix, stepping two entries into a node and restarting it:
//!
//! ```text
//! PROBE A: ack success=true match_index=2
//! PROBE B: after restart apply=(0, 0)
//! PROBE C: recovery report = surviving_records: 0
//! ```
//!
//! The follower told the leader it held entries 1 and 2 and kept nothing. A
//! quorum of followers doing that and restarting loses an entry the leader had
//! already reported committed, which is the durability property Raft exists to
//! provide.
//!
//! This is the same defect as the unpersisted vote, in the one place that fix
//! did not reach. Its own note said only the proposal path wrote a record --
//! true of appends as well, and only votes were added.
//!
//! **Entry-bearing appends only.** A heartbeat carries no entries and needs no
//! record; writing one per heartbeat per group would be an fsync per interval
//! for nothing. The last test here pins that, so the fix cannot be simplified
//! into persisting on every append.
//!
//! **The ack is withheld when the record does not land**, the same conservative
//! direction the vote path takes: a leader counting entries the follower cannot
//! honour after a restart is the whole defect.

use matrixraft::{
    AppendEntriesRequest, Config, LogEntry, LogId, Message, NodeOptions, NodeRuntime, Peer,
    ReplicaRole, StepResult,
};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 231;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 49_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 49_500 + node_id),
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
        "matrixraft-append-ack-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn options(base: &Path) -> NodeOptions {
    NodeOptions {
        group_id: GROUP_ID,
        node_id: 1,
        raft_addr: "127.0.0.1:49001".to_string(),
        snapshot_addr: "127.0.0.1:49501".to_string(),
        wal_dir: base.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config: Config::default(),
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

fn entry(index: u64, term: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term, index },
        payload: format!("payload-{index}").into_bytes(),
        is_command: false,
    }
}

fn append_request(entries: Vec<LogEntry>) -> AppendEntriesRequest {
    let leader_commit = entries
        .last()
        .map(|entry| entry.log_id.index)
        .unwrap_or_default();
    AppendEntriesRequest {
        group_id: GROUP_ID,
        term: 9,
        leader_id: 2,
        prev_log_id: None,
        entries,
        leader_commit,
        lease_epoch: 0,
    }
}

/// Steps the request in and returns whether the node acknowledged it.
fn step_append(runtime: &NodeRuntime, entries: Vec<LogEntry>) -> bool {
    match runtime
        .step(Message::AppendEntries {
            target: 1,
            request: append_request(entries),
        })
        .expect("the append is answered")
    {
        StepResult::AppendEntries(response) => response.success,
        other => panic!("expected an append response, got {other:?}"),
    }
}

#[test]
fn a_follower_still_holds_the_entries_it_acknowledged() {
    let base = base_dir("holds");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create the group");
        runtime.start().expect("start it");
        assert!(
            step_append(&runtime, vec![entry(1, 9), entry(2, 9)]),
            "the premise: the node accepts the append and so claims the entries"
        );
        runtime.shutdown().expect("shut it down");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create the group");
    restarted.start().expect("re-start it");
    let report = restarted
        .wal_recovery_report()
        .expect("a recovery report")
        .expect("the group recovered something");
    assert!(
        report.surviving_records > 0,
        "the acknowledged entries have to be on disk: {report:?}"
    );
    let recovered = report
        .recovered
        .as_ref()
        .expect("a recovered record for an acknowledged append");
    let indexes: Vec<u64> = recovered
        .entries
        .iter()
        .map(|entry| entry.log_id.index)
        .collect();
    assert!(
        indexes.contains(&1) && indexes.contains(&2),
        "both acknowledged entries have to come back, not just the last: {indexes:?}"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn the_restarted_follower_reports_the_progress_it_acknowledged() {
    // The ack claimed match_index 2, so a leader may count index 2 on this
    // node. After a restart the node has to agree.
    let base = base_dir("progress");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        assert!(step_append(&runtime, vec![entry(1, 9), entry(2, 9)]));
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
        health.commit_index, 2,
        "the committed index the follower acknowledged has to survive: {health:?}"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_group_that_acknowledged_nothing_recovers_nothing() {
    // The positive control. A node that wrote a record on every step, or on
    // start, would satisfy both tests above whatever the append path did.
    let base = base_dir("nothing");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    let report = restarted.wal_recovery_report().expect("a recovery report");
    let surviving = report.map(|report| report.surviving_records).unwrap_or(0);
    assert_eq!(
        surviving, 0,
        "a group that acknowledged nothing has nothing to recover, so the \
         records above came from the append and not from starting up"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_heartbeat_does_not_write_a_record() {
    // An append with no entries is a heartbeat. Persisting one would cost an
    // fsync per interval per group and promise nothing, so this pins that the
    // fix stays entry-bearing.
    let base = base_dir("heartbeat");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        // Accepted or refused, either way it carries no entries to keep.
        let _ = step_append(&runtime, Vec::new());
        let _ = step_append(&runtime, Vec::new());
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    let report = restarted.wal_recovery_report().expect("a recovery report");
    let surviving = report.map(|report| report.surviving_records).unwrap_or(0);
    assert_eq!(
        surviving, 0,
        "two heartbeats must not have written anything"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_batched_append_is_durable_too() {
    // `step_batch` persists once for a whole batch rather than per message, so
    // it has its own condition and needs its own test: fixing only the single
    // step would leave a host that batches its RPCs acknowledging entries it
    // does not keep.
    let base = base_dir("batched");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        let results = runtime
            .step_batch(vec![Message::AppendEntries {
                target: 1,
                request: append_request(vec![entry(1, 9), entry(2, 9)]),
            }])
            .expect("the batch is answered");
        let acked = matches!(
            results.first(),
            Some(StepResult::AppendEntries(response)) if response.success
        );
        assert!(
            acked,
            "the premise: the batch accepts the append: {results:?}"
        );
        runtime.shutdown().expect("shutdown");
    }

    let mut restarted = NodeRuntime::create(options(&base)).expect("re-create");
    restarted.start().expect("re-start");
    let report = restarted
        .wal_recovery_report()
        .expect("a recovery report")
        .expect("the group recovered something");
    assert!(
        report.surviving_records > 0,
        "a batched append has to be as durable as a single one: {report:?}"
    );
    let _ = restarted.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
