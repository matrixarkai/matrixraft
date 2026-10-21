// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A restored WAL record cannot put the log at the index ceiling.
//!
//! `restore_wal_record` is **public** and installs a host's log verbatim. The
//! continuity rule added for the snapshot case returns `Ok` early when the
//! record carries no snapshot, so it did not cover this: a record whose last
//! entry sat at `u64::MAX` was accepted, and the next proposal faulted while
//! minting `last_log_index + 1`.
//!
//! Measured before the fix:
//!
//! ```text
//! PROBE A: the record at the ceiling was RESTORED
//! now proposing against the restored log ...
//! panicked at src/facade/cluster_runtime.rs:3320: attempt to add with overflow
//! ```
//!
//! This is the host-supplied-value class, not the wire class -- the same shape
//! as an applier trusting a handler's reported index. A peer cannot get here:
//! the transport validator refuses an index at the ceiling, and a follower's
//! own contiguity check refuses one that arrives through `step` anyway (both
//! confirmed by probe).
//!
//! **What is pinned here and what is not.** The boundary is pinned: these
//! tests fail if the rule is removed. The interior sites changed alongside it
//! -- a refusal when a new index is minted, and saturating arithmetic in the
//! two record builders, the pipeline reset and the storage trait's
//! `first_index` -- are defence in depth, and once this boundary is closed no
//! route to them was found. They are deliberately **not** asserted here: a
//! test that cannot reach the state it claims to check passes for the wrong
//! reason, which this suite has already shipped once.

use matrixraft::{Config, LogEntry, LogId, Peer, RaftCluster, ReplicaRole, SnapshotMetadata};

const GROUP_ID: u64 = 211;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 45_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 45_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started() -> (RaftCluster, u64) {
    let mut cluster =
        RaftCluster::new(GROUP_ID, Config::default(), vec![peer(1), peer(2), peer(3)])
            .expect("build the cluster");
    cluster.start().expect("start the cluster");
    for i in 0..3u64 {
        cluster
            .propose(format!("entry-{i}").into_bytes())
            .expect("a write");
    }
    let leader = cluster.leader_id().expect("a leader");
    (cluster, leader)
}

fn entry_at(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: b"x".to_vec(),
        is_command: false,
    }
}

#[test]
fn a_record_whose_log_ends_at_the_ceiling_is_refused() {
    let (mut cluster, leader) = started();
    let mut record = cluster.wal_record_for(leader).expect("a record");
    record.installed_snapshot = None;
    record.entries_are_delta = false;
    record.entries = vec![entry_at(u64::MAX)];

    let error = cluster
        .restore_wal_record(record)
        .expect_err("a log that can never take another entry has to be refused");
    let message = error.to_string();
    assert!(
        message.contains("largest representable index"),
        "the refusal has to say what is wrong with the record: {message}"
    );
}

#[test]
fn a_record_whose_snapshot_is_at_the_ceiling_is_refused() {
    // The other field the rule covers. A snapshot at the ceiling leaves
    // `snapshot_index + 1` -- the first index the record claims to retain --
    // unrepresentable.
    //
    // The snapshot is CONSTRUCTED rather than mutated: this group installs none
    // by itself, and the first version of this test took an early return on
    // that and passed while asserting nothing.
    let (mut cluster, leader) = started();
    let mut record = cluster.wal_record_for(leader).expect("a record");
    assert!(
        record.installed_snapshot.is_none(),
        "the premise of constructing one: the group has not installed a snapshot"
    );
    record.entries = Vec::new();
    record.entries_are_delta = false;
    record.installed_snapshot = Some(SnapshotMetadata {
        snapshot_id: "snap-1".to_string(),
        last_log_id: LogId {
            term: 1,
            index: u64::MAX,
        },
        membership: vec![1, 2, 3],
        members: vec![peer(1), peer(2), peer(3)],
    });

    let error = cluster
        .restore_wal_record(record)
        .expect_err("a snapshot at the ceiling has to be refused too");
    assert!(
        error.to_string().contains("largest representable index"),
        "{error}"
    );
}

#[test]
fn a_record_whose_snapshot_is_below_the_ceiling_still_restores() {
    // The positive control for the snapshot arm specifically, so that arm
    // cannot pass by the rule rejecting every record that carries a snapshot.
    let (mut cluster, leader) = started();
    let mut record = cluster.wal_record_for(leader).expect("a record");
    record.entries = Vec::new();
    record.entries_are_delta = false;
    record.installed_snapshot = Some(SnapshotMetadata {
        snapshot_id: "snap-1".to_string(),
        last_log_id: LogId {
            term: 1,
            index: u64::MAX - 1,
        },
        membership: vec![1, 2, 3],
        members: vec![peer(1), peer(2), peer(3)],
    });
    cluster
        .restore_wal_record(record)
        .expect("one short of the ceiling is still a usable snapshot");
}

#[test]
fn an_ordinary_record_still_restores() {
    // Positive control. A rule that refused every record would satisfy the
    // tests above, and this engine recovers by restoring records.
    let (mut cluster, leader) = started();
    let record = cluster.wal_record_for(leader).expect("a record");
    cluster
        .restore_wal_record(record)
        .expect("a well formed record still restores");
}

#[test]
fn the_group_still_proposes_after_an_ordinary_restore() {
    // And the restored group is still usable, which is the point of restoring.
    let (mut cluster, leader) = started();
    let record = cluster.wal_record_for(leader).expect("a record");
    cluster.restore_wal_record(record).expect("restored");
    cluster
        .propose(b"after the restore".to_vec())
        .expect("the group still takes writes after recovery");
}
