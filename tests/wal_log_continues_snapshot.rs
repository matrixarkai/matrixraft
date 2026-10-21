// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A record's retained log has to continue the snapshot the record carries.
//!
//! The fence validation already related the fence to the snapshot three ways:
//! the fence's snapshot index equals the snapshot's last index, the declared
//! floor sits after the snapshot, and the hard state's committed index reaches
//! no further than the log and snapshot together do. None of the three says
//! where the log *begins*, so all three hold for a record whose snapshot ends
//! at 10 and whose entries start at 20 with 22 reported committed. Indices 11
//! to 19 are then in no snapshot and in no log, and both recovery entry points
//! gate on exactly these two validations -- so the record is chosen, and
//! `restore_wal_record` installs it verbatim.
//!
//! The comparable engine asserts the same relation where it loads its log
//! buffer: the front of the log equals the persisted initial state's index.
//!
//! Two things here are easy to get wrong and are pinned separately:
//!
//! 1. **The declared floor is not the log.** Moving the floor up to meet the
//!    entries makes the record internally consistent and leaves the hole
//!    exactly where it was, so a rule written against the floor rather than
//!    against the first entry would accept it.
//! 2. **A delta record must not be judged this way.** Its entries are only
//!    what was appended after the previous record in its segment, so they
//!    legitimately start well past the snapshot. Folding clears the flag,
//!    which is why every whole-log record is still checked.

use matrixraft::{
    matrixraft_recover_latest_wal_record, matrixraft_validate_apply_snapshot_fence,
    matrixraft_wal_checksum, ApplySnapshotFence, Config, HardState, LogEntry, LogId, Membership,
    Peer, RaftCluster, ReplicaRole, SnapshotMetadata, WalRecord,
};

const GROUP_ID: u64 = 404;
const SNAPSHOT_INDEX: u64 = 10;

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 4, index },
        payload: b"entry".to_vec(),
        is_command: true,
    }
}

/// A record carrying a snapshot ending at 10, entries from `first_entry_index`
/// through 22, and 22 reported committed.
fn record(first_entry_index: u64, first_retained_log_index: u64) -> WalRecord {
    let mut record = WalRecord {
        entries_are_delta: false,
        group_id: GROUP_ID,
        node_id: 1,
        hard_state: HardState {
            current_term: 4,
            voted_for: Some(1),
            committed: Some(LogId { term: 4, index: 22 }),
        },
        membership: Membership {
            group_id: GROUP_ID,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 4,
        },
        entries: (first_entry_index..=22).map(entry).collect(),
        installed_snapshot: Some(SnapshotMetadata {
            snapshot_id: "floor".to_string(),
            last_log_id: LogId {
                term: 4,
                index: SNAPSHOT_INDEX,
            },
            membership: vec![1, 2, 3],
            members: Vec::new(),
            payload_bytes: 0,
        }),
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: 22,
            commit_index: 22,
            installed_snapshot_index: SNAPSHOT_INDEX,
            first_retained_log_index,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

#[test]
fn a_log_continuing_the_snapshot_is_accepted() {
    // The positive control for every refusal below: without it a rule that
    // refused every record carrying a snapshot would pass them all.
    let continues = record(SNAPSHOT_INDEX + 1, SNAPSHOT_INDEX + 1);
    matrixraft_validate_apply_snapshot_fence(&continues)
        .expect("a log starting where the snapshot ends has no hole in it");
    matrixraft_recover_latest_wal_record(std::slice::from_ref(&continues))
        .expect("and recovery takes it");
}

#[test]
fn a_log_starting_past_the_snapshot_is_refused() {
    let holed = record(20, SNAPSHOT_INDEX + 1);
    let err = matrixraft_validate_apply_snapshot_fence(&holed)
        .expect_err("entries from 20 leave 11 through 19 in no snapshot and no log");
    let message = err.to_string();
    assert!(
        message.contains("retained log starts at 20") && message.contains("snapshot at 11"),
        "the refusal has to name the index the log starts at and the one it should \
         continue from, so the hole is readable from the error: {message}"
    );
    assert!(
        matrixraft_recover_latest_wal_record(std::slice::from_ref(&holed)).is_err(),
        "recovery gates on this validation, so it must not choose the record either"
    );
}

#[test]
fn the_hole_is_not_made_legal_by_moving_the_declared_floor_to_meet_it() {
    // Floor 20 and entries from 20: every relation the fence validation
    // checked before this rule holds, and 11 through 19 are still nowhere.
    let consistent = record(20, 20);
    let err = matrixraft_validate_apply_snapshot_fence(&consistent).expect_err(
        "a floor that agrees with the entries describes the hole accurately \
         rather than closing it",
    );
    assert!(
        err.to_string().contains("retained log starts at 20"),
        "the rule has to be about the first entry, not the declared floor: {err}"
    );
}

#[test]
fn a_delta_record_is_not_judged_on_where_its_entries_start() {
    // A delta carries only what was appended after the previous record in its
    // segment, so entries from 20 over a snapshot at 10 is the ordinary shape
    // and not a hole. Nothing folded reaches here with the flag set.
    let mut delta = record(20, SNAPSHOT_INDEX + 1);
    delta.entries_are_delta = true;
    delta.checksum = matrixraft_wal_checksum(&delta);
    matrixraft_validate_apply_snapshot_fence(&delta)
        .expect("a delta's first entry says nothing about where the log begins");
}

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 14_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 15_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started() -> RaftCluster {
    let mut cluster =
        RaftCluster::new(GROUP_ID, Config::default(), vec![peer(1), peer(2), peer(3)])
            .expect("build the cluster");
    cluster.start().expect("start the cluster");
    cluster
}

#[test]
fn restoring_a_log_that_continues_the_snapshot_leaves_no_hole() {
    // The positive control for the refusal below, and the reason the restore
    // is checked at all: this is the path that installs the log.
    let mut cluster = started();
    cluster
        .restore_wal_record(record(SNAPSHOT_INDEX + 1, SNAPSHOT_INDEX + 1))
        .expect("a log starting where the snapshot ends restores");

    let range = cluster.retained_log_range(1).expect("the restored range");
    assert_eq!(range.installed_snapshot_index, SNAPSHOT_INDEX);
    assert_eq!(
        range.first_log_index,
        SNAPSHOT_INDEX + 1,
        "the log has to begin where the snapshot ends"
    );
    for index in SNAPSHOT_INDEX + 1..=range.last_log_index {
        assert!(
            range.serves(index),
            "index {index} is above the snapshot, so the log has to serve it"
        );
    }
}

#[test]
fn restoring_a_log_that_starts_past_the_snapshot_is_refused() {
    // Both recovery entry points gate on the fence validation, but this one
    // does not go through them -- and before the rule existed it installed the
    // record verbatim, leaving node 1 reporting 22 committed with 11 through 19
    // in no snapshot and in no log.
    let mut cluster = started();
    let err = cluster
        .restore_wal_record(record(20, SNAPSHOT_INDEX + 1))
        .expect_err("a log starting at 20 over a snapshot ending at 10 has a hole in it");
    assert!(
        err.to_string().contains("retained log starts at 20"),
        "the refusal has to name where the log starts: {err}"
    );

    // And it is refused rather than half-applied: the node still holds what it
    // had before, which for a freshly started cluster is no snapshot at all.
    let range = cluster.retained_log_range(1).expect("the untouched range");
    assert_eq!(
        range.installed_snapshot_index, 0,
        "a refused restore must not have installed the snapshot it carried"
    );
}
