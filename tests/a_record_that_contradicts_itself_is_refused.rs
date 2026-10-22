// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WAL record carries its own hard state, its own installed snapshot and an
//! apply fence describing both. `matrixraft_validate_apply_snapshot_fence`
//! checks that the three agree, and two of its four rules had no test:
//! removing either left all 181 test targets green.
//!
//! | rule | had a test |
//! |---|---|
//! | the fence's applied index is within the committed index | yes |
//! | **the fence's commit index equals the hard state's** | **no** |
//! | **the fence names the index of the snapshot it ships with** | **no** |
//! | the log does not overlap the snapshot | yes (`snapshot_fence_rejects_snapshot_floor_overlap`) |
//!
//! The two untested rules are the *agreement* rules, and what they defend is
//! worth stating precisely rather than overselling. This engine's own writer
//! cannot produce a record that breaks them: `wal_record_for` **derives**
//! `installed_snapshot_index` from the snapshot it is about to store and
//! `commit_index` from the same node as the hard state, so the three agree by
//! construction. These rules guard the other direction -- recovery reading a
//! record this writer did not produce, whether by an older or a different
//! writer, or one assembled by hand through the public `WalRecord`.
//!
//! That is still a contract worth holding, because
//! `matrixraft_recover_latest_wal_record` picks the newest record that
//! *validates*: the validator is the only thing between a self-contradicting
//! record and the state a node comes back with, and a node that accepted one
//! would resume with an apply fence describing a commit point it does not hold.
//!
//! Both are asserted on their message rather than on the error variant: all
//! four rules return `RaftError::Storage`, and they run in order, so a fixture
//! that breaks two relations at once proves nothing about the second.

use matrixraft::{
    wal::{
        matrixraft_validate_apply_snapshot_fence, matrixraft_wal_checksum, HardState, WalRecord,
    },
    ApplySnapshotFence, LogEntry, LogId, Membership, RaftError, SnapshotMetadata,
};

/// A record that agrees with itself: a snapshot through index 8, a log running
/// 9..=10, a hard state committed at 10, and a fence naming all of it.
fn consistent_record() -> WalRecord {
    let mut record = WalRecord {
        entries_are_delta: false,
        group_id: 7,
        node_id: 1,
        hard_state: HardState {
            current_term: 3,
            voted_for: Some(1),
            committed: Some(LogId { term: 3, index: 10 }),
        },
        membership: Membership {
            group_id: 7,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries: (9..=10)
            .map(|index| LogEntry {
                log_id: LogId { term: 3, index },
                payload: b"write".to_vec(),
                is_command: true,
            })
            .collect(),
        installed_snapshot: Some(SnapshotMetadata {
            snapshot_id: "snapshot-8".to_string(),
            last_log_id: LogId { term: 3, index: 8 },
            membership: vec![1, 2, 3],
            members: Vec::new(),
            payload_bytes: 0,
        }),
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: 10,
            commit_index: 10,
            installed_snapshot_index: 8,
            first_retained_log_index: 9,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

fn refusal_of(record: &WalRecord) -> String {
    match matrixraft_validate_apply_snapshot_fence(record) {
        Err(RaftError::Storage(reason)) => reason,
        other => panic!("expected a storage refusal, got {other:?}"),
    }
}

#[test]
fn a_record_that_agrees_with_itself_is_accepted() {
    // The control: without it, each test below would also pass against a
    // validator that refused every record, which would make recovery
    // impossible rather than safe.
    matrixraft_validate_apply_snapshot_fence(&consistent_record())
        .expect("a record whose fence, hard state and snapshot agree is valid");
}

#[test]
fn a_fence_whose_commit_index_disagrees_with_the_hard_state_is_refused() {
    let mut record = consistent_record();
    // Lower the fence's commit index only. Its applied index stays at 10, so
    // the first rule -- applied within committed -- is still satisfied against
    // the hard state's 10, and this rule has to be the one that answers.
    record.apply_snapshot_fence.commit_index = 9;
    record.checksum = matrixraft_wal_checksum(&record);

    assert_eq!(
        refusal_of(&record),
        "apply snapshot fence commit index does not match hard state",
        "a record may not describe a commit point its own hard state denies"
    );
}

#[test]
fn a_fence_that_names_the_wrong_snapshot_index_is_refused() {
    let mut record = consistent_record();
    // The record still ships the snapshot ending at 8; only the fence's claim
    // about it moves. 7 is below the snapshot, so the overlap rule further
    // down stays satisfied by the log at 9 and cannot answer instead.
    record.apply_snapshot_fence.installed_snapshot_index = 7;
    record.checksum = matrixraft_wal_checksum(&record);

    assert_eq!(
        refusal_of(&record),
        "apply snapshot fence does not match installed snapshot",
        "the fence must name the snapshot the record actually carries"
    );
}

#[test]
fn a_record_with_no_snapshot_is_still_accepted() {
    // Both snapshot rules sit inside `if let Some(snapshot)`, which is why they
    // can be strict equalities at all: a snapshotless record has nothing for
    // the fence to disagree with.
    //
    // This is a control -- it pins that the validator does not *require* a
    // snapshot -- and not a test of that guard. **The guard is not separately
    // discriminable here, and deliberately so.** Measured: applying the two
    // rules unconditionally, against a zero stand-in snapshot, leaves this test
    // green, because a snapshotless record's fence carries zeroes and zero
    // matches the stand-in. Discriminating it would need a record with no
    // snapshot whose fence names one anyway -- and asserting what the engine
    // does with that would pin behaviour on a record `wal_record_for` cannot
    // produce, which is a decision for whoever wants that rule, not a contract
    // to freeze in passing.
    let mut record = consistent_record();
    record.installed_snapshot = None;
    record.entries = (1..=10)
        .map(|index| LogEntry {
            log_id: LogId { term: 3, index },
            payload: b"write".to_vec(),
            is_command: true,
        })
        .collect();
    record.apply_snapshot_fence.installed_snapshot_index = 0;
    record.apply_snapshot_fence.first_retained_log_index = 0;
    record.checksum = matrixraft_wal_checksum(&record);

    matrixraft_validate_apply_snapshot_fence(&record)
        .expect("a record with no snapshot has no snapshot to contradict");
}
