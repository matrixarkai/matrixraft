// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The persisted committed index only ever moves forward.
//!
//! Recovery reads the latest valid record, so a record carrying a lower
//! committed index than one already persisted un-commits entries that were
//! committed. Measured before the guard existed:
//!
//! ```text
//! committed index 10 appended: true
//! committed index 4 appended AFTER 10: true
//! segment (id, first, last): [(0, 10, 4)]
//! what recovery would read as committed: Some(4)
//! ```
//!
//! Two things wrong at once: entries 5 through 10 un-commit, and the segment's
//! own bounds invert, which makes any `first_index..=last_index` range over it
//! meaningless.
//!
//! The guard is on the committed index only. The record index that sets the
//! segment bounds falls back to the last ENTRY index when a record carries no
//! committed index, and that one may legitimately move back -- a record whose
//! entries end lower than the previous record's is a truncation, and a
//! truncated segment should say so. The last test pins that distinction.

use matrixraft::{
    wal::{matrixraft_wal_checksum, HardState, LocalRaftWal, WalRecord},
    ApplySnapshotFence, LogEntry, LogId, Membership,
};

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 4, index },
        payload: format!("e-{index}").into_bytes(),
        is_command: true,
    }
}

fn membership() -> Membership {
    Membership {
        group_id: 404,
        voters: vec![1, 2, 3],
        learners: Vec::new(),
        witnesses: Vec::new(),
        epoch: 4,
    }
}

fn committed_record(committed_index: u64) -> WalRecord {
    let mut record = WalRecord {
        entries_are_delta: false,
        group_id: 404,
        node_id: 1,
        hard_state: HardState {
            current_term: 4,
            voted_for: Some(1),
            committed: Some(LogId {
                term: 4,
                index: committed_index,
            }),
        },
        membership: membership(),
        entries: Vec::new(),
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: committed_index,
            commit_index: committed_index,
            installed_snapshot_index: 0,
            first_retained_log_index: 0,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

/// A record that commits nothing and carries entries, which is what a
/// truncation looks like.
fn entries_only_record(last_entry_index: u64) -> WalRecord {
    let mut record = WalRecord {
        entries_are_delta: false,
        group_id: 404,
        node_id: 1,
        hard_state: HardState {
            current_term: 4,
            voted_for: Some(1),
            committed: None,
        },
        membership: membership(),
        entries: vec![entry(last_entry_index)],
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: 0,
            commit_index: 0,
            installed_snapshot_index: 0,
            first_retained_log_index: 0,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

#[test]
fn a_record_committing_below_what_is_persisted_is_refused() {
    let mut wal = LocalRaftWal::new(8).expect("segmented wal");
    wal.append(committed_record(10))
        .expect("the first record is persisted");

    let error = wal
        .append(committed_record(4))
        .expect_err("a committed index below the persisted one has to be refused");
    let message = error.to_string();
    assert!(
        message.contains('4') && message.contains("10"),
        "the error has to name both indexes, got: {message}"
    );

    // And nothing was written: recovery still reads 10, and the segment's
    // bounds are still the right way round.
    let latest = wal
        .records()
        .last()
        .and_then(|record| record.hard_state.committed.as_ref().map(|c| c.index));
    assert_eq!(latest, Some(10), "the refused record must not be stored");
    let segment = &wal.segments()[0];
    assert!(
        segment.first_index <= segment.last_index,
        "segment bounds inverted: first {} last {}",
        segment.first_index,
        segment.last_index
    );
}

#[test]
fn committing_the_same_index_again_is_allowed() {
    // The guard is "not backward", not "strictly forward": a record that
    // re-persists the same committed index is how a hard-state-only write
    // looks, and refusing it would refuse ordinary operation.
    let mut wal = LocalRaftWal::new(8).expect("segmented wal");
    wal.append(committed_record(10)).expect("first");
    wal.append(committed_record(10))
        .expect("the same committed index again is not a regression");
    wal.append(committed_record(11))
        .expect("and forward is fine");
}

#[test]
fn a_truncating_record_may_still_move_the_entry_tail_back() {
    // The segment bound falls back to the last entry index for a record that
    // commits nothing, and that may legitimately move back -- it is what a
    // truncation looks like. Guarding it would refuse conflict resolution.
    let mut wal = LocalRaftWal::new(8).expect("segmented wal");
    wal.append(entries_only_record(12))
        .expect("entries up to 12");
    wal.append(entries_only_record(8))
        .expect("a truncation back to 8 is not a committed-index regression");
}

#[test]
fn the_guard_holds_across_a_segment_roll() {
    // The persisted index is read back across segments, not just within the
    // active one, so a roll must not reset it. One record per segment forces a
    // roll on every append.
    let mut wal = LocalRaftWal::new(1).expect("one record per segment");
    wal.append(committed_record(10)).expect("first segment");
    wal.append(committed_record(11)).expect("second segment");
    assert!(
        wal.segments().len() >= 2,
        "the roll has to have happened for this test to be about anything"
    );

    wal.append(committed_record(5))
        .expect_err("a regression is refused across a segment boundary too");
}

#[test]
fn the_persisted_index_is_the_highest_in_the_segment_not_the_first() {
    // Records sit in append order, so reading the first rather than the last
    // gives a lower persisted index and lets a regression through. Three
    // appends in one segment, with the regression landing between the two
    // committed indexes it holds.
    let mut wal = LocalRaftWal::new(8).expect("segmented wal");
    wal.append(committed_record(10)).expect("commits 10");
    wal.append(committed_record(11)).expect("commits 11");

    wal.append(committed_record(10))
        .expect_err("10 is below the persisted 11 and has to be refused");
}

#[test]
fn a_record_committing_nothing_does_not_reset_the_persisted_index() {
    // An entries-only record carries no committed index, and reading it as a
    // committed index of zero would let anything through afterwards.
    let mut wal = LocalRaftWal::new(8).expect("segmented wal");
    wal.append(committed_record(10)).expect("commits 10");
    wal.append(entries_only_record(11))
        .expect("an entries-only record is fine");

    wal.append(committed_record(4)).expect_err(
        "4 is still below the persisted 10, which the entries-only \
                     record did not erase",
    );
}
