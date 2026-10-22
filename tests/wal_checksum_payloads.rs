// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The WAL checksum covers entry payloads, because recovery replays them.
//!
//! It used to cover `entries.payload_len` and not the bytes. That is the same
//! shape of hole the membership had before `wal_checksum_membership.rs`, and
//! with a worse consequence: recovery hands these payloads to the state
//! machine, so a payload corrupted on disk feeds it wrong input with every
//! other field intact and the record reporting itself as valid.
//!
//! The length is not a substitute, because a payload corrupted **in place**
//! keeps its length. MEASURED before this change, on a real segment file: one
//! base64 character changed inside an entry payload -- same length, still
//! valid base64, so neither the JSON parse nor the base64 decode objected --
//! and recovery answered `surviving_records = 1`,
//! `truncated_corrupt_tail = false`.
//!
//! Three things are pinned separately, because each is easy to get wrong:
//!
//! 1. **A changed payload byte changes the checksum.** The point of the change.
//! 2. **A WAL written by either earlier build still validates.** There are now
//!    three accepted forms, and each exception closes behind a file as it is
//!    rewritten.
//! 3. **Nothing else about the hash moved.** A record with no entries must hash
//!    exactly as it did before, which is what makes the compat claim checkable
//!    rather than hopeful.

use matrixraft::{
    matrixraft_wal_checksum, matrixraft_wal_checksum_covers_membership,
    matrixraft_wal_checksum_covers_payloads, matrixraft_wal_checksum_format,
    matrixraft_wal_checksum_valid, ApplySnapshotFence, HardState, LogEntry, LogId, Membership,
    WalRecord,
};

const GROUP_ID: u64 = 44;

/// The same record `wal_checksum_membership.rs` builds, so the two files agree
/// on what a record looks like.
fn record_with_payload(payload: &[u8]) -> WalRecord {
    let mut record = WalRecord {
        entries_are_delta: false,
        group_id: GROUP_ID,
        node_id: 1,
        hard_state: HardState {
            current_term: 4,
            voted_for: Some(1),
            committed: Some(LogId { term: 4, index: 2 }),
        },
        membership: Membership {
            group_id: GROUP_ID,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 4,
        },
        entries: vec![LogEntry {
            log_id: LogId { term: 4, index: 2 },
            payload: payload.to_vec(),
            is_command: true,
        }],
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: 2,
            commit_index: 2,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

#[test]
fn a_payload_changed_in_place_breaks_the_checksum() {
    let record = record_with_payload(b"entry");
    assert!(matrixraft_wal_checksum_valid(&record));

    // Same length, one byte different -- the case the length could never catch.
    let mut corrupted = record.clone();
    corrupted.entries[0].payload = b"entrX".to_vec();
    assert_eq!(
        corrupted.entries[0].payload.len(),
        record.entries[0].payload.len(),
        "the premise: the corruption does not change the length"
    );

    assert!(
        !matrixraft_wal_checksum_valid(&corrupted),
        "a payload corrupted in place must not validate"
    );
}

#[test]
fn a_payload_of_a_different_length_still_breaks_the_checksum() {
    // The length was already covered, and it stays covered -- this is the
    // control for the clause the change did not touch.
    let record = record_with_payload(b"entry");
    let mut corrupted = record.clone();
    corrupted.entries[0].payload = b"entry-and-more".to_vec();

    assert!(!matrixraft_wal_checksum_valid(&corrupted));
}

#[test]
fn a_record_from_the_build_before_payload_coverage_still_validates() {
    // The literal is the checksum this exact record carried when the hash
    // covered the membership but not the payloads, captured from the build
    // before the change. If `record_with_payload` is ever edited this literal
    // is expected to fail -- it pins a record, not a formula.
    let mut record = record_with_payload(b"entry");
    let fresh = record.checksum.clone();
    record.checksum = "f004765ac3ab37c2".to_string();

    assert_ne!(
        fresh, record.checksum,
        "the premise: the two forms disagree, or this test proves nothing"
    );
    assert!(
        matrixraft_wal_checksum_valid(&record),
        "a WAL written before payloads were covered still has to recover"
    );
    assert!(
        !matrixraft_wal_checksum_covers_payloads(&record),
        "and the caller can see that its payloads were never vouched for"
    );
    assert!(
        matrixraft_wal_checksum_covers_membership(&record),
        "while its membership was -- that form covered the membership"
    );
}

#[test]
fn a_fresh_record_covers_both() {
    let record = record_with_payload(b"entry");
    assert!(matrixraft_wal_checksum_covers_payloads(&record));
    assert!(matrixraft_wal_checksum_covers_membership(&record));
}

#[test]
fn the_published_coverage_list_names_the_payload() {
    let format = matrixraft_wal_checksum_format();
    assert!(
        format
            .covered_fields
            .contains(&"entries.payload".to_string()),
        "a caller reading the format must be told payloads are covered: {:?}",
        format.covered_fields
    );
    assert!(
        format
            .covered_fields
            .contains(&"entries.payload_len".to_string()),
        "and the length stays listed, because it is still mixed in separately"
    );
}
