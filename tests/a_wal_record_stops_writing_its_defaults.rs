// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WAL record stops writing the fields a reader would supply anyway.
//!
//! Three fields were written on every record while holding the value any
//! deserializer already defaults to: an entry's `is_command` when it is not a
//! command, and a membership's `learners` and `witnesses` when the group has
//! neither. Measured on a record carrying one entry with a nine-byte payload,
//! that is **48 of 431 bytes, 11.1%**, spent on saying nothing:
//!
//! ```text
//! "is_command":false   19 B   (13 name + 5 value + 1 comma)
//! "learners":[]        14 B   (11 name + 2 value + 1 comma)
//! "witnesses":[]       15 B   (12 name + 2 value + 1 comma)
//! ```
//!
//! ## Why this is not a format change
//!
//! Each of the three already reads as `#[serde(default)]` -- `is_command` on
//! the WAL's own `EntryIn`, the two lists on `Membership` -- so a record written
//! without them is identical to a reader of any age. That is the same argument
//! the record already makes for `entries_are_delta` and `installed_snapshot`,
//! which stopped being written for the same reason.
//!
//! The checksum does not move either, and for a reason worth stating rather
//! than assuming: it is computed over the record's VALUES, not its key names,
//! and it mixes each membership list's LENGTH before its members. An absent
//! list reads back empty, so the length is still zero and the hash is unchanged.
//! `is_command` is not mixed at all.
//!
//! ## What this is worth, and when
//!
//! Nothing, while each record is flushed separately. A durable append costs
//! about one 4 KiB block whatever the record holds, which the sibling suite
//! `what_an_append_costs_the_device_not_the_file` measures as 9.95x the file.
//! Only at one fsync per batch does device-to-file reach 1.00x, and only there
//! does a smaller record convert one-for-one into fewer bytes written. Batch
//! first; this is the second lever, not the first.

#![allow(clippy::all)]

use matrixraft::{
    matrixraft_wal_checksum, matrixraft_wal_checksum_valid, ApplySnapshotFence, HardState,
    LogEntry, LogId, Membership, WalRecord,
};

const PAYLOAD: usize = 9;

fn record(is_command: bool, learners: Vec<u64>, witnesses: Vec<u64>) -> WalRecord {
    let mut value = WalRecord {
        group_id: 17,
        node_id: 1,
        hard_state: HardState {
            current_term: 7,
            voted_for: Some(1),
            committed: Some(LogId { term: 7, index: 42 }),
        },
        membership: Membership {
            group_id: 17,
            voters: vec![1, 2, 3],
            learners,
            witnesses,
            epoch: 4,
        },
        entries: vec![LogEntry {
            log_id: LogId { term: 7, index: 42 },
            payload: vec![7u8; PAYLOAD],
            is_command,
        }],
        entries_are_delta: false,
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: 42,
            commit_index: 42,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    };
    value.checksum = matrixraft_wal_checksum(&value);
    value
}

fn written(record: &WalRecord) -> String {
    serde_json::to_string(record).expect("a record serializes")
}

#[test]
fn the_ordinary_record_stops_writing_what_a_reader_defaults() {
    let json = written(&record(false, Vec::new(), Vec::new()));

    for absent in ["\"is_command\"", "\"learners\"", "\"witnesses\""] {
        assert!(
            !json.contains(absent),
            "{absent} is still written on an ordinary record: {json}"
        );
    }

    // The saving as an arithmetic in its own terms, not a number copied from a
    // run: each key costs its quoted name, its colon, its value and its comma.
    let is_command = "\"is_command\":false,".len();
    let learners = "\"learners\":[],".len();
    let witnesses = "\"witnesses\":[],".len();
    assert_eq!(
        is_command + learners + witnesses,
        48,
        "the three omissions should come to 48 bytes"
    );
    assert_eq!(
        json.len(),
        431 - 48,
        "an ordinary record should be 48 bytes smaller than the 431 it was"
    );
}

#[test]
fn a_record_written_without_them_reads_back_identical() {
    let original = record(false, Vec::new(), Vec::new());
    let json = written(&original);
    let read: WalRecord = serde_json::from_str(&json).expect("a record reads back");
    assert_eq!(read, original, "the omission must be invisible on read");
    assert!(
        matrixraft_wal_checksum_valid(&read),
        "the stored checksum must still validate what was read"
    );
}

#[test]
fn a_record_written_the_old_way_still_reads() {
    // The installed base is on-disk data from older builds, so the three fields
    // must still be accepted when present. Written out by hand rather than by
    // the serializer, because the serializer no longer produces this form.
    let legacy = concat!(
        r#"{"group_id":17,"node_id":1,"#,
        r#""hard_state":{"current_term":7,"voted_for":1,"committed":{"term":7,"index":42}},"#,
        r#""membership":{"group_id":17,"voters":[1,2,3],"learners":[],"witnesses":[],"epoch":4},"#,
        r#""entries":[{"log_id":{"term":7,"index":42},"payload":"BwcHBwcHBwcH","is_command":false}],"#,
        r#""apply_snapshot_fence":{"applied_index":42,"commit_index":42,"#,
        r#""installed_snapshot_index":0,"first_retained_log_index":1},"#,
        r#""checksum":"PLACEHOLDER"}"#
    );
    let expected = record(false, Vec::new(), Vec::new());
    let legacy = legacy.replace("PLACEHOLDER", &expected.checksum);

    let read: WalRecord = serde_json::from_str(&legacy).expect("a legacy record reads");
    assert_eq!(
        read, expected,
        "a record written the old way must read as the same record"
    );
    assert!(
        matrixraft_wal_checksum_valid(&read),
        "and its checksum must still validate -- the hash is over values, not key names"
    );
}

#[test]
fn a_value_that_is_set_is_still_written() {
    // The inverse arm. A skip that dropped a real value would satisfy every
    // assertion above, so each of the three is checked when it carries
    // something a reader could not have guessed.
    let command = written(&record(true, Vec::new(), Vec::new()));
    assert!(
        command.contains("\"is_command\":true"),
        "a command entry must still say so: {command}"
    );

    let populated = written(&record(false, vec![9], vec![8]));
    assert!(
        populated.contains("\"learners\":[9]"),
        "a real learner list must still be written: {populated}"
    );
    assert!(
        populated.contains("\"witnesses\":[8]"),
        "a real witness list must still be written: {populated}"
    );

    let read: WalRecord = serde_json::from_str(&populated).expect("reads back");
    assert_eq!(read.membership.learners, vec![9]);
    assert_eq!(read.membership.witnesses, vec![8]);
    assert!(matrixraft_wal_checksum_valid(&read));
}

#[test]
fn an_absent_list_hashes_as_an_empty_one() {
    // The claim the omission rests on: the checksum mixes each list's length
    // before its members, so a list that is absent on the wire and one that is
    // present-but-empty are the same record to the hash.
    let absent = record(false, Vec::new(), Vec::new());
    let json = written(&absent);
    assert!(
        !json.contains("learners"),
        "fixture: the list must be absent"
    );

    let read: WalRecord = serde_json::from_str(&json).expect("reads");
    assert_eq!(
        matrixraft_wal_checksum(&read),
        absent.checksum,
        "an absent list must hash exactly as the empty list it reads back as"
    );
}
