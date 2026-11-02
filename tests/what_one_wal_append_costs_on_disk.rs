// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one WAL append costs on disk, as a figure rather than a bound.
//!
//! Three assertions in this suite already cared about this cost, and all three
//! were one-sided bounds: a per-record cost "under 1000 bytes", a 4 KiB record
//! "under 6500 bytes", and a legacy record "over 16000". A bound that loose
//! cannot catch a regression -- the per-record figure is **73 bytes**, so the
//! existing bound would admit a thirteenfold envelope growth without going red.
//!
//! Measured, the cost is a line, and it resolves into integers exactly:
//!
//! ```text
//! 100 records, payload    0 B  ->   7,302 B on disk  =  100 *    73 + 2
//! 100 records, payload    9 B  ->   8,402 B on disk  =  100 *    84 + 2
//! 100 records, payload  300 B  ->  37,702 B on disk  =  100 *   377 + 2
//! 100 records, payload 3000 B  -> 307,702 B on disk  =  100 * 3,077 + 2
//! ```
//!
//! so **73 bytes per record, plus the payload, plus 2 bytes per segment file.**
//!
//! That `+ 2` is not a file header, and reading it as one would be wrong in a
//! way that matters: it is four bytes of segment magic (`MRWB`) less the two
//! bytes the *first* record saves by being the only one that is not a delta,
//! since proto3 does not write a `false`. Both halves are measured rather than
//! argued -- a fresh segment's first four bytes, and `entries_are_delta` read
//! back off each stored record.
//!
//! A payload byte now costs **exactly one byte**. It used to cost 4/3, because
//! the payload was base64 inside a JSON record. Between the 300-byte and the
//! 3000-byte point the extra disk and the extra payload are both 270,000
//! bytes, so the slope is asserted as an integer identity rather than as a
//! float comparison that would need a tolerance of its own.
//!
//! The figures are exact, not approximate, because a WAL record carries no
//! clock and no nondeterminism -- the same appends produce the same bytes. So
//! these are `assert_eq!` on whole numbers rather than tolerances.
//!
//! ## What the amplification actually is
//!
//! | payload | bytes per record | amplification |
//! |---|---|---|
//! | 9 B | 84 B | **9.33x** |
//! | 300 B | 377 B | 1.26x |
//! | 3000 B | 3077 B | 1.03x |
//!
//! A raft log carrying small commands pays about **nine times** its payload,
//! where it used to pay about forty-seven. What is left is the 73-byte
//! envelope, and a quarter of that is a single field: the checksum is a
//! 16-character hex string, so it spends 18 bytes of the 73 carrying a `u64`.
//! Stating it as a `fixed64` would cost 9. That is the next lever rather than
//! this one, and the last test below pins the width so it cannot drift either
//! way without going red.
//!
//! This file does not fix that. It pins the cost so that fixing it is visible,
//! and so that nothing quietly makes it worse.
//!
//! ## Verified by planting, not by passing
//!
//! A green figure proves nothing about what it would catch, so both regressions
//! this file exists for were planted against it and the reddening was read off
//! the output rather than predicted:
//!
//! - **Storing the whole log per record instead of a delta** reddens all four
//!   points, by roughly fiftyfold at the hundredth record -- which is the shape
//!   of the failure, since the cost would then grow with the log.
//! - **Widening the checksum** moves the empty-payload envelope and every
//!   point above it, and correctly leaves the slope identity alone, because the
//!   slope is a property of the payload field and not of the envelope.
//!
//! ## What these assertions cannot see
//!
//! - The exact byte counts are tied to **100 records at indices 1..=100**. The
//!   per-record cost drifts upward with the log because an index is a varint
//!   and gains a byte past 127: 139.20 B/record over a 10-record log becomes
//!   141.74 B over a 400-record one, both at a 64-byte payload. That is
//!   +1.82%, and the drift test below bounds it, because the failure it is
//!   really watching for is a return to storing the whole log per record, which
//!   would be a hundredfold rather than a percent.
//! - The slope identity is blind to the envelope, and the envelope assertions
//!   are blind to the slope. Both are asserted, separately and exactly.
//! - Nothing here measures fsync behaviour: this fixture sets
//!   `fsync_on_append: false` so that the figure is the encoding's cost and not
//!   the segment policy's.

use matrixraft::{
    matrixraft_read_wal_segment_file, matrixraft_write_wal_segment_file, ApplySnapshotFence,
    HardState, LogEntry, LogId, Membership, PersistentRaftWal, PersistentRaftWalOptions, WalRecord,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const RECORDS: u64 = 100;

/// Four bytes of magic open a segment file, and the first record in it is two
/// bytes smaller than the rest because it is the only one that is not a delta.
/// Both are measured by `the_segments_two_bytes_are_four_of_magic_less_the_first_records_two`.
const SEGMENT_MAGIC: u64 = 4;
const FIRST_RECORD_SAVING: u64 = 2;
const SEGMENT_OVERHEAD: u64 = SEGMENT_MAGIC - FIRST_RECORD_SAVING;
const PER_RECORD: u64 = 73;

/// Measured, exact: the same appends always produce the same bytes.
const DISK_AT_0: u64 = 7_302;
const DISK_AT_9: u64 = 8_402;
const DISK_AT_300: u64 = 37_702;
const DISK_AT_3000: u64 = 307_702;

fn temp_wal_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("rustraft-{name}-{}-{nonce}", std::process::id()))
}

/// One segment, no rolling, no fsync: so the figure is the encoding's cost and
/// not the segment policy's.
fn one_wide_segment(dir: PathBuf) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir,
        max_records_per_segment: 1_000_000,
        max_segment_bytes: u64::MAX,
        min_keep_segments: 1,
        fsync_on_append: false,
    }
}

fn record(last: u64, payload_len: usize) -> WalRecord {
    WalRecord {
        entries_are_delta: false,
        group_id: 9,
        node_id: 1,
        hard_state: HardState {
            current_term: 3,
            voted_for: Some(1),
            committed: Some(LogId {
                term: 3,
                index: last,
            }),
        },
        membership: Membership {
            group_id: 9,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 3,
        },
        entries: (1..=last)
            .map(|index| LogEntry {
                log_id: LogId { term: 3, index },
                payload: vec![b'x'; payload_len],
                is_command: true,
            })
            .collect(),
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: last,
            commit_index: last,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    }
}

fn stored_bytes(dir: &PathBuf) -> u64 {
    fs::read_dir(dir)
        .expect("read wal dir")
        .flatten()
        .filter_map(|entry| entry.metadata().ok())
        .map(|meta| meta.len())
        .sum()
}

/// Appends `records` records, each adding one entry of `payload_len` bytes,
/// and returns the bytes they occupy on disk.
fn disk_bytes(records: u64, payload_len: usize) -> u64 {
    let dir = temp_wal_dir("append-cost");
    let bytes = {
        let mut wal = PersistentRaftWal::open(one_wide_segment(dir.clone())).expect("open wal");
        for index in 1..=records {
            wal.append(record(index, payload_len)).expect("append");
        }
        stored_bytes(&dir)
    };
    let _ = fs::remove_dir_all(&dir);
    bytes
}

#[test]
fn one_append_costs_a_fixed_envelope_and_the_envelope_is_73_bytes() {
    assert_eq!(
        disk_bytes(RECORDS, 0),
        DISK_AT_0,
        "an empty payload costs {RECORDS} * {PER_RECORD} + {SEGMENT_OVERHEAD}; if this moved, a field was added to the WAL record or one changed width, and the envelope is now a different size"
    );
    assert_eq!(
        DISK_AT_0,
        RECORDS * PER_RECORD + SEGMENT_OVERHEAD,
        "the figure must resolve into a per-record cost and the segment's own bytes, or the reading above is wrong"
    );
}

/// The `+ 2` is magic less the first record's saving, not a file header.
///
/// Asserted because reading it as a header would be wrong in a way that only
/// shows up when the record count changes: a header is paid once per file
/// whatever the records do, whereas this term disappears if the first record
/// ever becomes a delta like the others.
#[test]
fn the_segments_two_bytes_are_four_of_magic_less_the_first_records_two() {
    assert_eq!(
        SEGMENT_OVERHEAD,
        SEGMENT_MAGIC - FIRST_RECORD_SAVING,
        "the segment term stopped resolving into the two things it is made of"
    );

    let dir = temp_wal_dir("segment-overhead");
    let mut wal = PersistentRaftWal::open(one_wide_segment(dir.clone())).expect("open wal");
    for index in 1..=3u64 {
        wal.append(record(index, 0)).expect("append");
    }
    drop(wal);

    let path = dir.join(format!("{:020}.wal", 0));
    let bytes = fs::read(&path).expect("read the segment");
    assert_eq!(
        &bytes[..SEGMENT_MAGIC as usize],
        b"MRWB",
        "the segment does not open with the four magic bytes this figure charges for"
    );

    let stored = matrixraft_read_wal_segment_file(&path).expect("read the records back");
    assert_eq!(stored.len(), 3, "three appends did not store three records");
    assert!(
        !stored[0].entries_are_delta,
        "the first record is a delta, so it no longer saves the two bytes this figure credits it"
    );
    assert!(
        stored[1..].iter().all(|record| record.entries_are_delta),
        "a later record is not a delta, so more than one record saves those two bytes"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_payload_byte_costs_one_byte_and_nothing_else() {
    let low = disk_bytes(RECORDS, 300);
    let high = disk_bytes(RECORDS, 3_000);
    assert_eq!(low, DISK_AT_300, "the 300-byte point moved");
    assert_eq!(high, DISK_AT_3000, "the 3000-byte point moved");

    // Both points frame their payload the same way -- a two-byte length varint
    // on the payload and on the entry holding it -- so the difference between
    // them is payload and nothing else, and the slope is exact. Asserted as an
    // integer identity rather than a float comparison, which would need a
    // tolerance nobody can justify.
    let extra_disk = high - low;
    let extra_payload = (3_000 - 300) * RECORDS;
    assert_eq!(
        extra_disk, extra_payload,
        "a payload byte must cost exactly one byte on disk: {extra_disk} disk bytes for {extra_payload} payload bytes is {:.6}, not 1.0",
        extra_disk as f64 / extra_payload as f64
    );
}

#[test]
fn a_small_command_pays_about_nine_times_its_own_size() {
    let bytes = disk_bytes(RECORDS, 9);
    assert_eq!(bytes, DISK_AT_9, "the 9-byte point moved");

    // The number this whole file exists to make visible. 73 bytes of envelope
    // around 9 bytes of command, where it was 412 bytes around the same 9.
    let payload_total = 9 * RECORDS;
    assert_eq!(
        bytes * 100 / payload_total,
        933,
        "a 9-byte command should cost 9.34x on disk, got {:.2}x",
        bytes as f64 / payload_total as f64
    );
}

#[test]
fn a_records_cost_does_not_grow_with_the_length_of_the_log() {
    // The real subject: records are stored as deltas, so appending to a long
    // log costs the same as appending to a short one. Stored whole, the 400th
    // record would carry 400 entries instead of one.
    let short = disk_bytes(10, 64) as f64 / 10.0;
    let long = disk_bytes(400, 64) as f64 / 400.0;
    let growth = long / short;
    assert!(
        growth < 1.05,
        "per-record cost grew {growth:.3}x from a 10-record log ({short:.1} B) to a 400-record one ({long:.1} B); a couple of percent is an index varint gaining a byte past 127, but a return to whole-log records would be a hundredfold"
    );
    assert!(
        growth > 1.0,
        "the figure is expected to drift up slightly as indices pass 127 and their varints widen -- if it stopped doing that, this test is no longer measuring what it thinks: {growth:.3}x"
    );
}

/// A quarter of the envelope is the checksum, spelled out in hex.
///
/// This is the one field worth naming, and it is pinned from both sides: a
/// narrower checksum would quietly improve every figure above and a wider one
/// would quietly worsen them, and either way the reader of this file should
/// find out from a red test rather than from arithmetic that no longer adds up.
#[test]
fn a_quarter_of_the_envelope_is_the_checksum_spelled_in_hex() {
    let dir = temp_wal_dir("checksum-width");
    let mut wal = PersistentRaftWal::open(one_wide_segment(dir.clone())).expect("open wal");
    for index in 1..=2u64 {
        wal.append(record(index, 0)).expect("append");
    }
    drop(wal);
    let stored = matrixraft_read_wal_segment_file(&dir.join(format!("{:020}.wal", 0)))
        .expect("read the records back");
    let _ = fs::remove_dir_all(&dir);

    // The second record, because it is a delta like every record after it.
    let record = stored[1].clone();
    assert_eq!(
        record.checksum.len(),
        16,
        "the stored checksum is {} hex characters, not 16, so the 18 bytes this file charges for it are wrong",
        record.checksum.len()
    );

    assert_eq!(
        framed_bytes(&record),
        PER_RECORD,
        "a single stored record does not weigh the per-record figure this file pins, so one of the two readings is wrong"
    );

    let mut without = record.clone();
    without.checksum = String::new();
    let cost = framed_bytes(&record) - framed_bytes(&without);
    assert_eq!(
        cost, 18,
        "the checksum costs {cost} bytes, not the tag, length and sixteen characters this file attributes to it"
    );
    assert_eq!(
        cost * 100 / PER_RECORD,
        24,
        "the checksum is {:.1}% of the envelope, not the ~24.7% this file claims",
        cost as f64 * 100.0 / PER_RECORD as f64
    );
}

/// One record's framed size, measured by writing it and weighing the file.
///
/// Goes through the WAL's own segment writer rather than re-deriving the frame,
/// so it cannot drift away from what an append actually puts on disk.
fn framed_bytes(record: &WalRecord) -> u64 {
    let dir = temp_wal_dir("framed-bytes");
    fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("one.wal");
    matrixraft_write_wal_segment_file(&path, std::slice::from_ref(record)).expect("write");
    let bytes = fs::metadata(&path).expect("meta").len() - SEGMENT_MAGIC;
    let _ = fs::remove_dir_all(&dir);
    bytes
}
