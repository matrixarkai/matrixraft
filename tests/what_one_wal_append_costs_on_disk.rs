// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one WAL append costs on disk, as a figure rather than a bound.
//!
//! Three assertions in this suite already cared about this cost, and all three
//! were one-sided bounds: a per-record cost "under 1000 bytes", a 4 KiB record
//! "under 6500 bytes", and a legacy record "over 16000". A bound that loose
//! cannot catch a regression -- the per-record figure is **412 bytes**, so the
//! existing bound would admit a 2.2x envelope growth without going red.
//!
//! Measured, the cost is a line, and it resolves into integers exactly:
//!
//! ```text
//! 100 records, payload    0 B  ->  41,243 B on disk  =  100 *   412 + 43
//! 100 records, payload  300 B  ->  81,243 B on disk  =  100 *   812 + 43
//! 100 records, payload 3000 B  -> 441,243 B on disk  =  100 *  4,412 + 43
//! ```
//!
//! so **43 bytes per segment file, plus 412 bytes per record, plus 4/3 of the
//! payload.** The 4/3 is base64, which is what the payload encoding costs after
//! the move off `serde_json`'s number array; it is the floor this format can
//! reach and nothing here is going to beat it.
//!
//! The figures are exact, not approximate, because a WAL record carries no
//! clock and no nondeterminism -- the same appends produce the same bytes. So
//! these are `assert_eq!` on whole numbers rather than tolerances, and the
//! slope is checked as an integer identity (`360_000 * 3 == 270_000 * 4`)
//! rather than a float comparison that would need a tolerance of its own.
//!
//! ## What the amplification actually is
//!
//! | payload | bytes per append | amplification |
//! |---|---|---|
//! | 9 B | 424.4 B | **47.2x** |
//! | 300 B | 812.4 B | 2.71x |
//! | 3000 B | 4412.4 B | 1.47x |
//!
//! A raft log carrying small commands pays about **forty-seven times** its payload.
//! That is not the payload encoding's fault -- it is the 412-byte envelope, and
//! **77.4% of a record is its own field names**: 328 bytes of `"name":` in a
//! 424-byte record carrying 9 bytes. The widest single field is
//! `apply_snapshot_fence` at 118 bytes, of which 73 are the four names
//! `applied_index`, `commit_index`, `installed_snapshot_index` and
//! `first_retained_log_index` spelled out to carry four integers. `membership`
//! costs 84 bytes and is re-serialized on every append although it changes only
//! on a configuration change.
//!
//! This file does not fix that. It pins the cost so that fixing it is visible,
//! and so that nothing quietly makes it worse.
//!
//! ## Where the payload encoding actually lives
//!
//! `wal_entry_payloads::serialize` is what encodes these entries, through an
//! `EntryOut` whose `payload` is an already-encoded `String`. The similarly
//! named `bytes_as_base64` is the **snapshot** path's codec and touches no WAL
//! byte: reverting it leaves every figure here identical, which is how its
//! scope got established. Worth knowing before planting against this file --
//! two codecs, one of them not on this path.
//!
//! ## Verified by planting, not by passing
//!
//! A green figure proves nothing about what it would catch. Both regressions
//! this file is for were planted against it: making a payload byte cost four
//! characters again reddens the slope and the 50x assertions and correctly
//! leaves the empty-payload envelope alone, and storing the whole log per
//! record reddens all four.
//!
//! ## What these assertions cannot see
//!
//! - The exact byte counts are tied to **100 records at indices 1..=100**. The
//!   per-record cost drifts upward with the log because an index is written in
//!   decimal: 529.4 B/record at 100 records becomes 532.9 B at 400. That is
//!   +0.66%, and the last test below bounds it, because the failure it is
//!   really watching for is a return to storing the whole log per record, which
//!   would be a hundredfold rather than a percent.
//! - The slope identity is blind to the envelope, and the envelope assertions
//!   are blind to the slope. Both are asserted, separately and exactly.

use matrixraft::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, PersistentRaftWal,
    PersistentRaftWalOptions, WalRecord,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const RECORDS: u64 = 100;
const SEGMENT_OVERHEAD: u64 = 43;
const PER_RECORD: u64 = 412;

/// Measured, exact: the same appends always produce the same bytes.
const DISK_AT_0: u64 = 41_243;
const DISK_AT_9: u64 = 42_443;
const DISK_AT_300: u64 = 81_243;
const DISK_AT_3000: u64 = 441_243;

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
fn one_append_costs_a_fixed_envelope_and_the_envelope_is_412_bytes() {
    assert_eq!(
        disk_bytes(RECORDS, 0),
        DISK_AT_0,
        "an empty payload costs {RECORDS} * {PER_RECORD} + {SEGMENT_OVERHEAD}; if this moved, a field was added to the WAL record or one was renamed, and the envelope is now a different size"
    );
    assert_eq!(
        DISK_AT_0,
        RECORDS * PER_RECORD + SEGMENT_OVERHEAD,
        "the figure must resolve into a per-record cost and one segment header, or the reading above is wrong"
    );
}

#[test]
fn a_payload_byte_costs_four_thirds_of_a_byte_and_nothing_else() {
    let low = disk_bytes(RECORDS, 300);
    let high = disk_bytes(RECORDS, 3_000);
    assert_eq!(low, DISK_AT_300, "the 300-byte point moved");
    assert_eq!(high, DISK_AT_3000, "the 3000-byte point moved");

    // Both payloads are multiples of three, so base64 adds no padding and the
    // slope is exact. Asserted as an integer identity rather than a float
    // comparison, which would need a tolerance nobody can justify.
    let extra_disk = high - low;
    let extra_payload = (3_000 - 300) * RECORDS;
    assert_eq!(
        extra_disk * 3,
        extra_payload * 4,
        "a payload byte must cost exactly 4/3 on disk: {extra_disk} disk bytes for {extra_payload} payload bytes is {:.6}, not {:.6}",
        extra_disk as f64 / extra_payload as f64,
        4.0 / 3.0
    );
}

#[test]
fn a_small_command_pays_about_forty_seven_times_its_own_size() {
    let bytes = disk_bytes(RECORDS, 9);
    assert_eq!(bytes, DISK_AT_9, "the 9-byte point moved");

    // The number this whole file exists to make visible. 412 bytes of envelope
    // around 9 bytes of command.
    let payload_total = 9 * RECORDS;
    assert_eq!(
        bytes * 100 / payload_total,
        4_715,
        "a 9-byte command should cost 47.15x on disk, got {:.2}x",
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
        "per-record cost grew {growth:.3}x from a 10-record log ({short:.1} B) to a 400-record one ({long:.1} B); a few percent is the index widening in decimal, but a return to whole-log records would be a hundredfold"
    );
    assert!(
        growth > 1.0,
        "the figure is expected to drift up slightly as indices gain digits -- if it stopped doing that, this test is no longer measuring what it thinks: {growth:.3}x"
    );
}
