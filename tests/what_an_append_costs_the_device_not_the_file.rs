// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What an append costs the **device**, which is not what it costs the file.
//!
//! A sibling suite pins the file cost: 441 bytes a record plus 4/3 of the
//! payload, so a 9-byte command occupies about fifty times its size on disk.
//! That measurement deliberately runs with `fsync_on_append: false` and reads
//! file lengths. Neither is what a durable raft pays.
//!
//! Durability is a block operation. A 441-byte record is a fourteenth of a
//! 4 KiB block, so forcing each record to the platter rewrites the same
//! partly-filled tail block over and over. Measured through
//! `/proc/self/io`'s `write_bytes`, which counts bytes the kernel sends to the
//! storage layer:
//!
//! | configuration | device / file | device bytes per payload byte |
//! |---|---|---|
//! | no fsync, 9-byte commands | 1.00x | 51x |
//! | **one fsync per record**, 9-byte commands | **9.95x** | **505x** |
//! | **one fsync per batch**, same records | **1.00x** | **51x** |
//! | one fsync per record, 4096-byte payloads | 1.69x | 2.4x |
//!
//! Three things worth taking from that.
//!
//! **The block multiplier is ~10x and it is arithmetic, not luck.** 4096/441
//! is 9.29 records per block, and the measurement reads 9.95 to 10.06 across
//! 10, 100 and 500 records -- a steady-state multiplier, not a start-up
//! artefact.
//!
//! **A per-record fsync costs about one block whatever the record holds.** 500
//! records cost 2,273,280 device bytes, which is 4,546 a record: one 4 KiB
//! block plus journal. So **shrinking the record buys nothing at all on this
//! workload** -- a 35-byte record would still cost its block. Any envelope
//! work has to be read against that floor.
//!
//! **Batching is where the file size starts to matter.** One fsync for the
//! batch takes device/file to 1.00x: the file *is* the device cost. That is
//! the configuration in which making a record smaller converts one-for-one
//! into fewer bytes written, and the only one.
//!
//! So the two levers are multiplicative and **ordered**: batch first, for
//! about tenfold, and only then does shrinking the envelope pay its own
//! tenfold. Done in the other order the second buys nothing.
//!
//! ## Why this is one test function and must stay one
//!
//! `write_bytes` in `/proc/self/io` is **process-wide**. Two tests measuring
//! it concurrently in the same binary would each bill the other's I/O, so
//! every measurement here lives in a single `#[test]` and the file holds no
//! other. Separate test binaries are separate processes and cannot interfere.
//! If this file ever grows a second test that touches the disk, these figures
//! stop meaning anything.
//!
//! ## What is asserted and what is only printed
//!
//! The **fsync counts are exact** and depend on nothing but the code, so they
//! are equalities. The **byte ratios are bounded, not pinned**: they come from
//! a kernel counter on a shared machine, and the thresholds sit far from the
//! measured values -- 9.95x asserted as "above 5", 1.00x as "below 2" -- so
//! the test says the effect is real without claiming a figure it cannot
//! reproduce to the byte. The exact numbers are in the table above and in the
//! printed output, which is where a figure that moves with the filesystem
//! belongs.

use matrixraft::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, PersistentRaftWal,
    PersistentRaftWalOptions, WalRecord,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const PAYLOAD: usize = 9;
const RECORDS: u64 = 500;

fn temp_wal_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("rustraft-{name}-{}-{nonce}", std::process::id()))
}

fn options(dir: PathBuf, fsync_on_append: bool) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir,
        max_records_per_segment: 1_000_000,
        max_segment_bytes: u64::MAX,
        min_keep_segments: 1,
        fsync_on_append,
    }
}

fn record(last: u64) -> WalRecord {
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
                payload: vec![b'x'; PAYLOAD],
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

/// Bytes this process has sent to the storage layer. `None` where the kernel
/// does not publish it, so the test can say so instead of failing on a host
/// that is not Linux.
fn write_bytes() -> Option<u64> {
    let text = fs::read_to_string("/proc/self/io").ok()?;
    text.lines()
        .find_map(|line| line.strip_prefix("write_bytes:"))
        .and_then(|rest| rest.trim().parse().ok())
}

fn stored_bytes(dir: &PathBuf) -> u64 {
    fs::read_dir(dir)
        .expect("read wal dir")
        .flatten()
        .filter_map(|entry| entry.metadata().ok())
        .map(|meta| meta.len())
        .sum()
}

struct Run {
    file: u64,
    device: u64,
    fsyncs: u64,
}

fn run(fsync_on_append: bool, batched: bool) -> Run {
    let dir = temp_wal_dir("device-cost");
    fs::create_dir_all(&dir).expect("create wal dir");
    let before = write_bytes().expect("write_bytes");
    let mut wal = PersistentRaftWal::open(options(dir.clone(), fsync_on_append)).expect("open wal");
    if batched {
        let batch: Vec<_> = (1..=RECORDS).map(record).collect();
        wal.append_batch(batch).expect("batch append");
    } else {
        for index in 1..=RECORDS {
            wal.append(record(index)).expect("append");
        }
    }
    let out = Run {
        file: stored_bytes(&dir),
        device: write_bytes().expect("write_bytes").saturating_sub(before),
        fsyncs: wal.fsync_count(),
    };
    drop(wal);
    let _ = fs::remove_dir_all(&dir);
    out
}

#[test]
fn a_durable_append_costs_a_block_and_batching_is_what_reclaims_it() {
    if write_bytes().is_none() {
        println!("/proc/self/io is not available here; device bytes cannot be measured");
        return;
    }

    let one_at_a_time = run(true, false);
    let batched = run(true, true);

    let payload_total = RECORDS * PAYLOAD as u64;
    for (label, r) in [
        ("one fsync per record", &one_at_a_time),
        ("one per batch", &batched),
    ] {
        println!(
            "{label:>22}  fsyncs={:>4}  file={:>8}B  device={:>9}B  device/file={:.2}x  per payload byte={:.1}x",
            r.fsyncs,
            r.file,
            r.device,
            r.device as f64 / r.file as f64,
            r.device as f64 / payload_total as f64
        );
    }

    // Exact, and independent of the machine.
    assert_eq!(
        one_at_a_time.fsyncs, RECORDS,
        "appending one at a time must pay one fsync per record"
    );
    assert_eq!(
        batched.fsyncs, 1,
        "a batch must pay exactly one fsync for all {RECORDS} records"
    );
    assert_eq!(
        one_at_a_time.file, batched.file,
        "both runs must store the same bytes, or the comparison below is between two different logs"
    );

    // Bounded, because these come from a kernel counter on a shared machine.
    // Measured 9.95x and 1.00x; the thresholds sit far from both.
    let unbatched_ratio = one_at_a_time.device as f64 / one_at_a_time.file as f64;
    let batched_ratio = batched.device as f64 / batched.file as f64;
    assert!(
        unbatched_ratio > 5.0,
        "a per-record fsync should write several times the file to the device, because each one flushes a partly-filled block: {unbatched_ratio:.2}x"
    );
    assert!(
        batched_ratio < 2.0,
        "one fsync for the batch should write about the file and no more -- this is the configuration in which a smaller record means fewer device bytes: {batched_ratio:.2}x"
    );
    assert!(
        one_at_a_time.device > batched.device * 4,
        "batching should cut device writes several-fold for the same log: {} B against {} B",
        one_at_a_time.device,
        batched.device
    );

    // The floor that makes envelope work pointless without batching: a
    // per-record fsync costs about a block whatever the record holds.
    let per_append = one_at_a_time.device / RECORDS;
    assert!(
        per_append > 2_048,
        "a durable append should cost on the order of a block, which is why shrinking a 441-byte record would not help here: {per_append} B"
    );
}
