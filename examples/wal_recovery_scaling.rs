// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Restart probe: how long does a node take to recover its WAL, and what does
//! holding it cost, as the log grows?
//!
//! Restart time is a scale metric in its own right -- it is how long a node is
//! unavailable after a crash or a deploy. `PersistentRaftWal` reads its
//! segments into memory on open, so this measures both the time and the
//! resident cost, and reports per-record figures so a non-linear term shows up
//! as a rising column rather than a bigger total.

use std::time::Instant;

use matrixraft::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, PersistentRaftWal,
    PersistentRaftWalOptions, WalRecord,
};

fn record(index: u64, payload_bytes: usize) -> WalRecord {
    WalRecord {
        group_id: 9,
        node_id: 1,
        hard_state: HardState {
            current_term: 3,
            voted_for: Some(1),
            committed: Some(LogId { term: 3, index }),
        },
        membership: Membership {
            group_id: 9,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 3,
        },
        entries: vec![LogEntry {
            log_id: LogId { term: 3, index },
            payload: vec![7u8; payload_bytes],
            is_command: true,
        }],
        entries_are_delta: false,
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: index,
            commit_index: index,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    }
}

fn rss_bytes() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            if let Ok(kb) = rest.trim().trim_end_matches(" kB").trim().parse::<u64>() {
                return kb * 1024;
            }
        }
    }
    0
}

fn options(dir: std::path::PathBuf) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir,
        // One segment, so this measures recovery of a log rather than segment
        // rolling.
        max_records_per_segment: 10_000_000,
        max_segment_bytes: u64::MAX,
        min_keep_segments: 1,
        // Off: this is about recovery cost, and fsync would make the write
        // phase dominate the run.
        fsync_on_append: false,
    }
}

fn main() {
    let payload_bytes: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(256);
    let records: usize = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(20_000);

    let dir = std::env::temp_dir().join(format!(
        "matrixraft-wal-recovery-{}-{records}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);

    let write_started = Instant::now();
    {
        let mut wal = PersistentRaftWal::open(options(dir.clone())).expect("open wal");
        for index in 1..=records as u64 {
            wal.append(record(index, payload_bytes)).expect("append");
        }
    }
    let write_seconds = write_started.elapsed().as_secs_f64();

    let on_disk: u64 = std::fs::read_dir(&dir)
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|e| e.metadata().ok().map(|m| m.len()))
                .sum()
        })
        .unwrap_or(0);

    let rss_before = rss_bytes();
    let open_started = Instant::now();
    let mut wal = PersistentRaftWal::open(options(dir.clone())).expect("reopen wal");
    let open_seconds = open_started.elapsed().as_secs_f64();

    let recover_started = Instant::now();
    let report = wal.recover().expect("recover");
    let recover_seconds = recover_started.elapsed().as_secs_f64();
    let rss_after = rss_bytes();

    assert_eq!(report.surviving_records, records);

    // `status` is the observability surface and gets polled routinely, so its
    // cost per call matters as much as recovery's one-off cost.
    let status_started = Instant::now();
    for _ in 0..20 {
        std::hint::black_box(wal.status());
    }
    let status_us = status_started.elapsed().as_secs_f64() * 1e6 / 20.0;

    std::hint::black_box(&wal);

    let total = open_seconds + recover_seconds;
    println!(
        "records={records:<7} payload={payload_bytes}B  on_disk={:.1} MiB  write={write_seconds:.3}s\n  \
         open={open_seconds:.3}s  recover={recover_seconds:.3}s  restart_total={total:.3}s  \
         us/record={:.1}  rss_held={:.1} MiB  bytes/record={:.0}  status={status_us:.1}us/call",
        on_disk as f64 / 1048576.0,
        total * 1e6 / records as f64,
        rss_after.saturating_sub(rss_before) as f64 / 1048576.0,
        rss_after.saturating_sub(rss_before) as f64 / records as f64,
    );

    drop(wal);
    let _ = std::fs::remove_dir_all(&dir);
}
