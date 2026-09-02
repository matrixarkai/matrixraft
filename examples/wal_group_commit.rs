// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Durability probe: what does a WAL append cost, and what does batching save?
//!
//! A durable append is its fsync and very little else. This appends the same
//! number of records at several batch sizes and reports both the throughput and
//! the fsync count, because the count is exact while the seconds move with the
//! disk and the load on it.
//!
//!     wal_group_commit <appends> <payload_bytes>

use std::time::Instant;

use matrixraft::{
    matrixraft_wal_checksum, ApplySnapshotFence, HardState, LogEntry, LogId, Membership,
    PersistentRaftWal, PersistentRaftWalOptions, RaftError, WalRecord,
};

fn record(index: u64, payload_bytes: usize) -> WalRecord {
    let mut record = WalRecord {
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
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

fn run(appends: u64, batch: u64, payload_bytes: usize) -> Result<(f64, u64), RaftError> {
    let dir = std::env::temp_dir().join(format!("mr-group-{appends}-{batch}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create dir");
    let mut options = PersistentRaftWalOptions::new(&dir);
    options.min_keep_segments = usize::MAX;
    let mut wal = PersistentRaftWal::open(options)?;

    let started = Instant::now();
    let mut index = 1;
    while index <= appends {
        let take = batch.min(appends - index + 1);
        if take == 1 {
            wal.append(record(index, payload_bytes))?;
        } else {
            let records = (index..index + take)
                .map(|i| record(i, payload_bytes))
                .collect();
            wal.append_batch(records)?;
        }
        index += take;
    }
    let seconds = started.elapsed().as_secs_f64();
    let fsyncs = wal.fsync_count();
    drop(wal);
    let _ = std::fs::remove_dir_all(&dir);
    Ok((seconds, fsyncs))
}

fn main() -> Result<(), RaftError> {
    let mut args = std::env::args().skip(1);
    let appends: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(2000);
    let payload_bytes: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(256);

    println!("appends={appends} payload_bytes={payload_bytes}");
    println!(
        "{:>6}  {:>9}  {:>9}  {:>14}  {:>8}",
        "batch", "seconds", "fsyncs", "appends/sec", "vs one"
    );
    let mut baseline: Option<f64> = None;
    for batch in [1_u64, 2, 8, 32, 128] {
        let (seconds, fsyncs) = run(appends, batch, payload_bytes)?;
        let rate = appends as f64 / seconds;
        let speedup = baseline.map(|b| b / seconds).unwrap_or(1.0);
        println!("{batch:>6}  {seconds:>9.3}  {fsyncs:>9}  {rate:>14.0}  {speedup:>7.1}x");
        if baseline.is_none() {
            baseline = Some(seconds);
        }
    }
    Ok(())
}
