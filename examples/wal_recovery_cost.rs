// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Restart probe: what does recovering a delta-encoded WAL cost?
//!
//! Recovery folds the stored records back into the log. Folding used to build a
//! whole-log record for *every* stored record, and each of those carries the log
//! as it stood at that point, so an N-record WAL materialised about N^2/2
//! entries at once.
//!
//! The records here are built the way `wal_record_for_coverage` builds them --
//! the whole log when the segment cannot extend, a delta when it can, and
//! checksummed by the builder, which is what `append_built` expects. A probe
//! that skips the checksum stores records the fold rejects, and then recovery
//! returns nothing and looks free.
//!
//! One log size per process, so a later run cannot reuse pages an earlier one
//! freed. Run it as: wal_recovery_cost <appends> <records_per_segment>

use std::time::Instant;

use matrixraft::{
    matrixraft_wal_checksum, ApplySnapshotFence, HardState, LogEntry, LogId, Membership,
    PersistentRaftWal, PersistentRaftWalOptions, RaftError, WalRecord,
};

const PAYLOAD_BYTES: usize = 64;

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 3, index },
        payload: vec![7u8; PAYLOAD_BYTES],
        is_command: true,
    }
}

/// The record `wal_record_for_coverage` would build for this coverage.
fn build(log: &[LogEntry], coverage: Option<(u64, u64, u64)>) -> Result<WalRecord, RaftError> {
    let last_index = log.last().map(|entry| entry.log_id.index).unwrap_or(0);
    let extends = coverage.and_then(|(first_index, last, last_term)| {
        let log_first = log.first()?.log_id.index;
        if log_first > first_index {
            return None;
        }
        let position = log.iter().position(|entry| entry.log_id.index == last)?;
        if log[position].log_id.term != last_term {
            return None;
        }
        Some(position + 1)
    });
    let (entries, entries_are_delta) = match extends {
        Some(from) => (log[from..].to_vec(), true),
        None => (log.to_vec(), false),
    };
    let mut record = WalRecord {
        group_id: 9,
        node_id: 1,
        hard_state: HardState {
            current_term: 3,
            voted_for: Some(1),
            committed: Some(LogId {
                term: 3,
                index: last_index,
            }),
        },
        membership: Membership {
            group_id: 9,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 3,
        },
        entries,
        entries_are_delta,
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: last_index,
            commit_index: last_index,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    Ok(record)
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

fn options(dir: &std::path::Path, per_segment: usize) -> PersistentRaftWalOptions {
    let mut options = PersistentRaftWalOptions::new(dir);
    options.max_records_per_segment = per_segment;
    options.fsync_on_append = false;
    options.min_keep_segments = usize::MAX;
    options
}

fn main() -> Result<(), RaftError> {
    let mut args = std::env::args().skip(1);
    let appends: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(2000);
    let per_segment: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(256);

    let dir = std::env::temp_dir().join(format!("mr-recover-{appends}-{per_segment}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create dir");

    {
        let mut wal = PersistentRaftWal::open(options(&dir, per_segment))?;
        let mut log: Vec<LogEntry> = Vec::new();
        for index in 1..=appends {
            log.push(entry(index));
            wal.append_built(|coverage| build(&log, coverage))?;
        }
    }

    // Everything above is setup. The restart is what is being measured.
    let before = rss_bytes();
    let started = Instant::now();
    let mut wal = PersistentRaftWal::open(options(&dir, per_segment))?;
    let report = wal.recover()?;
    let elapsed_ms = started.elapsed().as_millis();
    let growth = rss_bytes().saturating_sub(before);

    println!(
        "appends={appends} per_segment={per_segment} surviving={} recovered_entries={} \
         recover_ms={elapsed_ms} rss_bytes={growth} rss_per_append={}",
        report.surviving_records,
        report
            .recovered
            .as_ref()
            .map(|record| record.entries.len())
            .unwrap_or(0),
        growth / appends,
    );

    drop(wal);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
