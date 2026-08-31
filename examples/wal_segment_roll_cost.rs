// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Segment-roll probe: what does the first record of a segment cost?
//!
//! Every record but the first in a segment stores only what was appended since
//! the previous one. The first stores the whole retained log, so that a segment
//! can be read without reading any other. That is a deliberate trade, but its
//! price grows with the log: the k-th roll writes and retains roughly k*S
//! entries, so N appends cost about N^2/2S entries rather than N.
//!
//! This drives the WAL exactly the way `wal_record_for_coverage` does and
//! *counts* what is retained. Counting rather than timing is deliberate: the
//! quantity of interest is entries held, which is exact and machine-independent.
//!
//! One log size per process, so a later run cannot reuse pages an earlier one
//! freed. Run it as: wal_segment_roll_cost <appends> <records_per_segment>

use matrixraft::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, PersistentRaftWal,
    PersistentRaftWalOptions, RaftError, WalRecord,
};

const PAYLOAD_BYTES: usize = 64;

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 3, index },
        payload: vec![7u8; PAYLOAD_BYTES],
        is_command: true,
    }
}

/// The same shape `wal_record_for_coverage` builds, so the probe exercises the
/// production path rather than an idealised one.
fn build(log: &[LogEntry], coverage: Option<(u64, u64, u64)>) -> Result<WalRecord, RaftError> {
    let last_index = log.last().map(|e| e.log_id.index).unwrap_or(0);
    let extends = coverage.and_then(|(first_index, last, last_term)| {
        let log_first = log.first()?.log_id.index;
        if log_first > first_index {
            return None;
        }
        let position = log.iter().position(|e| e.log_id.index == last)?;
        if log[position].log_id.term != last_term {
            return None;
        }
        Some(position + 1)
    });
    let (entries, entries_are_delta) = match extends {
        Some(from) => (log[from..].to_vec(), true),
        None => (log.to_vec(), false),
    };
    Ok(WalRecord {
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
    })
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

fn dir_bytes(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

fn main() -> Result<(), RaftError> {
    let mut args = std::env::args().skip(1);
    let appends: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(2048);
    let per_segment: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(256);

    let dir = std::env::temp_dir().join(format!("mr-roll-{appends}-{per_segment}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create dir");

    let mut options = PersistentRaftWalOptions::new(&dir);
    options.max_records_per_segment = per_segment;
    options.fsync_on_append = false;
    options.min_keep_segments = usize::MAX;
    let mut wal = PersistentRaftWal::open(options)?;

    let rss_before = rss_bytes();
    let mut log: Vec<LogEntry> = Vec::new();
    for index in 1..=appends {
        log.push(entry(index));
        wal.append_built(|coverage| build(&log, coverage))?;
    }

    // What is actually held: entries summed over every retained record. Exact,
    // not an estimate, and independent of the allocator.
    let segments = wal.segments();
    let retained_entries: usize = segments
        .iter()
        .flat_map(|s| s.records.iter())
        .map(|r| r.entries.len())
        .sum();
    let whole_records = segments
        .iter()
        .flat_map(|s| s.records.iter())
        .filter(|r| !r.entries_are_delta)
        .count();
    let records: usize = segments.iter().map(|s| s.records.len()).sum();

    println!(
        "appends={appends} per_segment={per_segment} segments={} records={records} \
         whole_records={whole_records} retained_entries={retained_entries} \
         entries_per_append={:.1} disk_bytes={} disk_per_append={} rss_growth_bytes={}",
        segments.len(),
        retained_entries as f64 / appends as f64,
        dir_bytes(&dir),
        dir_bytes(&dir) / appends,
        rss_bytes().saturating_sub(rss_before),
    );

    drop(wal);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
