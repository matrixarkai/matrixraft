// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Where the memory of a retained WAL record actually goes.
//!
//! Segment rolls no longer retain the whole log, so what a record costs is now
//! its own fixed overhead rather than a copy of everything before it. This
//! attributes that overhead field by field, by walking the retained records and
//! summing capacities, so the answer is a count rather than an inference from
//! resident memory.
//!
//! Resident is reported alongside, because the gap between the two is the
//! allocator's, and it is worth knowing which of the two a change would move.

use matrixraft::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, PersistentRaftWal,
    PersistentRaftWalOptions, RaftError, WalRecord,
};

const PAYLOAD_BYTES: usize = 256;

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

#[derive(Default)]
struct Footprint {
    records: usize,
    record_structs: usize,
    membership_heap: usize,
    checksum_heap: usize,
    entry_vec_heap: usize,
    payload_heap: usize,
}

impl Footprint {
    fn add(&mut self, record: &WalRecord) {
        self.records += 1;
        self.record_structs += std::mem::size_of::<WalRecord>();
        self.membership_heap += (record.membership.voters.capacity()
            + record.membership.learners.capacity()
            + record.membership.witnesses.capacity())
            * std::mem::size_of::<u64>();
        self.checksum_heap += record.checksum.capacity();
        self.entry_vec_heap += record.entries.capacity() * std::mem::size_of::<LogEntry>();
        self.payload_heap += record
            .entries
            .iter()
            .map(|entry| entry.payload.capacity())
            .sum::<usize>();
    }

    fn total(&self) -> usize {
        self.record_structs
            + self.membership_heap
            + self.checksum_heap
            + self.entry_vec_heap
            + self.payload_heap
    }

    fn report(&self, rss_growth: u64) {
        let per = |bytes: usize| bytes as f64 / self.records.max(1) as f64;
        println!(
            "records_in_memory={} payload_bytes={PAYLOAD_BYTES}",
            self.records
        );
        println!(
            "  {:<22} {:>12} {:>10}",
            "field", "total bytes", "per record"
        );
        for (name, bytes) in [
            ("WalRecord struct", self.record_structs),
            ("membership vecs", self.membership_heap),
            ("checksum string", self.checksum_heap),
            ("entries vec", self.entry_vec_heap),
            ("payloads", self.payload_heap),
        ] {
            println!("  {name:<22} {bytes:>12} {:>10.1}", per(bytes));
        }
        println!(
            "  {:<22} {:>12} {:>10.1}",
            "counted total",
            self.total(),
            per(self.total())
        );
        println!(
            "  {:<22} {:>12} {:>10.1}",
            "resident growth",
            rss_growth,
            rss_growth as f64 / self.records.max(1) as f64
        );
        let useful = self.payload_heap as f64 / self.total().max(1) as f64 * 100.0;
        println!("  payload is {useful:.1}% of what is counted");
    }
}

fn record(index: u64) -> WalRecord {
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
            payload: vec![7u8; PAYLOAD_BYTES],
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

fn main() -> Result<(), RaftError> {
    let appends: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(20_000);
    let per_segment: usize = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(10_000);
    let dir = std::env::temp_dir().join(format!("mr-footprint-{appends}-{per_segment}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create dir");

    let mut options = PersistentRaftWalOptions::new(&dir);
    options.fsync_on_append = false;
    options.min_keep_segments = usize::MAX;
    options.max_records_per_segment = per_segment;
    let mut wal = PersistentRaftWal::open(options)?;

    let rss_before = rss_bytes();
    for index in 1..=appends {
        wal.append(record(index))?;
    }
    let rss_growth = rss_bytes().saturating_sub(rss_before);

    println!(
        "appends={appends} per_segment={per_segment} segments={}",
        wal.segments().len()
    );
    let mut footprint = Footprint::default();
    for segment in wal.segments() {
        for record in &segment.records {
            footprint.add(record);
        }
    }
    footprint.report(rss_growth);

    drop(wal);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
