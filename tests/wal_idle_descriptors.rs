// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! An idle WAL holds only the descriptor that protects its writer lock.
//!
//! This test lives in its own process because `/proc/self/fd` is process-wide.
//! Opening many WALs makes the per-group slope exact and separates it from the
//! small fixed set the test harness owns.

#![cfg(target_os = "linux")]

use matrixraft::{
    ApplySnapshotFence, HardState, LogId, Membership, PersistentRaftWal, PersistentRaftWalOptions,
    WalRecord,
};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const GROUPS: usize = 256;

fn fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("read process descriptors")
        .count()
}

fn root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-idle-wal-descriptors-{}-{nonce}",
        std::process::id()
    ))
}

fn options(dir: PathBuf) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir,
        max_records_per_segment: 1_000,
        max_segment_bytes: 1 << 20,
        min_keep_segments: 1,
        fsync_on_append: false,
    }
}

fn record(group_id: u64) -> WalRecord {
    WalRecord {
        entries_are_delta: false,
        group_id,
        node_id: 1,
        hard_state: HardState {
            current_term: 1,
            voted_for: Some(1),
            committed: Some(LogId { term: 1, index: 1 }),
        },
        membership: Membership {
            group_id,
            voters: vec![1],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries: Vec::new(),
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: 1,
            commit_index: 1,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    }
}

#[test]
fn idle_wals_hold_one_descriptor_and_open_the_segment_on_demand() {
    let root = root();
    let before = fd_count();
    let mut wals: Vec<_> = (0..GROUPS)
        .map(|group| {
            PersistentRaftWal::open(options(root.join(group.to_string())))
                .unwrap_or_else(|error| panic!("open group {group}: {error}"))
        })
        .collect();
    let idle = fd_count();

    assert_eq!(
        idle - before,
        GROUPS,
        "an idle WAL must retain its directory lock and no segment descriptor"
    );

    wals[0].append(record(1)).expect("first WAL append");
    assert_eq!(
        fd_count() - idle,
        1,
        "the first append must open and retain exactly one active segment"
    );

    drop(wals);
    let _ = std::fs::remove_dir_all(root);
}
