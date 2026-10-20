// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A durable write that fails is reported, and leaves the log it already had.
//!
//! **This file exists because I was wrong about something.** Three changes --
//! the fail-stop on a failed durable write, and the two peer-triggered panics
//! before it -- were written on the premise that a WAL append cannot be made to
//! fail from a test, so the fail-stop takes the write's *outcome* as an argument
//! rather than being tested through a real failure. The premise was false.
//!
//! The WAL holds its active segment open, so neither chmod nor unlinking the
//! file reaches it, and that is as far as I looked. But a segment **roll** opens
//! a new file, `max_records_per_segment` decides when a roll happens, and a test
//! chooses that number. With descriptors exhausted the roll's open fails, and
//! the append fails with it:
//!
//! ```text
//! storage error: failed to create WAL segment: Too many open files (os error 24).
//! ```
//!
//! So a real failed durable write is reachable, and these are the properties it
//! was worth reaching for.
//!
//! **This test is alone in its own binary**, for the reason
//! `tests/wal_descriptor_exhaustion.rs` is: the limit is per process, a sibling
//! test sharing it would fail for reasons of its own, and everything that could
//! itself need a descriptor -- an assertion's message included -- happens after
//! they are released.
//!
//! **What this pins, and what it cannot.** A roll both creates a segment file
//! and then opens it for appending, and with no descriptors left either can
//! trip first. Planting showed that: swallowing the create failure, or
//! reporting it as a different kind of error, both leave this green, because
//! the open immediately after fails too and produces the same sort of error.
//! So what is pinned is that a failed rolling append surfaces as a storage
//! error naming a WAL segment, costs nothing already written, and leaves the
//! log usable and recoverable -- **not** which of the two opens failed.
//! Changing the message does fail it.

use matrixraft::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, PersistentRaftWal,
    PersistentRaftWalOptions, RaftError, WalRecord,
};
use std::fs::File;
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 7;

fn record(index: u64) -> WalRecord {
    WalRecord {
        entries_are_delta: false,
        group_id: GROUP_ID,
        node_id: 1,
        hard_state: HardState {
            current_term: 1,
            voted_for: Some(1),
            committed: Some(LogId { term: 1, index }),
        },
        membership: Membership {
            group_id: GROUP_ID,
            voters: vec![1],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries: vec![LogEntry {
            log_id: LogId { term: 1, index },
            payload: b"x".to_vec(),
            is_command: true,
        }],
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

fn options(dir: &std::path::Path) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir: dir.to_path_buf(),
        // Two records a segment, so the third append has to roll -- which is
        // the open that can be made to fail.
        max_records_per_segment: 2,
        max_segment_bytes: 1 << 20,
        min_keep_segments: 1,
        fsync_on_append: true,
    }
}

#[test]
fn a_failed_rolling_append_is_reported_and_costs_nothing_already_written() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "matrixraft-failed-append-{}-{nonce}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a directory for the WAL");

    let mut wal = PersistentRaftWal::open(options(&dir)).expect("open the WAL");
    wal.append(record(1)).expect("the first append");
    wal.append(record(2)).expect("the second append");

    let probe_file = dir.join("probe");
    File::create(&probe_file).expect("a file to hold open");
    let mut held: Vec<File> = Vec::new();
    while let Ok(file) = File::open(&probe_file) {
        held.push(file);
        if held.len() > 1_000_000 {
            break;
        }
    }
    let exhausted = held.len();

    // The roll's open fails, so this append fails.
    let refused = wal.append(record(3));
    // What the WAL still holds, asked while the descriptors are still gone --
    // `records()` answers from memory, so it needs none.
    let during = wal.records().len();

    drop(held);

    assert!(
        exhausted > 16,
        "only {exhausted} descriptors were available, so this process has no \
         limit worth running into and the test cannot say anything"
    );
    let error = refused.expect_err("the rolling append cannot succeed with no descriptors left");
    assert!(
        matches!(error, RaftError::Storage(_)),
        "a write that did not land is a storage error, not something else: {error:?}"
    );
    assert!(
        error.to_string().contains("WAL segment"),
        "and it names the thing that could not be opened: {error}"
    );
    assert_eq!(
        during, 2,
        "the two records already written are still there: a failed append must \
         not cost what was already durable"
    );

    // And the log is still usable once the descriptors come back.
    wal.append(record(3)).expect("the append succeeds now");
    assert_eq!(wal.records().len(), 3);

    // Re-opened from disk, the whole log is readable -- so the failed attempt
    // left nothing behind that a reader stops at.
    drop(wal);
    let mut reopened = PersistentRaftWal::open(options(&dir)).expect("reopen");
    let report = reopened.recover().expect("recover");
    assert!(
        !report.truncated_corrupt_tail,
        "the failed append wrote nothing, so there is no torn tail to repair"
    );
    assert_eq!(
        reopened.records().len(),
        3,
        "all three records survive a restart"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
