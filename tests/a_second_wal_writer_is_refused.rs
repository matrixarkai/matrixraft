// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, PersistentRaftWal,
    PersistentRaftWalOptions, WalRecord,
};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_wal_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("rustraft-{name}-{}-{nonce}", std::process::id()))
}

fn wal_options(dir: PathBuf) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir,
        max_records_per_segment: 2,
        max_segment_bytes: 4096,
        min_keep_segments: 1,
        fsync_on_append: true,
    }
}

fn wal_record(index: u64) -> WalRecord {
    WalRecord {
        entries_are_delta: false,
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
            payload: format!("entry-{index}").into_bytes(),
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

#[test]
fn a_second_writer_on_the_same_directory_is_refused_not_ignored() {
    // Two `PersistentRaftWal`s on one directory each keep their own segment
    // state and write the same filenames, so one overwrites the other's
    // segment. MEASURED before this check: six appends through two handles
    // recovered as THREE records with `truncated_corrupt_tail = false` -- half
    // the records gone, and nothing reporting it.
    //
    // This does not make the directory safe to share. It makes the loss loud.
    let dir = temp_wal_dir("two-writers");
    let mut first = PersistentRaftWal::open(wal_options(dir.clone())).expect("first opens");
    let mut second = PersistentRaftWal::open(wal_options(dir.clone())).expect("second opens");

    first
        .append(wal_record(1))
        .expect("the first writer appends");
    // The second adopts whatever it finds, so its first append lands.
    second
        .append(wal_record(101))
        .expect("the second writer's first append adopts the file");

    // Now the first writer's picture of the file is stale.
    let error = first
        .append(wal_record(2))
        .expect_err("the first writer must notice the file moved underneath it");
    let message = error.to_string();
    assert!(
        message.contains("another writer is using"),
        "the error has to say what happened: {message}"
    );
    assert!(
        message.contains("bytes but this writer left it at"),
        "and name both lengths, so it is diagnosable: {message}"
    );
}

#[test]
fn one_writer_appending_many_times_is_unaffected() {
    // The control, and it is the one that matters: a check comparing the file
    // against the writer's own bookkeeping must not fire for the ordinary case,
    // including across the segment rolls this fixture provokes
    // (`max_records_per_segment` is 2).
    let dir = temp_wal_dir("one-writer");
    let mut wal = PersistentRaftWal::open(wal_options(dir)).expect("open");
    for index in 1..=12u64 {
        wal.append(wal_record(index))
            .unwrap_or_else(|err| panic!("append {index} must succeed: {err}"));
    }
    let report = wal.recover().expect("recover");
    assert!(
        report.surviving_records > 0,
        "the premise: the appends actually landed"
    );
}

#[test]
fn reopening_after_dropping_the_first_writer_still_works() {
    // The other control: the check must not turn sequential reopening -- which
    // `persistent_wal.rs` already relies on -- into a failure.
    let dir = temp_wal_dir("reopen");
    {
        let mut wal = PersistentRaftWal::open(wal_options(dir.clone())).expect("open");
        wal.append(wal_record(1)).expect("append");
    }
    let mut reopened = PersistentRaftWal::open(wal_options(dir)).expect("reopen");
    reopened
        .append(wal_record(2))
        .expect("a reopened writer adopts the file and appends");
}
