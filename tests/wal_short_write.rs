// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WAL file that ends mid-record recovers its prefix and repairs itself.
//!
//! This is the failure a crash actually leaves: the process died between the
//! write and its completion, so the file ends partway through a record.
//!
//! The segment reader has two arms for a bad tail -- one where the record does
//! not parse and one where it parses but fails its checksum -- and planting
//! them showed something I had assumed the other way round:
//! `corrupt_tail_for_test`, which the existing
//! `persistent_wal_truncates_corrupt_tail_on_recovery` uses, produces a record
//! that does not **parse**, so it exercises the same arm this file does.
//! Reverting the parse arm to an error fails four tests across both files;
//! reverting the checksum arm fails one, and not one of these.
//!
//! What this file adds is therefore not a different arm but a different
//! *cause*: the file is shortened from outside the crate, so recovery is faced
//! with a real short write rather than with whatever that test hook writes, and
//! none of it depends on the hook staying the same.
//!
//! **Why this property is the interesting one.** The comparable engine puts
//! three failpoints around a single write -- after its meta, before its log,
//! after its log -- because a crash between its meta and its log leaves a
//! specific inconsistency to recover from. Our record carries the hard state,
//! the entries, the membership and the apply fence together under one
//! checksum, in one append, so that window does not exist: a record is wholly
//! there or wholly gone. That is the claim, and this is what checks it rather
//! than asserting it.

use matrixraft::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, PersistentRaftWal,
    PersistentRaftWalOptions, WalRecord,
};
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
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries: (1..=index)
            .map(|i| LogEntry {
                log_id: LogId { term: 1, index: i },
                payload: vec![b'x'; 64],
                is_command: true,
            })
            .collect(),
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

fn wal_dir(name: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-short-write-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn options(dir: &std::path::Path) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir: dir.to_path_buf(),
        max_records_per_segment: 1_000,
        max_segment_bytes: 1 << 20,
        min_keep_segments: 1,
        fsync_on_append: true,
    }
}

/// Lops `bytes` off the newest file in `dir`, as a crash mid-write would.
fn end_the_newest_file_early(dir: &std::path::Path, bytes: u64) -> u64 {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .expect("read the wal directory")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        // Segments only. The directory also holds the writer's LOCK file,
        // which is empty and sorts after 000000.wal, so taking "the last file"
        // picked that instead and tried to lop bytes off nothing.
        .filter(|path| {
            path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("wal")
        })
        .collect();
    files.sort();
    let victim = files.last().expect("a segment file").clone();
    let len = std::fs::metadata(&victim).expect("metadata").len();
    assert!(
        len > bytes,
        "the segment is only {len} bytes, so lopping {bytes} would empty it"
    );
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&victim)
        .expect("open the segment for truncation");
    file.set_len(len - bytes).expect("truncate");
    file.sync_all().expect("sync");
    len - bytes
}

#[test]
fn a_file_ending_mid_record_recovers_its_whole_prefix() {
    let dir = wal_dir("prefix");
    let _ = std::fs::remove_dir_all(&dir);
    {
        let mut wal = PersistentRaftWal::open(options(&dir)).expect("open");
        for index in 1..=3u64 {
            wal.append(record(index)).expect("append");
        }
        assert_eq!(wal.records().len(), 3);
    }
    end_the_newest_file_early(&dir, 40);

    let mut reopened = PersistentRaftWal::open(options(&dir)).expect("reopen");
    let report = reopened.recover().expect("recover");
    assert!(
        report.truncated_corrupt_tail,
        "a file ending mid-record is a corrupt tail and has to be reported as one"
    );
    assert_eq!(
        report.surviving_records, 2,
        "the two whole records before the tear survive"
    );

    let records = reopened.records();
    assert_eq!(records.len(), 2);
    assert_eq!(
        records
            .iter()
            .map(|r| r.hard_state.committed.as_ref().map(|c| c.index))
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2)],
        "and they are the first two, in order, not a surviving fragment of the third"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_repaired_file_takes_appends_again() {
    // Recovery truncates the file to the last good offset. Without that the
    // next append lands after the torn bytes and every later read stops at
    // them, so the group would be stuck at the same two records for ever.
    let dir = wal_dir("reappend");
    let _ = std::fs::remove_dir_all(&dir);
    {
        let mut wal = PersistentRaftWal::open(options(&dir)).expect("open");
        for index in 1..=3u64 {
            wal.append(record(index)).expect("append");
        }
    }
    end_the_newest_file_early(&dir, 40);

    {
        let mut reopened = PersistentRaftWal::open(options(&dir)).expect("reopen");
        reopened.recover().expect("recover");
        reopened.append(record(9)).expect("append after the repair");
    }

    // Re-opened a second time, on purpose: `records()` answers from the
    // segments held in memory, so appending and reading back in one session
    // passes whether or not the file itself was repaired. Planting the repair
    // away left that version of this test green. Only a fresh read from disk
    // can tell whether the new record landed somewhere a reader will reach.
    let mut after_restart = PersistentRaftWal::open(options(&dir)).expect("reopen again");
    after_restart.recover().expect("recover again");
    let records = after_restart.records();
    assert_eq!(
        records.len(),
        3,
        "two survivors plus the new one: {:?}",
        records
            .iter()
            .map(|r| r.hard_state.committed.as_ref().map(|c| c.index))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        records
            .last()
            .and_then(|r| r.hard_state.committed.as_ref())
            .map(|c| c.index),
        Some(9),
        "the new record is readable, so the file was repaired rather than appended past"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_untouched_file_is_not_reported_as_torn() {
    // The positive control. A recovery that always reported a corrupt tail
    // would satisfy both tests above.
    let dir = wal_dir("intact");
    let _ = std::fs::remove_dir_all(&dir);
    {
        let mut wal = PersistentRaftWal::open(options(&dir)).expect("open");
        for index in 1..=3u64 {
            wal.append(record(index)).expect("append");
        }
    }

    let mut reopened = PersistentRaftWal::open(options(&dir)).expect("reopen");
    let report = reopened.recover().expect("recover");
    assert!(
        !report.truncated_corrupt_tail,
        "nothing was torn, so nothing should be reported torn"
    );
    assert_eq!(report.surviving_records, 3);
    let _ = std::fs::remove_dir_all(&dir);
}
