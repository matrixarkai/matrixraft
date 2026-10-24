// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WAL directory takes one writer at a time.
//!
//! **What this used to be, and why it changed.** Two `PersistentRaftWal`
//! handles on one directory each keep their own segment state in memory and
//! write the same filenames, so one overwrites the other's segment. Every file
//! is left individually well-formed, so recovery has nothing to flag: measured,
//! six appends through two handles recovered as **three records with
//! `truncated_corrupt_tail: false`** -- half the records gone, and nothing
//! reporting it.
//!
//! The first version of this file could only make that loss *loud*, by having
//! each writer compare the segment against the length its own last append left
//! behind. The directory is now **locked**, so the second handle never opens and
//! the loss cannot start. The length check remains underneath as a second line,
//! and `a_file_that_moves_under_the_writer_is_still_noticed` keeps it honest.
//!
//! **The lock.** A `LOCK` file in the directory, held with `File::try_lock` --
//! an advisory whole-file lock, `flock` on Unix. Three properties earn it:
//!
//! * it does not block, so a second writer is told at once rather than waiting
//!   on a holder that may never let go;
//! * the kernel releases it when the holding process dies, so a crash leaves no
//!   lock to clear by hand. `a_lock_does_not_outlive_the_process_holding_it`
//!   proves that with a real child process rather than asserting it;
//! * it is held per open file description, so **two handles inside one process
//!   conflict too**. That is the case actually measured above, and the one a
//!   POSIX record lock (`fcntl`) would miss, since those are per process.

use matrixraft::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, PersistentRaftWal,
    PersistentRaftWalOptions, WalRecord,
};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Set on the child in `a_lock_does_not_outlive_the_process_holding_it`, which
/// is the only thing that reads it.
const CHILD_LOCK_DIR: &str = "MATRIXRAFT_TEST_CHILD_LOCK_DIR";

/// Printed by the child once it holds the lock. Without it a child that ran no
/// test would be indistinguishable from one that did its job.
const CHILD_MARKER: &str = "matrixraft-child-took-the-lock";

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
fn a_second_writer_on_the_same_directory_is_refused() {
    let dir = temp_wal_dir("two-writers");
    let mut first = PersistentRaftWal::open(wal_options(dir.clone())).expect("the first opens");
    first
        .append(wal_record(1))
        .expect("and is the writer for this directory");

    let error = PersistentRaftWal::open(wal_options(dir.clone()))
        .expect_err("a second writer must not open the directory at all");
    let message = error.to_string();
    assert!(
        message.contains("already open for writing by another handle"),
        "the refusal has to say what is wrong: {message}"
    );
    assert!(
        message.contains(&dir.display().to_string()),
        "and name the directory, since a host runs many: {message}"
    );
    assert!(
        message.contains("LOCK"),
        "and name the lock file, so an operator can check who holds it: {message}"
    );

    // The refusal changed nothing: the first writer still owns the directory
    // and its next append lands.
    first
        .append(wal_record(2))
        .expect("the holder is unaffected by a refused second open");

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn the_lock_file_is_created_once_and_then_reused() {
    let dir = temp_wal_dir("lock-file");
    let lock_path = dir.join("LOCK");
    assert!(!lock_path.exists(), "the premise: nothing there yet");

    {
        let mut wal = PersistentRaftWal::open(wal_options(dir.clone())).expect("first open");
        wal.append(wal_record(1)).expect("append");
        assert!(lock_path.exists(), "the first open creates the lock file");
    }

    // Reopening must reuse it rather than failing on an existing file, and must
    // not truncate it either -- the handle is opened without `truncate`, so a
    // directory that has been opened before is indistinguishable from a fresh
    // one as far as this file is concerned.
    let mut reopened = PersistentRaftWal::open(wal_options(dir.clone())).expect("reopen");
    reopened.append(wal_record(2)).expect("append after reopen");
    assert!(lock_path.exists(), "and it is still there");

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn reopening_after_dropping_the_first_writer_still_works() {
    // The contract a careless lock breaks, and the reason the lock is held in a
    // field rather than taken and forgotten: `persistent_wal.rs` reopens
    // directories, and so does every restart.
    let dir = temp_wal_dir("reopen");
    {
        let mut wal = PersistentRaftWal::open(wal_options(dir.clone())).expect("open");
        wal.append(wal_record(1)).expect("append");
    }
    let mut reopened = PersistentRaftWal::open(wal_options(dir.clone())).expect("reopen");
    reopened
        .append(wal_record(2))
        .expect("a reopened writer adopts the file and appends");

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn one_writer_appending_many_times_is_unaffected() {
    // The control that matters: neither the lock nor the length check may fire
    // for the ordinary case, including across the segment rolls this fixture
    // provokes (`max_records_per_segment` is 2).
    let dir = temp_wal_dir("one-writer");
    let mut wal = PersistentRaftWal::open(wal_options(dir.clone())).expect("open");
    for index in 1..=12u64 {
        wal.append(wal_record(index))
            .unwrap_or_else(|err| panic!("append {index} must succeed: {err}"));
    }
    let report = wal.recover().expect("recover");
    assert!(
        report.surviving_records > 0,
        "the premise: the appends actually landed"
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_file_that_moves_under_the_writer_is_still_noticed() {
    // The length check from before the lock, kept and kept tested.
    //
    // The lock stops a second `PersistentRaftWal`, which is what was losing
    // records. It cannot stop everything: an advisory lock is advisory, so a
    // filesystem that ignores it, or any process that simply writes to the
    // segment, still moves the file under this writer. So the check is driven
    // here by an ORDINARY FILE WRITE rather than by a second handle -- which is
    // the only way left to reach it, and an honest statement of what it covers.
    let dir = temp_wal_dir("moved-file");
    let mut wal = PersistentRaftWal::open(wal_options(dir.clone())).expect("open");
    wal.append(wal_record(1)).expect("the first append");

    let segment = std::fs::read_dir(&dir)
        .expect("read the directory")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("wal"))
        .expect("a segment file exists");
    let mut outside = std::fs::OpenOptions::new()
        .append(true)
        .open(&segment)
        .expect("open the segment as any other process would");
    outside
        .write_all(b"bytes this writer did not write\n")
        .expect("write behind its back");
    outside.flush().expect("flush");
    drop(outside);

    let error = wal
        .append(wal_record(2))
        .expect_err("the writer must notice the file moved underneath it");
    let message = error.to_string();
    assert!(
        message.contains("another writer is using"),
        "the error has to say what happened: {message}"
    );
    assert!(
        message.contains("bytes but this writer left it at"),
        "and name both lengths, so it is diagnosable: {message}"
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_lock_does_not_outlive_the_process_holding_it() {
    // The claim that kept this lock unwritten for two sessions: that a crash
    // would leave a stale lock and a node that will not start until somebody
    // deletes a file by hand. It is FALSE for this mechanism -- the kernel drops
    // an advisory lock when the holding process dies -- and a claim that
    // decided a design deserves a test rather than a sentence.
    //
    // A child process takes the lock and leaks the handle, so nothing in its
    // own code releases it; then it exits. If the lock outlived it, the open
    // below would be refused.
    if let Ok(dir) = std::env::var(CHILD_LOCK_DIR) {
        // Running AS the child. Leak the handle: only process exit releases it.
        let wal = PersistentRaftWal::open(wal_options(PathBuf::from(dir)))
            .expect("the child takes the lock");
        std::mem::forget(wal);
        // Printed so the parent can tell "the child locked and exited" from
        // "the child ran no test at all" -- a filter that matches nothing still
        // exits zero, which would make this whole test vacuous.
        println!("{CHILD_MARKER}");
        return;
    }

    let dir = temp_wal_dir("outlive");
    std::fs::create_dir_all(&dir).expect("create the directory");

    let status = std::process::Command::new(std::env::current_exe().expect("this test binary"))
        // `--nocapture` so the child's marker reaches this process:
        // libtest swallows a test's stdout otherwise, and the marker is
        // the only evidence the child actually ran.
        .args([
            "--exact",
            "a_lock_does_not_outlive_the_process_holding_it",
            "--nocapture",
        ])
        .env(CHILD_LOCK_DIR, &dir)
        .output()
        .expect("run the child");
    assert!(
        status.status.success(),
        "the child must have taken the lock and exited cleanly: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let child_stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        child_stdout.contains(CHILD_MARKER),
        "the child ran no test at all, so this proves nothing: {child_stdout}"
    );
    assert!(
        dir.join("LOCK").exists(),
        "the lock FILE survives the process, which is what makes the question \
         worth asking"
    );

    // And the lock itself did not.
    let mut after = PersistentRaftWal::open(wal_options(dir.clone()))
        .expect("the directory is available once the holder is gone");
    after
        .append(wal_record(1))
        .expect("and is writable, not merely openable");

    let _ = std::fs::remove_dir_all(dir);
}
