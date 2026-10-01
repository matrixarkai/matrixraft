// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a host is told when it runs out of file descriptors.
//!
//! Every raft group holds its own WAL open, so the first limit a multi-group
//! store meets is `RLIMIT_NOFILE`, not anything in this crate: measured at one
//! descriptor per group, 4,119 held for 4096 groups. The OS says "Too many
//! open files" about whichever group happened to be over the line, which is a
//! symptom and not a cause. This pins the sentence that supplies the cause.
//!
//! The descriptors are exhausted for real rather than simulated, by opening one
//! existing file over and over. That is why this test is alone in its own
//! binary: the limit is per-process, and a sibling test sharing the process
//! while they are all held would fail for reasons of its own. Everything that
//! could itself need a descriptor -- an assertion's output included -- happens
//! after they are released.

use matrixraft::{PersistentRaftWal, PersistentRaftWalOptions, RaftError};
use std::fs::File;

#[test]
fn running_out_of_descriptors_says_what_a_group_costs() {
    let dir = std::env::temp_dir().join(format!(
        "matrixraft-descriptors-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a directory for the WAL");
    // A file that exists and costs nothing to open again: the point is to use
    // up descriptors, not inodes.
    let probe = dir.join("probe");
    File::create(&probe).expect("a file to hold open");

    let mut held: Vec<File> = Vec::new();
    while let Ok(file) = File::open(&probe) {
        held.push(file);
        assert!(
            held.len() < 1_000_000,
            "a million descriptors and still opening: this process has no \
             limit to run into, so the test cannot say anything"
        );
    }
    let exhausted = held.len();

    let opened = PersistentRaftWal::open(PersistentRaftWalOptions {
        dir: dir.clone(),
        max_records_per_segment: 1_000,
        max_segment_bytes: 1 << 20,
        min_keep_segments: 1,
        fsync_on_append: false,
    });

    // Release them before asserting anything: a failing assertion has to be
    // able to write its message.
    drop(held);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        exhausted > 0,
        "no descriptors were taken, so the WAL was not opened under exhaustion"
    );
    let message = match opened {
        Err(RaftError::Storage(message)) => message,
        Err(other) => panic!("expected a storage error, got {other:?}"),
        Ok(_) => panic!("a WAL opened with no descriptors left, after {exhausted} were held"),
    };
    assert!(
        message.contains("Too many open files"),
        "the OS condition is missing, so this is some other failure: {message}"
    );
    assert!(
        message.contains("one file descriptor per group"),
        "the host is told the symptom and not the cause: {message}"
    );
    assert!(
        message.contains("RLIMIT_NOFILE"),
        "the message does not name the knob to raise: {message}"
    );
}
