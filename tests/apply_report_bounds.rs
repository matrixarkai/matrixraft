// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A handler cannot report an index its batch never carried.
//!
//! An `ApplyHandler` is implemented by the host and **returns** the index it
//! reached. Nothing bounded that number to the work it was given, so a handler
//! answering 9,000 for a batch carrying 1 through 3 moved the group's applied
//! index to 9,000:
//!
//! ```text
//! progress: applied_index: 9000, batches_applied: 1, tasks_applied: 1
//! wait_for_applied(5000) reached: true
//! ```
//!
//! That last line is the defect. `wait_for_applied` is the read fence, and it
//! released for an index nothing had applied -- the one thing the fence exists
//! to prevent. The comparable engine does not trust its own state machine here
//! either: a handler returning success with entries left unconsumed is a fatal
//! event there.
//!
//! The index is bounded now, and the over-report is **counted** rather than
//! only corrected, because a handler doing this is a bug in the host's code and
//! a silently fixed number gives it nothing to find.
//!
//! **The ceiling has to include a snapshot's index**, not just the last entry's.
//! A snapshot-only task carries no entries and moves the group by installing;
//! a ceiling taken from entries alone would clamp it to zero and stop snapshot
//! installs from advancing anything. That is pinned separately below.

use matrixraft::{
    applier::{Applier, ApplierOptions, ApplyHandler, ApplyReport},
    ApplyTask, DriverGroupKey, LogEntry, LogId, SnapshotMetadata,
};
use std::sync::Arc;
use std::time::Duration;

/// Reports a fixed index whatever it is handed.
#[derive(Debug)]
struct Reporting(u64);

impl ApplyHandler for Reporting {
    fn apply(&self, _tasks: Vec<ApplyTask>) -> u64 {
        self.0
    }
}

/// Stalls at a fixed index whatever it is handed.
#[derive(Debug)]
struct Stalling(u64);

impl ApplyHandler for Stalling {
    fn apply(&self, _tasks: Vec<ApplyTask>) -> u64 {
        self.0
    }

    fn apply_reporting(&self, _tasks: Vec<ApplyTask>) -> ApplyReport {
        ApplyReport::Stalled {
            applied_index: self.0,
            reason: "refused".to_string(),
        }
    }
}

fn entries(indexes: std::ops::RangeInclusive<u64>) -> ApplyTask {
    ApplyTask {
        entries: indexes
            .map(|index| LogEntry {
                log_id: LogId { term: 1, index },
                payload: b"e".to_vec(),
                is_command: true,
            })
            .collect(),
        snapshot: None,
    }
}

fn snapshot_only(index: u64) -> ApplyTask {
    ApplyTask {
        entries: Vec::new(),
        snapshot: Some(SnapshotMetadata {
            snapshot_id: "installed".to_string(),
            last_log_id: LogId { term: 1, index },
            membership: vec![1],
            members: Vec::new(),
            payload_bytes: 0,
        }),
    }
}

fn applier_with(handler: Arc<dyn ApplyHandler>) -> (Applier, DriverGroupKey) {
    let applier = Applier::start(ApplierOptions::default()).expect("an applier");
    let key = DriverGroupKey::new(1, 1);
    applier.register_group(key, handler).expect("register");
    (applier, key)
}

/// Waits for the group to have applied something, so the assertions below do
/// not race the apply thread.
fn settle(applier: &Applier, key: DriverGroupKey) {
    for _ in 0..200 {
        if applier
            .progress(key)
            .is_some_and(|progress| progress.tasks_applied > 0)
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("the group never applied the task it was given");
}

#[test]
fn a_handler_cannot_report_past_what_its_batch_carried() {
    let (mut applier, key) = applier_with(Arc::new(Reporting(9_000)));
    applier.submit(key, entries(1..=3)).expect("submit");
    settle(&applier, key);

    let progress = applier.progress(key).expect("progress");
    assert_eq!(
        progress.applied_index, 3,
        "the batch carried 1 through 3, so 3 is as far as the group can have got"
    );
    assert_eq!(
        progress.over_reported_applies, 1,
        "and the host is told its handler over-reported rather than only being corrected"
    );
    assert!(
        !applier
            .wait_for_applied(key, 5_000, Duration::from_millis(50))
            .expect("wait")
            .reached(),
        "the read fence must not release for an index nothing applied"
    );
    applier.stop();
}

#[test]
fn a_snapshot_only_task_still_moves_the_group() {
    // The ceiling has to count a snapshot's index. Taken from entries alone it
    // is zero here, and this group would never advance at all.
    let (mut applier, key) = applier_with(Arc::new(Reporting(40)));
    applier.submit(key, snapshot_only(40)).expect("submit");
    settle(&applier, key);

    let progress = applier.progress(key).expect("progress");
    assert_eq!(
        progress.applied_index, 40,
        "a snapshot-only task moves the group by installing, not by applying"
    );
    assert_eq!(
        progress.over_reported_applies, 0,
        "and reporting the snapshot's own index is not an over-report"
    );
    applier.stop();
}

#[test]
fn an_honest_handler_is_not_counted_as_over_reporting() {
    let (mut applier, key) = applier_with(Arc::new(Reporting(3)));
    applier.submit(key, entries(1..=3)).expect("submit");
    settle(&applier, key);

    let progress = applier.progress(key).expect("progress");
    assert_eq!(progress.applied_index, 3);
    assert_eq!(
        progress.over_reported_applies, 0,
        "reporting exactly the batch's last index is the honest answer"
    );
    applier.stop();
}

#[test]
fn a_handler_that_did_not_finish_still_reports_how_far_it_got() {
    // Bounding must not raise a low number: a partial apply is the existing
    // behaviour and the conservative direction for a read waiting on a later
    // index.
    let (mut applier, key) = applier_with(Arc::new(Reporting(2)));
    applier.submit(key, entries(1..=3)).expect("submit");
    settle(&applier, key);

    let progress = applier.progress(key).expect("progress");
    assert_eq!(
        progress.applied_index, 2,
        "the handler got to 2 of 3 and the group's index says so"
    );
    assert_eq!(progress.over_reported_applies, 0);
    applier.stop();
}

#[test]
fn a_stall_cannot_report_past_the_batch_either() {
    // The stalled arm carries its own index into `ApplyStall`, and a stop
    // beyond the batch is no more meaningful than a success beyond it.
    let (mut applier, key) = applier_with(Arc::new(Stalling(9_000)));
    applier.submit(key, entries(1..=3)).expect("submit");
    for _ in 0..200 {
        if applier.apply_stall(key).is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let stall = applier.apply_stall(key).expect("the group stalled");
    assert_eq!(
        stall.applied_index, 3,
        "the stall reports where the group stopped, bounded by what it was given"
    );
    applier.stop();
}
