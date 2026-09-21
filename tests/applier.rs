// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Many groups applying on a shared pool, and a read waiting for its group.
//!
//! Every wait polls or uses the applier's own bounded wait; nothing sleeps a
//! fixed time and then asserts, because that asserts the machine.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use matrixraft::{
    matrixraft_apply_task_through_index, Applier, ApplierOptions, ApplyHandler, ApplyTask,
    ApplyWait, DriverGroupKey, LogEntry, LogId,
};

/// Applies everything it is given and reports the highest index in the batch.
#[derive(Debug, Default)]
struct Recorder {
    batches: Mutex<Vec<usize>>,
    applied: AtomicU64,
}

impl Recorder {
    fn batch_sizes(&self) -> Vec<usize> {
        self.batches.lock().expect("recorder mutex").clone()
    }
    fn total(&self) -> usize {
        self.batches.lock().expect("recorder mutex").iter().sum()
    }
    fn largest_batch(&self) -> usize {
        self.batch_sizes().into_iter().max().unwrap_or(0)
    }
}

impl ApplyHandler for Recorder {
    fn apply(&self, tasks: Vec<ApplyTask>) -> u64 {
        let highest = tasks
            .iter()
            .map(matrixraft_apply_task_through_index)
            .max()
            .unwrap_or(0);
        self.batches
            .lock()
            .expect("recorder mutex")
            .push(tasks.len());
        self.applied.store(highest, Ordering::Relaxed);
        highest
    }
}

fn wait_until(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    let settled = done();
    if !settled {
        eprintln!("wait_until({what}) gave up after {timeout:?}");
    }
    settled
}

/// An apply task carrying one entry at `index`, which is the crate's own
/// `ApplyTask` rather than a shape invented for these tests.
fn task(index: u64) -> ApplyTask {
    ApplyTask {
        entries: vec![LogEntry {
            log_id: LogId { term: 1, index },
            payload: vec![0u8; 8],
            is_command: false,
        }],
        snapshot: None,
    }
}

#[test]
fn one_pool_applies_for_every_group() {
    let applier = Applier::start(ApplierOptions {
        applier_num: 3,
        ..ApplierOptions::default()
    })
    .expect("applier");

    let groups = 128_u64;
    let recorders: Vec<Arc<Recorder>> =
        (0..groups).map(|_| Arc::new(Recorder::default())).collect();
    for (index, recorder) in recorders.iter().enumerate() {
        applier
            .register_group(
                DriverGroupKey::new(index as u64 + 1, 1),
                Arc::clone(recorder) as Arc<dyn ApplyHandler>,
            )
            .expect("register");
    }
    assert_eq!(applier.group_count(), groups as usize);
    assert_eq!(
        applier.thread_count(),
        3,
        "three appliers, whatever the group count"
    );

    for group_id in 1..=groups {
        applier
            .submit(DriverGroupKey::new(group_id, 1), task(7))
            .expect("submit");
    }

    assert!(
        wait_until("every group applies", Duration::from_secs(30), || {
            recorders.iter().all(|r| r.total() >= 1)
        }),
        "only {} of {groups} groups applied",
        recorders.iter().filter(|r| r.total() >= 1).count()
    );

    for group_id in 1..=groups {
        let progress = applier
            .progress(DriverGroupKey::new(group_id, 1))
            .expect("progress");
        assert_eq!(
            progress.applied_index, 7,
            "group {group_id} should have applied through 7"
        );
    }
}

#[test]
fn a_batch_respects_apply_max_batch_count() {
    let applier = Applier::start(ApplierOptions {
        applier_num: 1,
        apply_max_batch_count: 8,
        max_queue_depth: 8192,
        ..ApplierOptions::default()
    })
    .expect("applier");
    let key = DriverGroupKey::new(1, 1);
    let recorder = Arc::new(Recorder::default());
    applier
        .register_group(key, Arc::clone(&recorder) as Arc<dyn ApplyHandler>)
        .expect("register");

    let submitted = 500_u64;
    for index in 1..=submitted {
        applier.submit(key, task(index)).expect("submit");
    }
    assert!(
        wait_until("all tasks apply", Duration::from_secs(30), || {
            recorder.total() as u64 >= submitted
        }),
        "only {} of {submitted} applied",
        recorder.total()
    );

    let largest = recorder.largest_batch();
    assert!(
        largest <= 8,
        "apply_max_batch_count is 8, so no call should carry more; largest was {largest}"
    );
    assert!(
        recorder.batch_sizes().len() > 1,
        "500 tasks at 8 per batch should be many calls, not one"
    );
}

#[test]
fn a_read_waits_until_its_group_has_applied_and_not_before() {
    // The safety property: reached is never reported early. A handler that
    // applies only when released lets the test hold the group behind the index
    // a read needs and check both sides of the boundary.
    let applier = Applier::start(ApplierOptions {
        applier_num: 1,
        ..ApplierOptions::default()
    })
    .expect("applier");

    struct Gated {
        gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
    }
    impl ApplyHandler for Gated {
        fn apply(&self, tasks: Vec<ApplyTask>) -> u64 {
            let (lock, cv) = &*self.gate;
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut released = lock.lock().expect("gate mutex");
            while !*released {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let (next, _) = cv
                    .wait_timeout(released, deadline - now)
                    .expect("gate mutex");
                released = next;
            }
            tasks
                .iter()
                .map(matrixraft_apply_task_through_index)
                .max()
                .unwrap_or(0)
        }
    }

    let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let key = DriverGroupKey::new(4, 1);
    applier
        .register_group(
            key,
            Arc::new(Gated {
                gate: Arc::clone(&gate),
            }),
        )
        .expect("register");
    applier.submit(key, task(12)).expect("submit");

    // Held behind the gate, so the wait must time out rather than report
    // reached. This is the assertion that matters: a read served here would be
    // a stale read.
    let waited = applier
        .wait_for_applied(key, 12, Duration::from_millis(200))
        .expect("wait");
    assert!(
        !waited.reached(),
        "a group that has not applied must not report reached: {waited:?}"
    );
    assert_eq!(waited.applied_index(), 0);

    // Release, and the same wait now succeeds.
    {
        let (lock, cv) = &*gate;
        *lock.lock().expect("gate mutex") = true;
        cv.notify_all();
    }
    let waited = applier
        .wait_for_applied(key, 12, Duration::from_secs(20))
        .expect("wait");
    assert!(
        waited.reached(),
        "after applying, the wait should report reached: {waited:?}"
    );
    assert!(waited.applied_index() >= 12);
}

#[test]
fn a_wait_for_an_index_already_applied_returns_at_once() {
    let applier = Applier::start(ApplierOptions::default()).expect("applier");
    let key = DriverGroupKey::new(9, 1);
    let recorder = Arc::new(Recorder::default());
    applier
        .register_group(key, Arc::clone(&recorder) as Arc<dyn ApplyHandler>)
        .expect("register");
    applier.submit(key, task(30)).expect("submit");
    assert!(
        wait_until("the task applies", Duration::from_secs(20), || {
            applier
                .progress(key)
                .map(|p| p.applied_index >= 30)
                .unwrap_or(false)
        }),
        "the task never applied"
    );

    // Already past it, so no waiting should happen at all.
    let started = Instant::now();
    let waited = applier
        .wait_for_applied(key, 10, Duration::from_secs(20))
        .expect("wait");
    assert!(waited.reached());
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "an index already applied should not wait: took {:?}",
        started.elapsed()
    );
}

#[test]
fn the_applied_index_never_moves_backwards() {
    // A handler that reports an older index than the group reached must not
    // un-apply it: a read that was safe cannot become unsafe.
    struct Backwards {
        calls: AtomicU64,
    }
    impl ApplyHandler for Backwards {
        fn apply(&self, _tasks: Vec<ApplyTask>) -> u64 {
            // 50 first, then 5.
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                50
            } else {
                5
            }
        }
    }

    let applier = Applier::start(ApplierOptions {
        applier_num: 1,
        ..ApplierOptions::default()
    })
    .expect("applier");
    let key = DriverGroupKey::new(2, 1);
    applier
        .register_group(
            key,
            Arc::new(Backwards {
                calls: AtomicU64::new(0),
            }),
        )
        .expect("register");

    applier.submit(key, task(50)).expect("submit");
    assert!(
        wait_until("the first apply", Duration::from_secs(20), || {
            applier
                .progress(key)
                .map(|p| p.applied_index == 50)
                .unwrap_or(false)
        }),
        "the first apply never landed"
    );

    applier.submit(key, task(5)).expect("submit");
    assert!(
        wait_until("the second apply", Duration::from_secs(20), || {
            applier
                .progress(key)
                .map(|p| p.batches_applied >= 2)
                .unwrap_or(false)
        }),
        "the second apply never landed"
    );
    assert_eq!(
        applier.progress(key).expect("progress").applied_index,
        50,
        "a lower report must not move the applied index back"
    );
}

#[test]
fn waiting_on_a_group_that_is_not_registered_is_an_error() {
    let applier = Applier::start(ApplierOptions::default()).expect("applier");
    let key = DriverGroupKey::new(77, 1);
    assert!(applier
        .wait_for_applied(key, 1, Duration::from_millis(10))
        .is_err());
    assert!(applier.submit(key, task(1)).is_err());
    assert!(applier.progress(key).is_none());
}

#[test]
fn an_applier_needs_threads_and_a_batch_size() {
    assert!(Applier::start(ApplierOptions {
        applier_num: 0,
        ..ApplierOptions::default()
    })
    .is_err());
    assert!(Applier::start(ApplierOptions {
        apply_max_batch_count: 0,
        ..ApplierOptions::default()
    })
    .is_err());
}

#[test]
fn cancelling_a_group_wakes_what_is_waiting_on_it() {
    // A waiter must not outlive the group it waits for.
    let applier = Arc::new(Applier::start(ApplierOptions::default()).expect("applier"));
    let key = DriverGroupKey::new(3, 1);
    let recorder = Arc::new(Recorder::default());
    applier
        .register_group(key, Arc::clone(&recorder) as Arc<dyn ApplyHandler>)
        .expect("register");

    let waiter_applier = Arc::clone(&applier);
    let started = Instant::now();
    let waiter = std::thread::spawn(move || {
        // A timeout far longer than the test should take, so finishing early
        // proves the cancel released it rather than the clock running out.
        waiter_applier.wait_for_applied(key, 9_999, Duration::from_secs(60))
    });

    assert!(applier.cancel_group(key));
    let outcome = waiter.join().expect("waiter");

    // Two orderings are possible and both are correct, which is why this
    // asserts the property rather than one of them: the waiter either looked
    // the group up before the cancel and was woken with GroupGone, or looked it
    // up afterwards and found nothing. What must never happen is that it stays
    // blocked, or that it reports the index as reached.
    match outcome {
        Ok(wait) => {
            assert!(
                matches!(wait, ApplyWait::GroupGone { .. }),
                "a cancelled group must report GroupGone, not a timeout or a reach: {wait:?}"
            );
            assert!(!wait.reached());
        }
        Err(error) => {
            // Looked up after the removal: the group is simply not there.
            assert!(
                format!("{error:?}").contains("NodeNotFound"),
                "the only acceptable error here is a missing group: {error:?}"
            );
        }
    }
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the cancel must not leave a waiter blocked, but it took {:?}",
        started.elapsed()
    );
    assert_eq!(applier.group_count(), 0);
}

#[test]
fn a_tasks_through_index_is_its_last_entrys() {
    assert_eq!(matrixraft_apply_task_through_index(&task(42)), 42);

    let many = ApplyTask {
        entries: vec![
            LogEntry {
                log_id: LogId { term: 1, index: 7 },
                payload: Vec::new(),
                is_command: false,
            },
            LogEntry {
                log_id: LogId { term: 1, index: 9 },
                payload: Vec::new(),
                is_command: false,
            },
        ],
        snapshot: None,
    };
    assert_eq!(matrixraft_apply_task_through_index(&many), 9);

    // A snapshot-only task carries no entries; it moves the group by
    // installing, which is not this function's business.
    let snapshot_only = ApplyTask {
        entries: Vec::new(),
        snapshot: None,
    };
    assert_eq!(matrixraft_apply_task_through_index(&snapshot_only), 0);
}
