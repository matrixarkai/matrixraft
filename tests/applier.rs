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
    matrixraft_apply_task_through_index, Applier, ApplierOptions, ApplyHandler, ApplyReport,
    ApplyTask, ApplyWait, DriverGroupKey, LogEntry, LogId,
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

/// Applies up to `refuse_at` and then says it cannot go further, the way a
/// state machine that has met an entry it does not understand would.
#[derive(Debug)]
struct Refuser {
    refuse_at: u64,
    batches: AtomicU64,
    applied: AtomicU64,
    /// Every index this handler was handed, so a test can say what it was
    /// offered and not merely how often.
    seen: Mutex<Vec<u64>>,
}

impl Refuser {
    fn new(refuse_at: u64) -> Self {
        Self {
            refuse_at,
            batches: AtomicU64::new(0),
            applied: AtomicU64::new(0),
            seen: Mutex::new(Vec::new()),
        }
    }
    fn batches(&self) -> u64 {
        self.batches.load(Ordering::Relaxed)
    }
    fn seen(&self) -> Vec<u64> {
        self.seen.lock().expect("seen mutex").clone()
    }
    fn highest_seen(&self) -> u64 {
        self.seen().into_iter().max().unwrap_or(0)
    }
}

impl ApplyHandler for Refuser {
    fn apply(&self, _tasks: Vec<ApplyTask>) -> u64 {
        unreachable!("this handler reports through apply_reporting")
    }

    fn apply_reporting(&self, tasks: Vec<ApplyTask>) -> ApplyReport {
        self.batches.fetch_add(1, Ordering::Relaxed);
        {
            let mut seen = self.seen.lock().expect("seen mutex");
            seen.extend(tasks.iter().map(matrixraft_apply_task_through_index));
        }
        let highest = tasks
            .iter()
            .map(matrixraft_apply_task_through_index)
            .max()
            .unwrap_or(0);
        if highest >= self.refuse_at {
            // Stopped *below* the entry it refuses: the applied index is the
            // last one it took, not the one it could not.
            let applied_index = self.refuse_at.saturating_sub(1);
            self.applied.store(applied_index, Ordering::Relaxed);
            return ApplyReport::Stalled {
                applied_index,
                reason: format!("entry {} is not something this fsm knows", self.refuse_at),
            };
        }
        self.applied.store(highest, Ordering::Relaxed);
        ApplyReport::reached(highest)
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

#[test]
fn a_submit_the_worker_has_already_stalled_is_refused_and_counted() {
    // The slow-submitter case, made deterministic and kept in the suite.
    //
    // `a_stalled_group_is_not_handed_anything_else` tolerates a submit the
    // worker has already stalled, because on a contended runner it loses that
    // race -- that is what failed in CI. But the branch that tolerates it only
    // runs when the race is lost, and the suite does not arrange for that, so
    // the tolerance would be code no test exercises: a plant widening it to
    // accept **any** submit error would change nothing anywhere.
    //
    // 50ms a task is the same thing the runner did, and it loses the race every
    // time. So this asserts what the tolerance is allowed to tolerate: the
    // error is the stall, the stall has really happened, and the refusal is
    // counted as a stalled one.
    let applier = Applier::start(ApplierOptions {
        applier_num: 1,
        apply_max_batch_count: 1,
        ..ApplierOptions::default()
    })
    .expect("applier");
    let handler = Arc::new(Refuser::new(2));
    let key = DriverGroupKey::new(52, 1);
    applier
        .register_group(key, Arc::clone(&handler) as Arc<dyn ApplyHandler>)
        .expect("register");

    let mut refusals = Vec::new();
    for index in 1..=4 {
        std::thread::sleep(Duration::from_millis(50));
        if let Err(error) = applier.submit(key, task(index)) {
            refusals.push(format!("{error:?}"));
        }
    }

    assert!(
        !refusals.is_empty(),
        "the worker has to win this race, or this test asserts nothing: a 50ms gap a \
         task and it still kept up with the submitter"
    );
    for message in &refusals {
        assert!(
            message.contains("stalled"),
            "the only submit a stalled group may refuse is one it refuses *because* it \
             has stalled: {message}"
        );
    }
    assert!(
        applier.apply_stall(key).is_some(),
        "and the stall it reported has to be a stall it is still in"
    );
    assert_eq!(
        applier.stalled_refusals(),
        refusals.len() as u64,
        "every refusal is counted as a stalled one"
    );
    assert!(
        handler.highest_seen() <= 2,
        "nothing past the refused entry reached the handler: {:?}",
        handler.seen()
    );
}

#[test]
fn a_stalled_group_is_not_handed_anything_else() {
    let applier = Applier::start(ApplierOptions {
        applier_num: 1,
        apply_max_batch_count: 1,
        ..ApplierOptions::default()
    })
    .expect("applier");
    let handler = Arc::new(Refuser::new(2));
    let key = DriverGroupKey::new(51, 1);
    applier
        .register_group(key, Arc::clone(&handler) as Arc<dyn ApplyHandler>)
        .expect("register");

    // Four tasks, refusing at the second, so two are already queued behind the
    // one that stalls. Which of the two guards stops them depends on whether
    // the pool hands them over in one call or in several -- that is the pools
    // decision and it is timing -- so what is asserted is the property both
    // guards exist for: nothing past the refused entry reaches the handler.
    //
    // **A submit here can be refused, and that is not a failure.** The worker
    // only has to get through task 1, refuse entry 2 and stall the group
    // before this loop reaches task 3 -- which is this test's own subject
    // arriving earlier than its submission loop. A stalled group refuses new
    // work, so `submit` returns the stall. This failed in CI on a contended
    // runner while passing locally; slowing the loop by 50ms a task reproduces
    // it every time.
    //
    // It still has to be *that* refusal. A submit that fails while the group
    // has not stalled is a real failure, and the count is carried to the
    // refusal assertion below so an early one cannot hide there either.
    let mut refused_while_stalled = 0;
    for index in 1..=4 {
        if let Err(error) = applier.submit(key, task(index)) {
            assert!(
                applier.apply_stall(key).is_some(),
                "submit {index} was refused while the group had not stalled: {error:?}"
            );
            refused_while_stalled += 1;
        }
    }

    assert!(
        wait_until("the group stalls", Duration::from_secs(20), || applier
            .apply_stall(key)
            .is_some()),
        "the handler refused entry 3 and the applier did not notice"
    );
    let stall = applier.apply_stall(key).expect("a stall");
    assert_eq!(
        stall.applied_index, 1,
        "it stopped below the entry it refused"
    );
    assert!(
        stall.reason.contains("entry 2"),
        "reason was {:?}",
        stall.reason
    );
    assert_eq!(
        applier.progress(key).expect("progress").applied_index,
        1,
        "the applied index is what the handler got through"
    );
    assert!(
        handler.highest_seen() <= 2,
        "the handler was offered {:?}, which is past the entry it refused",
        handler.seen()
    );

    // Nothing more reaches the handler, and a submit is refused rather than
    // queued for a group that cannot take it.
    let batches_at_stall = handler.batches();
    let refused = applier.submit(key, task(4));
    assert!(refused.is_err(), "a stalled group must refuse a submit");
    let message = format!("{:?}", refused.expect_err("an error"));
    assert!(message.contains("stalled"), "error was {message}");
    assert_eq!(
        applier.stalled_refusals(),
        refused_while_stalled + 1,
        "every refusal has to be a stalled one: {refused_while_stalled} from the \
         submission loop racing the worker, and the one made deliberately here"
    );
    assert!(
        applier.stalled_refusals() <= applier.refused_tasks(),
        "a stalled refusal is one of the refusals"
    );

    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        handler.batches(),
        batches_at_stall,
        "the handler was called again after it said it could not go further"
    );
}

#[test]
fn a_resumed_group_applies_again() {
    let applier = Applier::start(ApplierOptions {
        applier_num: 1,
        apply_max_batch_count: 1,
        ..ApplierOptions::default()
    })
    .expect("applier");
    // Refuses nothing once resumed, because the index it refuses is behind the
    // tasks that follow -- which is the shape a snapshot install produces.
    let handler = Arc::new(Refuser::new(2));
    let key = DriverGroupKey::new(52, 1);
    applier
        .register_group(key, Arc::clone(&handler) as Arc<dyn ApplyHandler>)
        .expect("register");

    applier.submit(key, task(2)).expect("the task it refuses");
    assert!(
        wait_until("the group stalls", Duration::from_secs(20), || applier
            .apply_stall(key)
            .is_some()),
        "no stall"
    );

    assert!(applier.resume_group(key), "it was stalled");
    assert!(
        !applier.resume_group(key),
        "resuming twice is idempotent and reports that nothing was stalled"
    );
    assert!(applier.apply_stall(key).is_none());

    // And the group takes work again.
    applier
        .submit(key, task(1))
        .expect("a resumed group accepts a submit");
    assert!(
        wait_until("it applies again", Duration::from_secs(20), || {
            applier.progress(key).expect("progress").applied_index == 1
        }),
        "progress was {:?}",
        applier.progress(key)
    );
}

#[test]
fn a_waiter_on_a_stalled_group_is_not_told_it_timed_out() {
    // A timeout tells a host to try again. A stall tells it not to, and the
    // difference is the whole reason this is a separate answer.
    let applier = Applier::start(ApplierOptions {
        applier_num: 1,
        apply_max_batch_count: 1,
        ..ApplierOptions::default()
    })
    .expect("applier");
    let handler = Arc::new(Refuser::new(2));
    let key = DriverGroupKey::new(53, 1);
    applier
        .register_group(key, Arc::clone(&handler) as Arc<dyn ApplyHandler>)
        .expect("register");

    applier.submit(key, task(2)).expect("submit");
    assert!(
        wait_until("the group stalls", Duration::from_secs(20), || applier
            .apply_stall(key)
            .is_some()),
        "no stall"
    );

    // A generous timeout: the answer must come from the stall, not from the
    // clock running out, so a long timeout is what makes the test meaningful.
    let waited = applier
        .wait_for_applied(key, 9, Duration::from_secs(10))
        .expect("wait");
    assert_eq!(
        waited,
        ApplyWait::Stalled { applied_index: 1 },
        "a wait on a stalled group reported {waited:?}"
    );
    assert!(
        !waited.reached(),
        "a stalled group has not reached the index"
    );

    // Resuming wakes the waiter rather than leaving it to its timeout, so a
    // wait started before the resume still gets its answer from the group.
    assert!(applier.resume_group(key));
    let after = applier
        .wait_for_applied(key, 1, Duration::from_secs(10))
        .expect("wait");
    assert_eq!(after, ApplyWait::Reached { applied_index: 1 });
}

#[test]
fn a_handler_that_only_implements_apply_never_stalls() {
    // The default body of `apply_reporting` is what keeps every existing host
    // working, so it is worth an assertion of its own rather than being
    // assumed from the other tests passing.
    let applier = Applier::start(ApplierOptions {
        applier_num: 1,
        ..ApplierOptions::default()
    })
    .expect("applier");
    let handler = Arc::new(Recorder::default());
    let key = DriverGroupKey::new(54, 1);
    applier
        .register_group(key, Arc::clone(&handler) as Arc<dyn ApplyHandler>)
        .expect("register");

    applier.submit(key, task(7)).expect("submit");
    assert!(
        wait_until("it applies", Duration::from_secs(20), || {
            applier.progress(key).expect("progress").applied_index == 7
        }),
        "progress was {:?}",
        applier.progress(key)
    );
    assert!(applier.apply_stall(key).is_none());
    assert_eq!(applier.stalled_refusals(), 0);
}
