// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The driver ticks many groups from one thread.
//!
//! Every wait here polls to a deadline rather than sleeping a fixed time. A
//! fixed sleep turns a loaded machine into a red suite, which is how
//! `a_busy_node_still_ticks` came to demand a rate no runner could supply.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use matrixraft::{
    Driver, DriverGroupKey, DriverMailHandler, DriverOptions, DriverTickReceiver, DriverWorkerPool,
    MailPriority,
};

/// Counts the ticks it is sent.
#[derive(Debug, Default)]
struct TickCounter {
    ticks: AtomicU64,
}

impl TickCounter {
    fn count(&self) -> u64 {
        self.ticks.load(Ordering::Relaxed)
    }
}

impl DriverTickReceiver for TickCounter {
    fn fire_tick(&self) {
        self.ticks.fetch_add(1, Ordering::Relaxed);
    }
}

/// Polls until `done` or the deadline, so the assertion afterwards is about
/// behaviour rather than about how busy the machine was.
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

fn options(worker_num: usize, tick_interval_ms: u64) -> DriverOptions {
    DriverOptions {
        worker_num,
        tick_interval_ms,
        ..DriverOptions::default()
    }
}

#[test]
fn one_ticker_carries_every_group() {
    // The point of the driver: 256 groups, and the thread count is still the
    // options' and not the group count's.
    let driver = Driver::start(options(4, 5)).expect("driver");
    let groups = 256_u64;

    let counters: Vec<Arc<TickCounter>> = (0..groups)
        .map(|_| Arc::new(TickCounter::default()))
        .collect();
    for (index, counter) in counters.iter().enumerate() {
        driver
            .register_group(
                DriverGroupKey::new(index as u64 + 1, 1),
                Arc::clone(counter) as Arc<dyn DriverTickReceiver>,
            )
            .expect("register");
    }

    assert_eq!(driver.group_count(), groups as usize);
    assert_eq!(
        driver.thread_count(),
        5,
        "four workers and one ticker, whatever the group count"
    );

    // Three each, not one: a driver that fired every group once and never
    // rescheduled would satisfy "at least one tick" and be useless.
    let rounds = 3_u64;
    let all_ticked = wait_until(
        "every group ticks three times",
        Duration::from_secs(30),
        || counters.iter().all(|counter| counter.count() >= rounds),
    );
    let ticked = counters.iter().filter(|c| c.count() >= rounds).count();
    assert!(
        all_ticked,
        "one ticker must keep all {groups} groups ticking, but only {ticked} reached {rounds}"
    );

    let stats = driver.stats();
    assert!(
        stats.ticks_fired >= groups * rounds,
        "at least {} ticks, saw {}",
        groups * rounds,
        stats.ticks_fired
    );
}

#[test]
fn a_group_keeps_its_own_interval() {
    // A driver carries a fast group beside a slow one without either paying
    // for the other. Asserted as an ordering, not a ratio: a ratio would be
    // asserting the machine's timing rather than the schedule.
    let driver = Driver::start(options(1, 100)).expect("driver");
    let fast = Arc::new(TickCounter::default());
    let slow = Arc::new(TickCounter::default());

    driver
        .register_group_every(
            DriverGroupKey::new(1, 1),
            Arc::clone(&fast) as Arc<dyn DriverTickReceiver>,
            2,
        )
        .expect("register fast");
    driver
        .register_group_every(
            DriverGroupKey::new(2, 1),
            Arc::clone(&slow) as Arc<dyn DriverTickReceiver>,
            200,
        )
        .expect("register slow");

    assert!(
        wait_until(
            "the fast group ticks 20 times",
            Duration::from_secs(20),
            || { fast.count() >= 20 }
        ),
        "a 2ms group did not reach 20 ticks; it had {}",
        fast.count()
    );

    assert!(
        fast.count() > slow.count(),
        "a 2ms group should outpace a 200ms one: fast={} slow={}",
        fast.count(),
        slow.count()
    );
}

#[test]
fn a_cancelled_group_stops_ticking() {
    let driver = Driver::start(options(1, 2)).expect("driver");
    let counter = Arc::new(TickCounter::default());
    let key = DriverGroupKey::new(7, 1);
    driver
        .register_group(key, Arc::clone(&counter) as Arc<dyn DriverTickReceiver>)
        .expect("register");

    assert!(
        wait_until("the group ticks", Duration::from_secs(20), || counter
            .count()
            >= 3),
        "the group never ticked, so cancelling it proves nothing"
    );

    assert!(driver.cancel_group(key), "cancel should find the group");
    assert!(!driver.cancel_group(key), "cancelling twice finds nothing");
    assert_eq!(driver.group_count(), 0);

    // One tick can already be in flight, so settle before taking the reading.
    std::thread::sleep(Duration::from_millis(50));
    let after_cancel = counter.count();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        counter.count(),
        after_cancel,
        "a cancelled group must stop ticking: 100 intervals passed and it moved"
    );
}

#[test]
fn a_group_cannot_register_twice() {
    let driver = Driver::start(DriverOptions::default()).expect("driver");
    let key = DriverGroupKey::new(3, 2);
    let counter = Arc::new(TickCounter::default());
    driver
        .register_group(key, Arc::clone(&counter) as Arc<dyn DriverTickReceiver>)
        .expect("first register");
    let again = driver.register_group(key, counter as Arc<dyn DriverTickReceiver>);
    assert!(again.is_err(), "the same group twice must be refused");
    assert_eq!(driver.group_count(), 1);
}

#[test]
fn a_zero_interval_takes_the_drivers_default_not_a_busy_loop() {
    // The trap this avoids is the one MatrixRaftOptions had: a zero that
    // becomes `max(1)` is a 1ms hot loop, not "unset".
    let driver = Driver::start(options(1, 250)).expect("driver");
    let counter = Arc::new(TickCounter::default());
    driver
        .register_group_every(
            DriverGroupKey::new(9, 1),
            Arc::clone(&counter) as Arc<dyn DriverTickReceiver>,
            0,
        )
        .expect("register");

    // At the driver's 250ms a group has ticked once or twice in 600ms; at a
    // 1ms busy loop it would be in the hundreds.
    std::thread::sleep(Duration::from_millis(600));
    let ticks = counter.count();
    assert!(
        ticks <= 20,
        "a zero interval took a busy loop rather than the 250ms default: {ticks} ticks in 600ms"
    );
}

#[test]
fn a_driver_needs_a_worker_and_a_tick() {
    assert!(
        Driver::start(options(0, 10)).is_err(),
        "a driver with no workers is not a driver"
    );
    assert!(
        Driver::start(options(1, 0)).is_err(),
        "a zero tick interval would be a busy loop"
    );
}

#[test]
fn stopping_a_driver_stops_its_ticker() {
    let mut driver = Driver::start(options(1, 2)).expect("driver");
    let counter = Arc::new(TickCounter::default());
    driver
        .register_group(
            DriverGroupKey::new(11, 1),
            Arc::clone(&counter) as Arc<dyn DriverTickReceiver>,
        )
        .expect("register");
    assert!(
        wait_until("the group ticks", Duration::from_secs(20), || counter
            .count()
            >= 2),
        "the group never ticked"
    );

    driver.stop();
    let after_stop = counter.count();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        counter.count(),
        after_stop,
        "a stopped driver must not keep ticking"
    );
    // Idempotent, because Drop calls it again.
    driver.stop();
}

// ---------------------------------------------------------------------------
// The worker half: many groups' mail on a fixed pool of threads.
// ---------------------------------------------------------------------------

/// Records the mail it is handed, so a test can ask what arrived and from where.
#[derive(Debug, Default)]
struct MailRecorder {
    batches: AtomicU64,
    mails: Mutex<Vec<u64>>,
}

impl MailRecorder {
    fn received(&self) -> Vec<u64> {
        self.mails.lock().expect("recorder mutex").clone()
    }
    fn count(&self) -> usize {
        self.mails.lock().expect("recorder mutex").len()
    }
    fn batch_count(&self) -> u64 {
        self.batches.load(Ordering::Relaxed)
    }
}

impl DriverMailHandler<u64> for MailRecorder {
    fn handle_mail(&self, mails: Vec<u64>) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.mails.lock().expect("recorder mutex").extend(mails);
    }
}

#[test]
fn one_pool_carries_every_groups_mail() {
    let pool: DriverWorkerPool<u64> = DriverWorkerPool::start(DriverOptions {
        worker_num: 2,
        max_messages_each_poll: 8,
        ..DriverOptions::default()
    })
    .expect("pool");

    let groups = 256_u64;
    let recorders: Vec<Arc<MailRecorder>> = (0..groups)
        .map(|_| Arc::new(MailRecorder::default()))
        .collect();
    for (index, recorder) in recorders.iter().enumerate() {
        pool.register_group(
            DriverGroupKey::new(index as u64 + 1, 1),
            Arc::clone(recorder) as Arc<dyn DriverMailHandler<u64>>,
        )
        .expect("register");
    }
    assert_eq!(pool.group_count(), groups as usize);
    assert_eq!(
        pool.thread_count(),
        2,
        "two workers, whatever the group count"
    );

    for index in 0..groups {
        pool.send(
            DriverGroupKey::new(index + 1, 1),
            MailPriority::Normal,
            index + 1,
        )
        .expect("send");
    }

    assert!(
        wait_until("every group gets its mail", Duration::from_secs(30), || {
            recorders.iter().all(|recorder| recorder.count() >= 1)
        }),
        "two workers must carry all {groups} groups; {} had mail",
        recorders.iter().filter(|r| r.count() >= 1).count()
    );

    let stats = pool.stats();
    assert_eq!(
        stats.mails_handled, groups,
        "every mail sent should be handled exactly once"
    );
    assert_eq!(stats.mails_refused, 0);
}

#[test]
fn mail_reaches_the_group_it_was_addressed_to() {
    // The pool gives each group its own slot because two groups can share a
    // node id. If that routing were wrong this is what would catch it.
    let pool: DriverWorkerPool<u64> = DriverWorkerPool::start(DriverOptions {
        worker_num: 3,
        ..DriverOptions::default()
    })
    .expect("pool");

    // Same node id in every group, deliberately.
    let keys: Vec<DriverGroupKey> = (1..=8).map(|g| DriverGroupKey::new(g, 1)).collect();
    let recorders: Vec<Arc<MailRecorder>> = keys
        .iter()
        .map(|key| {
            let recorder = Arc::new(MailRecorder::default());
            pool.register_group(
                *key,
                Arc::clone(&recorder) as Arc<dyn DriverMailHandler<u64>>,
            )
            .expect("register");
            recorder
        })
        .collect();

    // Group g gets exactly the mails g*1000 + 0..20.
    for (index, key) in keys.iter().enumerate() {
        let base = (index as u64 + 1) * 1000;
        for n in 0..20 {
            pool.send(*key, MailPriority::Normal, base + n)
                .expect("send");
        }
    }

    assert!(
        wait_until("all mail arrives", Duration::from_secs(30), || {
            recorders.iter().all(|recorder| recorder.count() == 20)
        }),
        "counts were {:?}",
        recorders.iter().map(|r| r.count()).collect::<Vec<_>>()
    );

    for (index, recorder) in recorders.iter().enumerate() {
        let base = (index as u64 + 1) * 1000;
        let mut got = recorder.received();
        got.sort_unstable();
        let want: Vec<u64> = (0..20).map(|n| base + n).collect();
        assert_eq!(
            got,
            want,
            "group {} got mail addressed elsewhere",
            index as u64 + 1
        );
    }
}

#[test]
fn a_group_at_its_queue_depth_refuses_mail() {
    // A queue bound is what stops one stuck group from growing without limit.
    // No workers would mean nothing drains, but a pool needs at least one, so
    // the handler blocks instead.
    let pool: DriverWorkerPool<u64> = DriverWorkerPool::start(DriverOptions {
        worker_num: 1,
        max_queue_depth: 4,
        ..DriverOptions::default()
    })
    .expect("pool");

    let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    struct Blocker(Arc<(Mutex<bool>, std::sync::Condvar)>);
    impl DriverMailHandler<u64> for Blocker {
        fn handle_mail(&self, _mails: Vec<u64>) {
            // Bounded, so a failing assertion below cannot leave this worker
            // parked forever and turn a test failure into a hung suite.
            let (lock, cv) = &*self.0;
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
        }
    }

    let key = DriverGroupKey::new(1, 1);
    pool.register_group(key, Arc::new(Blocker(Arc::clone(&gate))))
        .expect("register");

    // Fill past the depth. The first send may be taken by the worker before the
    // rest queue, so refuse is asserted over the run and not on one call.
    let mut refused = 0;
    for n in 0..64 {
        if pool.send(key, MailPriority::Normal, n).is_err() {
            refused += 1;
        }
    }
    assert!(
        refused > 0,
        "a depth of 4 must refuse some of 64 sends; it refused none"
    );
    assert_eq!(pool.stats().mails_refused, refused);

    // Let the handler go so the pool can shut down.
    {
        let (lock, cv) = &*gate;
        *lock.lock().expect("gate mutex") = true;
        cv.notify_all();
    }
}

#[test]
fn a_cancelled_group_takes_no_more_mail() {
    let pool: DriverWorkerPool<u64> =
        DriverWorkerPool::start(DriverOptions::default()).expect("pool");
    let key = DriverGroupKey::new(5, 1);
    let recorder = Arc::new(MailRecorder::default());
    pool.register_group(
        key,
        Arc::clone(&recorder) as Arc<dyn DriverMailHandler<u64>>,
    )
    .expect("register");
    pool.send(key, MailPriority::Normal, 1).expect("send");
    assert!(
        wait_until("the mail arrives", Duration::from_secs(20), || recorder
            .count()
            >= 1),
        "mail never arrived, so cancelling proves nothing"
    );

    assert!(pool.cancel_group(key));
    assert!(!pool.cancel_group(key));
    assert_eq!(pool.group_count(), 0);
    assert!(
        pool.send(key, MailPriority::Normal, 2).is_err(),
        "a cancelled group must not accept mail"
    );
}

#[test]
fn stopping_the_pool_joins_its_workers() {
    // Workers block on the selector rather than polling a timeout, so stop has
    // to wake them. If the sentinel were wrong this test would hang, not fail.
    let mut pool: DriverWorkerPool<u64> = DriverWorkerPool::start(DriverOptions {
        worker_num: 4,
        ..DriverOptions::default()
    })
    .expect("pool");
    let recorder = Arc::new(MailRecorder::default());
    let key = DriverGroupKey::new(1, 1);
    pool.register_group(
        key,
        Arc::clone(&recorder) as Arc<dyn DriverMailHandler<u64>>,
    )
    .expect("register");
    pool.send(key, MailPriority::Normal, 42).expect("send");
    assert!(
        wait_until("mail arrives", Duration::from_secs(20), || recorder.count()
            >= 1),
        "mail never arrived"
    );

    pool.stop();
    pool.stop(); // idempotent, because Drop calls it too
    assert!(recorder.batch_count() >= 1);
}

#[test]
fn a_pool_batches_rather_than_handing_over_one_at_a_time() {
    // max_messages_each_poll is a batch size; a pool that handed mail over one
    // at a time would do the same work with far more handoffs.
    let pool: DriverWorkerPool<u64> = DriverWorkerPool::start(DriverOptions {
        worker_num: 1,
        max_messages_each_poll: 32,
        max_queue_depth: 8192,
        ..DriverOptions::default()
    })
    .expect("pool");
    let key = DriverGroupKey::new(1, 1);
    let recorder = Arc::new(MailRecorder::default());
    pool.register_group(
        key,
        Arc::clone(&recorder) as Arc<dyn DriverMailHandler<u64>>,
    )
    .expect("register");

    let sent = 2000_u64;
    for n in 0..sent {
        pool.send(key, MailPriority::Normal, n).expect("send");
    }
    assert!(
        wait_until("all mail arrives", Duration::from_secs(30), || {
            recorder.count() as u64 >= sent
        }),
        "only {} of {sent} arrived",
        recorder.count()
    );

    let batches = recorder.batch_count();
    assert!(
        batches < sent,
        "mail should arrive in batches, not one call per mail: {batches} batches for {sent} mails"
    );
}

/// Remembers how big each batch was, which is what a byte budget changes.
#[derive(Debug, Default)]
struct BatchRecorder {
    batches: Mutex<Vec<usize>>,
}

impl BatchRecorder {
    fn total(&self) -> usize {
        self.batches.lock().expect("recorder mutex").iter().sum()
    }
    fn largest_batch(&self) -> usize {
        self.batches
            .lock()
            .expect("recorder mutex")
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
    }
}

impl DriverMailHandler<u64> for BatchRecorder {
    fn handle_mail(&self, mails: Vec<u64>) {
        self.batches
            .lock()
            .expect("recorder mutex")
            .push(mails.len());
    }
}

#[test]
fn a_byte_budget_splits_a_batch_and_a_count_bound_alone_does_not() {
    // `driver_batch_bytes` shipped inert because the pool cannot know how big a
    // Mail is. A host that says gets the budget; one that does not gets the
    // count bound, and `start` is explicit about which.
    let options = DriverOptions {
        worker_num: 1,
        max_messages_each_poll: 64,
        max_queue_depth: 8192,
        driver_batch_bytes: 40, // four mails of ten bytes
        ..DriverOptions::default()
    };
    let key = DriverGroupKey::new(1, 1);
    let sent = 64_u64;

    // With a size function: every batch must respect the budget.
    let sized: DriverWorkerPool<u64> =
        DriverWorkerPool::start_with_mail_size(options, Arc::new(|_mail: &u64| 10)).expect("pool");
    let sized_recorder = Arc::new(BatchRecorder::default());
    sized
        .register_group(
            key,
            Arc::clone(&sized_recorder) as Arc<dyn DriverMailHandler<u64>>,
        )
        .expect("register");
    for n in 0..sent {
        sized.send(key, MailPriority::Normal, n).expect("send");
    }
    assert!(
        wait_until("sized mail arrives", Duration::from_secs(30), || {
            sized_recorder.total() >= sent as usize
        }),
        "only {} of {sent} arrived",
        sized_recorder.total()
    );
    let largest = sized_recorder.largest_batch();
    assert!(
        largest <= 4,
        "a 40 byte budget over 10 byte mails must not hand over more than 4 at once; \
         the largest batch was {largest}"
    );

    // Without one: the same options, and the budget cannot apply.
    let unsized_pool: DriverWorkerPool<u64> = DriverWorkerPool::start(options).expect("pool");
    let unsized_recorder = Arc::new(BatchRecorder::default());
    unsized_pool
        .register_group(
            key,
            Arc::clone(&unsized_recorder) as Arc<dyn DriverMailHandler<u64>>,
        )
        .expect("register");
    for n in 0..sent {
        unsized_pool
            .send(key, MailPriority::Normal, n)
            .expect("send");
    }
    assert!(
        wait_until("unsized mail arrives", Duration::from_secs(30), || {
            unsized_recorder.total() >= sent as usize
        }),
        "only {} of {sent} arrived",
        unsized_recorder.total()
    );
    assert!(
        unsized_recorder.largest_batch() > 4,
        "with no size function the byte budget cannot apply, so batches should \
         exceed it; the largest was {}",
        unsized_recorder.largest_batch()
    );
}

#[test]
fn a_mail_larger_than_the_whole_budget_still_arrives() {
    // A budget that a single mail cannot fit under must not strand that mail.
    let options = DriverOptions {
        worker_num: 1,
        driver_batch_bytes: 8,
        ..DriverOptions::default()
    };
    let pool: DriverWorkerPool<u64> =
        DriverWorkerPool::start_with_mail_size(options, Arc::new(|_mail: &u64| 4096))
            .expect("pool");
    let key = DriverGroupKey::new(1, 1);
    let recorder = Arc::new(BatchRecorder::default());
    pool.register_group(
        key,
        Arc::clone(&recorder) as Arc<dyn DriverMailHandler<u64>>,
    )
    .expect("register");
    for n in 0..5 {
        pool.send(key, MailPriority::Normal, n).expect("send");
    }
    assert!(
        wait_until("oversized mail arrives", Duration::from_secs(30), || {
            recorder.total() >= 5
        }),
        "an oversized mail was stranded: {} of 5 arrived",
        recorder.total()
    );
    assert_eq!(
        recorder.largest_batch(),
        1,
        "each mail is over the whole budget, so each should go on its own"
    );
}
