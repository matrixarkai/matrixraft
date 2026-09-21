// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The driver ticks many groups from one thread.
//!
//! Every wait here polls to a deadline rather than sleeping a fixed time. A
//! fixed sleep turns a loaded machine into a red suite, which is how
//! `a_busy_node_still_ticks` came to demand a rate no runner could supply.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use matrixraft::{Driver, DriverGroupKey, DriverOptions, DriverTickReceiver};

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
