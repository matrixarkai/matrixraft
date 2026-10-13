// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The driver allocates nothing to tick, at any group count.
//!
//! It used to allocate once per wake-up: the ticker built a fresh list of what
//! came due, fired it with the lock dropped, and dropped the list. That cost did
//! not scale with the groups, because one wake covers every group due at that
//! millisecond, and the table this file was first written around shows it
//! dividing away:
//!
//! | groups | allocations per tick, when the list was built per wake |
//! |---|---|
//! | 1 | 1.0000 |
//! | 4 | 0.2500 |
//! | 16 | 0.1875 |
//! | 64 | 0.0781 |
//! | 256 | 0.0273 |
//!
//! The ticker now keeps one list for its whole life and lends it to each call,
//! so there is no allocation to divide. Both figures read 0.0000, and this file
//! asserts that of each arm rather than comparing them: a per-wake cost coming
//! back lifts the one-group figure, a per-group cost lifts both, and a ceiling
//! on each catches either. The old shape -- many-group against one-group -- can
//! no longer express it, because zero is not a quarter of zero.
//!
//! It also says what `tests/tick_window.rs` is not. That test runs a single
//! hosted group and reads about 0.25 allocations a tick, which is the per-group
//! cost of a hosted tick: a quarter, because the group runs a four-tick election
//! cycle. It read 1.25 while a whole allocation of it was this per-wake cost.

use matrixraft::{Driver, DriverGroupKey, DriverOptions, DriverTickReceiver};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[derive(Debug, Default)]
struct Counter(AtomicU64);

impl DriverTickReceiver for Counter {
    fn fire_tick(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn measure(groups: u64) -> f64 {
    let counter = Arc::new(Counter::default());
    let driver = Driver::start(DriverOptions {
        worker_num: 1,
        tick_interval_ms: 1,
        ..DriverOptions::default()
    })
    .expect("driver");
    for group_id in 1..=groups {
        driver
            .register_group(
                DriverGroupKey::new(group_id, 1),
                Arc::clone(&counter) as Arc<dyn DriverTickReceiver>,
            )
            .expect("register");
    }

    std::thread::sleep(Duration::from_millis(300));
    let ticks_before = counter.0.load(Ordering::Relaxed);
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(600));
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    let ticks = (counter.0.load(Ordering::Relaxed) - ticks_before) as f64;
    assert!(ticks > 100.0, "only {ticks} ticks");
    let per_tick = allocations as f64 / ticks;
    println!("groups={groups:<6} {allocations} allocations over {ticks:.0} ticks = {per_tick:.4} per tick");
    per_tick
}

#[test]
fn the_driver_allocates_nothing_to_tick() {
    // The control first, and it is not optional: an assertion that a figure is
    // near zero passes just as well when the instrument is broken. Prove the
    // counter moves for an allocation this test makes itself before believing a
    // zero it reports for the driver.
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let proof: Vec<u64> = (0..64).collect();
    std::hint::black_box(&proof);
    let counted = ALLOCATIONS.load(Ordering::Relaxed) - before;
    assert!(
        counted > 0,
        "the allocator counter did not move for a vector this test built, so a \
         zero it reports for the driver means nothing"
    );
    drop(proof);

    // Two group counts rather than the five the table was drawn from, because
    // two tell a per-wake cost from a per-group one and each costs two seconds
    // of wall clock.
    let one = measure(1);
    let many = measure(64);

    // A ceiling on each arm. A list built per wake would put the one-group
    // figure back near 1.0; anything per group would lift both. A twentieth of
    // an allocation is slack for whatever else the process does in the window,
    // not room for either to return.
    const CEILING_PER_TICK: f64 = 0.05;
    assert!(
        one <= CEILING_PER_TICK,
        "one group allocates {one:.4} a tick, above {CEILING_PER_TICK}. The \
         ticker is allocating to tick again -- most likely building a list of \
         what came due instead of reusing the one it is lent."
    );
    assert!(
        many <= CEILING_PER_TICK,
        "64 groups allocate {many:.4} a tick, above {CEILING_PER_TICK}. A cost \
         that shows at 64 groups and not at one is per group, which a store with \
         many groups a shard pays in full."
    );
}
