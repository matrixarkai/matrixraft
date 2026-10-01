// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The driver's own allocation is per wake, not per group.
//!
//! A ticker builds a list of what came due and fires it with the lock dropped, so
//! it allocates once per wake-up however many groups that wake covers. With one
//! group that is one allocation a tick; with many it divides away:
//!
//! | groups | allocations per tick |
//! |---|---|
//! | 1 | 1.0000 |
//! | 4 | 0.2500 |
//! | 16 | 0.1875 |
//! | 64 | 0.0781 |
//! | 256 | 0.0273 |
//!
//! That shape is what this guards. A change making the driver allocate per group
//! rather than per wake would leave the one-group figure alone and lift the
//! many-group one, which is the comparison asserted below rather than either
//! number on its own.
//!
//! It also says what `tests/tick_window.rs` is not. That test runs a single hosted
//! group and reads about 1.25 allocations a tick; a whole allocation of that is
//! this per-wake cost, which a store with many groups a shard does not pay. The
//! per-group cost of a tick is the quarter, not the one and a quarter.

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
fn the_drivers_allocation_is_per_wake_and_not_per_group() {
    // Two points rather than the five the table above was drawn from, because two
    // are enough to tell a per-wake cost from a per-group one and each costs two
    // seconds of wall clock.
    let one = measure(1);
    let many = measure(64);

    // Per group, the many-group run would read the same as the single-group one.
    // Per wake, it reads about a sixteenth of it -- one wake covering the groups
    // that came due together.
    assert!(
        many < one / 4.0,
        "64 groups allocate {many:.4} a tick against one group's {one:.4}. Closer \
         than a quarter means the driver is allocating per group rather than per \
         wake, which is a cost a store with many groups a shard would pay in full."
    );
    // And the control: if neither figure were measuring anything, both would be
    // zero and the comparison above would hold vacuously.
    assert!(
        one > 0.5,
        "one group allocated {one:.4} a tick, so there is nothing here to amortise \
         and the comparison says nothing"
    );
}
