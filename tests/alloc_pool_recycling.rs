// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A worker can reuse storage returned by a consuming mail handler.
//!
//! Selection and channel-drain buffers already live for the worker's lifetime.
//! The final owned `Vec` was the remaining allocation per non-zero-sized
//! delivery because the handler consumed and dropped it. A recycling handler
//! drains the items and returns that allocation for the next group visit.
//!
//! One test, in a file of its own, because the counter is the process
//! allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use matrixraft::{
    DriverGroupKey, DriverMailHandler, DriverOptions, DriverWorkerPool, MailPriority,
};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

#[derive(Default)]
struct RecyclingCounter {
    handled: AtomicU64,
}

impl DriverMailHandler<u64> for RecyclingCounter {
    fn handle_mail(&self, mails: Vec<u64>) {
        self.handled
            .fetch_add(mails.len() as u64, Ordering::Relaxed);
    }

    fn handle_mail_recycling(&self, mut mails: Vec<u64>) -> Vec<u64> {
        let count = mails.len() as u64;
        mails.clear();
        self.handled.fetch_add(count, Ordering::Relaxed);
        mails
    }
}

fn wait_for(handler: &RecyclingCounter, want: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while handler.handled.load(Ordering::Relaxed) < want {
        assert!(
            Instant::now() < deadline,
            "only {} of {want} mails were handled",
            handler.handled.load(Ordering::Relaxed)
        );
        std::thread::yield_now();
    }
}

#[test]
fn a_recycling_handler_allocates_nothing_after_warmup() {
    let rounds = 20_000u64;
    let key = DriverGroupKey::new(1, 1);
    let pool = DriverWorkerPool::<u64>::start(DriverOptions {
        worker_num: 1,
        max_messages_each_poll: 1,
        max_queue_depth: 8,
        ..DriverOptions::default()
    })
    .expect("pool");
    let handler = Arc::new(RecyclingCounter::default());
    pool.register_group(key, Arc::clone(&handler) as Arc<dyn DriverMailHandler<u64>>)
        .expect("register");

    for done in 1..=1_000 {
        pool.send(key, MailPriority::Normal, done)
            .expect("warm send");
        wait_for(&handler, done);
    }

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for done in 1..=rounds {
        pool.send(key, MailPriority::Normal, done)
            .expect("measured send");
        wait_for(&handler, 1_000 + done);
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    let per_delivery = allocations as f64 / rounds as f64;
    println!("allocations per recycled delivery: {per_delivery:.4}");
    assert_eq!(
        allocations, 0,
        "a warmed recycling handler should retain every delivery buffer"
    );
}
