// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Byte-bounded worker delivery reuses both sides of the split.
//!
//! The worker first gathers a channel drain, then cuts it into byte-sized
//! handler calls. Both vectors live with the worker and a recycling handler
//! returns the smaller one, so a warmed drain spanning several calls needs no
//! fresh storage.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
struct GatedRecyclingCounter {
    handled: AtomicU64,
    blocked: AtomicBool,
    release: AtomicBool,
}

impl DriverMailHandler<u64> for GatedRecyclingCounter {
    fn handle_mail(&self, mails: Vec<u64>) {
        self.handled
            .fetch_add(mails.len() as u64, Ordering::Relaxed);
    }

    fn handle_mail_recycling(&self, mut mails: Vec<u64>) -> Vec<u64> {
        let count = mails.len() as u64;
        if count == 1 && !self.release.load(Ordering::Acquire) {
            self.blocked.store(true, Ordering::Release);
            while !self.release.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
        }
        mails.clear();
        self.handled.fetch_add(count, Ordering::Release);
        mails
    }
}

fn wait_until(mut condition: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !condition() {
        assert!(Instant::now() < deadline, "{message}");
        std::thread::yield_now();
    }
}

fn deliver_split_round(
    pool: &DriverWorkerPool<u64>,
    handler: &GatedRecyclingCounter,
    key: DriverGroupKey,
    handled_before: u64,
) {
    handler.blocked.store(false, Ordering::Relaxed);
    handler.release.store(false, Ordering::Release);
    pool.send(key, MailPriority::Normal, 0).expect("gate send");
    wait_until(
        || handler.blocked.load(Ordering::Acquire),
        "worker did not enter the gate",
    );
    for mail in 1..=8 {
        pool.send(key, MailPriority::Normal, mail)
            .expect("batched send");
    }
    handler.release.store(true, Ordering::Release);
    wait_until(
        || handler.handled.load(Ordering::Acquire) == handled_before + 9,
        "worker did not finish the split delivery",
    );
}

#[test]
fn byte_bounded_recycling_allocates_nothing_after_warmup() {
    let rounds = 2_000u64;
    let key = DriverGroupKey::new(1, 1);
    let pool = DriverWorkerPool::<u64>::start_with_mail_size(
        DriverOptions {
            worker_num: 1,
            max_messages_each_poll: 1,
            max_queue_depth: 16,
            driver_batch_bytes: 4,
            ..DriverOptions::default()
        },
        Arc::new(|_mail: &u64| 1),
    )
    .expect("pool");
    let handler = Arc::new(GatedRecyclingCounter::default());
    pool.register_group(key, Arc::clone(&handler) as Arc<dyn DriverMailHandler<u64>>)
        .expect("register");

    for round in 0..100 {
        deliver_split_round(&pool, &handler, key, round * 9);
    }

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for round in 0..rounds {
        deliver_split_round(&pool, &handler, key, 900 + round * 9);
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    let per_round = allocations as f64 / rounds as f64;
    println!("allocations per byte-bounded recycled drain: {per_round:.4}");
    assert_eq!(
        allocations, 0,
        "warmed byte-bounded delivery should retain both worker buffers"
    );
}
