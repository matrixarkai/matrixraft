// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What does the worker pool allocate to deliver one batch of a group's mail?
//!
//! `examples/mail_cycle_allocations.rs` measures the channel on its own. This
//! measures the path a hosted group's mail actually takes: a send into the
//! pool, a worker selecting the channel, draining it, and handing the mail to
//! the group's handler.

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
struct Counter {
    mails: AtomicU64,
}

impl DriverMailHandler<u64> for Counter {
    fn handle_mail(&self, mails: Vec<u64>) {
        self.mails.fetch_add(mails.len() as u64, Ordering::Relaxed);
    }
}

fn main() {
    let per_send: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(8);
    let rounds: u64 = 20_000;

    let pool = DriverWorkerPool::<u64>::start(DriverOptions {
        worker_num: 1,
        // Deep enough that the probe is not measuring refusals: the sender runs
        // ahead of the worker on purpose, and a refused mail is never handled.
        max_queue_depth: 1 << 20,
        ..DriverOptions::default()
    })
    .expect("pool starts");
    let handler = Arc::new(Counter::default());
    let key = DriverGroupKey::new(1, 1);
    pool.register_group(key, Arc::clone(&handler) as Arc<dyn DriverMailHandler<u64>>)
        .expect("group registers");

    let send_round = |round: u64| {
        for mail in 0..per_send {
            // Asserted rather than ignored: a refused mail is never handled, so
            // a silent refusal would show up as a hang in `wait_for` instead of
            // as the queue being too shallow for this probe.
            pool.send(key, MailPriority::Normal, round * per_send + mail)
                .expect("the queue is deep enough for this probe");
        }
    };
    let wait_for = |want: u64| {
        let deadline = Instant::now() + Duration::from_secs(30);
        while handler.mails.load(Ordering::Relaxed) < want {
            if Instant::now() > deadline {
                panic!(
                    "only {} of {want} mails were handled",
                    handler.mails.load(Ordering::Relaxed)
                );
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    };

    // Warm up, so what is measured is the steady state rather than the growth
    // of buffers that then stay put.
    for round in 0..1_000 {
        send_round(round);
    }
    wait_for(1_000 * per_send);

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for round in 1_000..(1_000 + rounds) {
        send_round(round);
    }
    wait_for((1_000 + rounds) * per_send);
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;

    println!("mails per send round: {per_send}");
    println!("rounds:               {rounds}");
    println!(
        "allocations per round: {:.4}",
        allocations as f64 / rounds as f64
    );
    println!(
        "allocations per mail:  {:.4}",
        allocations as f64 / (rounds * per_send) as f64
    );
}
