// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What does a host save by handing a group its mail in one call?
//!
//! Both arms are in this one binary: the same mails, to the same group, sent
//! one at a time and then as a batch. Same process, same build, alternating
//! order, so there is no second tree to get wrong -- what differs between the
//! two readings is the call and nothing else.
//!
//! Run as `pool_batch_send <mails per round>`.

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

struct Reading {
    ns_per_mail: f64,
    allocations_per_mail: f64,
}

fn main() {
    let per_round: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(8);
    let rounds: u64 = 20_000;

    let pool = DriverWorkerPool::<u64>::start(DriverOptions {
        worker_num: 1,
        // Deep enough that the probe is not measuring refusals.
        max_queue_depth: 1 << 20,
        ..DriverOptions::default()
    })
    .expect("pool starts");
    let handler = Arc::new(Counter::default());
    let key = DriverGroupKey::new(1, 1);
    pool.register_group(key, Arc::clone(&handler) as Arc<dyn DriverMailHandler<u64>>)
        .expect("group registers");

    let handled = || handler.mails.load(Ordering::Relaxed);
    let wait_for = |want: u64| {
        let deadline = Instant::now() + Duration::from_secs(60);
        while handled() < want {
            if Instant::now() > deadline {
                panic!("only {} of {want} mails were handled", handled());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    };

    let one_at_a_time = |rounds: u64| {
        for round in 0..rounds {
            for mail in 0..per_round {
                pool.send(key, MailPriority::Normal, round * per_round + mail)
                    .expect("the queue is deep enough for this probe");
            }
        }
    };
    let in_one_call = |rounds: u64| {
        for round in 0..rounds {
            let batch: Vec<u64> = (0..per_round)
                .map(|mail| round * per_round + mail)
                .collect();
            pool.send_many(key, MailPriority::Normal, batch)
                .expect("the queue is deep enough for this probe");
        }
    };

    let measure = |label: &str, send: &dyn Fn(u64)| -> Reading {
        // Warm up, so what is measured is the steady state rather than the
        // growth of buffers that then stay put.
        send(1_000);
        wait_for(handled());
        let before_alloc = ALLOCATIONS.load(Ordering::Relaxed);
        let before_handled = handled();
        let started = Instant::now();
        send(rounds);
        let elapsed = started.elapsed();
        let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before_alloc;
        // Drained before the next arm, so one arm's backlog is not the other
        // arm's starting condition.
        wait_for(before_handled + rounds * per_round);
        let mails = (rounds * per_round) as f64;
        let reading = Reading {
            ns_per_mail: elapsed.as_nanos() as f64 / mails,
            allocations_per_mail: allocations as f64 / mails,
        };
        println!(
            "  {label:<14} {:>8.1} ns/mail   {:>7.4} allocations/mail",
            reading.ns_per_mail, reading.allocations_per_mail
        );
        reading
    };

    println!("mails per round: {per_round}, rounds: {rounds}");
    // Alternating, and the second pass is the one quoted: the first pass of
    // either arm pays for whatever the other arm left cold.
    println!("pass 1");
    measure("send", &one_at_a_time);
    measure("send_many", &in_one_call);
    println!("pass 2");
    let single = measure("send", &one_at_a_time);
    let batched = measure("send_many", &in_one_call);

    println!(
        "pass 2 ratio:    {:.2}x the time, {:.2}x the allocations",
        single.ns_per_mail / batched.ns_per_mail,
        single.allocations_per_mail / batched.allocations_per_mail.max(f64::MIN_POSITIVE)
    );
    println!("mails handled:   {}", handled());
}
