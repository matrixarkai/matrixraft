// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What does one mailbox fetch allocate?
//!
//! A mailbox hands a group's tasks over in priority order, bounded by the
//! poll limit. The output vector's size is decided by the lanes and the limit
//! before anything moves, so a fetch should ask the allocator once however
//! many mails it carries.
//!
//! Run as `mailbox_fetch_allocations <mails per fetch>`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use matrixraft::{MailBox, MailBoxFetchPolicy, MailPriority};

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

fn main() {
    let per_fetch: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(64);
    let rounds: usize = 20_000;

    let mailbox = MailBox::<u64>::new(1 << 20);
    let policy = MailBoxFetchPolicy {
        limit: per_fetch,
        timeout_ms: 0,
        include_until: MailPriority::Urgent,
    };

    let mut fetched = 0_usize;
    // Warm up: the lanes reach their steady capacity and stay there, so what
    // is counted below is the fetch and not the queues growing.
    for round in 0..1_000 {
        for mail in 0..per_fetch {
            mailbox.send(MailPriority::Normal, (round * per_fetch + mail) as u64);
        }
        fetched += mailbox.fetch(policy).len();
    }

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let started = Instant::now();
    for round in 0..rounds {
        for mail in 0..per_fetch {
            mailbox.send(MailPriority::Normal, (round * per_fetch + mail) as u64);
        }
        fetched += mailbox.fetch(policy).len();
    }
    let elapsed = started.elapsed();
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;

    println!("mails per fetch:        {per_fetch}");
    println!("fetches:                {rounds}");
    println!("mails fetched:          {fetched}");
    println!(
        "allocations per fetch:  {:.4}",
        allocations as f64 / rounds as f64
    );
    // The send loop is inside the timed region, so this is a round and not a
    // fetch in isolation. Both arms pay the same sends, which is what makes the
    // comparison between them mean something; the absolute figure does not
    // describe one fetch on its own.
    println!(
        "ns per round:           {:.1}",
        elapsed.as_nanos() as f64 / rounds as f64
    );
}
