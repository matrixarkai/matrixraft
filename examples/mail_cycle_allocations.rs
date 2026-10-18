// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What does one send-and-fetch cycle allocate on a group's mail channel?
//!
//! Two things used to grow with the work. `consume_checked` moved the queued
//! mail out by swapping three fresh deques in, which left the channel at zero
//! capacity, so the next send had to grow the buffer again -- every cycle, for
//! every group on every worker. And the vector `fetch` returns was built with
//! `Vec::new()` and filled from three staging deques in turn, so it reallocated
//! once per non-empty priority.
//!
//! The shape of a cycle is given per priority -- urgent, normal, slowly --
//! because spreading the same mail across priorities is what exposes the second
//! one.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{ChannelSelector, MailChannel, MailPriority};

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
    let cycles: u64 = 20_000;
    let warmup: u64 = 1_000;
    println!("{:>24}  {:>18}", "cycle shape", "allocations/cycle");

    for (label, per_priority) in [
        ("1 normal", [0u64, 1, 0]),
        ("64 normal", [0, 64, 0]),
        ("1 of each priority", [1, 1, 1]),
        ("16 of each priority", [16, 16, 16]),
    ] {
        let selector = ChannelSelector::<u64>::new();
        let channel = MailChannel::<u64>::new(1, 1_000_000);
        let priorities = [
            MailPriority::Urgent,
            MailPriority::Normal,
            MailPriority::Slowly,
        ];
        let mut drained = 0u64;

        let cycle = || {
            for (priority, count) in priorities.iter().zip(per_priority.iter()) {
                for mail in 0..*count {
                    channel.send(*priority, mail);
                }
            }
            channel.fetch(&selector).len() as u64
        };

        // Warm up, so the figure is the steady state rather than the first
        // growth of a buffer that then stays put.
        for _ in 0..warmup {
            drained += cycle();
        }
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        for _ in 0..cycles {
            drained += cycle();
        }
        let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;

        let total: u64 = per_priority.iter().sum();
        assert_eq!(
            drained,
            total * (cycles + warmup),
            "every mail sent should come back out"
        );
        println!("{label:>24}  {:>18.4}", allocations as f64 / cycles as f64);
    }
}
