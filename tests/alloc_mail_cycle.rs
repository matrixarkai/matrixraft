// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A group's mail channel keeps the buffer it has earned.
//!
//! `consume_checked` used to move the queued mail out by swapping three fresh
//! deques in, which left the channel at zero capacity. The next send grew the
//! buffer again, so a cycle of 64 mails allocated six times and a cycle of one
//! allocated twice -- every cycle, for every group on every worker.
//!
//! One test, in a file of its own, because the counter is the process
//! allocator: a second test in the same binary would be allocating at the same
//! time and neither figure would belong to either test.

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

/// Allocations per send-and-fetch cycle, and the mails that came back out.
///
/// `per_priority` is urgent, normal and slowly in that order, because the shape
/// matters: the staging area is one deque per priority, and the vector `fetch`
/// returns is filled from each of them in turn.
fn measure(per_priority: [u64; 3], cycles: u64) -> (f64, u64) {
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

    // Warm up, so what is measured is the steady state rather than the first
    // growth of a buffer that then stays put.
    for _ in 0..1_000 {
        drained += cycle();
    }
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..cycles {
        drained += cycle();
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    (allocations as f64 / cycles as f64, drained)
}

#[test]
fn a_mail_cycle_allocates_once_however_many_mails_it_carries() {
    let cycles = 20_000;

    // The control first. A cycle has to allocate something -- `fetch` returns a
    // vector -- so a figure of zero would mean the measurement is not running,
    // not that the channel became free.
    let (one, drained_one) = measure([0, 1, 0], cycles);
    assert_eq!(
        drained_one,
        cycles + 1_000,
        "every mail sent should come back out"
    );
    assert!(
        one > 0.0,
        "a cycle allocates the vector it returns, so zero means nothing was measured"
    );

    // The claim: one allocation a cycle, whatever the batch and however it is
    // spread across the priorities. Both of those used to grow it -- on main the
    // four shapes below read 2.0, 6.0, 4.0 and 12.0.
    //
    // The mixed shapes are what hold the sizing of the returned vector: it is
    // filled from three staging deques in turn, so an unsized vector
    // reallocates once per non-empty priority. Without a mixed shape that half
    // would be untested, and a single-priority version of this test stayed
    // green when the sizing was removed.
    for per_priority in [[0u64, 1, 0], [0, 64, 0], [1, 1, 1], [16, 16, 16]] {
        let (per_cycle, drained) = measure(per_priority, cycles);
        let total: u64 = per_priority.iter().sum();
        assert_eq!(
            drained,
            total * (cycles + 1_000),
            "every mail sent should come back out at {per_priority:?} per cycle"
        );
        assert!(
            per_cycle <= 1.05,
            "{per_priority:?} mails a cycle allocated {per_cycle:.4} times, so either the channel is growing its buffer again or the returned vector is"
        );
    }
}
