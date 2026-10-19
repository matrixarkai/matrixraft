// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A mailbox fetch asks the allocator once.
//!
//! The vector a fetch returns started empty and grew by doubling, which is an
//! allocation per power of two for a size that is decided before anything
//! moves: the lanes hold what they hold and the poll limit says how much of it
//! goes. On main a fetch of 8 mails allocated twice, 64 five times and 512
//! eight times.
//!
//! One test, in a file of its own, because the counter is the process
//! allocator: a second test in the same binary would be allocating at the same
//! time and neither figure would belong to either test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Allocations per fetch, and the mails that came back out.
///
/// `per_priority` is urgent, normal and slowly in that order, because the shape
/// matters: the output is filled from one lane after another, so an unsized
/// vector reallocates once per non-empty lane on top of its own growth.
fn measure(per_priority: [usize; 3], fetches: usize) -> (f64, usize) {
    let mailbox = MailBox::<u64>::new(1 << 20);
    let priorities = [
        MailPriority::Urgent,
        MailPriority::Normal,
        MailPriority::Slowly,
    ];
    let total: usize = per_priority.iter().sum();
    let policy = MailBoxFetchPolicy {
        limit: total.max(1),
        timeout_ms: 0,
        include_until: MailPriority::Urgent,
    };
    let mut drained = 0usize;

    let round = || {
        for (priority, count) in priorities.iter().zip(per_priority.iter()) {
            for mail in 0..*count {
                mailbox.send(*priority, mail as u64);
            }
        }
        mailbox.fetch(policy).len()
    };

    // Warm up, so what is measured is the steady state rather than the first
    // growth of lanes that then stay put.
    for _ in 0..1_000 {
        drained += round();
    }
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..fetches {
        drained += round();
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    (allocations as f64 / fetches as f64, drained)
}

#[test]
fn a_mailbox_fetch_allocates_once_however_many_mails_it_carries() {
    // Fewer rounds than the mail-cycle test needs: this figure has no variance
    // at all -- five runs an arm read exactly 1.0000 and exactly 8.0000 -- so
    // the rounds are here to amortise the warm-up, not to average anything.
    let fetches = 5_000;

    // The control first. A fetch has to allocate something -- it returns a
    // vector -- so a figure of zero would mean the measurement is not running,
    // not that the fetch became free.
    let (one, drained_one) = measure([0, 1, 0], fetches);
    assert_eq!(
        drained_one,
        fetches + 1_000,
        "every mail sent should come back out"
    );
    assert!(
        one > 0.0,
        "a fetch allocates the vector it returns, so zero means nothing was measured"
    );

    // The claim: one allocation a fetch, whatever the batch and however it is
    // spread across the lanes. On main these four shapes read 1.0, 2.0, 5.0 and
    // 8.0 -- the first is already one, which is why the larger shapes are here.
    //
    // The mixed shapes hold the part a single-lane test would miss: the output
    // is filled from three lanes in turn, so a vector sized from only the first
    // lane would still reallocate.
    for per_priority in [
        [0usize, 1, 0],
        [0, 8, 0],
        [0, 512, 0],
        [8, 8, 8],
        [170, 171, 171],
    ] {
        let (per_fetch, drained) = measure(per_priority, fetches);
        let total: usize = per_priority.iter().sum();
        assert_eq!(
            drained,
            total * (fetches + 1_000),
            "every mail sent should come back out at {per_priority:?} per fetch"
        );
        assert!(
            per_fetch <= 1.05,
            "{per_priority:?} mails a fetch allocated {per_fetch:.4} times, so the returned vector is growing again"
        );
    }
}
