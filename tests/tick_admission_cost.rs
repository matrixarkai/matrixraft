// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `TickBackpressure` is not wired to anything, and wiring it as written would
//! cost an allocation on every tick.
//!
//! It is implemented in `src/node.rs`, exported from `lib.rs`, and exercised by
//! `tests/node_runtime.rs` — and its only callers are those tests. The runtime
//! bounds pending ticks by a different route: `PooledGroup::ticks_due` is an
//! `AtomicU64` that `try_run_tick` clears with `swap(0)`, so every tick that
//! arrived while the group was busy coalesces into one and the pending count is
//! bounded at one by construction rather than by a limit. What came of each is
//! counted as `ticks_in_place`, `ticks_handed_over`, `ticks_fired`,
//! `ticks_dropped` and `ticks_skipped`.
//!
//! So this is a second design for a problem the runtime already solves. That is
//! worth a test rather than a comment, because the trap is specific: `admit_tick`
//! returns a `TickAdmission` carrying `reason: String`, which allocates. A hosted
//! group ticking currently allocates **nothing** — `tests/tick_window.rs` holds
//! that at 0.05 a tick — so putting this on the tick path would put a whole
//! allocation back, for a bound that is already enforced.

use matrixraft::TickBackpressure;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

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
static ALLOCATOR: Counting = Counting;

#[test]
fn admitting_a_tick_allocates_so_it_cannot_go_on_the_tick_path_as_written() {
    // The control first: an assertion that a count is non-zero says nothing if
    // the counter never moves at all.
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let proof: Vec<u64> = (0..32).collect();
    std::hint::black_box(&proof);
    assert!(
        ALLOCATIONS.load(Ordering::Relaxed) - before > 0,
        "the allocator counter did not move for a vector this test built, so what \
         it reports below means nothing"
    );
    drop(proof);

    let mut ticks = TickBackpressure::new(1);

    // Warm whatever is lazy, so a one-off is not charged to the measured calls.
    std::hint::black_box(ticks.admit_tick());
    ticks.complete_tick();

    const ROUNDS: usize = 1_000;
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..ROUNDS {
        let admission = ticks.admit_tick();
        std::hint::black_box(&admission);
        ticks.complete_tick();
    }
    let per_call = (ALLOCATIONS.load(Ordering::Relaxed) - before) as f64 / ROUNDS as f64;

    println!();
    println!("  admit_tick + complete_tick: {per_call:.3} allocations a call");
    println!();

    assert!(
        per_call >= 0.9,
        "admit_tick allocated {per_call:.3} times a call. If that is now zero the \
         reason string has stopped being owned, and this file's argument -- that \
         wiring it would cost an allocation a tick -- no longer holds. Check \
         whether the runtime should use it after all, and retarget or delete this."
    );

    // And the rejection path allocates too, which is the one that would fire under
    // the pressure the type exists to report.
    let mut saturated = TickBackpressure::new(1);
    std::hint::black_box(saturated.admit_tick());
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let rejected = saturated.admit_tick();
    let on_rejection = ALLOCATIONS.load(Ordering::Relaxed) - before;
    assert!(!rejected.accepted, "the second tick must be refused");
    assert!(
        on_rejection > 0,
        "the refusal path allocated nothing, so only the accept path carries the \
         cost and the argument above is narrower than stated"
    );
}

#[test]
fn the_runtime_bounds_pending_ticks_by_coalescing_instead() {
    // Not a test of the runtime -- that is `tests/tick_window.rs`'s job. This
    // pins the shape of the type so the two designs can be compared: a LIMIT that
    // refuses, against a swap that collapses. A limit of one refuses the second
    // tick while the first is outstanding; coalescing accepts it and runs one
    // tick for both, which is why the runtime never needed to refuse.
    let mut ticks = TickBackpressure::new(1);
    assert!(ticks.admit_tick().accepted, "the first is admitted");
    let second = ticks.admit_tick();
    assert!(
        !second.accepted,
        "the second is refused while one is pending"
    );
    assert_eq!(
        second.reason, "pending_tick_limit_reached",
        "and it says why, in an owned string"
    );
    assert_eq!(ticks.rejected_ticks, 1);

    assert!(ticks.complete_tick(), "the outstanding tick completes");
    assert!(
        ticks.admit_tick().accepted,
        "and then another is admitted, so the limit is a gate rather than a budget"
    );
}
