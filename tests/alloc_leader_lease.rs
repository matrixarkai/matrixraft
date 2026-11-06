// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Renewing a leader lease retains no heap scratch.
//!
//! Lease checks sit on the read path. A renewal used to build a vector of
//! every voting peer's expiry, sort it, and then allocate a diagnostic reason
//! even when the caller requested only a boolean. The work grew with group
//! membership and repeated on every expired lease.
//!
//! At 33 voters the measured cost moved from 6.00 allocations per renewal to
//! 0.00. The guard checks both three voters and 33 so the common stack path and
//! the large-membership path must remain allocation-free.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{LeaderLease, LeasePeer, ReplicaRole};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = TRACK_ALLOCATIONS.try_with(|tracking| {
            if tracking.get() {
                ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn lease_with_voters(voters: u64) -> LeaderLease {
    let mut lease = LeaderLease::new(1, 50);
    lease.update_members((1..=voters).map(|node_id| LeasePeer {
        node_id,
        role: ReplicaRole::Voter,
    }));
    lease.reset(1);
    lease
}

fn renewal_allocations(voters: u64) -> usize {
    let mut lease = lease_with_voters(voters);
    let rounds = 200u64;
    let last_confirming_peer = voters / 2 + 1;

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    for round in 1..=rounds {
        let now_ms = round * 100;
        for node_id in 2..=last_confirming_peer {
            assert!(lease.on_recv_lease_confirm(1, node_id, now_ms, 50));
        }
        assert!(lease.in_lease(1, now_ms));
        assert_eq!(lease.last_active_lease_end_ms(), Some(now_ms + 50));
    }
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    ALLOCATIONS.load(Ordering::Relaxed)
}

#[test]
fn lease_renewal_allocates_nothing_after_setup() {
    for voters in [3, 33] {
        let allocations = renewal_allocations(voters);
        let per_renewal = allocations as f64 / 200.0;
        println!("allocations per {voters}-member lease renewal: {per_renewal:.2}");
        assert_eq!(
            allocations, 0,
            "a warmed {voters}-member lease renewal should retain no heap scratch, got {per_renewal:.2} allocations per renewal"
        );
    }
}
