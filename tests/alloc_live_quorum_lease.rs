// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Renewing a leader lease from the live membership retains no heap scratch.
//!
//! The confirmation maps keep their nodes after warmup, so walking the live
//! quorum should not first materialize every eligible node id.
//!
//! Measured before and after stack chunking:
//!
//! | voters | before | after |
//! |---|---:|---:|
//! | 3 | 1.00 allocations / 32 B | 0.00 / 0 B |
//! | 33 | 5.00 allocations / 992 B | 0.00 / 0 B |

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = TRACK_ALLOCATIONS.try_with(|tracking| {
            if tracking.get() {
                ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
                BYTES.fetch_add(layout.size(), Ordering::Relaxed);
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

fn voter(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 60_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 61_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn renewal_cost(voters: u64, rounds: usize) -> (usize, usize) {
    let mut cluster = RaftCluster::new(17, Config::default(), (1..=voters).map(voter).collect())
        .expect("cluster");
    cluster.start().expect("start");
    assert_eq!(cluster.leader_id(), Some(1));

    for _ in 0..8 {
        assert!(cluster.renew_leader_lease_from_live_quorum());
    }

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    for _ in 0..rounds {
        assert!(cluster.renew_leader_lease_from_live_quorum());
    }
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    (
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

#[test]
fn live_quorum_renewal_allocates_nothing_after_setup() {
    let rounds = 200;
    let mut measured = Vec::new();
    for voters in [3, 33] {
        let (allocations, bytes) = renewal_cost(voters, rounds);
        let per_renewal = allocations as f64 / rounds as f64;
        let bytes_per_renewal = bytes as f64 / rounds as f64;
        println!(
            "{voters} voters: {per_renewal:.2} allocations and {bytes_per_renewal:.1} bytes per renewal"
        );
        measured.push((voters, allocations, per_renewal, bytes_per_renewal));
    }
    for (voters, allocations, per_renewal, bytes_per_renewal) in measured {
        assert_eq!(
            allocations, 0,
            "a warmed {voters}-voter live-quorum renewal should allocate nothing, got {per_renewal:.2} allocations and {bytes_per_renewal:.1} bytes per call"
        );
    }
}
