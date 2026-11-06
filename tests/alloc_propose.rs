// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A proposal does not pay an allocation per follower.
//!
//! Replication used to allocate a discarded flush result per follower. After
//! that was removed, proposal admission still built separate vectors for node
//! ids, lease acknowledgements, and commit candidates. The node-id vector now
//! serves as commit scratch, while confirmations enter the fresh lease epoch
//! directly during the replication loop.
//!
//! Measured per proposal, at a 256-byte payload:
//!
//! | peers | before | after |
//! |---|---|---|
//! | 1 | 5.02 | 3.02 |
//! | 3 | 5.06 | 3.06 |
//! | 5 | 6.10 | 3.10 |
//! | 7 | 6.14 | 3.14 |
//! | 9 | 7.18 | 3.18 |
//!
//! The slope guard catches work added per peer. The absolute guard catches a
//! fixed vector rebuilt per proposal, which a slope-only test cannot see.
//!
//! One test, in a file of its own, because the counter is the process
//! allocator: a second test in the same binary would be allocating at the same
//! time and neither figure would belong to either test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

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

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 21_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 22_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// Allocations per proposal in a cluster of `peers` nodes.
///
/// The payload is cloned by this harness for every proposal, so one allocation
/// of the figure is the harness's and is the same in every row -- it cancels
/// out of the slope, which is what is asserted.
fn allocations_per_proposal(peers: usize, proposals: usize) -> f64 {
    let members = (1..=peers as u64).map(peer).collect::<Vec<_>>();
    let mut cluster = RaftCluster::new(7, Config::default(), members).expect("cluster");
    cluster.start().expect("start");
    let payload = vec![7u8; 256];

    // Warm outside the window: the first proposals grow what they grow, and
    // the question is the steady state.
    for _ in 0..8 {
        cluster.propose(payload.clone()).expect("warm");
    }

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..proposals {
        cluster.propose(payload.clone()).expect("propose");
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    allocations as f64 / proposals as f64
}

#[test]
fn a_proposal_does_not_allocate_per_follower() {
    let at_three = allocations_per_proposal(3, 200);
    let at_nine = allocations_per_proposal(9, 200);
    let per_peer = (at_nine - at_three) / 6.0;

    // Printed because a guard that only says "too high" sends the next reader
    // to re-measure what it already knew.
    println!("allocations per proposal: 3 peers {at_three:.2}, 9 peers {at_nine:.2}");
    println!("per additional peer: {per_peer:.3}");

    assert!(
        per_peer < 0.45,
        "a proposal should not pay an allocation per follower: {per_peer:.3} per peer \
         ({at_three:.2} at 3 peers, {at_nine:.2} at 9); inspect the replication \
         loop for buffers created once per follower"
    );
    assert!(
        at_three < 4.0 && at_nine < 4.0,
        "proposal scratch should stay below four allocations: got {at_three:.2} at three \
         peers and {at_nine:.2} at nine; rebuilding lease acknowledgements or commit \
         candidates pushes this above the bound"
    );
}
