// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A proposal does not pay an allocation per follower.
//!
//! It used to pay two. One was the vector `flush_append_batch` returned, which
//! the replication path **discards** -- an allocation per follower for a value
//! nobody read. The other was the acknowledgement list, which started at
//! capacity one and grew by doubling as peers acknowledged, for a length the
//! node count already says.
//!
//! Measured per proposal, at a 256-byte payload:
//!
//! | peers | before | after |
//! |---|---|---|
//! | 1 | 5.02 | 5.02 |
//! | 3 | 9.06 | 7.06 |
//! | 5 | 13.10 | 8.10 |
//! | 7 | 15.14 | 8.14 |
//! | 9 | 19.18 | 9.18 |
//!
//! The one-peer row is the control: a single-node cluster has no follower to
//! flush to and one acknowledgement, so it exercises none of this and did not
//! move.
//!
//! **This asserts the slope first**, because the slope is what the change is
//! about: a fixed cost added elsewhere in `propose` should not fail this test
//! for an unrelated reason, while anything added *per peer* deserves to be
//! looked at.
//!
//! The thresholds are set from a measured ladder rather than guessed. Each row
//! is this code with one piece put back, allocations per additional peer:
//!
//! | state | per peer |
//! |---|---|
//! | as shipped | **0.353** |
//! | the lent buffer is never cleared | 0.520 |
//! | the acknowledgement list grows by doubling again | 0.687 |
//! | a fresh flush vector per follower again | 1.353 |
//! | both put back, which is what main had | 1.687 |
//!
//! So the bound is **0.45**: above the shipped figure with room, and below
//! every one of those. A first attempt at this test used 0.8, which the
//! acknowledgement list and the missing `clear` both slipped under -- a
//! threshold picked to be safely above the good number is not necessarily
//! below the bad ones, and only measuring the ladder shows that.
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
         ({at_three:.2} at 3 peers, {at_nine:.2} at 9). The ladder in this file's \
         header says which piece each value corresponds to -- 0.52 is a missing \
         `clear`, 0.69 the acknowledgement list, 1.35 a flush vector per follower"
    );
    assert!(
        at_nine < 11.0,
        "nine peers should cost under eleven allocations a proposal, got {at_nine:.2}; \
         it was 19.18 when every follower allocated its own flush vector and the \
         acknowledgement list grew by doubling"
    );
}
