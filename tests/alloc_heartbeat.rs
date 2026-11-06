// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A heartbeat round does not cost more per peer the more peers there are.
//!
//! A heartbeat is the highest-frequency thing a leader does -- every tick, to
//! every peer, whether or not there is anything to replicate -- so a cost here
//! is multiplied by the tick rate as well as the fan-out.
//!
//! It used to call the **public** response handler, which recomputes both
//! group-wide aggregates per response. `refresh_commit_index` collects and
//! sorts a vector over every quorum node, so a round of N responses allocated
//! N vectors of N. The propose path had already been given the deferral for
//! exactly this reason; the heartbeat never got it.
//!
//! Allocations per round, at a 256-byte configuration:
//!
//! | peers reached | before | after | per peer before | after |
//! |---|---|---|---|---|
//! | 2 | 2.00 | 0.00 | 1.00 | 0.00 |
//! | 4 | 3.00 | 0.00 | 0.75 | 0.00 |
//! | 6 | 4.00 | 0.00 | 0.67 | 0.00 |
//! | 8 | 5.00 | 0.00 | 0.62 | 0.00 |
//!
//! Peer ids move through fixed stack chunks, and commit selection uses a stack
//! order statistic with an allocation-free fallback for larger memberships.
//!
//! One test, in a file of its own, because the counter is the process
//! allocator.

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
        raft_addr: format!("127.0.0.1:{}", 51_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 52_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// `(allocations per round, peers reached)`.
fn per_round(peers: usize, rounds: usize) -> (f64, u64) {
    let members = (1..=peers as u64).map(peer).collect::<Vec<_>>();
    let mut cluster = RaftCluster::new(7, Config::default(), members).expect("cluster");
    cluster.start().expect("start");

    // Reached is checked on every round: a cheap figure must not be able to
    // come from a broadcast that reached nobody.
    let reached = cluster.broadcast_heartbeat().expect("heartbeat");
    assert!(reached > 0, "{peers} peers should reach somebody");
    for _ in 0..8 {
        cluster.broadcast_heartbeat().expect("warm");
    }

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..rounds {
        let sent = cluster.broadcast_heartbeat().expect("heartbeat");
        assert_eq!(sent, reached, "every round must reach the same peers");
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    (allocations as f64 / rounds as f64, reached)
}

#[test]
fn a_heartbeat_allocates_nothing_after_warmup() {
    let (small, small_reached) = per_round(3, 300);
    // Thirty-three members cross both stack bounds: routing needs more than
    // one peer chunk and commit selection uses its large-membership path.
    let (large, large_reached) = per_round(33, 300);
    let small_per_peer = small / small_reached as f64;
    let large_per_peer = large / large_reached as f64;
    let slope = (large - small) / (large_reached - small_reached) as f64;

    println!("per round: {small_reached} peers {small:.2}, {large_reached} peers {large:.2}");
    println!("per peer: {small_per_peer:.2} -> {large_per_peer:.2}, slope {slope:.3}");

    assert_eq!(
        small, 0.0,
        "a warmed heartbeat should retain no heap scratch"
    );
    assert_eq!(
        large, 0.0,
        "a larger group should also need no heap scratch"
    );
    assert!(
        large_per_peer <= small_per_peer,
        "the cost per peer must not grow with the group: {small_per_peer:.2} at \
         {small_reached} peers and {large_per_peer:.2} at {large_reached}. That rise is \
         the signature of a per-response pass over every node -- it was 1.50 and 3.25 \
         when the heartbeat recomputed both aggregates per response"
    );
    assert!(
        slope < 1.0,
        "a heartbeat should cost well under one allocation per additional peer, got \
         {slope:.3}; it was about 3.83 before the round's aggregates were deferred"
    );
}
