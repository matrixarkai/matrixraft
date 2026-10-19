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
//! | 2 | 3.00 | 2.00 | 1.50 | 1.00 |
//! | 4 | 9.00 | 3.00 | 2.25 | 0.75 |
//! | 6 | 14.00 | 4.00 | 2.33 | 0.67 |
//! | 8 | 26.00 | 5.00 | 3.25 | 0.62 |
//!
//! **This asserts the shape, not a constant.** The defect was that the cost
//! per peer *rose* with the group; the fix is that it does not. A shape
//! assertion needs no tuned threshold and cannot drift, so it says what the
//! change is actually about. A loose slope bound sits beside it to catch a
//! regression that stays linear but gets much worse.
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
fn a_heartbeat_does_not_cost_more_per_peer_as_the_group_grows() {
    let (small, small_reached) = per_round(3, 300);
    let (large, large_reached) = per_round(9, 300);
    let small_per_peer = small / small_reached as f64;
    let large_per_peer = large / large_reached as f64;
    let slope = (large - small) / (large_reached - small_reached) as f64;

    println!("per round: {small_reached} peers {small:.2}, {large_reached} peers {large:.2}");
    println!("per peer: {small_per_peer:.2} -> {large_per_peer:.2}, slope {slope:.3}");

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
