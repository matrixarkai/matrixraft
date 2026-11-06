// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Selecting a failover target does not allocate per tied follower.
//!
//! `closest_follower` first prefers the highest replicated index, then the
//! lowest node id above the current leader, wrapping to the lowest id when
//! needed. Materializing and sorting every tied candidate makes failover
//! selection grow heap work with the group precisely when many replicas are
//! equally caught up.
//!
//! Measured before and after the single-pass selector:
//!
//! | peers | before | after |
//! |---|---:|---:|
//! | 3 | 1.00 allocations / 32 B | 0.00 / 0 B |
//! | 33 | 4.00 allocations / 480 B | 0.00 / 0 B |

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

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 54_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 55_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn selection_cost(peers: u64, rounds: usize) -> (usize, usize) {
    let mut cluster =
        RaftCluster::new(14, Config::default(), (1..=peers).map(peer).collect()).expect("cluster");
    cluster.start().expect("start");
    assert_eq!(cluster.leader_id(), Some(1));
    assert_eq!(cluster.closest_follower(), Some(2));

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    for _ in 0..rounds {
        assert_eq!(cluster.closest_follower(), Some(2));
    }
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    (
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

#[test]
fn tied_failover_selection_allocates_nothing_after_setup() {
    let rounds = 200;
    let mut measured = Vec::new();
    for peers in [3, 33] {
        let (allocations, bytes) = selection_cost(peers, rounds);
        let per_selection = allocations as f64 / rounds as f64;
        let bytes_per_selection = bytes as f64 / rounds as f64;
        println!(
            "{peers} peers: {per_selection:.2} allocations and {bytes_per_selection:.1} bytes per selection"
        );
        measured.push((peers, allocations, per_selection, bytes_per_selection));
    }
    for (peers, allocations, per_selection, bytes_per_selection) in measured {
        assert_eq!(
            allocations, 0,
            "a warmed {peers}-member failover selection should allocate nothing, got {per_selection:.2} allocations and {bytes_per_selection:.1} bytes per call"
        );
    }
}
