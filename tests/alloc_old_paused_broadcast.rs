// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Idle paused-peer commit checks retain no membership-sized heap scratch.
//!
//! Every leader heartbeat checks whether a formerly paused replication pipeline
//! needs an empty append. Most checks send nothing, so scanning the followers
//! should not first materialize every eligible node id.
//!
//! Measured before and after stack chunking:
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
        raft_addr: format!("127.0.0.1:{}", 58_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 59_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn broadcast_cost(peers: u64, rounds: usize) -> (usize, usize) {
    let mut cluster =
        RaftCluster::new(16, Config::default(), (1..=peers).map(peer).collect()).expect("cluster");
    cluster.start().expect("start");
    assert_eq!(cluster.leader_id(), Some(1));

    for _ in 0..8 {
        assert_eq!(
            cluster
                .broadcast_commit_index_to_old_paused_peers()
                .expect("warm broadcast"),
            0
        );
    }

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    for _ in 0..rounds {
        assert_eq!(
            cluster
                .broadcast_commit_index_to_old_paused_peers()
                .expect("broadcast"),
            0,
            "an idle pipeline should not need an empty append"
        );
    }
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    (
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

#[test]
fn idle_paused_peer_checks_allocate_nothing_after_setup() {
    let rounds = 200;
    let mut measured = Vec::new();
    for peers in [3, 33] {
        let (allocations, bytes) = broadcast_cost(peers, rounds);
        let per_broadcast = allocations as f64 / rounds as f64;
        let bytes_per_broadcast = bytes as f64 / rounds as f64;
        println!(
            "{peers} peers: {per_broadcast:.2} allocations and {bytes_per_broadcast:.1} bytes per check"
        );
        measured.push((peers, allocations, per_broadcast, bytes_per_broadcast));
    }
    for (peers, allocations, per_broadcast, bytes_per_broadcast) in measured {
        assert_eq!(
            allocations, 0,
            "a warmed {peers}-member paused-peer check should allocate nothing, got {per_broadcast:.2} allocations and {bytes_per_broadcast:.1} bytes per call"
        );
    }
}
