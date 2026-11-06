// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Repeated campaigns do not allocate node-id scratch.
//!
//! A campaign resets every replication pipeline for the new leader. Building
//! a vector of all node ids first makes that reset allocate more as membership
//! grows. The cluster stays stopped in this probe, so campaigning does not
//! append or replicate a leader no-op and the measurement isolates the reset.
//!
//! Measured before and after stack chunking:
//!
//! | peers | before | after |
//! |---|---:|---:|
//! | 3 | 1.00 allocations / 24 B | 0.00 / 0 B |
//! | 33 | 1.00 allocations / 264 B | 0.00 / 0 B |

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
        raft_addr: format!("127.0.0.1:{}", 56_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 57_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn campaign_cost(peers: u64, rounds: usize) -> (usize, usize) {
    let mut cluster =
        RaftCluster::new(15, Config::default(), (1..=peers).map(peer).collect()).expect("cluster");

    cluster.campaign(1, true).expect("warm campaign");
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    for round in 0..rounds {
        let candidate = if round % 2 == 0 { 2 } else { 1 };
        cluster.campaign(candidate, true).expect("campaign");
        assert_eq!(cluster.leader_id(), Some(candidate));
    }
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    (
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

#[test]
fn pipeline_reset_allocates_nothing_after_setup() {
    let rounds = 200;
    let mut measured = Vec::new();
    for peers in [3, 33] {
        let (allocations, bytes) = campaign_cost(peers, rounds);
        let per_campaign = allocations as f64 / rounds as f64;
        let bytes_per_campaign = bytes as f64 / rounds as f64;
        println!(
            "{peers} peers: {per_campaign:.2} allocations and {bytes_per_campaign:.1} bytes per campaign"
        );
        measured.push((peers, allocations, per_campaign, bytes_per_campaign));
    }
    for (peers, allocations, per_campaign, bytes_per_campaign) in measured {
        assert_eq!(
            allocations, 0,
            "a warmed {peers}-member campaign should allocate no pipeline-reset scratch, got {per_campaign:.2} allocations and {bytes_per_campaign:.1} bytes per campaign"
        );
    }
}
