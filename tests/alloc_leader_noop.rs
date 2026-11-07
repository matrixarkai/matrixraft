// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Broadcasting the entry for a new leader does not allocate node-id scratch.
//!
//! A running campaign necessarily allocates the entry payload and its shared
//! record. The node walk itself should not add another allocation whose bytes
//! grow with membership.
//!
//! Measured before and after direct node iteration:
//!
//! | voters | before | after |
//! |---|---:|---:|
//! | 3 | 3.00 allocations / 93 B | 2.00 / 69 B |
//! | 33 | 3.00 allocations / 333 B | 2.00 / 69 B |

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
        raft_addr: format!("127.0.0.1:{}", 62_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 63_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn campaign_cost(voters: u64) -> (usize, usize) {
    let mut cluster = RaftCluster::new(18, Config::default(), (1..=voters).map(voter).collect())
        .expect("cluster");
    cluster.start().expect("start");
    assert_eq!(cluster.leader_id(), Some(1));

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    for candidate in [2, 1, 2] {
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
fn leader_noop_broadcast_keeps_only_entry_allocations() {
    let rounds = 3.0;
    let mut measured = Vec::new();
    for voters in [3, 33] {
        let (allocations, bytes) = campaign_cost(voters);
        let per_campaign = allocations as f64 / rounds;
        let bytes_per_campaign = bytes as f64 / rounds;
        println!(
            "{voters} voters: {per_campaign:.2} allocations and {bytes_per_campaign:.1} bytes per campaign"
        );
        measured.push((voters, allocations, per_campaign, bytes_per_campaign));
    }
    for (voters, allocations, per_campaign, bytes_per_campaign) in measured {
        assert_eq!(
            allocations, 6,
            "three running campaigns for {voters} voters should allocate only the payload and shared entry, got {per_campaign:.2} allocations and {bytes_per_campaign:.1} bytes per campaign"
        );
    }
}
