// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Read-quorum reporting allocates only the three lists it returns.
//!
//! A 33-member voter+witness report used to allocate 15 times and 1,672 bytes:
//! three temporary membership lists plus incrementally grown result lists.
//! Exact capacities leave one allocation for each returned non-empty list.

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

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 31_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 32_000 + node_id),
        role,
        auto_promote: false,
    }
}

#[test]
fn mixed_role_read_quorum_avoids_membership_scratch() {
    let members = (1..=17)
        .map(|node_id| peer(node_id, ReplicaRole::Voter))
        .chain((18..=33).map(|node_id| peer(node_id, ReplicaRole::Witness)))
        .collect();
    let mut cluster = RaftCluster::new(21, Config::default(), members).expect("cluster");
    cluster.start().expect("start");

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let report = cluster.read_quorum_report();
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    println!("33-member read quorum: {allocations} allocations and {bytes} bytes");
    assert_eq!(report.live_voters, (1..=17).collect::<Vec<_>>());
    assert_eq!(report.live_witnesses, (18..=33).collect::<Vec<_>>());
    assert_eq!(report.acknowledgements, (1..=33).collect::<Vec<_>>());
    assert_eq!(report.required, 17);
    assert!(report.reached);
    assert_eq!(allocations, 3, "read quorum rebuilt membership scratch");
    assert_eq!(bytes, 528, "read quorum retained unexpected heap scratch");
}
