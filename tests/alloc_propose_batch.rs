// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Batch admission grows replica logs once rather than once per capacity step.
//!
//! A 1,024-entry batch on five replicas measured 1.068 allocations and 242.8
//! allocated bytes per proposal when each replica log grew geometrically. One
//! reservation per replica reduced that to 1.029 allocations and 123.1 bytes.

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
        raft_addr: format!("127.0.0.1:{}", 51_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 52_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

#[test]
fn batch_admission_grows_each_replica_log_once() {
    let members = (1..=5u64).map(peer).collect::<Vec<_>>();
    let mut cluster = RaftCluster::new(7, Config::default(), members).expect("cluster");
    cluster.start().expect("start");
    let proposal_count = 1_024;
    let payloads = vec![Vec::new(); proposal_count];

    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let log_ids = cluster.propose_batch(payloads).expect("propose batch");
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    assert_eq!(log_ids.len(), proposal_count);
    let first_index = log_ids.first().expect("first log id").index;
    let last_index = log_ids.last().expect("last log id").index;
    assert_eq!(
        last_index - first_index + 1,
        proposal_count as u64,
        "the batch should occupy one contiguous log range"
    );

    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    let allocations_per_proposal = allocations as f64 / proposal_count as f64;
    let bytes_per_proposal = bytes as f64 / proposal_count as f64;
    println!(
        "batch admission: {allocations_per_proposal:.3} allocations/proposal, \
         {bytes_per_proposal:.1} bytes/proposal"
    );

    assert!(
        allocations_per_proposal < 1.05,
        "replica logs should grow once for the batch: got \
         {allocations_per_proposal:.3} allocations per proposal"
    );
    assert!(
        bytes_per_proposal < 180.0,
        "batch admission should avoid geometric replica-log growth: got \
         {bytes_per_proposal:.1} bytes per proposal"
    );
}
