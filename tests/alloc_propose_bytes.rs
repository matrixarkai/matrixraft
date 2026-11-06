// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A proposal does not allocate a kilobyte of bookkeeping.
//!
//! `alloc_propose.rs` bounds the allocation **count** against the peer count,
//! which is the per-follower question. This bounds the **bytes** at a zero
//! payload, which is the fixed-cost question: whatever a proposal allocates
//! with no payload to carry is structure, and structure rebuilt every proposal
//! is structure that could have been kept.
//!
//! Earlier measurements found two large avoidable sites by recording every
//! allocation size in `examples/propose_allocation_sizes.rs`:
//!
//! - a **544-byte** `BTreeMap` node, because the leader-lease renewal cleared
//!   the confirmations and refilled them, and `clear()` frees a map's nodes.
//!   `(NodeId, LeaderLeaseConfirmation)` is 8 + 40 bytes and a leaf holds
//!   eleven: 16 + 11x8 + 11x40 = 544.
//! - a **192-byte** `Vec<InflightAppend>` at its minimum capacity of four,
//!   lent to a flush whose result the replication path discards.
//!
//! The current change removes the remaining node-id vector: the same 200-round
//! probe moved from **193.6** to **153.6 bytes per proposal**. The bound is 180,
//! between those measured states, so rebuilding that vector fails the test.
//!
//! The allocator is process-wide, but counting is enabled only on the thread
//! exercising the synchronous proposal path so test-harness work cannot enter
//! the measurement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

static BYTES: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static TRACK_BYTES: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = TRACK_BYTES.try_with(|tracking| {
            if tracking.get() {
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
        raft_addr: format!("127.0.0.1:{}", 41_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 42_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

#[test]
fn a_proposal_does_not_allocate_a_kilobyte_of_bookkeeping() {
    let members = (1..=5u64).map(peer).collect::<Vec<_>>();
    let mut cluster = RaftCluster::new(7, Config::default(), members).expect("cluster");
    cluster.start().expect("start");

    // Warm outside the window: the first proposals grow what they grow, and
    // the question is the steady state.
    for _ in 0..16 {
        cluster.propose(Vec::new()).expect("warm");
    }

    let proposals = 200;
    TRACK_BYTES.with(|tracking| tracking.set(false));
    BYTES.store(0, Ordering::Relaxed);
    TRACK_BYTES.with(|tracking| tracking.set(true));
    let mut last_index = 0;
    for _ in 0..proposals {
        // Asserted rather than ignored: an arm that has stopped doing the work
        // prints a very good number.
        let log_id = cluster.propose(Vec::new()).expect("propose");
        assert!(
            log_id.index > last_index,
            "every proposal has to append: {log_id:?} after {last_index}"
        );
        last_index = log_id.index;
    }
    TRACK_BYTES.with(|tracking| tracking.set(false));
    let bytes = BYTES.load(Ordering::Relaxed);
    let per_proposal = bytes as f64 / proposals as f64;

    // Printed because a guard that only says "too high" sends the next reader
    // to re-measure what it already knew.
    println!("bytes per proposal at five peers, zero payload: {per_proposal:.1}");

    assert!(
        per_proposal < 180.0,
        "a proposal with no payload should stay below 180 bytes of allocation, got \
         {per_proposal:.1}; rebuilding the node-id vector raises the measured figure to 193.6"
    );
}
