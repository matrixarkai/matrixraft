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
//! Two were, found by recording the size of every allocation a proposal makes
//! and reading the histogram -- `examples/propose_allocation_sizes.rs`, where a
//! size identifies a site the way a profile would:
//!
//! - a **544-byte** `BTreeMap` node, because the leader-lease renewal cleared
//!   the confirmations and refilled them, and `clear()` frees a map's nodes.
//!   `(NodeId, LeaderLeaseConfirmation)` is 8 + 40 bytes and a leaf holds
//!   eleven: 16 + 11x8 + 11x40 = 544.
//! - a **192-byte** `Vec<InflightAppend>` at its minimum capacity of four,
//!   lent to a flush whose result the replication path discards.
//!
//! The ladder behind the bound, from that example at five peers and a zero
//! payload, each row this code with one piece put back:
//!
//! | state | bytes/proposal |
//! |---|---|
//! | as shipped | **393.6** |
//! | the lent buffer put back | 585.6 |
//! | the lease-confirmation `clear()` put back | 937.6 |
//! | both put back, which is what main had | 1,129.6 |
//! | a fresh buffer per peer, the pre-lending shape | 1,161.6 |
//!
//! The "both put back" row reproduces the 1,129.6 that main measures directly,
//! which is the check that the ladder is measuring what it claims. An earlier
//! version of it put a fresh buffer back *inside* the per-peer loop and
//! labelled that row "the lent buffer put back" -- a different and worse state,
//! and now the last row rather than the second.
//!
//! So the bound is **450**: above the shipped figure with room, and below every
//! one of those. This test's own figure is lower than 393.6 because it spreads
//! the same amortised growth over 200 proposals rather than 50; the bound sits
//! in the gap either way, and the figure is printed so the next reader does not
//! have to wonder which instrument said what.
//!
//! One test, in a file of its own, because the counter is the process
//! allocator: a second test in the same binary would be allocating at the same
//! time and neither figure would belong to either test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

static BYTES: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
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
    let before = BYTES.load(Ordering::Relaxed);
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
    let bytes = BYTES.load(Ordering::Relaxed) - before;
    let per_proposal = bytes as f64 / proposals as f64;

    // Printed because a guard that only says "too high" sends the next reader
    // to re-measure what it already knew.
    println!("bytes per proposal at five peers, zero payload: {per_proposal:.1}");

    assert!(
        per_proposal < 450.0,
        "a proposal with no payload should not allocate {per_proposal:.1} bytes. The ladder in \
         this file's header says which piece each value corresponds to -- about 586 is the lent \
         flush buffer back, about 938 the lease-confirmation clear() back, about 1,130 both"
    );
}
