// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a repeated leader transfer allocates.
//!
//! Retaining the record of a finished transfer was supposed to be free, because
//! the record stays in the `Box` the slot already held instead of that `Box`
//! being freed and an identically sized one asked for on the next transfer.
//! "Supposed to be free" is the kind of claim that wants measuring, so this
//! counts.
//!
//! Each cycle begins a transfer and aborts it, and allocates five times for it
//! before this change: four `String`s -- the admission's reason, the record's
//! reason, the copy of it in the record the call returns, and the abort's
//! reason -- and the `Box`. Measured A/B against the same tree with only the
//! reuse undone, it is **5.000 allocations a cycle before and 4.000 after**:
//! exactly the `Box`, and nothing else moved.
//!
//! The four `String`s stay. Each one carries different text, so there is
//! nothing to reuse there and this test is not claiming otherwise; they are
//! what the headroom in the ceiling is for.
//!
//! It needs its own test binary because a `#[global_allocator]` is per-process
//! and would otherwise count every other test's allocations too.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 52_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 53_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

const CYCLES: usize = 200;

/// A ceiling, not a target: lowering it is the point. The measured figure is
/// 4.000 and a `Box` per cycle puts it at 5.000, so the ceiling sits between
/// them rather than comfortably above both. A threshold set above the number it
/// is meant to catch asserts nothing.
const CEILING_PER_CYCLE: f64 = 4.5;

#[test]
fn repeating_a_leader_transfer_does_not_reallocate_its_record() {
    let mut cluster = RaftCluster::new(9, Config::default(), vec![peer(1), peer(2), peer(3)])
        .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
        .set_node_healthy(2, false)
        .expect("hold the transferee back so the transfer stays in flight");
    cluster.propose(b"before".to_vec()).expect("propose");

    // One cycle outside the window, so the first `Box` and any lazily built
    // state are not counted against the steady state this is about.
    cluster
        .begin_leader_transfer(2)
        .expect("begin")
        .expect("in flight");
    assert!(cluster.abort_leader_transfer("warmup"));

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..CYCLES {
        let state = cluster
            .begin_leader_transfer(2)
            .expect("begin")
            .expect("in flight");
        std::hint::black_box(state.transferee_id);
        assert!(cluster.abort_leader_transfer("cycle"));
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;

    // The record survived every cycle, so the loop measured the retaining path
    // rather than a path that quietly stopped recording.
    let record = cluster.last_leader_transfer().expect("record retained");
    assert_eq!(record.reason, "cycle");

    let per_cycle = allocations as f64 / CYCLES as f64;
    println!("transfer cycles: {allocations} allocations over {CYCLES} cycles = {per_cycle:.3} per cycle");
    assert!(
        per_cycle <= CEILING_PER_CYCLE,
        "a transfer cycle now allocates {per_cycle:.3} times, above the {CEILING_PER_CYCLE} recorded here. \
         Lowering this is the point; raising it needs a reason in the message."
    );
}
