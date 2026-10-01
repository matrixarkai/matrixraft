// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one tick of a group's leader lease allocates.
//!
//! Found by sampling a running store with `gdb`, because this box has no
//! `perf`: of the ticker-thread stacks caught doing work rather than waiting,
//! the most common callee was `RaftCluster::leader_lease_quorum_reached`, and
//! `malloc` and `free` were on top of stack in a fifth of them. A tick that
//! allocates is a tick that costs more than it needs to, and every group pays it
//! on every interval -- at 8192 groups on a 10ms interval that is 819,200 lease
//! checks a second.
//!
//! This pins the number rather than fixing it. The cost is a ceiling, not a
//! target: lowering it is the point, and a change that does so has to lower the
//! ceiling in the same breath, which is what makes this worth having.
//!
//! Counting allocations rather than timing them is deliberate. The box this runs
//! on is shared and often carries two to ten cores of someone else's work, which
//! makes a microsecond unmeasurable and an allocation count exactly as true as
//! it would be on an idle machine.
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

fn voter(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 50_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 51_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

#[test]
fn a_leader_lease_tick_allocates_and_this_is_how_much() {
    // A single voter, so it takes leadership at once. With three voters and no
    // election there is no leader, `leader_lease_quorum_reached` returns before
    // it allocates anything, and this measured 1.12 allocations a tick that had
    // nothing to do with the path it claims to be about.
    let mut cluster = RaftCluster::new(7, Config::default(), vec![voter(1)]).expect("a cluster");
    cluster.start().expect("start");
    assert!(
        Config::default().enable_lease_read,
        "lease reads are off by default now, so this measures an early return"
    );
    // The precondition that makes the measurement mean what it says: without a
    // leader the quorum check is one `is_none` and a return.
    assert_eq!(
        cluster.leader_id(),
        Some(1),
        "the single voter did not take leadership, so the lease path is not reached"
    );

    // Warm, so that anything allocated once is not counted against the ticks.
    for _ in 0..16 {
        cluster.tick_leader_lease(1);
    }

    // Well inside the lease. `leader_lease_ms` defaults to 500 and each tick
    // here advances the clock by one, so 16 + 128 keeps every measured tick in
    // the state this is about: a leader whose lease is still good. A first
    // version ran 4,096 ticks, walked past the expiry a tenth of the way in, and
    // averaged the allocating path together with the early return -- 1.12 a
    // tick, a number describing neither.
    let ticks = 128usize;
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..ticks {
        cluster.tick_leader_lease(1);
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    let per_tick = allocations as f64 / ticks as f64;

    // None at all, which is a stronger claim than a ceiling and so is asserted as
    // an equality. It was three when this test was written: a `Vec` for the
    // acknowledgements, a `Membership` built to look them up in, and a second
    // `Membership` built by `refresh_witness_commit_quorum_policy` to count roles
    // that were already on its nodes. All three are gone, and at 8192 groups on a
    // 10ms interval that is 2.46 million allocations a second that no longer
    // happen.
    //
    // An equality is also its own control. The ceiling this replaced needed a
    // separate assertion that the tick allocated *something*, or it would have
    // held for a build that did nothing at all -- and that assertion is what
    // caught the moment this reached zero.
    assert_eq!(
        allocations, 0,
        "a lease tick allocated {per_tick:.2} times ({allocations} over {ticks} \
         ticks). It allocates nothing; if that has changed deliberately, say what \
         now needs the heap on a path every group walks every interval."
    );

    println!("lease tick allocations: {per_tick:.2} per tick over {ticks} ticks");
}
