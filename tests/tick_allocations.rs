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

    // The rest of the row, in this test for the reason given at its definition.
    the_per_tick_cluster_calls_allocate_nothing(&mut cluster);
}

/// Counts allocations over `times` calls, after warming whatever allocates once.
fn per_call<F: FnMut()>(times: usize, mut body: F) -> f64 {
    for _ in 0..8 {
        body();
    }
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..times {
        body();
    }
    (ALLOCATIONS.load(Ordering::Relaxed) - before) as f64 / times as f64
}

// Deliberately part of the test above rather than a second `#[test]`.
//
// The counter is the process's, so it cannot tell whose allocation it just saw.
// Split across two tests, each one's delta picked up whatever the harness was
// doing on its own threads while the other started and finished: a lease tick came
// out at 0.01 allocations, `leader_id` at 0.08, `tick_peer_liveness` at 0.02, none
// of which is a thing those functions do, and the attribution moved between runs.
// A mutex around each measurement did not help, because the threads contaminating
// the count were never the other test's.
//
// A `--test-threads=1` on a command line would have hidden it. The gate runs
// `cargo test` with its own thread count, so the arrangement has to be right here.
fn the_per_tick_cluster_calls_allocate_nothing(cluster: &mut RaftCluster) {
    // A group's tick calls these in turn, so each is paid by every group on every
    // interval. The lease one was three allocations and is now none; this is the
    // rest of the row, so that a later change cannot move the cost from the part
    // that is measured into a part that is not.
    //
    // The state is a single voter holding its own leadership, which is what the
    // hosting probe runs and what a store of idle groups looks like. One honest
    // limit on it: `tick_peer_liveness` returns the peers it found offline, so in
    // a state where a peer times out it allocates for the list and should. There
    // are no peers here to go offline.
    assert_eq!(
        cluster.leader_id(),
        Some(1),
        "no leader, so these calls take different paths than a tick does"
    );

    let calls: Vec<(&str, f64)> = vec![
        (
            "tick_leader_lease",
            per_call(64, || {
                cluster.tick_leader_lease(1);
            }),
        ),
        (
            "tick_follower_lease",
            per_call(64, || {
                cluster.tick_follower_lease(1);
            }),
        ),
        (
            "tick_peer_liveness",
            per_call(64, || {
                let offline = cluster.tick_peer_liveness(1);
                std::hint::black_box(&offline);
            }),
        ),
        (
            "leader_id",
            per_call(64, || {
                std::hint::black_box(cluster.leader_id());
            }),
        ),
        // The rest of what a tick calls. Added because the hosted tick measures
        // 1.250 allocations and the driver's own per-wake list accounts for 1.000
        // of that (`tests/alloc_driver.rs`): a quarter of an allocation a tick was
        // being paid somewhere in this row and the four calls above are all zero,
        // so the row was not the whole row.
        (
            "broadcast_heartbeat",
            per_call(64, || {
                let _ = cluster.broadcast_heartbeat();
            }),
        ),
        (
            "tick_snapshot_trigger",
            per_call(64, || {
                std::hint::black_box(cluster.tick_snapshot_trigger());
            }),
        ),
        (
            "broadcast_commit_index_to_old_paused_peers",
            per_call(64, || {
                let _ = cluster.broadcast_commit_index_to_old_paused_peers();
            }),
        ),
        (
            "step_down_leader_if_lost_quorum",
            per_call(64, || {
                std::hint::black_box(cluster.step_down_leader_if_lost_quorum());
            }),
        ),
        (
            "leader_transfer_state",
            per_call(64, || {
                std::hint::black_box(cluster.leader_transfer_state().is_some());
            }),
        ),
    ];

    // Printed whether or not the assertion below fires: a zero is as much a
    // measurement as a hit, and without the figures a later reader cannot tell a
    // call that was checked from one that was never on the list.
    for (name, per) in &calls {
        println!("  {name:<44} {per:.4} allocations a call");
    }

    let allocating: Vec<String> = calls
        .iter()
        .filter(|(_, per)| *per > 0.0)
        .map(|(name, per)| format!("{name} at {per:.2} a call"))
        .collect();
    assert!(
        allocating.is_empty(),
        "these per-tick calls allocate: {}",
        allocating.join(", ")
    );
    // The control: an empty list of calls would also report nothing allocating.
    assert_eq!(calls.len(), 9, "the list of per-tick calls changed size");
    // And the leadership the row was measured under is still the leadership it was
    // asserted under. `step_down_leader_if_lost_quorum` is in the list and would
    // have moved it if the single voter had somehow stopped being a quorum, which
    // would make every figure above describe a different state than a tick's.
    assert_eq!(
        cluster.leader_id(),
        Some(1),
        "the row moved the leadership it was measuring"
    );
}
