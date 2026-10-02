// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the raft calls a tick makes cost, which is almost none of a tick.
//!
//! An idle hosted group's tick costs about **2,200 ns** -- cores divided by ticks
//! at 65,536 groups -- and that figure times groups-over-interval is the whole CPU
//! budget of a hosted store. The obvious place to look for it is the dozen calls a
//! tick makes into the cluster.
//!
//! Their **arithmetic** is not it: the whole row is about 45 ns in release here.
//! But that is 50,000 calls in a row on one cluster, which is this test's purpose
//! and also its limit -- it holds one group's state in L1 and lets the branch
//! predictor learn it, which is not what a store does.
//!
//! Called once each across 2,000 separately allocated clusters the same row costs
//! 120-200 ns in allocation order and 200-660 ns in a scattered one -- several
//! times this, varying with how busy the machine is -- and cutting the tick body
//! in a running store puts the region at roughly 500 ns. So these calls are most
//! of a tick after all; what is cheap is what they compute, and what is expensive
//! is reaching the state they read. The lever is locality.
//! `tests/tick_locality.rs` is the measurement.
//!
//! (In a debug build this row is 861 ns, which is what the suite prints; the
//! figures above are release, so compare like with like.) The
//! cost is the hosting around them: the wake, the lock, the scheduling, and the
//! parts of `NodeCore::tick` that are not calls into the cluster.
//!
//! So this guards against one of them becoming *algorithmically* expensive -- a
//! lock, an allocation, a scan of something that grew. It cannot see a cost that
//! is cache misses, because it is built to avoid them, which is why the ceiling is
//! loose and why it is the wrong instrument for asking what a tick costs.
//!
//! Two notes on the instrument. The statistic is the **minimum** over repeats
//! rather than a mean: this box is shared and carries several cores of other work,
//! so a mean measures the load and a minimum measures the code. Measured that way
//! the row is steady to about 5% across whole runs -- 46.9, 52.0, 46.9, 48.2 ns --
//! where the wall-clock instruments here vary by a factor of two. And it is timed
//! in-process rather than sampled because `gdb -p` cannot attach to a sibling on
//! this box (`ptrace_scope` is 1) and hangs rather than failing.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};
use std::time::Instant;

fn voter(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 52_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 53_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// Nanoseconds a call, timed around the loop rather than each iteration so the
/// clock is not most of what is measured.
fn ns_per_call<F: FnMut()>(times: usize, mut body: F) -> f64 {
    for _ in 0..1_000 {
        body();
    }
    let began = Instant::now();
    for _ in 0..times {
        body();
    }
    began.elapsed().as_nanos() as f64 / times as f64
}

#[test]
fn the_raft_calls_a_tick_makes_are_not_what_a_tick_costs() {
    let mut cluster = RaftCluster::new(7, Config::default(), vec![voter(1)]).expect("a cluster");
    cluster.start().expect("start");
    assert_eq!(
        cluster.leader_id(),
        Some(1),
        "no leader, so these calls take different paths than a tick does"
    );

    const TIMES: usize = 50_000;
    const REPEATS: usize = 5;
    let mut best: Vec<(&str, f64)> = Vec::new();

    macro_rules! timed {
        ($name:expr, $body:expr) => {{
            let mut lowest = f64::MAX;
            for _ in 0..REPEATS {
                let ns = ns_per_call(TIMES, $body);
                if ns < lowest {
                    lowest = ns;
                }
            }
            best.push(($name, lowest));
        }};
    }

    timed!("tick_leader_lease", || {
        let _ = cluster.tick_leader_lease(1);
    });
    timed!("tick_follower_lease", || {
        cluster.tick_follower_lease(1);
    });
    timed!("tick_peer_liveness", || {
        std::hint::black_box(cluster.tick_peer_liveness(1));
    });
    timed!("leader_id", || {
        std::hint::black_box(cluster.leader_id());
    });
    timed!("broadcast_heartbeat", || {
        let _ = cluster.broadcast_heartbeat();
    });
    timed!("tick_snapshot_trigger", || {
        std::hint::black_box(cluster.tick_snapshot_trigger());
    });
    timed!("broadcast_commit_index_to_old_paused_peers", || {
        let _ = cluster.broadcast_commit_index_to_old_paused_peers();
    });
    timed!("step_down_leader_if_lost_quorum", || {
        std::hint::black_box(cluster.step_down_leader_if_lost_quorum());
    });
    timed!("leader_transfer_state", || {
        std::hint::black_box(cluster.leader_transfer_state().is_some());
    });

    best.sort_by(|a, b| b.1.partial_cmp(&a.1).expect("comparable"));
    let total: f64 = best.iter().map(|(_, ns)| ns).sum();
    let profile_name = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    println!(
        "
  dearest first, minimum of {REPEATS} runs of {TIMES} calls, {profile_name} build:"
    );
    for (name, ns) in &best {
        println!(
            "    {name:<44} {ns:>8.1} ns  {:>5.1}% of the row",
            100.0 * ns / total
        );
    }
    println!("    {:<44} {total:>8.1} ns", "the row together");

    // Loose on purpose: about five times the measured row. This is here to catch a
    // call that starts doing real work -- taking a lock, allocating, scanning
    // something that grew -- not to hold 48 ns. A tick has some 2,200 ns in it, so
    // even this ceiling is a tenth of one.
    //
    // Two ceilings, because the suite runs `cargo test`, and that is a **debug**
    // build: this row is 45 ns in release and 861 ns in debug, a factor of 19. One
    // number would either fail every gate or never catch anything. Both are steady
    // -- 46.9/52.0/46.9/48.2 in release, 861.5/861.4 in debug.
    let (ceiling_ns, profile) = if cfg!(debug_assertions) {
        (2_500.0, "debug")
    } else {
        (250.0, "release")
    };
    assert!(
        total <= ceiling_ns,
        "the per-tick calls now cost {total:.1} ns together on a {profile} build, over the {ceiling_ns} ns recorded. They were 45 ns in release and 861 ns in debug, so one of them has started doing real work."
    );
    // The control: an empty list would come in under any ceiling at all.
    assert_eq!(best.len(), 9, "the list of per-tick calls changed size");
    assert_eq!(
        cluster.leader_id(),
        Some(1),
        "the row moved the leadership it was measuring"
    );
}
