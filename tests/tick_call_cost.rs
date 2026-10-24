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
    // THE CEILING IS LOOSE BECAUSE THE INSTRUMENT IS, and that is the finding
    // rather than an excuse. A wall-clock microbenchmark on this box cannot
    // resolve better than about 2x, so it catches a call that starts allocating
    // on every tick only if the allocation is large.
    //
    // HOW LARGE, measured by planting one inside `broadcast_heartbeat`:
    //
    //   healthy                             1321-2750 ns   (load-dependent)
    //   a 64-element Vec per call              2811 ns     NOT caught
    //   a 1024-element Vec per call          423407 ns     caught
    //
    // The middle row is the point. 2811 ns sits INSIDE the healthy band, so the
    // old 2500 ns ceiling "caught" it only by accident -- it could not tell that
    // plant from an ordinary busy run, and it failed on ordinary busy runs too.
    // A threshold inside the noise carries no information either way. This one
    // sits above the band, so when it fails it means something.
    //
    // Measured, on the SAME CODE, in debug: 1321, 1352, 1401, 1405, 1574 and
    // 1893 ns across runs, and 2166-2750 ns while the suite's other targets are
    // running. A 2x spread from load alone, on a box that sits near a load
    // average of 16. The 861 ns in the notes above was measured on a quieter
    // machine, and the 2500 ns ceiling set against it failed in a full-suite run
    // and passed alone -- a load detector, not a regression detector.
    //
    // CHECKED BEFORE WIDENING IT, because a threshold raised over a real
    // regression is worse than no threshold at all. At #102 -- the commit that
    // last touched this file -- the row measures 1321-1893 ns on this box today,
    // and main measures 1352 ns in the same conditions. The bands are the same,
    // so nothing regressed: the machine moved.
    //
    // AND A SHARE GUARD WAS TRIED AND DOES NOT WORK, recorded so it is not
    // re-attempted on the assumption that it would. "No call may exceed N% of
    // the row" looks load-invariant, since every call is measured in the same
    // run. It is not: the dearest call is 32.9% of the row on a quiet machine
    // and 39.1% on a busy one, because it suffers most from the pressure. A
    // planted allocation-and-scan inside `broadcast_heartbeat` moved it only to
    // 42.9%. Healthy-under-load and planted-on-idle are 39.1% against 42.9%, so
    // no threshold separates them.
    let (ceiling_ns, profile) = if cfg!(debug_assertions) {
        (6_000.0, "debug")
    } else {
        (600.0, "release")
    };
    assert!(
        total <= ceiling_ns,
        "the per-tick calls now cost {total:.1} ns together on a {profile} build, over the {ceiling_ns} ns ceiling. Load alone moves this row 2x on this box, so suspect the machine first -- but if it is over this, every call has grown and something in the tick path is allocating."
    );
    // The control: an empty list would come in under any ceiling at all.
    assert_eq!(best.len(), 9, "the list of per-tick calls changed size");
    assert_eq!(
        cluster.leader_id(),
        Some(1),
        "the row moved the leadership it was measuring"
    );
}
