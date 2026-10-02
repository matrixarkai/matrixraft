// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a tick costs is reaching the group, not the arithmetic it then does.
//!
//! `tests/tick_call_cost.rs` times each call 50,000 times in a row on one cluster
//! and reports ~45ns for the row. A real tick calls each one **once per interval
//! on a different group**. Two reasons that might differ:
//!
//! 1. State. `tick_leader_lease(1)` called 50,000 times advances the lease by 50
//!    seconds; after it expires the call may take a cheap early return that a real
//!    tick never reaches.
//! 2. Memory. One cluster stays in L1; a store sweeps thousands of them.
//!
//! So: the same calls, once each, across many freshly built clusters.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};
use std::time::Instant;

fn voter(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 54_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 55_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn fresh(group_id: u64) -> RaftCluster {
    let mut cluster =
        RaftCluster::new(group_id, Config::default(), vec![voter(1)]).expect("a cluster");
    cluster.start().expect("start");
    cluster
}

#[test]
fn the_same_calls_cost_several_times_more_when_the_groups_are_scattered() {
    const CLUSTERS: usize = 2_000;
    const REPEATS: usize = 3;

    // Built up front, so construction is not in the timed sweep. 2,000 of them is
    // a working set in the tens of MiB, like a small store.
    // Boxed, so they are scattered in the heap the way separately created groups
    // are, rather than packed into one contiguous Vec that a prefetcher walks for
    // free.
    let mut many: Vec<Box<RaftCluster>> = (1..=CLUSTERS as u64)
        .map(|id| Box::new(fresh(id)))
        .collect();
    assert_eq!(many[0].leader_id(), Some(1), "no leader, wrong path");

    // A fixed stride coprime to the count, so every cluster is visited exactly
    // once in an order the prefetcher cannot follow. Deterministic, so the arms
    // are comparable run to run and no crate is needed for it.
    let order: Vec<usize> = (0..CLUSTERS).map(|i| (i * 1_237) % CLUSTERS).collect();
    {
        let mut seen = vec![false; CLUSTERS];
        for &i in &order {
            seen[i] = true;
        }
        assert!(
            seen.iter().all(|hit| *hit),
            "the stride does not visit every cluster, so the two orders touch              different amounts of memory and the comparison means nothing"
        );
    }

    // One tick's worth of calls, once on each cluster in turn.
    let mut spread = f64::MAX;
    for _ in 0..REPEATS {
        let began = Instant::now();
        for cluster in many.iter_mut() {
            let _ = cluster.tick_leader_lease(1);
            cluster.tick_follower_lease(1);
            std::hint::black_box(cluster.tick_peer_liveness(1));
            std::hint::black_box(cluster.leader_id());
            let _ = cluster.broadcast_heartbeat();
            std::hint::black_box(cluster.tick_snapshot_trigger());
            let _ = cluster.broadcast_commit_index_to_old_paused_peers();
            std::hint::black_box(cluster.step_down_leader_if_lost_quorum());
            std::hint::black_box(cluster.leader_transfer_state().is_some());
        }
        let ns = began.elapsed().as_nanos() as f64 / CLUSTERS as f64;
        if ns < spread {
            spread = ns;
        }
    }

    // The same row, the way the shipped test does it: one cluster, over and over.
    let mut one = fresh(9_999);
    let mut hammered = f64::MAX;
    for _ in 0..REPEATS {
        let began = Instant::now();
        for _ in 0..CLUSTERS {
            let _ = one.tick_leader_lease(1);
            one.tick_follower_lease(1);
            std::hint::black_box(one.tick_peer_liveness(1));
            std::hint::black_box(one.leader_id());
            let _ = one.broadcast_heartbeat();
            std::hint::black_box(one.tick_snapshot_trigger());
            let _ = one.broadcast_commit_index_to_old_paused_peers();
            std::hint::black_box(one.step_down_leader_if_lost_quorum());
            std::hint::black_box(one.leader_transfer_state().is_some());
        }
        let ns = began.elapsed().as_nanos() as f64 / CLUSTERS as f64;
        if ns < hammered {
            hammered = ns;
        }
    }

    let mut scattered = f64::MAX;
    for _ in 0..REPEATS {
        let began = Instant::now();
        for &i in &order {
            let cluster = &mut many[i];
            let _ = cluster.tick_leader_lease(1);
            cluster.tick_follower_lease(1);
            std::hint::black_box(cluster.tick_peer_liveness(1));
            std::hint::black_box(cluster.leader_id());
            let _ = cluster.broadcast_heartbeat();
            std::hint::black_box(cluster.tick_snapshot_trigger());
            let _ = cluster.broadcast_commit_index_to_old_paused_peers();
            std::hint::black_box(cluster.step_down_leader_if_lost_quorum());
            std::hint::black_box(cluster.leader_transfer_state().is_some());
        }
        let ns = began.elapsed().as_nanos() as f64 / CLUSTERS as f64;
        if ns < scattered {
            scattered = ns;
        }
    }

    println!("  one call each, {CLUSTERS} clusters in order    : {spread:>8.1} ns a row");
    println!("  the same, visited in a scattered order   : {scattered:>8.1} ns a row");
    println!("  {CLUSTERS} calls on one cluster             : {hammered:>8.1} ns a row");
    println!();
    println!(
        "  in order against hammered   : {:>5.1}x",
        spread / hammered
    );
    println!(
        "  scattered against in order  : {:>5.1}x",
        scattered / spread
    );
    println!(
        "  scattered against hammered  : {:>5.1}x",
        scattered / hammered
    );
    println!("  (in situ, cutting the tick body puts the same region at about 508 ns)");

    // The claim is the ratio, not any of the figures: the same work, the same
    // number of clusters, reached in a different order. A machine where this is
    // flat would be one where locality does not matter, and there is no such
    // machine -- so a small ratio here means the arms stopped differing, not that
    // the finding stopped being true.
    assert!(
        scattered > spread,
        "scattered access ({scattered:.1} ns) was not dearer than ordered ({spread:.1} ns), so the two orders are not reaching memory differently and this measures nothing"
    );
    // Only the ordering is asserted, and only with a slim margin. Over five release
    // runs the scattered arm cost 2.1 to 9.9 times the hammered one (median about
    // 7x) and 1.65 to 4.4 times the ordered one -- the direction never moved, the
    // size moved by a factor of five with how busy the box was. Pinning a
    // multiplier here would be pinning the neighbours' load.
    assert!(
        scattered > 1.15 * spread,
        "scattered access cost {scattered:.1} ns against {spread:.1} ns in allocation order, which is not the several-fold difference recorded. Either a group's tick state has become small enough to stop missing cache -- worth knowing -- or these two arms have stopped reaching memory differently."
    );
}
