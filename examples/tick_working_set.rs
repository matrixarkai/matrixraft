// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a tick costs against how many groups a shard holds.
//!
//! A tick's work does not change with the group count. What changes is whether
//! the group it reaches is still in cache, so this runs the same calls the same
//! number of times in two orders: one group over and over, and a group apiece
//! across two thousand of them.
//!
//! An example rather than a test, deliberately. The direction reproduces and the
//! multiplier does not, so there is nothing here to assert that would not be a
//! flaky test rather than a finding. `tests/tick_locality.rs` is the assertion
//! this leaves to it, and it gates itself on whether the box can resolve the
//! comparison at all.
//!
//! Run it with:
//!
//! ```text
//! cargo run --release --example tick_working_set
//! ```

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};
use std::time::Instant;

fn voter(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 58_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 59_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// A single voter, which is its own quorum and so reaches leadership without a
/// transport. The tick path taken is the leader's, which is what a hosted group
/// runs.
fn leader(group_id: u64) -> RaftCluster {
    let mut cluster =
        RaftCluster::new(group_id, Config::default(), vec![voter(1)]).expect("a cluster");
    cluster.start().expect("start");
    cluster
        .campaign(1, false)
        .expect("a lone voter is its own quorum");
    cluster
}

/// What the hosted tick in `node_runtime` does to a cluster, in its order,
/// restricted to the calls reachable from outside the crate.
///
/// Left out, because the fields they read are crate-private: the
/// `received_live_leader_heartbeat` check, two `nodes` lookups, and the
/// lagging-peer scan, one walk of `nodes`. Both are cheap on this shape -- a
/// single voter, so one entry -- but a three-voter group pays more, and that is
/// not in these figures.
fn one_tick(cluster: &mut RaftCluster) {
    let node_id = 1;
    let interval = 100;
    let _ = cluster.tick_leader_lease(interval);
    cluster.tick_follower_lease(interval);
    if cluster.leader_id() == Some(node_id) {
        for _peer in cluster.tick_peer_liveness(interval) {}
    }
    let _ = cluster.broadcast_heartbeat();
    let _ = cluster.step_down_leader_if_lost_quorum();
    let _ = cluster.tick_snapshot_trigger();
    let _ = cluster.broadcast_commit_index_to_old_paused_peers();
    if cluster.leader_transfer_state().is_some() {
        let _ = cluster.try_complete_leader_transfer();
    }
}

fn main() {
    const SPREAD: usize = 2_000;
    const HAMMER: usize = 20_000;
    const ROUNDS: usize = 6;

    println!();
    println!(
        "size_of::<RaftCluster>() = {} bytes of inline state",
        std::mem::size_of::<RaftCluster>()
    );
    println!("(only the inline part: the maps and vectors it holds point elsewhere)");
    println!();
    println!("Same calls, same count, two orders. ABBA, {ROUNDS} rounds.");
    println!();

    let mut hammered: Vec<f64> = Vec::new();
    let mut spread: Vec<f64> = Vec::new();

    let hammer_once = || {
        let mut cluster = leader(1);
        let began = Instant::now();
        for _ in 0..HAMMER {
            one_tick(&mut cluster);
        }
        began.elapsed().as_nanos() as f64 / HAMMER as f64
    };
    let spread_once = || {
        let mut clusters: Vec<RaftCluster> = (0..SPREAD).map(|i| leader(i as u64 + 1)).collect();
        let began = Instant::now();
        for cluster in clusters.iter_mut() {
            one_tick(cluster);
        }
        let per = began.elapsed().as_nanos() as f64 / SPREAD as f64;
        drop(clusters);
        per
    };

    for round in 0..ROUNDS {
        if round % 2 == 0 {
            hammered.push(hammer_once());
            spread.push(spread_once());
            spread.push(spread_once());
            hammered.push(hammer_once());
        } else {
            spread.push(spread_once());
            hammered.push(hammer_once());
            hammered.push(hammer_once());
            spread.push(spread_once());
        }
    }

    let low = |v: &Vec<f64>| v.iter().copied().fold(f64::MAX, f64::min);
    let high = |v: &Vec<f64>| v.iter().copied().fold(0.0, f64::max);

    println!("{:>28}  {:>10}  {:>10}", "", "best", "worst");
    println!(
        "{:>28}  {:>7.1} ns  {:>7.1} ns",
        "one group, ticked in a row",
        low(&hammered),
        high(&hammered)
    );
    println!(
        "{:>28}  {:>7.1} ns  {:>7.1} ns",
        format!("{SPREAD} groups, one tick each"),
        low(&spread),
        high(&spread)
    );
    println!();
    println!(
        "  best against best  {:>5.2}x        worst against worst  {:>5.2}x",
        low(&spread) / low(&hammered),
        high(&spread) / high(&hammered)
    );
    println!();
    println!("The direction holds; the multiplier does not travel. Four runs on one");
    println!("Ryzen 5850U read 2.33x, 2.56x, 3.32x and 3.80x -- the hammered arm at");
    println!("47 to 65 ns, the spread arm at 126 to 248. Read the two columns");
    println!("together: when the worst of an arm is several times its own best, the");
    println!("box is not resolving the comparison and that ratio is a coin toss.");
    println!();
    println!("What the spread does NOT support is a located knee. Walking the group");
    println!("count from 32 to 16,384 gave a clean rise across per-core L2 on a quiet");
    println!("box and would not reproduce an hour later, with the worst of a row three");
    println!("to five times its own best -- more than the effect being looked for. So");
    println!("this reports two orders rather than a curve.");
    println!();
    println!("The groups are visited in the order they were built, which is the");
    println!("cheap order, and the driver visits them in registration order, so");
    println!("that matches. A scattered order costs more again, which");
    println!("tests/tick_locality.rs measures.");
    println!();
}
