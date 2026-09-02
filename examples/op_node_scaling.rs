// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Scaling probe across operations, not just proposals.
//!
//! `propose_node_scaling` found that a proposal recomputed two group-wide
//! aggregates once per peer, making it O(N^2) in group size. That shape is not
//! specific to proposing: any operation that loops over peers and recomputes
//! something group-wide inside the loop has it. This times several operations
//! against group size so the same signature shows up if it is there.
//!
//! Read the growth column. An operation that touches every peer should grow
//! about linearly with the group; markedly faster than linear is worth reading
//! the code for.

use std::time::Instant;

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn cluster_of(nodes: u64) -> RaftCluster {
    let peers: Vec<Peer> = (1..=nodes).map(peer).collect();
    let mut cluster = RaftCluster::new(7, Config::default(), peers).expect("valid cluster");
    cluster.start().expect("start");
    cluster.campaign(1, true).expect("campaign");
    cluster
}

fn time_op(nodes: u64, iterations: usize, op: &str) -> f64 {
    let mut cluster = cluster_of(nodes);
    // Give every operation a log to work against.
    for _ in 0..200 {
        cluster.propose(vec![7u8; 64]).expect("seed propose");
    }
    let started = Instant::now();
    for i in 0..iterations {
        match op {
            "tick" => {
                let _ = cluster.tick_peer_liveness(1);
            }
            "status" => {
                let _ = cluster.status(1);
            }
            "membership" => {
                let _ = cluster.membership();
            }
            "heartbeat" => {
                let _ = cluster.tick_leader_lease(1);
            }
            "propose" => {
                cluster.propose(vec![7u8; 64]).expect("propose");
            }
            other => panic!("unknown op {other}"),
        }
        std::hint::black_box(i);
    }
    started.elapsed().as_secs_f64()
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(4000);
    println!("iterations={iterations}");
    for op in ["propose", "tick", "heartbeat", "status", "membership"] {
        println!("\n--- {op}");
        println!(
            "{:>6}  {:>10}  {:>12}  {:>10}",
            "nodes", "seconds", "us/op", "vs 1 node"
        );
        let mut baseline: Option<f64> = None;
        for nodes in [1_u64, 3, 5, 7, 9] {
            let seconds = time_op(nodes, iterations, op);
            let us = seconds * 1e6 / iterations as f64;
            let ratio = baseline.map(|base| seconds / base).unwrap_or(1.0);
            println!("{nodes:>6}  {seconds:>10.4}  {us:>12.3}  {ratio:>9.2}x");
            baseline.get_or_insert(seconds);
        }
    }
}
