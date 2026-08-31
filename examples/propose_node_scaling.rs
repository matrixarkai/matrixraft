// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Scaling probe: how does the cost of one proposal grow with the number of
//! nodes in the group?
//!
//! `propose_scaling` shows the per-propose cost is flat in *log length*. This
//! asks the other question. `RaftCluster` models every node in one object, so a
//! proposal appends the entry to each node's log -- if the payload is copied
//! per node, the cost per proposal grows with the group size, and the growth
//! shows up only at large payloads where the copy dominates the bookkeeping.
//!
//! Read the `us/entry-byte` column: if it is flat across group sizes the
//! payload is not being copied per node, and if it rises with the group the
//! copies are real.

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

fn run(nodes: u64, entries: usize, payload_bytes: usize) -> f64 {
    let peers: Vec<Peer> = (1..=nodes).map(peer).collect();
    let mut cluster = RaftCluster::new(7, Config::default(), peers).expect("valid cluster");
    cluster.start().expect("start");
    cluster.campaign(1, true).expect("campaign");

    let payload = vec![200_u8; payload_bytes];
    let started = Instant::now();
    for _ in 0..entries {
        cluster.propose(payload.clone()).expect("propose");
    }
    let elapsed = started.elapsed().as_secs_f64();
    assert!(cluster.leader_id().is_some());
    elapsed
}

fn main() {
    let payload_bytes: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(4096);
    let entries: usize = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(4000);
    println!("payload_bytes={payload_bytes}  entries={entries}");
    println!(
        "{:>6}  {:>10}  {:>12}  {:>14}  {:>8}",
        "nodes", "seconds", "us/propose", "ns/entry-byte", "vs 1 node"
    );
    let mut baseline: Option<f64> = None;
    for nodes in [1_u64, 3, 5, 7, 9] {
        // Odd sizes only: an even voter count changes the quorum arithmetic and
        // would confound the copy signal with a different commit path.
        let seconds = run(nodes, entries, payload_bytes);
        let us = seconds * 1e6 / entries as f64;
        let ns_per_byte = seconds * 1e9 / (entries as f64 * payload_bytes as f64);
        let ratio = baseline.map(|base| seconds / base).unwrap_or(1.0);
        println!("{nodes:>6}  {seconds:>10.4}  {us:>12.2}  {ns_per_byte:>14.2}  {ratio:>8.2}x");
        baseline.get_or_insert(seconds);
    }
}
