// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Scaling probe: proposes N entries into a 3-node cluster and reports the
//! wall time per size. A per-propose cost that is constant in log length gives
//! a ~2x time ratio when N doubles; a cost that is linear in log length gives
//! ~4x.

use std::time::Instant;

use matrixraft::{RaftCluster, RaftConfig, RustRaftPeer, RustRaftReplicaRole};

fn peer(node_id: u64, role: RustRaftReplicaRole) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn run(entries: usize, payload_bytes: usize) -> f64 {
    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("start");
    cluster.campaign(1, true).expect("campaign");

    let payload = vec![7_u8; payload_bytes];
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
        .unwrap_or(64);
    println!("payload_bytes={payload_bytes}");
    println!(
        "{:>8}  {:>12}  {:>12}  {:>8}",
        "entries", "seconds", "us/propose", "ratio"
    );
    let mut previous: Option<(usize, f64)> = None;
    for entries in [1000_usize, 2000, 4000, 8000, 16000] {
        let seconds = run(entries, payload_bytes);
        let per = seconds * 1e6 / entries as f64;
        let ratio = previous
            .map(|(_, prev_seconds)| seconds / prev_seconds)
            .unwrap_or(f64::NAN);
        println!("{entries:>8}  {seconds:>12.4}  {per:>12.2}  {ratio:>8.2}");
        previous = Some((entries, seconds));
    }
}
