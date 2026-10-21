// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A leadership transfer is refused unless the leader is healthy.
//!
//! `transfer_leader` reads `self.leader_id`, looks the node up, and then asks
//! two more things of it: that its role is `Leader`, and that it is healthy.
//! Planting each clause of that condition away against the whole suite left
//! both green, so neither was held by anything across 1,019 tests.
//!
//! Only one of the two turned out to be reachable, and this file covers that
//! one. MEASURED: with the leader marked unhealthy, `transfer_leader` answers
//! `Err(RaftError::NoLeader)` -- the guard fires -- and nothing noticed when
//! the clause was removed.
//!
//! The other clause, `leader.raft_role != StateRole::Leader`, is deliberately
//! **not** tested here, because I could not construct the state it guards: a
//! higher-term append moves `leader_id` to the new leader *and* sets that
//! node's role to `Leader`, so the two never disagreed, and a `leader_id` that
//! is `None` is refused by the `let Some(..) else` above it instead. It is
//! defence in depth against a state this engine does not appear to reach, and
//! a test asserting it would pass without exercising it.
//!
//! What the covered clause is worth: a transfer sends a `TimeoutNow` that makes
//! the target campaign. Driving that from a leader that is no longer healthy
//! starts an election on the word of a node that is not in a position to be
//! asking.

use matrixraft::{Peer, RaftCluster, RaftError, ReplicaRole};

const GROUP: u64 = 86;
const TARGET: u64 = 2;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 36_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 37_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started_cluster() -> RaftCluster {
    let mut cluster = RaftCluster::new(GROUP, Default::default(), vec![peer(1), peer(2), peer(3)])
        .expect("build the cluster");
    cluster.start().expect("start the cluster");
    cluster
}

#[test]
fn an_unhealthy_leader_cannot_hand_off_leadership() {
    let mut cluster = started_cluster();
    let leader = cluster.leader_id().expect("a leader after start");

    cluster
        .set_node_healthy(leader, false)
        .expect("mark the leader unhealthy");

    let error = cluster
        .transfer_leader(TARGET)
        .expect_err("an unhealthy leader must not drive a transfer");
    assert!(
        matches!(error, RaftError::NoLeader),
        "the refusal is that there is no leader to transfer from, got {error:?}"
    );
}

#[test]
fn a_healthy_leader_can_hand_off_leadership() {
    // The positive control, and it is load-bearing: without it, a guard that
    // refused every transfer would pass the test above.
    let mut cluster = started_cluster();
    assert!(
        cluster.leader_id().is_some(),
        "the premise -- there is a leader to transfer from"
    );

    cluster
        .transfer_leader(TARGET)
        .expect("a healthy leader hands off");
}
