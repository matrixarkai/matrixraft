// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Lagging-peer replication yields after its configured entry budget.

use matrixraft::{Config, Peer, PipelineLimits, RaftCluster, ReplicaRole};

fn voter(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 53_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 54_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

#[test]
fn catchup_advances_by_the_configured_entry_budget() {
    let config = Config::default().with_pipeline_limits(PipelineLimits {
        max_replicate_batch_entries: 3,
        ..PipelineLimits::default()
    });
    let mut cluster = RaftCluster::new(7, config, (1..=3).map(voter).collect()).expect("cluster");
    cluster.start().expect("start");
    cluster.set_node_healthy(2, false).expect("isolate peer");
    for value in 0..8_u8 {
        cluster.propose(vec![value]).expect("propose");
    }
    let leader_index = cluster.status(1).expect("leader status").last_log_index;
    let isolated_index = cluster.status(2).expect("peer status").last_log_index;
    assert_eq!(leader_index - isolated_index, 8);
    cluster.set_node_healthy(2, true).expect("heal peer");

    let first = cluster.catch_up_peer(2).expect("first round");
    assert_eq!(first.learner_match_index_after - isolated_index, 3);
    assert!(!first.caught_up);

    let second = cluster.catch_up_peer(2).expect("second round");
    assert_eq!(
        second.learner_match_index_after - first.learner_match_index_after,
        3
    );
    assert!(!second.caught_up);

    let third = cluster.catch_up_peer(2).expect("third round");
    assert_eq!(
        third.learner_match_index_after - second.learner_match_index_after,
        2
    );
    assert!(third.caught_up);
    assert_eq!(third.learner_match_index_after, leader_index);
}
