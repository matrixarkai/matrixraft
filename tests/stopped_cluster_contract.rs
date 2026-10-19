// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a stopped cluster refuses, and what it deliberately does not.
//!
//! `stop` sets a flag, and for most of this type's life almost nothing read
//! it: of 121 public operations on `RaftCluster`, `propose` and `campaign`
//! were the only two that refused. A host that stopped a group could still add
//! a peer to it, promote one, resign its leader or trigger a snapshot, and the
//! operation was carried out on a cluster whose leader lease had already been
//! invalidated.
//!
//! The comparable engine gates this at the node: every client-facing call
//! takes a pass, `Stop` shuts the gate, and a call that arrives afterwards is
//! answered "raft node is not started" rather than served.
//!
//! This file is the boundary written down. Both halves matter -- a list of
//! what refuses is only trustworthy next to a list of what does not, and each
//! entry in the second list has a reason.

use matrixraft::{
    AppendEntriesRequest, Config, LogId, Peer, RaftCluster, RaftError, ReadIndexRequest,
    ReplicaRole,
};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 31_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 32_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// A started cluster with one entry in it, then stopped.
fn stopped() -> RaftCluster {
    let mut cluster =
        RaftCluster::new(7, Config::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster
        .propose(b"before the stop".to_vec())
        .expect("propose");
    cluster.stop().expect("stop");
    cluster
}

fn refused(result: Result<impl std::fmt::Debug, RaftError>, operation: &str) {
    match result {
        Err(RaftError::InvalidRequest(message)) => assert!(
            message.contains("while cluster is stopped"),
            "{operation} should be refused for being stopped, got {message:?}"
        ),
        other => panic!("{operation} on a stopped cluster should be refused, got {other:?}"),
    }
}

#[test]
fn a_stopped_cluster_refuses_client_operations() {
    refused(stopped().propose(b"after".to_vec()), "propose");
    refused(
        stopped().read_index(ReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 0,
            allow_lease_read: false,
        }),
        "read_index",
    );
    refused(stopped().trigger_snapshot(), "trigger_snapshot");
    refused(stopped().add_peer(peer(4)), "add_peer");
    refused(stopped().add_learner(peer(4)), "add_learner");
    refused(stopped().add_witness(peer(4)), "add_witness");
    refused(stopped().remove_peer(2), "remove_peer");
    refused(stopped().promote_peer(2), "promote_peer");
    refused(stopped().resign_leader("test"), "resign_leader");
}

#[test]
fn a_stopped_cluster_still_drains_what_is_in_flight() {
    // Inbound message handling is deliberately **not** gated. It is how a host
    // settles what is already on the wire, and refusing it would strand those
    // messages rather than finish them.
    let mut cluster = stopped();
    let last = cluster.status(2).expect("status").last_log_index;
    let response = cluster
        .append_entries_to(
            2,
            AppendEntriesRequest {
                group_id: 7,
                term: cluster.status(2).expect("status").term,
                leader_id: cluster.leader_id().expect("leader"),
                prev_log_id: Some(LogId {
                    term: cluster.status(2).expect("status").term,
                    index: last,
                }),
                entries: Vec::new(),
                leader_commit: last,
                lease_epoch: 0,
            },
        )
        .expect("a stopped cluster still answers an append");
    assert!(response.success);
}

#[test]
fn a_stopped_cluster_can_still_be_read_and_restarted() {
    // Inspection is not gated either: a stopped cluster is exactly when a host
    // wants to know what it holds. And `start` has to work on one, or stopping
    // would be one-way.
    let mut cluster = stopped();
    assert!(cluster.leader_id().is_some());
    assert!(cluster.status(1).is_ok());
    assert!(cluster.membership().voters.len() >= 3);

    cluster.start().expect("a stopped cluster restarts");
    cluster
        .propose(b"after the restart".to_vec())
        .expect("and serves again");
}

#[test]
fn leader_transfer_is_not_gated_and_this_is_why() {
    // `transfer_leader` reports `NoLeader` on a cluster that was never
    // started, and a **running** cluster always has a leader -- `start`
    // campaigns one. So gating the transfer would make `RaftError::NoLeader`
    // unreachable from this entry point and silently replace a tested contract
    // with a dead branch.
    //
    // That is a change worth making deliberately, with the dead path cleaned
    // up, rather than as a side effect of adding a gate. Until then the
    // behaviour is pinned here so nobody has to rediscover the reason.
    let mut never_started =
        RaftCluster::new(7, Config::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    assert_eq!(
        never_started.transfer_leader(2),
        Err(RaftError::NoLeader),
        "the error that gating would make unreachable"
    );

    // And on a stopped-after-running cluster it is not the stopped error
    // either: it proceeds, because the gate is not there.
    let mut cluster = stopped();
    let outcome = cluster.transfer_leader(2);
    if let Err(RaftError::InvalidRequest(message)) = outcome {
        assert!(
            !message.contains("while cluster is stopped"),
            "leader transfer is deliberately ungated, got {message:?}"
        );
    }
}
