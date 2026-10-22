// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A group cannot be built out of nodes that could never lead.
//!
//! `RaftCluster::new` refuses a membership with no voter in it, and nothing held
//! that -- planting the guard away let a group be constructed that can never
//! elect a leader, so every propose and every leader read would fail forever
//! with no way to recover except rebuilding the group.
//!
//! **The witness case is the one worth having.** A witness
//! `participates_in_quorum()` but `can_be_leader()` is false, so a
//! witness-only group would have a perfectly good quorum and no candidate it
//! could ever vote for. Counting quorum members is not the same question as
//! "could anyone lead", and only the second one makes a group viable.
//!
//! Found by sweeping the engine's single-clause refusals -- the guards with no
//! `&&` in them, which an earlier sweep of multi-clause conditions never
//! listed.

use matrixraft::{Peer, RaftCluster, ReplicaRole};

const GROUP: u64 = 98;

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 56_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 57_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn build(peers: Vec<Peer>) -> Result<RaftCluster, matrixraft::RaftError> {
    RaftCluster::new(GROUP, Default::default(), peers)
}

fn assert_refused_for_want_of_a_voter(peers: Vec<Peer>, what: &str) {
    let error = build(peers).expect_err(&format!(
        "a group of {what} has nobody who could ever lead, so it must be refused"
    ));
    assert!(
        error.to_string().contains("requires at least one voter"),
        "and the error has to say why: {error}"
    );
}

#[test]
fn a_group_of_only_learners_is_refused() {
    assert_refused_for_want_of_a_voter(
        vec![
            peer(1, ReplicaRole::Learner),
            peer(2, ReplicaRole::Learner),
            peer(3, ReplicaRole::Learner),
        ],
        "learners",
    );
}

#[test]
fn a_group_of_only_witnesses_is_refused_despite_having_a_quorum() {
    // The case that distinguishes the two questions: witnesses DO participate
    // in the quorum, so this group could count votes all day. It just has
    // nobody to vote for.
    assert_refused_for_want_of_a_voter(
        vec![
            peer(1, ReplicaRole::Witness),
            peer(2, ReplicaRole::Witness),
            peer(3, ReplicaRole::Witness),
        ],
        "witnesses",
    );
}

#[test]
fn a_single_learner_is_refused_too() {
    // The degenerate size, in case a one-node group ever takes a shortcut.
    assert_refused_for_want_of_a_voter(vec![peer(1, ReplicaRole::Learner)], "one learner");
}

#[test]
fn one_voter_among_learners_is_enough() {
    // The positive control, and it is what stops this being satisfied by a
    // guard that refuses every membership: one node that could lead is the
    // whole requirement, not a majority of them.
    let cluster = build(vec![
        peer(1, ReplicaRole::Voter),
        peer(2, ReplicaRole::Learner),
        peer(3, ReplicaRole::Learner),
    ])
    .expect("one voter is enough to build a group");
    assert_eq!(
        cluster.status(1).expect("the voter's status").group_id,
        GROUP,
        "and the group it built is the one asked for"
    );
}
