// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A learner joins the quorum only once it has the committed log.
//!
//! Promotion is not a label change: the moment a learner becomes a voter it
//! counts toward every quorum the group computes -- commit, election and the
//! leader lease. Promoting one that is behind therefore raises the bar for
//! committing anything while adding a member that cannot yet acknowledge, and
//! hands an election vote to a node whose log is stale.
//!
//! `promote_peer` refuses exactly that, and nothing pinned it: with the check
//! removed, all 179 test targets stayed green.
//!
//! The partition is what makes the test possible at all. A learner in a healthy
//! group is caught up by ordinary replication before anything can ask it to be
//! promoted -- the first version of this test promoted successfully and proved
//! nothing. Taking the learner out of contact first is what produces the state
//! the rule exists for.

use matrixraft::{
    AppendEntriesResponse, Config, Peer, RaftCluster, RaftError, ReplicaRole, SnapshotState,
    StateRole,
};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

/// Three voters and a learner, started.
fn started() -> RaftCluster {
    let mut cluster = RaftCluster::new(
        7,
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
            peer(4, ReplicaRole::Learner),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
}

/// Commit three entries with the learner out of contact, so the group's
/// committed log runs ahead of what node 4 holds. Returns the leader and the
/// committed index.
fn commit_without_the_learner(cluster: &mut RaftCluster) -> (u64, u64) {
    let leader = cluster.leader_id().expect("a leader");
    let term = cluster.status(leader).expect("leader status").term;
    cluster
        .set_node_healthy(4, false)
        .expect("the learner goes out of contact");

    for _ in 0..3 {
        cluster.propose(b"payload".to_vec()).expect("propose");
    }
    let last = cluster
        .status(leader)
        .expect("leader status")
        .last_log_index;
    for peer_id in [2u64, 3] {
        cluster.catch_up_peer(peer_id).expect("replicate to peer");
        cluster
            .handle_append_entries_response(
                leader,
                peer_id,
                AppendEntriesResponse {
                    term,
                    success: true,
                    match_index: last,
                    rejection_hint: None,
                    rejected_index: None,
                    require_snapshot: None,
                    snapshot_state: SnapshotState::None,
                    lease_confirmation_epoch: 0,
                    lease_duration_ms: 0,
                },
            )
            .expect("quorum acknowledgement");
    }

    let commit_index = cluster.status(leader).expect("leader status").commit_index;
    assert_eq!(
        commit_index, last,
        "the voters alone are a quorum, so the entries commit without the learner"
    );
    assert!(
        cluster.status(4).expect("learner status").last_log_index < commit_index,
        "the premise: the learner really is behind the committed log"
    );
    (leader, commit_index)
}

#[test]
fn a_learner_behind_the_committed_log_is_not_promoted() {
    let mut cluster = started();
    let (_leader, commit_index) = commit_without_the_learner(&mut cluster);

    let error = cluster
        .promote_peer(4)
        .expect_err("a learner behind the committed log is refused promotion");
    assert!(
        matches!(&error, RaftError::InvalidRequest(reason)
            if reason.contains("behind committed index")),
        "the refusal must name why, so it cannot be confused with the role check: {error:?}"
    );

    assert_eq!(
        cluster.status(4).expect("learner status").role,
        StateRole::Learner,
        "and the refusal must leave it a learner"
    );
    assert!(
        !cluster.membership().voters.contains(&4),
        "a refused promotion must not put it in the voter set, where it would \
         count toward every quorum the group computes"
    );
    assert!(
        cluster.membership().learners.contains(&4),
        "it stays where it was"
    );
    let _ = commit_index;
}

#[test]
fn a_learner_that_has_caught_up_is_promoted() {
    // The control. Without it the test above would also pass against an engine
    // that never promoted anything -- and promotion is how a group grows.
    let mut cluster = started();
    let (leader, commit_index) = commit_without_the_learner(&mut cluster);

    cluster
        .set_node_healthy(4, true)
        .expect("the learner comes back");
    cluster
        .catch_up_peer(4)
        .expect("and is caught up from the log");
    assert_eq!(
        cluster.status(4).expect("learner status").last_log_index,
        commit_index,
        "caught up means exactly level with the committed log, which is the \
         boundary the rule allows -- not past it"
    );

    cluster
        .promote_peer(4)
        .expect("a caught-up learner may be promoted");

    assert!(
        cluster.membership().voters.contains(&4),
        "and it joins the voters"
    );
    assert!(
        !cluster.membership().learners.contains(&4),
        "leaving the learners"
    );
    assert_eq!(
        cluster.status(leader).expect("leader status").commit_index,
        commit_index,
        "promoting a caught-up member costs the group nothing already committed"
    );
}

#[test]
fn only_a_learner_can_be_promoted() {
    // The sibling rule in the same function, and the reason the refusal above
    // asserts its message: both return the same error type, so a test that
    // only checked for an error could not tell which rule answered.
    let mut cluster = started();
    commit_without_the_learner(&mut cluster);

    let error = cluster
        .promote_peer(2)
        .expect_err("a voter is not a candidate for promotion");
    assert!(
        matches!(&error, RaftError::InvalidRequest(reason)
            if reason.contains("is not a learner")),
        "the refusal must name the role, not the log: {error:?}"
    );
    assert!(
        cluster.membership().voters.contains(&2),
        "and it stays a voter"
    );
}
