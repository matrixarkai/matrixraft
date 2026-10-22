// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! An unhealthy node is refused everywhere it could still act.
//!
//! Health is the engine's record of whether a node is reachable and serving.
//! Ten places in the cluster runtime refuse something on the strength of it,
//! and **eight of them had no test at all**: removing any one of the eight left
//! all 176 test targets green. Only the two clauses in `begin_leader_transfer`
//! were already held.
//!
//! Each is a separate entry point with its own callers, so a rule held in one
//! place is not held in the others:
//!
//! | what is refused | entry point | observable |
//! |---|---|---|
//! | campaigning | `campaign` | `node N is not healthy` |
//! | becoming leader on a vote quorum | `handle_vote_response_from` | the same message, by a different route |
//! | becoming candidate on a pre-vote quorum | `handle_vote_response_from` | the same message again |
//! | being given a pre-vote | `pre_vote` | `candidate_unhealthy` |
//! | a peer's pre-vote | `vote_to`, pre-vote arm | `candidate_unhealthy` |
//! | a peer's vote | `vote_to`, vote arm | `candidate_unhealthy` |
//! | accepting a proposal | `propose_with_options` | `NotLeader` |
//! | serving a read | `read_index` | `node_unhealthy` |
//!
//! Three of those produce the *same* message by three different routes, which
//! is exactly why one test for the rule is not enough: the message proves a
//! refusal happened, and only the route proves *which* refusal.
//!
//! Every test pairs the refusal with the identical call made while the node is
//! healthy. Health is a liveness signal, so a rule that refused a healthy node
//! too would be an outage rather than a safeguard -- and `set_node_healthy`
//! would be unusable as the partition tool the rest of the suite relies on.

use matrixraft::{
    AppendEntriesResponse, Config, LogId, Peer, RaftCluster, RaftError, ReadIndexRequest,
    ReplicaRole, SnapshotState, StateRole, VoteRequest, VoteResponse,
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

/// Four voters, started. Node 4 is the one made unhealthy in most tests; a
/// fourth voter means node 4 can be taken out without costing the quorum,
/// so nothing below passes merely because the group went dark.
fn started() -> RaftCluster {
    let mut cluster = RaftCluster::new(
        7,
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
            peer(4, ReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
}

/// A vote request whose log is fresh enough to be granted, so a refusal is
/// about the candidate's health and nothing else.
fn vote_request(candidate_id: u64, pre_vote: bool) -> VoteRequest {
    VoteRequest {
        group_id: 7,
        term: 9,
        candidate_id,
        last_log_id: Some(LogId { term: 1, index: 1 }),
        pre_vote,
        force: false,
    }
}

fn granted_response() -> VoteResponse {
    VoteResponse {
        term: 1,
        vote_granted: true,
        reason: "granted".to_string(),
    }
}

/// Have every peer acknowledge the entry `start()` wrote, so the leader's
/// commit and applied indexes reach it. Without this a read is refused for
/// being behind rather than for anything to do with health.
fn acknowledge_the_first_entry(cluster: &mut RaftCluster, leader: u64) {
    let term = cluster.status(leader).expect("leader status").term;
    for peer_id in [2u64, 3, 4] {
        if peer_id == leader {
            continue;
        }
        cluster.catch_up_peer(peer_id).expect("replicate to peer");
        cluster
            .handle_append_entries_response(
                leader,
                peer_id,
                AppendEntriesResponse {
                    term,
                    success: true,
                    match_index: 1,
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
    assert_eq!(
        cluster.status(leader).expect("leader status").applied_index,
        1,
        "the premise: the group has applied its first entry"
    );
}

fn read_by(node_id: u64) -> ReadIndexRequest {
    ReadIndexRequest {
        group_id: 7,
        requester_id: node_id,
        min_commit_index: 0,
        allow_lease_read: false,
    }
}

fn is_unhealthy_refusal(error: &RaftError, node_id: u64) -> bool {
    matches!(error, RaftError::InvalidRequest(reason)
        if reason.contains(&node_id.to_string()) && reason.contains("not healthy"))
}

#[test]
fn an_unhealthy_node_cannot_campaign() {
    let mut cluster = started();
    cluster
        .set_node_healthy(4, false)
        .expect("node 4 goes dark");

    let error = cluster
        .campaign(4, false)
        .expect_err("an unhealthy node is refused a campaign");
    assert!(
        is_unhealthy_refusal(&error, 4),
        "the refusal must be about health: {error:?}"
    );

    // The control: healed, the identical call is allowed.
    cluster
        .set_node_healthy(4, true)
        .expect("node 4 comes back");
    cluster
        .campaign(4, false)
        .expect("a healthy voter may campaign");
}

#[test]
fn an_unhealthy_node_is_not_made_leader_by_a_vote_quorum() {
    let mut cluster = started();
    cluster
        .set_node_healthy(4, false)
        .expect("node 4 goes dark");

    cluster
        .handle_vote_response_from(4, 1, granted_response(), false)
        .expect("one response is short of a quorum");
    let error = cluster
        .handle_vote_response_from(4, 2, granted_response(), false)
        .expect_err("the response reaching a quorum is refused");
    assert!(
        is_unhealthy_refusal(&error, 4),
        "the refusal must be about health: {error:?}"
    );
    assert_ne!(
        cluster.leader_id(),
        Some(4),
        "an unhealthy node must not end up holding the leadership"
    );

    // The control, and the reason this is not just broken quorum handling: a
    // healthy node reaching the same quorum does become the leader.
    let mut healthy = started();
    healthy
        .handle_vote_response_from(4, 1, granted_response(), false)
        .expect("first response");
    healthy
        .handle_vote_response_from(4, 2, granted_response(), false)
        .expect("a healthy node reaching a vote quorum becomes leader");
    assert_eq!(
        healthy.leader_id(),
        Some(4),
        "the same two responses elect a healthy node"
    );
}

#[test]
fn an_unhealthy_node_is_not_made_candidate_by_a_pre_vote_quorum() {
    let mut cluster = started();
    cluster
        .set_node_healthy(4, false)
        .expect("node 4 goes dark");
    let term_before = cluster.status(4).expect("status").term;

    cluster
        .handle_vote_response_from(4, 1, granted_response(), true)
        .expect("one response is short of a quorum");
    let error = cluster
        .handle_vote_response_from(4, 2, granted_response(), true)
        .expect_err("the response reaching a quorum is refused");
    assert!(
        is_unhealthy_refusal(&error, 4),
        "the refusal must be about health: {error:?}"
    );
    assert_eq!(
        cluster.status(4).expect("status").term,
        term_before,
        "a refused candidacy must not raise an unhealthy node's term"
    );

    let mut healthy = started();
    healthy
        .handle_vote_response_from(4, 1, granted_response(), true)
        .expect("first response");
    healthy
        .handle_vote_response_from(4, 2, granted_response(), true)
        .expect("a healthy node reaching a pre-vote quorum becomes a candidate");
    assert_eq!(
        healthy.status(4).expect("status").role,
        StateRole::Candidate,
        "the same two responses make a healthy node a candidate"
    );
}

#[test]
fn an_unhealthy_node_is_refused_a_pre_vote() {
    let mut cluster = started();
    cluster
        .set_node_healthy(4, false)
        .expect("node 4 goes dark");

    let refused = cluster.pre_vote(4).expect("a pre-vote is answered");
    assert!(!refused.vote_granted, "an unhealthy node gets no pre-vote");
    assert_eq!(
        refused.reason, "candidate_unhealthy",
        "the refusal must name health, not a stale log or a prohibited election"
    );

    cluster
        .set_node_healthy(4, true)
        .expect("node 4 comes back");
    let granted = cluster.pre_vote(4).expect("a pre-vote is answered");
    assert!(
        granted.vote_granted,
        "a healthy voter is given a pre-vote: {granted:?}"
    );
}

#[test]
fn a_peer_will_not_pre_vote_for_an_unhealthy_candidate() {
    let mut cluster = started();
    cluster
        .set_node_healthy(4, false)
        .expect("node 4 goes dark");

    let refused = cluster
        .vote_to(3, vote_request(4, true))
        .expect("a pre-vote request is answered");
    assert!(!refused.vote_granted, "a peer must not pre-vote for it");
    assert_eq!(refused.reason, "candidate_unhealthy");

    // The same request, differing only in the candidate's health.
    cluster
        .set_node_healthy(4, true)
        .expect("node 4 comes back");
    let granted = cluster
        .vote_to(3, vote_request(4, true))
        .expect("a pre-vote request is answered");
    assert!(
        granted.vote_granted,
        "the identical request for a healthy candidate is granted: {granted:?}"
    );
}

#[test]
fn a_peer_will_not_vote_for_an_unhealthy_candidate() {
    let mut cluster = started();
    cluster
        .set_node_healthy(4, false)
        .expect("node 4 goes dark");

    let refused = cluster
        .vote_to(3, vote_request(4, false))
        .expect("a vote request is answered");
    assert!(!refused.vote_granted, "a peer must not vote for it");
    assert_eq!(refused.reason, "candidate_unhealthy");

    cluster
        .set_node_healthy(4, true)
        .expect("node 4 comes back");
    let granted = cluster
        .vote_to(3, vote_request(4, false))
        .expect("a vote request is answered");
    assert!(
        granted.vote_granted,
        "the identical request for a healthy candidate is granted: {granted:?}"
    );
}

#[test]
fn an_unhealthy_leader_does_not_accept_a_proposal() {
    let mut cluster = started();
    let leader = cluster.leader_id().expect("a leader");

    cluster
        .set_node_healthy(leader, false)
        .expect("the leader goes dark");
    // It is still the leader -- which is the point. The refusal below is the
    // health rule talking, not the absence of a leader, and asserting this
    // first is what separates the two.
    assert_eq!(
        cluster.leader_id(),
        Some(leader),
        "an unhealthy leader is still the leader on record"
    );

    let error = cluster
        .propose(b"written-by-a-leader-that-cannot-serve".to_vec())
        .expect_err("an unhealthy leader is refused a proposal");
    assert!(
        matches!(error, RaftError::NotLeader(id) if id == leader),
        "the refusal must name the leader that cannot serve: {error:?}"
    );

    cluster
        .set_node_healthy(leader, true)
        .expect("the leader comes back");
    cluster
        .propose(b"written-once-it-can-serve-again".to_vec())
        .expect("a healthy leader accepts a proposal");
}

#[test]
fn an_unhealthy_node_is_refused_a_read() {
    let mut cluster = started();
    // The reader has to be the leader. A follower's read is refused as
    // `not_leader` one line further down, so a follower could not tell the
    // health refusal from the ordinary one -- and the health check comes
    // first, which is the ordering this pins.
    let leader = cluster.leader_id().expect("a leader");
    // And the group has to have acknowledged its first entry, or the control
    // below is refused as `applied_index_behind_read_index` and proves
    // nothing about health.
    acknowledge_the_first_entry(&mut cluster, leader);

    cluster
        .set_node_healthy(leader, false)
        .expect("the leader goes dark");

    let refused = cluster
        .read_index(read_by(leader))
        .expect("a read is answered");
    assert!(
        !refused.safe,
        "a node that cannot be reached must not report a safe read"
    );
    assert_eq!(
        refused.reason, "node_unhealthy",
        "the refusal must name health, not a quorum or a fence"
    );
    assert_eq!(
        refused.read_index,
        cluster.status(leader).expect("leader status").commit_index,
        "a refused read reports where the node stands, never a point ahead of it"
    );

    cluster
        .set_node_healthy(leader, true)
        .expect("the leader comes back");
    let allowed = cluster
        .read_index(read_by(leader))
        .expect("a read is answered");
    assert!(
        allowed.safe,
        "a healthy leader in a healthy group may read: {allowed:?}"
    );
}
