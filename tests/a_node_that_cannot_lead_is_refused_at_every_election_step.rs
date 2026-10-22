// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A node that cannot lead is refused at *every* step of an election, not just
//! the first one.
//!
//! Learners and witnesses carry data but must never hold the leadership. That
//! single rule is enforced in seven separate places in the cluster runtime,
//! and five of them had no test: removing any one of the five left all 174
//! test targets green. Each is an independent entry point, so a rule held in
//! one place is not held in the others:
//!
//! | step | entry point |
//! |---|---|
//! | asking to campaign | `campaign` |
//! | being asked for a pre-vote | `pre_vote` |
//! | a peer being asked to pre-vote for it | `vote_to`, pre-vote arm |
//! | a peer being asked to vote for it | `vote_to`, vote arm |
//! | a pre-vote quorum arriving for it | `handle_vote_response_from` |
//!
//! **The witness is the case that carries the rule**, and it is why none of
//! this can be simplified to a membership count: a witness *does* count toward
//! a quorum, so a witness is a full participant in every election -- it simply
//! may not be the one elected. A test built only on learners would pass against
//! an engine that confused "takes part in elections" with "may win one".
//!
//! Every test below pairs the refusal with the same request made for a voter,
//! because a refusal that also stops a legitimate candidate is not a safety
//! rule, it is an outage.

use matrixraft::{
    Config, LogId, Peer, RaftCluster, RaftError, ReplicaRole, StateRole, VoteRequest, VoteResponse,
};

/// The two roles that may not lead. Both are exercised everywhere below; the
/// witness is the substantive one, since it counts toward quorum.
const CANNOT_LEAD: [ReplicaRole; 2] = [ReplicaRole::Learner, ReplicaRole::Witness];

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

/// Three voters plus node 4 in `role`, started. Node 4 is the candidate under
/// test and node 2 is the voter used as the control, so the two differ in
/// nothing but their role.
fn started_with(role: ReplicaRole) -> RaftCluster {
    let mut cluster = RaftCluster::new(
        7,
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
            peer(4, role),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
}

/// A vote request from `candidate_id` whose log is fresh enough to be granted,
/// so that any refusal below is about the candidate's role and nothing else.
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

fn cannot_become(error: &RaftError, what: &str) -> bool {
    matches!(error, RaftError::InvalidRequest(reason)
        if reason.contains("cannot become") && reason.contains(what))
}

#[test]
fn a_node_that_cannot_lead_cannot_campaign() {
    for role in CANNOT_LEAD {
        let mut cluster = started_with(role);
        let error = cluster
            .campaign(4, false)
            .expect_err("a node that cannot lead is refused a campaign");
        assert!(
            cannot_become(&error, "leader"),
            "{role:?} must be refused for its role, not for something incidental: {error:?}"
        );
    }

    // The control: the same call for a voter is allowed. Without this, the
    // test above would also pass against a runtime that refused everyone.
    let mut cluster = started_with(ReplicaRole::Witness);
    cluster
        .campaign(2, false)
        .expect("a voter may campaign, or the group could never elect");
}

#[test]
fn a_node_that_cannot_lead_is_refused_a_pre_vote() {
    for role in CANNOT_LEAD {
        let cluster = started_with(role);
        let response = cluster.pre_vote(4).expect("a pre-vote is answered");
        assert!(
            !response.vote_granted,
            "{role:?} must not be given a pre-vote"
        );
        assert_eq!(
            response.reason, "candidate_cannot_be_leader",
            "the refusal must name the role, so it cannot be confused with a stale log"
        );
    }

    let cluster = started_with(ReplicaRole::Witness);
    let granted = cluster.pre_vote(2).expect("a pre-vote is answered");
    assert!(
        granted.vote_granted,
        "a voter must be given a pre-vote: {granted:?}"
    );
}

#[test]
fn a_peer_will_not_pre_vote_for_a_candidate_that_cannot_lead() {
    for role in CANNOT_LEAD {
        let mut cluster = started_with(role);
        let response = cluster
            .vote_to(3, vote_request(4, true))
            .expect("a pre-vote request is answered");
        assert!(
            !response.vote_granted,
            "a peer must not pre-vote for {role:?}"
        );
        assert_eq!(response.reason, "candidate_cannot_be_leader");
    }

    // The same request, differing only in the candidate's role, is granted.
    let mut cluster = started_with(ReplicaRole::Witness);
    let granted = cluster
        .vote_to(3, vote_request(2, true))
        .expect("a pre-vote request is answered");
    assert!(
        granted.vote_granted,
        "the identical request for a voter is granted: {granted:?}"
    );
}

#[test]
fn a_peer_will_not_vote_for_a_candidate_that_cannot_lead() {
    for role in CANNOT_LEAD {
        let mut cluster = started_with(role);
        let response = cluster
            .vote_to(3, vote_request(4, false))
            .expect("a vote request is answered");
        assert!(!response.vote_granted, "a peer must not vote for {role:?}");
        assert_eq!(response.reason, "candidate_cannot_be_leader");
    }

    let mut cluster = started_with(ReplicaRole::Witness);
    let granted = cluster
        .vote_to(3, vote_request(2, false))
        .expect("a vote request is answered");
    assert!(
        granted.vote_granted,
        "the identical request for a voter is granted: {granted:?}"
    );
}

#[test]
fn a_pre_vote_quorum_does_not_make_a_non_candidate_into_one() {
    // The last step, and the one with real consequences: becoming a candidate
    // raises the term. A node that cannot lead must not raise the term of a
    // group it can never be elected in.
    for role in CANNOT_LEAD {
        let mut cluster = started_with(role);
        let term_before = cluster.status(4).expect("status").term;
        let role_before = cluster.status(4).expect("status").role;
        let granted = VoteResponse {
            term: term_before,
            vote_granted: true,
            reason: "granted".to_string(),
        };

        cluster
            .handle_vote_response_from(4, 1, granted.clone(), true)
            .expect("one response is short of a quorum");
        let error = cluster
            .handle_vote_response_from(4, 2, granted, true)
            .expect_err("the response that reaches a quorum is refused");
        assert!(
            cannot_become(&error, "candidate"),
            "{role:?} must be refused for its role: {error:?}"
        );

        let after = cluster.status(4).expect("status");
        assert_eq!(
            after.term, term_before,
            "a refused candidacy must not raise {role:?}'s term"
        );
        assert_eq!(
            after.role, role_before,
            "a refused candidacy must leave {role:?}'s raft role alone"
        );
    }

    // The control, and the reason this cannot be read as "quorum handling is
    // broken": the same two responses make a voter a candidate and move its
    // term, which is what an election is.
    let mut cluster = started_with(ReplicaRole::Witness);
    let term_before = cluster.status(2).expect("status").term;
    let granted = VoteResponse {
        term: term_before,
        vote_granted: true,
        reason: "granted".to_string(),
    };
    cluster
        .handle_vote_response_from(2, 1, granted.clone(), true)
        .expect("first response");
    cluster
        .handle_vote_response_from(2, 3, granted, true)
        .expect("the response that reaches a quorum is accepted for a voter");
    let after = cluster.status(2).expect("status");
    assert_eq!(
        after.role,
        StateRole::Candidate,
        "a voter reaching a pre-vote quorum becomes a candidate"
    );
    assert!(
        after.term > term_before,
        "and raises its term: {} is not past {term_before}",
        after.term
    );
}
