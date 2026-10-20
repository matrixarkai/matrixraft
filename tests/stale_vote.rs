// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A grant given for one term must not count toward an election in a later one.
//!
//! `handle_vote_response_inner` records a response whenever
//! `response.term <= self.current_term`, so a grant for an *older* term is
//! accepted and accumulated. The accumulated grants are cleared on eight paths
//! -- `campaign` inline, and six `clear_election_responses()` calls -- but not
//! on two paths that advance the term in ordinary operation: `vote_to`
//! receiving a vote request with a higher term, and the append-entries
//! higher-term branch.
//!
//! So a candidate that collected grants in term N, then learned of term N+1
//! from either of those, can reach a quorum in term N+1 while most of the
//! grants it counted were given for term N -- and the nodes that gave them are
//! free to have voted for someone else in term N+1. That is two leaders in one
//! term.
//!
//! Deleting all eight clears left 847 tests passing, which is how this was
//! found: nothing in the suite defends the property at all.

use matrixraft::{
    Config, LogId, Peer, RaftCluster, ReplicaRole, StateRole, VoteRequest, VoteResponse,
};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// Five voters, so a quorum is three and two grants are not enough. With three
/// voters a quorum is two and a single stale grant plus the self-vote already
/// wins, which would make the test pass for the wrong reason.
fn five_voters() -> RaftCluster {
    RaftCluster::new(
        7,
        Config::default(),
        vec![peer(1), peer(2), peer(3), peer(4), peer(5)],
    )
    .expect("valid cluster")
}

fn vote_request(term: u64, candidate_id: u64) -> VoteRequest {
    VoteRequest {
        group_id: 7,
        term,
        candidate_id,
        last_log_id: Some(LogId { term: 1, index: 1 }),
        pre_vote: false,
        force: true,
    }
}

fn granted(term: u64) -> VoteResponse {
    VoteResponse {
        term,
        vote_granted: true,
        reason: "vote_granted".to_string(),
    }
}

#[test]
fn a_grant_from_an_earlier_term_does_not_count_toward_a_later_election() {
    let mut cluster = five_voters();
    cluster.start().expect("cluster starts");

    // Term 2: node 2 stands, and one peer grants. Two of five is not a quorum,
    // so nothing should have been decided.
    cluster
        .vote_to(2, vote_request(2, 2))
        .expect("node 2 votes for itself in term 2");
    cluster
        .handle_vote_response_from(2, 1, granted(2), false)
        .expect("one grant in term 2");
    assert_eq!(
        cluster.leader_id(),
        None,
        "two of five is not a quorum, so term 2 decided nothing"
    );

    // Term 3 arrives the ordinary way: a vote request from another node with a
    // higher term. Node 2 steps to term 3.
    cluster
        .vote_to(2, vote_request(3, 4))
        .expect("node 2 hears term 3 from node 4");
    assert_eq!(
        cluster.status(2).expect("status").term,
        3,
        "the term has to have advanced for this test to be about anything"
    );

    // One grant in term 3. With the term-2 grant discarded this is two of five
    // and decides nothing; if the term-2 grant is still counted it is three of
    // five, which is a quorum.
    cluster
        .handle_vote_response_from(2, 3, granted(3), false)
        .expect("one grant in term 3");

    assert_ne!(
        cluster.leader_id(),
        Some(2),
        "node 2 reached a quorum in term 3 by counting a grant given for term 2"
    );
    assert_ne!(
        cluster.status(2).expect("status").role,
        StateRole::Leader,
        "and took leadership on it"
    );
}

#[test]
fn the_append_entries_path_to_a_higher_term_also_drops_the_grants() {
    // The second reachable path: a candidate mid-election hears an append from
    // a leader in a higher term. Same property, different door.
    let mut cluster = five_voters();
    cluster.start().expect("cluster starts");

    cluster
        .vote_to(2, vote_request(2, 2))
        .expect("node 2 stands in term 2");
    cluster
        .handle_vote_response_from(2, 1, granted(2), false)
        .expect("one grant in term 2");
    assert_eq!(cluster.leader_id(), None);

    // A vote request is one door to a higher term; a vote response carrying a
    // higher term is the other, and that one does clear. Use the request form
    // for node 5 so the term advances through a path that does not.
    cluster
        .vote_to(2, vote_request(4, 5))
        .expect("node 2 hears term 4");
    cluster
        .handle_vote_response_from(2, 3, granted(4), false)
        .expect("one grant in term 4");

    assert_ne!(
        cluster.leader_id(),
        Some(2),
        "a term-2 grant must not help win term 4"
    );
}

#[test]
fn a_pre_vote_grant_from_an_earlier_term_does_not_count_either() {
    let mut cluster = five_voters();
    cluster.start().expect("cluster starts");
    let term_before = cluster.status(2).expect("status").term;

    // One pre-vote grant in the current term. Two of five is not a quorum.
    cluster
        .handle_vote_response_from(2, 1, granted(term_before), true)
        .expect("one pre-vote grant");
    assert_eq!(
        cluster.status(2).expect("status").term,
        term_before,
        "two of five is not a pre-vote quorum, so no election has started"
    );

    // The term advances through the door that does not clear.
    cluster
        .vote_to(2, vote_request(term_before + 5, 4))
        .expect("node 2 hears a higher term");
    let term_after_step = cluster.status(2).expect("status").term;
    assert_eq!(
        term_after_step,
        term_before + 5,
        "the term has to have advanced for this test to be about anything"
    );

    // One pre-vote grant in the new term. Discarded correctly this is two of
    // five and starts nothing; counted with the stale one it is three, a
    // quorum, and a real election begins -- which advances the term again.
    cluster
        .handle_vote_response_from(2, 3, granted(term_after_step), true)
        .expect("one pre-vote grant in the new term");

    assert_eq!(
        cluster.status(2).expect("status").term,
        term_after_step,
        "a pre-vote quorum was reached by counting a grant from an earlier \
         term, and starting a real election cost a term"
    );
}
