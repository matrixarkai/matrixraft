// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A term at its largest representable value is refused, not overflowed.
//!
//! Raft puts no upper bound on a term, and this engine adopts a higher term
//! straight off the wire -- `if request.term > self.current_term` then
//! `self.current_term = request.term`. So **one peer message** is enough to
//! park `current_term` at `u64::MAX`, and the two places that then add one to
//! it were unguarded:
//!
//! - `campaign`, which did `self.current_term += 1`
//! - `pre_vote`, which reported `self.current_term + 1` as the granted term
//!
//! Measured before the fix: a single `vote_to` carrying `u64::MAX` and the
//! next call to either one panicked with "attempt to add with overflow". A
//! debug build takes the panic; a release build is worse, wrapping the term
//! round to **0** and campaigning there.
//!
//! **Why refusing, and not saturating.** A saturated pre-vote would report
//! `current_term` as the term it granted -- a vote for a term that has already
//! been used -- and a saturated campaign would re-enter its own current term.
//! Clamping is right for a cursor and wrong for a term the rest of the
//! protocol compares against, so both sites refuse instead.

use matrixraft::{Config, LogId, Peer, RaftCluster, ReplicaRole, VoteRequest};

const GROUP_ID: u64 = 171;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 41_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 41_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started() -> RaftCluster {
    let mut cluster =
        RaftCluster::new(GROUP_ID, Config::default(), vec![peer(1), peer(2), peer(3)])
            .expect("build the cluster");
    cluster.start().expect("start the cluster");
    cluster
}

/// Adopt `term` the way a peer does: one vote request carrying it.
fn drive_term_to(cluster: &mut RaftCluster, term: u64) {
    let response = cluster
        .vote_to(
            1,
            VoteRequest {
                group_id: GROUP_ID,
                term,
                candidate_id: 2,
                last_log_id: Some(LogId { term: 1, index: 1 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("the vote is answered");
    assert!(
        response.vote_granted,
        "the setup depends on this term being adopted: {response:?}"
    );
}

#[test]
fn one_peer_message_is_enough_to_reach_the_largest_term() {
    // The premise of every test below. Nothing bounds an incoming term, and
    // `force` is not needed -- this is an ordinary vote request.
    let mut cluster = started();
    drive_term_to(&mut cluster, u64::MAX);
    let response = cluster.pre_vote(2).expect("the pre-vote is answered");
    assert_eq!(
        response.term,
        u64::MAX,
        "the node is sitting at the ceiling, which is what makes the \
         increments below reachable: {response:?}"
    );
}

#[test]
fn the_pre_vote_refuses_a_term_it_cannot_represent() {
    let mut cluster = started();
    drive_term_to(&mut cluster, u64::MAX);

    let response = cluster.pre_vote(2).expect("answered, not panicked");
    assert!(
        !response.vote_granted,
        "there is no next term to campaign in, so the pre-vote cannot be \
         granted: {response:?}"
    );
    assert_eq!(response.reason, "term_exhausted");
    assert_eq!(
        response.term,
        u64::MAX,
        "and the reported term is the one the node actually holds, not a \
         wrapped 0: {response:?}"
    );
}

#[test]
fn the_campaign_refuses_a_term_it_cannot_represent() {
    let mut cluster = started();
    drive_term_to(&mut cluster, u64::MAX);

    // Node 2, not node 1: node 1 is already the leader, and `campaign`
    // returns early for the sitting leader without reaching its term bump.
    let error = cluster
        .campaign(2, false)
        .expect_err("a campaign that cannot increment the term has to fail");
    let message = error.to_string();
    assert!(
        message.contains("largest representable value"),
        "the refusal has to name why it refused: {message}"
    );
}

#[test]
fn a_forced_campaign_is_refused_at_the_ceiling_too() {
    // `forced` skips the prohibited-election and lease guards, so it reaches
    // the term bump by a different route; most of this crate's own callers
    // pass `true`.
    let mut cluster = started();
    drive_term_to(&mut cluster, u64::MAX);
    assert!(
        cluster.campaign(2, true).is_err(),
        "a forced campaign still cannot invent a term"
    );
}

#[test]
fn the_refusals_are_stable_and_leave_the_node_answering() {
    // A refusal that corrupted state on the way out would show up as a second
    // call behaving differently, or as the node no longer answering at all.
    let mut cluster = started();
    drive_term_to(&mut cluster, u64::MAX);

    assert!(cluster.campaign(2, false).is_err());
    assert!(
        cluster.campaign(2, false).is_err(),
        "the second refusal has to match the first"
    );
    let response = cluster.pre_vote(2).expect("still answering");
    assert_eq!(
        response.reason, "term_exhausted",
        "and the pre-vote still reports the same thing after two refused \
         campaigns: {response:?}"
    );
}

#[test]
fn an_ordinary_pre_vote_still_grants_the_next_term() {
    // The positive control for the pre-vote arm. Without it, a pre-vote that
    // refused everything would satisfy the ceiling test above.
    let mut cluster = started();
    drive_term_to(&mut cluster, 5);
    let response = cluster.pre_vote(2).expect("the pre-vote is answered");
    assert!(response.vote_granted, "{response:?}");
    assert_eq!(response.reason, "pre_vote_granted");
    assert_eq!(
        response.term, 6,
        "the granted term is the one after the term the node holds: {response:?}"
    );
}

#[test]
fn an_ordinary_campaign_still_bumps_the_term() {
    // The positive control for the campaign arm: a campaign that always
    // failed would satisfy the ceiling tests.
    let mut cluster = started();
    cluster
        .campaign(2, false)
        .expect("an ordinary campaign succeeds");
    let response = cluster.pre_vote(3).expect("the pre-vote is answered");
    assert_eq!(
        response.term, 3,
        "the cluster starts at term 1 and the campaign took it to 2, so the \
         next campaignable term is 3: {response:?}"
    );
}
