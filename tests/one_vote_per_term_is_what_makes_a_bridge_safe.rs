// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A voter grants one vote per term, which is what makes a bridge partition safe.
//!
//! A **bridge** is the partition where one node sees both halves while the halves
//! cannot see each other: B reaches A and C, A and C cannot reach each other. It
//! is the topology worth worrying about because **both halves can reach a quorum
//! through the bridge** -- in a three-voter group, A has itself plus B, and C has
//! itself plus B, so each can count two of three. Nothing about connectivity
//! stops either of them winning.
//!
//! What stops two leaders in one term is the bridge node itself: it grants **one
//! vote per term**. Three cases decide that, and they are easy to confuse:
//!
//! | ask | answer | why |
//! |---|---|---|
//! | the same candidate, again, same term | **granted** | a retried RPC must not be refused, or a dropped response costs an election |
//! | a different candidate, same term | **refused**, `already_voted` | the safety property |
//! | any candidate, a later term | **granted** | a higher term clears the vote, or the group could never elect again |
//!
//! No test asked one voter for two different candidates in one term before this.
//!
//! **What this file does not do** is build the topology. A bridge is expressible
//! today -- `InMemoryRaftTransport` holds its peers per instance, so giving each
//! node its own transport with only the peers it can reach registered produces
//! one -- but every partition in the suite goes through `set_node_healthy`, which
//! is per-node health and cannot express asymmetry. This pins the rule the
//! topology relies on; the topology itself is still unexercised, and saying so is
//! better than implying otherwise.

use matrixraft::{LogId, Peer, RaftCluster, ReplicaRole, VoteRequest, VoteResponse};

const GROUP: u64 = 95;
/// The bridge: the node both candidates can reach.
const BRIDGE: u64 = 2;
const CANDIDATE_A: u64 = 1;
const CANDIDATE_C: u64 = 3;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 50_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 51_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// A started three-voter group, and a term above the one it settled on.
///
/// The term has to be higher than the cluster's own: a vote arriving at or below
/// it is answered `known_leader` by the rule above this one, which would make
/// every case below look the same for the wrong reason.
fn group_and_a_fresh_term() -> (RaftCluster, u64) {
    let mut cluster = RaftCluster::new(
        GROUP,
        Default::default(),
        vec![peer(CANDIDATE_A), peer(BRIDGE), peer(CANDIDATE_C)],
    )
    .expect("build the group");
    cluster.start().expect("start the group");
    let settled = cluster.status(BRIDGE).expect("bridge status").term;
    (cluster, settled + 5)
}

fn ask_for_vote(cluster: &mut RaftCluster, candidate: u64, term: u64) -> VoteResponse {
    cluster
        .vote_to(
            BRIDGE,
            VoteRequest {
                group_id: GROUP,
                term,
                candidate_id: candidate,
                last_log_id: Some(LogId { term: 1, index: 1 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("the vote is answered")
}

#[test]
fn a_second_candidate_in_the_same_term_is_refused() {
    // The safety property: with both halves able to count the bridge, this
    // refusal is the only thing standing between them and two leaders in one
    // term.
    let (mut cluster, term) = group_and_a_fresh_term();

    let first = ask_for_vote(&mut cluster, CANDIDATE_A, term);
    assert!(
        first.vote_granted,
        "the premise: the bridge votes for the first candidate ({})",
        first.reason
    );

    let second = ask_for_vote(&mut cluster, CANDIDATE_C, term);
    assert!(
        !second.vote_granted,
        "the bridge must not vote twice in one term"
    );
    assert_eq!(second.reason, "already_voted");
    assert_eq!(
        second.term, term,
        "and it answers at the term it voted in, so the loser can tell why"
    );
}

#[test]
fn the_same_candidate_asking_again_is_re_granted() {
    // Idempotence, and it is load-bearing rather than a nicety: a vote response
    // that is dropped makes the candidate retry, and a voter that refused its
    // own candidate would cost an election for no reason.
    let (mut cluster, term) = group_and_a_fresh_term();

    assert!(ask_for_vote(&mut cluster, CANDIDATE_A, term).vote_granted);
    let again = ask_for_vote(&mut cluster, CANDIDATE_A, term);

    assert!(
        again.vote_granted,
        "a retry from the same candidate in the same term is the same vote: {}",
        again.reason
    );
    assert_eq!(again.reason, "vote_granted");
}

#[test]
fn a_later_term_clears_the_vote() {
    // Liveness. Without this the refusal above would be permanent and the group
    // could never elect anyone again -- which is also why the refusal has to be
    // scoped to the term rather than to the candidate.
    //
    // **What this pins, precisely.** Two places clear `voted_for` on a higher
    // term: one keyed on the CLUSTER's term, one on the TARGET NODE's. Deleting
    // the per-node one leaves *this* test green, because the cluster-keyed site
    // already covers the case built here -- a plant showed exactly that. So this
    // one pins the PROPERTY, and
    // `a_voter_lagging_the_cluster_term_still_clears_its_vote` below pins the
    // per-node site.
    let (mut cluster, term) = group_and_a_fresh_term();

    assert!(ask_for_vote(&mut cluster, CANDIDATE_A, term).vote_granted);
    assert_eq!(
        ask_for_vote(&mut cluster, CANDIDATE_C, term).reason,
        "already_voted",
        "the premise: the bridge is spent for this term"
    );

    let next = ask_for_vote(&mut cluster, CANDIDATE_C, term + 1);
    assert!(
        next.vote_granted,
        "a higher term clears the vote: {}",
        next.reason
    );
    assert_eq!(next.term, term + 1);
}

#[test]
fn a_voter_lagging_the_cluster_term_still_clears_its_vote() {
    // The case that discriminates the two clearing sites, and the reason it took
    // a second attempt to find: the obvious one does not, because the
    // cluster-keyed site covers it.
    //
    // A voter can sit **behind** the cluster's term -- it simply has not been
    // asked anything lately -- and then the cluster-keyed condition
    // (`request.term > self.current_term`) is false while the per-node one
    // (`request.term > target.hard_state.current_term`) is true. Only the
    // per-node site can clear the vote here, so this is where it is pinned.
    //
    // MEASURED with that site removed: this grant becomes `already_voted`.
    let (mut cluster, term) = group_and_a_fresh_term();

    // The bridge spends its vote at a term below where the cluster will end up.
    assert!(ask_for_vote(&mut cluster, CANDIDATE_A, term).vote_granted);

    // Raise the CLUSTER's term by asking a different node, which also clears
    // `leader_id` -- without that the `known_leader` rule answers first and this
    // never reaches the clearing site at all.
    let ahead = term + 5;
    let other = cluster
        .vote_to(
            CANDIDATE_A,
            VoteRequest {
                group_id: GROUP,
                term: ahead,
                candidate_id: CANDIDATE_C,
                last_log_id: Some(LogId { term: 1, index: 1 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("the other node answers");
    assert!(
        other.vote_granted,
        "the premise: the cluster moves to {ahead}"
    );

    // The premise, asserted rather than assumed: the bridge lags.
    assert_eq!(
        cluster.status(BRIDGE).expect("bridge status").term,
        term,
        "the bridge has not been asked anything since, so it still sits at {term}"
    );
    assert_eq!(
        cluster.status(CANDIDATE_A).expect("other status").term,
        ahead,
        "while the cluster has moved on, which is what makes the two sites differ"
    );

    let lagging = ask_for_vote(&mut cluster, CANDIDATE_C, ahead);
    assert!(
        lagging.vote_granted,
        "a term above the voter's own clears its vote even when the cluster is          already there: {}",
        lagging.reason
    );
    assert_eq!(
        cluster.status(BRIDGE).expect("bridge status").term,
        ahead,
        "and the voter catches up"
    );
}
