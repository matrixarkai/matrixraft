// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A node with no vote to cast has no vote to withhold either.
//!
//! `record_vote_response` discards an answer from any node that does not count
//! toward the quorum, before anything is recorded. For a *granted* vote that
//! refusal is belt and braces -- the quorum count walks the membership's voters
//! and witnesses and simply never finds a learner's id, so a stray grant could
//! not elect anybody. **The refusal tally is the half with no such filter:**
//!
//! ```text
//! let rejected = votes.values().filter(|granted| !**granted).count();
//! let participants = membership.voters.len() + membership.witnesses.len();
//! if rejected > participants - quorum_size { .. abandon the election .. }
//! ```
//!
//! `rejected` counts **every value in the map**, while the denominator it is
//! compared against counts only voters and witnesses. So an answer from outside
//! that set, if it were ever recorded, would be weighed against a population it
//! is not part of -- and the candidate would give up an election it had not
//! lost.
//!
//! Measured, with the guard's first clause removed: three voters and one
//! learner, one honest voter refusal and then the learner's, and the leader
//! steps down to follower with `leader_id` cleared. On the engine as it stands
//! it keeps the leadership, and it takes a *second voter* to end it.
//!
//! The guard carries a second clause for the witness policy, and that one is
//! not belt and braces either: a witness **does** count toward a quorum, so its
//! answer is weighed -- unless `ignore_witness` says witnesses are not to be
//! counted, in which case it has to be discarded exactly as a learner's is.
//! Both settings are asserted here, because a clause that fires under either
//! policy is as wrong as one that never fires.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole, StateRole, VoteResponse};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

/// Three voters and a fourth node in `fourth_role`, started, with the witness
/// policy set. The fourth node is the one whose refusal must not count.
fn started_with(fourth_role: ReplicaRole, ignore_witness: bool) -> RaftCluster {
    let mut cluster = RaftCluster::new(
        7,
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
            peer(4, fourth_role),
        ],
    )
    .expect("valid cluster");
    cluster.set_ignore_witness(ignore_witness);
    cluster.start().expect("cluster starts");
    cluster
}

/// Five voters and a witness, started. A wider group than the tests above
/// need, because the threshold the engine compares against leaves no room for
/// a witness's answer to be decisive at three voters.
fn five_voters_and_a_witness(ignore_witness: bool) -> RaftCluster {
    let mut cluster = RaftCluster::new(
        7,
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
            peer(4, ReplicaRole::Voter),
            peer(5, ReplicaRole::Voter),
            peer(6, ReplicaRole::Witness),
        ],
    )
    .expect("valid cluster");
    cluster.set_ignore_witness(ignore_witness);
    cluster.start().expect("cluster starts");
    cluster
}

fn refusal(term: u64) -> VoteResponse {
    VoteResponse {
        term,
        vote_granted: false,
        reason: "refused".to_string(),
    }
}

/// Deliver a refusal from `peer_id` to the leader and report whether the
/// leader still holds the leadership afterwards.
fn still_leading_after_refusal_from(cluster: &mut RaftCluster, peer_id: u64) -> bool {
    let leader = cluster
        .leader_id()
        .expect("the premise: there is a leader to lose");
    let term = cluster.status(leader).expect("leader status").term;
    cluster
        .handle_vote_response_from(leader, peer_id, refusal(term), false)
        .expect("a refusal is accepted by the entry point");
    cluster.leader_id() == Some(leader)
        && cluster.status(leader).expect("leader status").role == StateRole::Leader
}

#[test]
fn a_learners_refusal_does_not_lose_an_election() {
    // Three voters, so the quorum is 2 and the engine gives up once refusals
    // exceed 3 - 2 = 1. One honest refusal is therefore survivable and two are
    // not, which is what makes the learner's answer decisive if it is counted.
    let mut cluster = started_with(ReplicaRole::Learner, false);

    assert!(
        still_leading_after_refusal_from(&mut cluster, 2),
        "one voter refusing is not a lost election"
    );
    assert!(
        still_leading_after_refusal_from(&mut cluster, 4),
        "and a learner has no vote to withhold, so its refusal changes nothing"
    );
}

#[test]
fn a_second_voters_refusal_does_lose_it() {
    // The control for the test above, and the reason it cannot be read as "the
    // engine never gives up an election": the same sequence with a real voter
    // in the learner's place ends the leadership.
    let mut cluster = started_with(ReplicaRole::Learner, false);
    let leader = cluster.leader_id().expect("a leader");

    assert!(still_leading_after_refusal_from(&mut cluster, 2));
    assert!(
        !still_leading_after_refusal_from(&mut cluster, 3),
        "two of three voters refusing is a genuinely lost election"
    );
    assert_eq!(
        cluster.leader_id(),
        None,
        "and the leadership is given up rather than merely reported lost"
    );
    assert_eq!(
        cluster.status(leader).expect("status").role,
        StateRole::Follower,
        "the node that was leading steps down"
    );
}

#[test]
fn an_ignored_witness_refusal_does_not_lose_an_election() {
    // FIVE voters and a witness, because at three the arithmetic cannot tell
    // the clause apart: with `ignore_witness` set the quorum is the voters'
    // own, so the engine gives up once refusals exceed 6 - 3 = 3, and the
    // witness's answer can only be the fourth. Three voters refusing and then
    // the witness is therefore exactly the boundary -- and measured with this
    // clause removed, that fourth answer is what ends the leadership.
    let mut cluster = five_voters_and_a_witness(true);

    for voter in [2u64, 3, 4] {
        assert!(
            still_leading_after_refusal_from(&mut cluster, voter),
            "three of five voters refusing is below the threshold the engine uses"
        );
    }
    assert!(
        still_leading_after_refusal_from(&mut cluster, 6),
        "and a witness the policy does not count toward a quorum cannot refuse \
         one either, so it must not be the answer that tips it"
    );
}

#[test]
fn a_counted_witness_refusal_does_lose_an_election() {
    // The control for the clause: with witnesses counted, the quorum of the
    // four participants is 3, so refusals must exceed 4 - 3 = 1 -- and the
    // witness's answer is the second one. This is what stops the clause above
    // from being read as "a witness never counts", which would silently turn
    // the policy flag into a constant.
    let mut cluster = started_with(ReplicaRole::Witness, false);

    assert!(still_leading_after_refusal_from(&mut cluster, 2));
    assert!(
        !still_leading_after_refusal_from(&mut cluster, 4),
        "a witness that counts toward a quorum does refuse one"
    );
    assert_eq!(
        cluster.leader_id(),
        None,
        "so the election is abandoned, exactly as a voter's refusal would do"
    );
}
