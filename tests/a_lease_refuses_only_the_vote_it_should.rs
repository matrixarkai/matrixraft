// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The lease refuses a vote only when it should: a higher term, and lease reads
//! actually enabled.
//!
//! `vote_to` answers `in_lease` from a four-clause condition, placed before the
//! `pre_vote` branch so it governs pre-votes too:
//!
//! ```text
//! request.term > target_node.hard_state.current_term
//!     && !request.force
//!     && self.config.enable_lease_read
//!     && if target is Leader { self.leader_lease_valid }
//!        else { self.is_follower_lease_valid() }
//! ```
//!
//! Planted clause by clause against the whole suite, the lease itself is well
//! held -- both arms, and `startup_follower_lease_blocks_pre_vote_until_carried_duration_expires`
//! covers the pre-vote case. `!request.force` is held too. **The other two were
//! caught by nothing** across 1,038 tests, and both are reachable.
//!
//! ## `enable_lease_read` is not redundant, for an asymmetric reason
//!
//! The follower arm guards itself: `is_follower_lease_valid()` is
//! `enable_lease_read && follower_lease_valid`. **The leader arm does not** --
//! it reads `leader_lease_valid` directly, and `set_leader_lease_valid` is
//! public and does not consult the feature switch. So this clause is the only
//! thing standing between a disabled feature and a lease refusal.
//!
//! MEASURED, leader flag set by hand and a higher-term vote sent to the leader:
//!
//! | `enable_lease_read` | answer |
//! |---|---|
//! | `false` | `vote_granted` |
//! | `true` | refused, `in_lease` |
//!
//! Without the clause, a leader would refuse to step down for a legitimate
//! higher-term candidate on the strength of a lease it is not supposed to be
//! honouring -- an election that cannot finish.
//!
//! This is the same asymmetry as the read path's `lease_read` conjunction: one
//! arm self-guards, the other does not, and the feature clause covers for it.
//!
//! ## The higher-term clause routes, rather than refuses
//!
//! A vote at the node's *current* term is not a lease question at all. MEASURED,
//! with a follower holding a valid lease: a same-term request is answered
//! `known_leader` and a higher-term one `in_lease`. Drop the clause and the
//! same-term request is answered `in_lease` instead -- a true statement about
//! the lease, but the wrong rule for the request, and it hides the answer the
//! caller needs.

use matrixraft::{Config, LogId, Peer, RaftCluster, ReplicaRole, VoteRequest, VoteResponse};

const GROUP: u64 = 91;
const FOLLOWER: u64 = 2;
const CANDIDATE: u64 = 3;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 46_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 47_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn cluster(enable_lease_read: bool) -> RaftCluster {
    let config = Config {
        enable_lease_read,
        ..Default::default()
    };
    let mut cluster =
        RaftCluster::new(GROUP, config, vec![peer(1), peer(2), peer(3)]).expect("build");
    cluster.start().expect("start");
    cluster
}

fn vote(cluster: &mut RaftCluster, target: u64, term: u64) -> VoteResponse {
    cluster
        .vote_to(
            target,
            VoteRequest {
                group_id: GROUP,
                term,
                candidate_id: CANDIDATE,
                last_log_id: Some(LogId { term: 1, index: 1 }),
                pre_vote: false,
                force: false,
            },
        )
        .expect("the vote is answered")
}

/// A leader whose lease flag has been set by hand, and a term above its own.
fn leader_with_the_flag_set(enable_lease_read: bool) -> (RaftCluster, u64, u64) {
    let mut cluster = cluster(enable_lease_read);
    let leader = cluster.leader_id().expect("a leader after start");
    cluster.set_leader_lease_valid(true);
    let higher_term = cluster.status(leader).expect("leader status").term + 5;
    (cluster, leader, higher_term)
}

#[test]
fn with_lease_reads_disabled_a_leader_lease_does_not_refuse_a_vote() {
    let (mut cluster, leader, higher_term) = leader_with_the_flag_set(false);

    let response = vote(&mut cluster, leader, higher_term);
    assert_ne!(
        response.reason, "in_lease",
        "lease reads are off, so no lease may refuse a vote"
    );
    assert!(
        response.vote_granted,
        "and the higher-term candidate gets its vote: {}",
        response.reason
    );
}

#[test]
fn with_lease_reads_enabled_the_same_vote_is_refused() {
    // The control for the test above: same flag, same term, same target. The
    // feature switch is the only difference, which is what makes the clause
    // attributable.
    let (mut cluster, leader, higher_term) = leader_with_the_flag_set(true);

    let response = vote(&mut cluster, leader, higher_term);
    assert!(!response.vote_granted);
    assert_eq!(response.reason, "in_lease");
}

#[test]
fn a_vote_at_the_nodes_own_term_is_not_a_lease_question() {
    let mut cluster = cluster(true);
    assert!(
        cluster.receive_follower_lease_item(1),
        "the premise: the follower holds a lease"
    );
    let same_term = cluster.status(FOLLOWER).expect("follower status").term;

    let response = vote(&mut cluster, FOLLOWER, same_term);
    assert_ne!(
        response.reason, "in_lease",
        "a request at the node's own term is answered by another rule"
    );
}

#[test]
fn a_higher_term_vote_into_a_follower_lease_is_refused() {
    // The control for the test above: same cluster, same lease, only the term
    // differs, so the refusal is attributable to the term and not the lease.
    let mut cluster = cluster(true);
    cluster.receive_follower_lease_item(1);
    let higher_term = cluster.status(FOLLOWER).expect("follower status").term + 5;

    let response = vote(&mut cluster, FOLLOWER, higher_term);
    assert!(!response.vote_granted);
    assert_eq!(response.reason, "in_lease");
}
