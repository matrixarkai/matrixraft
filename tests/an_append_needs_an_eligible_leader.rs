// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! An append is refused unless the node claiming to be leader could actually be
//! one, and it is refused **before** the term is adopted.
//!
//! The gate in `append_entries_with_membership_change_indexes_to` asks three
//! things of `request.leader_id` at once: that it is a node at all, that its
//! role `can_be_leader()`, and that it is healthy. Planting each of the three
//! away in turn showed that two were defended and one was not:
//!
//! | planted away | caught by |
//! |---|---|
//! | an unknown `leader_id` is accepted | `removed_peer_cannot_collect_votes_or_step_down_leader` |
//! | an unhealthy leader is accepted | `stopped_leader_cannot_drive_append_or_snapshot` |
//! | a learner or witness is accepted as leader | **nothing, across 1,015 tests** |
//!
//! So this file covers the third. `can_be_leader()` is `Voter` only, and the
//! role it most matters for is `Witness`: a witness holds **no data**, so a
//! follower that accepted a witness's append would be taking a log from a node
//! that has none of it, and would set `leader_id` to a node that can never
//! serve one.
//!
//! The term is the sharper half. The gate sits before `self.current_term =
//! request.term`, so an ineligible claimant cannot even move a follower's term
//! -- which would be enough on its own to make that follower reject the real
//! leader's appends and start an election.

use matrixraft::{
    AppendEntriesRequest, LogEntry, LogId, Peer, RaftCluster, ReplicaRole, StatusSnapshot,
};

const GROUP: u64 = 83;
/// The follower under test. A voter, so nothing about its own role is at stake.
const TARGET: u64 = 2;
/// The node that claims leadership, whose role each test varies.
const CLAIMANT: u64 = 3;
/// Above the cluster's own term, so adopting it would be visible.
const CLAIMED_TERM: u64 = 4;

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 30_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 31_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: b"appended-by-a-claimant".to_vec(),
        is_command: false,
    }
}

/// A cluster of two voters plus `CLAIMANT` in the role under test.
fn cluster_with_claimant_as(role: ReplicaRole) -> RaftCluster {
    let mut cluster = RaftCluster::new(
        GROUP,
        Default::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(TARGET, ReplicaRole::Voter),
            peer(CLAIMANT, role),
        ],
    )
    .expect("build the cluster");
    cluster.start().expect("start the cluster");
    cluster
}

/// An append from `CLAIMANT`, in a shape a real leader's would be accepted in.
fn append_from_claimant(cluster: &mut RaftCluster) -> (bool, StatusSnapshot, StatusSnapshot) {
    let before = cluster.status(TARGET).expect("status before");
    let request = AppendEntriesRequest {
        group_id: GROUP,
        term: CLAIMED_TERM,
        leader_id: CLAIMANT,
        prev_log_id: None,
        entries: vec![entry(1), entry(2)],
        leader_commit: 0,
        lease_epoch: 0,
    };
    let response = cluster
        .append_entries_to(TARGET, request)
        .expect("the append is answered, not errored");
    let after = cluster.status(TARGET).expect("status after");
    (response.success, before, after)
}

fn assert_refused_and_inert(role: ReplicaRole) {
    let mut cluster = cluster_with_claimant_as(role);
    let (success, before, after) = append_from_claimant(&mut cluster);

    assert!(
        !success,
        "a {role:?} cannot be leader, so its append must be refused"
    );
    assert_eq!(
        after.last_log_index, before.last_log_index,
        "a refused append must not leave entries behind"
    );
    assert_eq!(
        after.term, before.term,
        "and it must not move the follower's term -- the gate runs before the \
         term is adopted, which is what stops an ineligible claimant disrupting \
         the real leader"
    );
    assert_ne!(
        after.leader_id,
        Some(CLAIMANT),
        "nor may it become the follower's leader"
    );
}

#[test]
fn a_learner_claiming_leadership_cannot_append() {
    assert_refused_and_inert(ReplicaRole::Learner);
}

#[test]
fn a_witness_claiming_leadership_cannot_append() {
    // The role this matters most for: a witness holds no data at all.
    assert_refused_and_inert(ReplicaRole::Witness);
}

#[test]
fn a_voter_claiming_leadership_is_accepted() {
    // The positive control, and the reason the assertions above are evidence:
    // the request shape is one a voter IS accepted in, so the role is the only
    // thing the two cases differ by. Without this, a gate that refused every
    // append would pass the tests above.
    let mut cluster = cluster_with_claimant_as(ReplicaRole::Voter);
    let (success, before, after) = append_from_claimant(&mut cluster);

    assert!(success, "a healthy voter's append is accepted");
    assert!(
        after.last_log_index > before.last_log_index,
        "the entries land: {} -> {}",
        before.last_log_index,
        after.last_log_index
    );
    assert_eq!(
        after.term, CLAIMED_TERM,
        "and an accepted append DOES adopt the leader's term, which is what \
         makes the term assertions above meaningful"
    );
}

#[test]
fn only_a_voter_can_be_leader() {
    // The rule the gate consults, pinned on its own. This does not catch a gate
    // that stops consulting it -- the three tests above do that -- but it names
    // the contract, so a change to the rule itself is a deliberate one.
    assert!(ReplicaRole::Voter.can_be_leader());
    assert!(!ReplicaRole::Learner.can_be_leader());
    assert!(!ReplicaRole::Witness.can_be_leader());
}
