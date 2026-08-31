// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{FollowerLease, LeaderLease, LeasePeer, ReplicaRole};

fn voters(ids: &[u64]) -> Vec<LeasePeer> {
    ids.iter()
        .copied()
        .map(|node_id| LeasePeer {
            node_id,
            role: ReplicaRole::Voter,
        })
        .collect()
}

fn peers_with_roles(items: &[(u64, ReplicaRole)]) -> Vec<LeasePeer> {
    items
        .iter()
        .copied()
        .map(|(node_id, role)| LeasePeer { node_id, role })
        .collect()
}

#[test]
fn leader_lease_renews_from_quorum_confirmations() {
    let mut lease = LeaderLease::new(1, 50);
    lease.update_members(voters(&[1, 2, 3]));
    lease.reset(1);

    assert!(!lease.in_lease(1, 100));
    assert!(lease.on_recv_lease_confirm(1, 2, 95, 50));
    assert!(lease.in_lease(1, 100));
    assert_eq!(lease.last_active_lease_end_ms(), Some(145));

    assert!(lease.on_recv_lease_confirm(1, 3, 115, 50));
    assert!(lease.in_lease(1, 120));
    assert_eq!(lease.last_active_lease_end_ms(), Some(145));
    assert!(lease.in_lease(1, 146));
    assert_eq!(lease.last_active_lease_end_ms(), Some(165));

    assert!(!lease.in_lease(1, 166));
    assert!(lease.on_recv_lease_confirm(1, 2, 166, 50));
    assert!(lease.in_lease(1, 166));
    assert_eq!(lease.last_active_lease_end_ms(), Some(216));
}

#[test]
fn leader_lease_keeps_reduced_follower_duration_from_rewinding() {
    let mut lease = LeaderLease::new(1, 100);
    lease.update_members(voters(&[1, 2, 3]));
    lease.reset(1);

    assert!(lease.on_recv_lease_confirm(1, 2, 95, 100));
    assert!(lease.in_lease(1, 100));
    let lease_end = lease.last_active_lease_end_ms();

    assert!(lease.on_recv_lease_confirm(1, 2, 150, 30));
    assert!(lease.in_lease(1, 150));
    assert_eq!(lease.last_active_lease_end_ms(), lease_end);

    assert!(!lease.in_lease(1, 196));
    assert!(lease.on_recv_lease_confirm(1, 2, 196, 30));
    assert!(lease.in_lease(1, 196));
    assert_eq!(lease.last_active_lease_end_ms(), Some(226));
}

#[test]
fn leader_lease_ignores_unknown_and_stale_confirmations() {
    let mut lease = LeaderLease::new(1, 100);
    lease.update_members(voters(&[1, 2, 3]));
    lease.reset(1);

    assert!(!lease.on_recv_lease_confirm(1, 4, 90, 100));
    assert!(!lease.in_lease(1, 100));

    assert!(lease.on_recv_lease_confirm(1, 2, 95, 100));
    assert!(lease.in_lease(1, 100));
    let lease_end = lease.last_active_lease_end_ms();

    assert!(!lease.on_recv_lease_confirm(1, 2, 80, 1_000));
    assert_eq!(lease.last_active_lease_end_ms(), lease_end);
}

#[test]
fn leader_lease_quorum_math_tracks_membership_changes() {
    let mut lease = LeaderLease::new(1, 100);
    lease.reset(1);

    lease.update_members(voters(&[1]));
    assert!(lease.in_lease(1, 10));

    lease.update_members(voters(&[1, 2, 3, 4, 5]));
    assert!(!lease.in_lease(1, 111));
    assert!(lease.on_recv_lease_confirm(1, 2, 111, 100));
    assert!(!lease.in_lease(1, 111));
    assert!(lease.on_recv_lease_confirm(1, 3, 111, 100));
    assert!(lease.in_lease(1, 111));
    assert_eq!(lease.status(1, 111).quorum_size, 3);

    lease.update_members(voters(&[1, 3, 4, 5]));
    assert!(lease.in_lease(1, 112));
    assert!(!lease.in_lease(1, 212));
    assert!(lease.on_recv_lease_confirm(1, 3, 212, 100));
    assert!(!lease.in_lease(1, 212));
    assert!(lease.on_recv_lease_confirm(1, 5, 212, 100));
    assert!(lease.in_lease(1, 212));
}

#[test]
fn leader_lease_excludes_learners_and_resets_promoted_learner_state() {
    let mut lease = LeaderLease::new(1, 100);
    lease.update_members(peers_with_roles(&[
        (1, ReplicaRole::Voter),
        (2, ReplicaRole::Voter),
        (3, ReplicaRole::Learner),
    ]));
    lease.reset(1);

    assert!(lease.on_recv_lease_confirm(1, 3, 95, 100));
    assert!(!lease.in_lease(1, 100));
    assert!(lease.on_recv_lease_confirm(1, 2, 95, 100));
    assert!(lease.in_lease(1, 100));
    assert_eq!(lease.status(1, 100).voting_peer_count, 2);

    lease.update_members(peers_with_roles(&[
        (1, ReplicaRole::Voter),
        (2, ReplicaRole::Voter),
        (3, ReplicaRole::Voter),
    ]));
    assert!(lease.on_recv_lease_confirm(1, 3, 90, 1_000));
}

#[test]
fn follower_lease_tracks_epoch_monotonicity_and_restart_forbidden_window() {
    let mut lease = FollowerLease::new(2, 100, 0, 10);
    lease.reset(1);
    assert!(!lease.in_lease(1, 10));

    assert!(lease.on_recv_lease_item(1, 10, 20));
    assert_eq!(lease.max_met_epoch_id(1), 10);
    assert!(lease.in_lease(1, 119));
    assert!(!lease.on_recv_lease_item(1, 9, 30));
    assert_eq!(lease.received_lease_end_ms(), Some(120));
    assert!(!lease.in_lease(1, 120));

    lease.reset(2);
    assert!(lease.on_recv_lease_item(2, 5, 130));
    assert!(lease.in_lease(2, 229));
    assert!(!lease.on_recv_lease_item(2, 4, 140));
    assert_eq!(lease.received_lease_end_ms(), Some(230));

    let mut restarted = FollowerLease::new(2, 100, 100, 500);
    restarted.reset(3);
    assert!(restarted.in_lease(3, 599));
    assert!(!restarted.in_lease(3, 600));
}

#[test]
fn leader_lease_rejects_a_stale_term_instead_of_aborting() {
    let mut lease = LeaderLease::new(1, 50);
    lease.update_members(voters(&[1, 2, 3]));
    lease.reset(7);

    // Establish a real lease in the current term so the rejections below are
    // distinguishable from "there was never a lease".
    assert!(lease.on_recv_lease_confirm(7, 2, 95, 50));
    assert!(lease.in_lease(7, 100));

    // A confirmation from a term we have moved past is dropped, exactly as a
    // confirmation from an unknown peer already was.
    assert!(!lease.on_recv_lease_confirm(6, 3, 115, 50));
    assert!(!lease.on_recv_lease_confirm(8, 3, 115, 50));
    assert!(!lease.on_recv_lease_confirm(7, 99, 115, 50));

    // A leader must not confirm its own lease; that is a drop, not a crash.
    assert!(!lease.on_recv_lease_confirm(7, 1, 115, 50));

    // Querying with a stale term reports "no lease", and says why.
    assert!(!lease.in_lease(6, 100));
    let stale = lease.status(6, 100);
    assert!(!stale.in_lease);
    assert_eq!(stale.lease_end_ms, None);
    assert_eq!(stale.reason, "term_mismatch");
    assert_eq!(stale.voting_peer_count, 3);
    assert_eq!(stale.quorum_size, 2);

    // None of that disturbed the lease actually held for the current term.
    assert!(lease.in_lease(7, 100));
    assert_eq!(lease.status(7, 100).reason, "active");
}

#[test]
fn follower_lease_rejects_a_stale_term_instead_of_aborting() {
    let mut lease = FollowerLease::new(2, 50, 0, 0);
    lease.reset(7);

    assert!(lease.on_recv_lease_item(7, 90, 100));
    assert!(lease.in_lease(7, 120));
    assert_eq!(lease.max_met_epoch_id(7), 90);

    // Stale and future terms are both rejected rather than fatal.
    assert!(!lease.on_recv_lease_item(6, 200, 120));
    assert!(!lease.on_recv_lease_item(8, 200, 120));

    // A query in another term is simply "no lease", and nothing was met there.
    assert!(!lease.in_lease(6, 120));
    assert_eq!(lease.max_met_epoch_id(6), 0);

    // The current term is untouched by any of it.
    assert!(lease.in_lease(7, 120));
    assert_eq!(lease.max_met_epoch_id(7), 90);
}
