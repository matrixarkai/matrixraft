// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    RustRaftFollowerLease, RustRaftLeaderLease, RustRaftLeasePeer, RustRaftReplicaRole,
};

fn voters(ids: &[u64]) -> Vec<RustRaftLeasePeer> {
    ids.iter()
        .copied()
        .map(|node_id| RustRaftLeasePeer {
            node_id,
            role: RustRaftReplicaRole::Voter,
        })
        .collect()
}

fn peers_with_roles(items: &[(u64, RustRaftReplicaRole)]) -> Vec<RustRaftLeasePeer> {
    items
        .iter()
        .copied()
        .map(|(node_id, role)| RustRaftLeasePeer { node_id, role })
        .collect()
}

#[test]
fn leader_lease_renews_from_quorum_confirmations_like_matrixraft() {
    let mut lease = RustRaftLeaderLease::new(1, 50);
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
    let mut lease = RustRaftLeaderLease::new(1, 100);
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
    let mut lease = RustRaftLeaderLease::new(1, 100);
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
    let mut lease = RustRaftLeaderLease::new(1, 100);
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
    let mut lease = RustRaftLeaderLease::new(1, 100);
    lease.update_members(peers_with_roles(&[
        (1, RustRaftReplicaRole::Voter),
        (2, RustRaftReplicaRole::Voter),
        (3, RustRaftReplicaRole::Learner),
    ]));
    lease.reset(1);

    assert!(lease.on_recv_lease_confirm(1, 3, 95, 100));
    assert!(!lease.in_lease(1, 100));
    assert!(lease.on_recv_lease_confirm(1, 2, 95, 100));
    assert!(lease.in_lease(1, 100));
    assert_eq!(lease.status(1, 100).voting_peer_count, 2);

    lease.update_members(peers_with_roles(&[
        (1, RustRaftReplicaRole::Voter),
        (2, RustRaftReplicaRole::Voter),
        (3, RustRaftReplicaRole::Voter),
    ]));
    assert!(lease.on_recv_lease_confirm(1, 3, 90, 1_000));
}

#[test]
fn follower_lease_tracks_epoch_monotonicity_and_restart_forbidden_window() {
    let mut lease = RustRaftFollowerLease::new(2, 100, 0, 10);
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

    let mut restarted = RustRaftFollowerLease::new(2, 100, 100, 500);
    restarted.reset(3);
    assert!(restarted.in_lease(3, 599));
    assert!(!restarted.in_lease(3, 600));
}
