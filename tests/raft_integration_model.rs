// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_learner_promotion_decision, matrixraft_read_safety_runtime_decision,
    matrixraft_recover_latest_wal_record, matrixraft_wal_checksum, RustRaftApplySnapshotFence,
    RustRaftHardState, RustRaftLogEntry, RustRaftLogId, RustRaftMembership, RustRaftPeerStatus,
    RustRaftReadSafetyOperation, RustRaftReadSafetyRuntimeInput, RustRaftReplicaRole, RustRaftRole,
    RustRaftSnapshotMeta, RustRaftStatusSnapshot, RustRaftWalRecord,
};

#[derive(Clone)]
struct ModelNode {
    id: u64,
    role: RustRaftRole,
    replica_role: RustRaftReplicaRole,
    commit_index: u64,
    applied_index: u64,
    last_snapshot_index: u64,
    restarted: bool,
}

fn three_node_cluster() -> Vec<ModelNode> {
    vec![
        ModelNode {
            id: 1,
            role: RustRaftRole::Leader,
            replica_role: RustRaftReplicaRole::Voter,
            commit_index: 0,
            applied_index: 0,
            last_snapshot_index: 0,
            restarted: false,
        },
        ModelNode {
            id: 2,
            role: RustRaftRole::Follower,
            replica_role: RustRaftReplicaRole::Voter,
            commit_index: 0,
            applied_index: 0,
            last_snapshot_index: 0,
            restarted: false,
        },
        ModelNode {
            id: 3,
            role: RustRaftRole::Follower,
            replica_role: RustRaftReplicaRole::Voter,
            commit_index: 0,
            applied_index: 0,
            last_snapshot_index: 0,
            restarted: false,
        },
    ]
}

fn replicate(nodes: &mut [ModelNode], index: u64) {
    for node in nodes {
        node.commit_index = index;
        node.applied_index = index;
    }
}

fn status_for(node: &ModelNode, nodes: &[ModelNode]) -> RustRaftStatusSnapshot {
    RustRaftStatusSnapshot {
        group_id: 7,
        node_id: node.id,
        role: node.role,
        term: 4,
        leader_id: nodes
            .iter()
            .find(|candidate| candidate.role == RustRaftRole::Leader)
            .map(|leader| leader.id),
        commit_index: node.commit_index,
        applied_index: node.applied_index,
        last_log_index: node.commit_index,
        last_snapshot_index: node.last_snapshot_index,
        peers: nodes
            .iter()
            .filter(|peer| peer.id != node.id)
            .map(|peer| RustRaftPeerStatus {
                node_id: peer.id,
                matched: peer.commit_index,
                next_index: peer.commit_index + 1,
                learner: peer.replica_role == RustRaftReplicaRole::Learner,
                healthy: true,
                lag: node.commit_index.saturating_sub(peer.commit_index),
            })
            .collect(),
    }
}

fn wal_record(node: &ModelNode) -> RustRaftWalRecord {
    let mut record = RustRaftWalRecord {
        entries_are_delta: false,
        group_id: 7,
        node_id: node.id,
        hard_state: RustRaftHardState {
            current_term: 4,
            voted_for: Some(1),
            committed: Some(RustRaftLogId {
                term: 4,
                index: node.commit_index,
            }),
        },
        membership: RustRaftMembership {
            group_id: 7,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries: vec![RustRaftLogEntry {
            log_id: RustRaftLogId {
                term: 4,
                index: node.commit_index,
            },
            payload: b"replicated-command".to_vec(),
            is_command: true,
        }],
        installed_snapshot: (node.last_snapshot_index > 0).then(|| RustRaftSnapshotMeta {
            snapshot_id: format!("snapshot-{}", node.last_snapshot_index),
            last_log_id: RustRaftLogId {
                term: 4,
                index: node.last_snapshot_index,
            },
            membership: vec![1, 2, 3],
            members: Vec::new(),
        }),
        apply_snapshot_fence: RustRaftApplySnapshotFence {
            applied_index: node.applied_index,
            commit_index: node.commit_index,
            installed_snapshot_index: node.last_snapshot_index,
            first_retained_log_index: if node.last_snapshot_index > 0 {
                node.last_snapshot_index + 1
            } else {
                0
            },
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

#[test]
fn three_node_replication_and_restart_recover_committed_state() {
    let mut nodes = three_node_cluster();
    replicate(&mut nodes, 3);
    nodes[1].restarted = true;

    let recovered = matrixraft_recover_latest_wal_record(&[
        wal_record(&nodes[0]),
        wal_record(&nodes[1]),
        wal_record(&nodes[2]),
    ])
    .unwrap();

    assert_eq!(recovered.hard_state.committed.unwrap().index, 3);
    assert!(nodes.iter().all(|node| node.applied_index == 3));
}

#[test]
fn learner_catchup_promotion_and_witness_quorum_are_modeled() {
    let mut nodes = three_node_cluster();
    nodes.push(ModelNode {
        id: 4,
        role: RustRaftRole::Learner,
        replica_role: RustRaftReplicaRole::Learner,
        commit_index: 0,
        applied_index: 0,
        last_snapshot_index: 0,
        restarted: false,
    });
    replicate(&mut nodes, 5);

    let leader_status = status_for(&nodes[0], &nodes);
    assert!(matrixraft_learner_promotion_decision(&leader_status, 4, 0).promotable);

    nodes[3].replica_role = RustRaftReplicaRole::Witness;
    assert!(nodes[3].replica_role.participates_in_quorum());
    assert!(!nodes[3].replica_role.can_serve_data());
}

#[test]
fn leader_failover_and_transfer_preserve_read_safety() {
    let mut nodes = three_node_cluster();
    replicate(&mut nodes, 6);
    nodes[0].role = RustRaftRole::Follower;
    nodes[1].role = RustRaftRole::Leader;

    let decision = matrixraft_read_safety_runtime_decision(RustRaftReadSafetyRuntimeInput {
        operation: RustRaftReadSafetyOperation::ReadIndex,
        node_id: 2,
        leader_id: 2,
        node_alive: true,
        role_can_serve_data: true,
        leader_lease_valid: true,
        has_majority: true,
        node_commit_index: nodes[1].commit_index,
        leader_commit_index: nodes[1].commit_index,
        max_stale_index_lag: 0,
    });

    assert!(decision.allowed);
    assert_eq!(status_for(&nodes[1], &nodes).leader_id, Some(2));
}

#[test]
fn snapshot_install_after_compaction_keeps_fence_and_recovery_valid() {
    let mut nodes = three_node_cluster();
    replicate(&mut nodes, 9);
    nodes[2].last_snapshot_index = 9;

    let recovered = matrixraft_recover_latest_wal_record(&[wal_record(&nodes[2])]).unwrap();
    assert_eq!(
        recovered
            .installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_log_id.index),
        Some(9)
    );
    assert_eq!(recovered.apply_snapshot_fence.first_retained_log_index, 10);
}
