// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_append_safety_decision, matrixraft_learner_promotion_decision,
    matrixraft_membership_readiness_report, matrixraft_read_safety_decision,
    matrixraft_recover_latest_wal_record, matrixraft_validate_apply_snapshot_fence,
    matrixraft_wal_checksum, AppendEntriesRequest, ApplySnapshotFence, HardState, LogEntry, LogId,
    Membership, MembershipScope, MembershipTransitionEvidence, MembershipTransitionKind,
    PeerStatus, PendingReadIndexQueue, ReadIndexRequest, ReplicaRole, SnapshotMetadata, StateRole,
    StatusSnapshot, WalRecord,
};

fn status(role: StateRole, applied_index: u64) -> StatusSnapshot {
    StatusSnapshot {
        group_id: 7,
        node_id: 1,
        role,
        term: 3,
        leader_id: Some(1),
        commit_index: 10,
        applied_index,
        last_log_index: 10,
        last_snapshot_index: 4,
        peers: vec![PeerStatus {
            node_id: 2,
            matched: 10,
            next_index: 11,
            learner: true,
            healthy: true,
            lag: 0,
        }],
    }
}

fn wal_record(commit_index: u64, snapshot_index: Option<u64>) -> WalRecord {
    let snapshot = snapshot_index.map(|index| SnapshotMetadata {
        snapshot_id: format!("snapshot-{index}"),
        last_log_id: LogId { term: 3, index },
        membership: vec![1, 2, 3],
        members: Vec::new(),
    });
    let mut record = WalRecord {
        entries_are_delta: false,
        group_id: 7,
        node_id: 1,
        hard_state: HardState {
            current_term: 3,
            voted_for: Some(1),
            committed: Some(LogId {
                term: 3,
                index: commit_index,
            }),
        },
        membership: Membership {
            group_id: 7,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries: vec![LogEntry {
            log_id: LogId {
                term: 3,
                index: commit_index,
            },
            payload: b"write".to_vec(),
            is_command: true,
        }],
        installed_snapshot: snapshot,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: commit_index,
            commit_index,
            installed_snapshot_index: snapshot_index.unwrap_or_default(),
            first_retained_log_index: snapshot_index.map(|index| index + 1).unwrap_or_default(),
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

fn transition(
    scope: MembershipScope,
    transition: MembershipTransitionKind,
) -> MembershipTransitionEvidence {
    MembershipTransitionEvidence {
        scope,
        transition,
        before_voters: match transition {
            MembershipTransitionKind::ScaleDown => vec![1, 2, 3, 4],
            _ => vec![1, 2, 3],
        },
        after_voters: match transition {
            MembershipTransitionKind::ScaleUp => vec![1, 2, 3, 4],
            MembershipTransitionKind::ScaleDown => vec![1, 2, 3],
            MembershipTransitionKind::Failover => vec![1, 2, 3],
        },
        before_learners: match transition {
            MembershipTransitionKind::ScaleUp => vec![4],
            _ => Vec::new(),
        },
        after_learners: Vec::new(),
        leader_before: Some(1),
        leader_after: Some(2),
        failed_or_removed_nodes: match transition {
            MembershipTransitionKind::Failover => vec![1],
            MembershipTransitionKind::ScaleDown => vec![4],
            MembershipTransitionKind::ScaleUp => Vec::new(),
        },
        added_nodes: match transition {
            MembershipTransitionKind::ScaleUp => vec![4],
            _ => Vec::new(),
        },
        caught_up_nodes: vec![1, 2, 3, 4],
        commit_index_before: 10,
        commit_index_after: 12,
        applied_index_after: 12,
        joint_consensus_used: !matches!(transition, MembershipTransitionKind::Failover),
        old_majority_preserved: true,
        new_majority_reached: true,
        joint_old_quorum_size: match transition {
            MembershipTransitionKind::ScaleDown => 3,
            MembershipTransitionKind::ScaleUp => 2,
            MembershipTransitionKind::Failover => 0,
        },
        joint_new_quorum_size: match transition {
            MembershipTransitionKind::ScaleUp => 3,
            MembershipTransitionKind::ScaleDown => 2,
            MembershipTransitionKind::Failover => 0,
        },
        joint_acknowledged_voters: match transition {
            MembershipTransitionKind::ScaleUp | MembershipTransitionKind::ScaleDown => {
                vec![1, 2, 3, 4]
            }
            MembershipTransitionKind::Failover => Vec::new(),
        },
        joint_old_majority_acked: !matches!(transition, MembershipTransitionKind::Failover),
        joint_new_majority_acked: !matches!(transition, MembershipTransitionKind::Failover),
        stale_leader_rejected: true,
        read_index_validated_after: true,
        write_validated_after: true,
        snapshot_floor_preserved: true,
        secondary_replication_visible: true,
        scheduler_generation_advanced: matches!(scope, MembershipScope::Metaserver),
        blockers: Vec::new(),
    }
}

#[test]
fn raft_safety_helpers_reject_non_leader_and_apply_lag() {
    let follower_decision = matrixraft_read_safety_decision(
        &status(StateRole::Follower, 10),
        &ReadIndexRequest {
            group_id: 7,
            requester_id: 2,
            min_commit_index: 10,
            allow_lease_read: true,
        },
    );
    assert!(!follower_decision.safe);
    assert_eq!(follower_decision.reason, "not_leader");

    let lag_decision = matrixraft_read_safety_decision(
        &status(StateRole::Leader, 9),
        &ReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 10,
            allow_lease_read: false,
        },
    );
    assert!(!lag_decision.safe);
    assert_eq!(lag_decision.reason, "apply_lag");
}

#[test]
fn pending_read_index_queue_releases_only_after_apply_fence() {
    let mut queue = PendingReadIndexQueue::new();
    queue.push(
        ReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 10,
            allow_lease_read: false,
        },
        10,
    );
    queue.push(
        ReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 12,
            allow_lease_read: true,
        },
        11,
    );

    assert_eq!(queue.pending_len(), 2);
    assert!(queue.notify_applied(9).is_empty());
    assert_eq!(queue.pending_len(), 2);

    let first = queue.notify_applied(10);
    assert_eq!(first.len(), 1);
    assert!(first[0].ready);
    assert_eq!(first[0].read_index, 10);
    assert_eq!(first[0].applied_index, 10);
    assert_eq!(first[0].reason, "read_index_applied");
    assert_eq!(queue.pending_len(), 1);

    assert!(queue.notify_applied(11).is_empty());
    let second = queue.notify_applied(12);
    assert_eq!(second.len(), 1);
    assert!(second[0].ready);
    assert_eq!(second[0].read_index, 11);
    assert_eq!(second[0].request.min_commit_index, 12);
    assert!(queue.is_empty());
}

#[test]
fn pending_read_index_queue_releases_waiters_on_node_removal() {
    let mut queue = PendingReadIndexQueue::new();
    queue.push(
        ReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 10,
            allow_lease_read: false,
        },
        10,
    );
    queue.push(
        ReadIndexRequest {
            group_id: 7,
            requester_id: 2,
            min_commit_index: 11,
            allow_lease_read: false,
        },
        11,
    );

    let released = queue.release_all(8, "not_leader");
    assert_eq!(released.len(), 2);
    assert!(released.iter().all(|result| !result.ready));
    assert!(released.iter().all(|result| result.reason == "not_leader"));
    assert_eq!(
        released
            .iter()
            .map(|result| result.request.requester_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(queue.is_empty());
}

#[test]
fn membership_transitions_require_safe_failover_scale_up_and_scale_down() {
    let transitions = [MembershipScope::Metaserver, MembershipScope::DataNode]
        .into_iter()
        .flat_map(|scope| {
            [
                MembershipTransitionKind::Failover,
                MembershipTransitionKind::ScaleUp,
                MembershipTransitionKind::ScaleDown,
            ]
            .into_iter()
            .map(move |kind| transition(scope, kind))
        })
        .collect::<Vec<_>>();

    let report = matrixraft_membership_readiness_report(&transitions);
    assert!(report.ready, "{report:#?}");
    assert_eq!(report.decisions.len(), 6);
}

#[test]
fn wal_recovery_uses_latest_record_with_valid_snapshot_fence() {
    let old = wal_record(10, Some(8));
    let mut corrupt_new = wal_record(11, Some(9));
    corrupt_new.apply_snapshot_fence.applied_index = 12;

    let recovered = matrixraft_recover_latest_wal_record(&[old.clone(), corrupt_new]).unwrap();
    assert_eq!(recovered.hard_state.committed.unwrap().index, 10);
    assert_eq!(recovered.checksum, old.checksum);
}

#[test]
fn snapshot_fence_rejects_snapshot_floor_overlap() {
    let mut record = wal_record(10, Some(8));
    record.apply_snapshot_fence.first_retained_log_index = 8;

    let err = matrixraft_validate_apply_snapshot_fence(&record).unwrap_err();
    assert!(err.to_string().contains("overlaps installed snapshot"));
}

#[test]
fn compacted_entry_rejection_blocks_prev_log_before_snapshot_floor() {
    let decision = matrixraft_append_safety_decision(
        9,
        8,
        &AppendEntriesRequest {
            group_id: 7,
            term: 3,
            leader_id: 1,
            prev_log_id: Some(LogId { term: 2, index: 8 }),
            entries: Vec::new(),
            leader_commit: 10,
            lease_epoch: 0,
        },
    );

    assert!(!decision.accepted);
    assert!(decision.rejected_compacted_entry);
}

#[test]
fn read_safety_and_learner_promotion_accept_caught_up_learner() {
    let status = status(StateRole::Leader, 10);
    let read = matrixraft_read_safety_decision(
        &status,
        &ReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 10,
            allow_lease_read: true,
        },
    );
    assert!(read.safe);
    assert!(read.lease_read);

    let learner = matrixraft_learner_promotion_decision(&status, 2, 0);
    assert!(learner.promotable);
    assert!(ReplicaRole::Witness.participates_in_quorum());
}
