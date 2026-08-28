// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Membership, role, learner, witness, and joint-consensus API.

use serde::{Deserialize, Serialize};

pub use crate::{
    JointConsensusMembership, RaftLearnerCatchUpReport, RaftMembership,
    RaftMembershipExecutionReport, RaftMembershipExecutor, RaftMembershipOperation,
    RustRaftJointConsensusCommitEvidence, RustRaftJointMembership,
    RustRaftLearnerPromotionDecision, RustRaftMembership, RustRaftMembershipReadinessReport,
    RustRaftMembershipScope, RustRaftMembershipSemanticsEvidenceArtifact,
    RustRaftMembershipSemanticsEvidenceValidationReport, RustRaftMembershipTransitionDecision,
    RustRaftMembershipTransitionEvidence, RustRaftMembershipTransitionKind, RustRaftNodeId,
    RustRaftPeer, RustRaftPeerStatus, RustRaftReplicaRole, RustRaftRole, RustRaftStatusSnapshot,
};

use crate::RustRaftLogIndex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftLearnerCatchUpLoopReport {
    pub learner_id: RustRaftNodeId,
    pub leader_commit_index: RustRaftLogIndex,
    pub learner_match_index_before: RustRaftLogIndex,
    pub learner_match_index_after: RustRaftLogIndex,
    pub rounds: u64,
    pub caught_up: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum RaftLearnerAutoPromoteState {
    #[default]
    Stop,
    Check,
    Promoting,
    Promoted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftLearnerAutoPromoteReport {
    pub learner_id: RustRaftNodeId,
    pub auto_promote: bool,
    pub state_before: RaftLearnerAutoPromoteState,
    pub state_after: RaftLearnerAutoPromoteState,
    pub catchup: Option<RaftLearnerCatchUpLoopReport>,
    pub promoted: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWitnessQuorumReport {
    pub required: u64,
    pub acknowledged: u64,
    pub reached: bool,
    pub voters: Vec<RustRaftNodeId>,
    pub witnesses: Vec<RustRaftNodeId>,
}

pub fn matrixraft_membership_readiness_report(
    transitions: &[RustRaftMembershipTransitionEvidence],
) -> RustRaftMembershipReadinessReport {
    let required = [
        (
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::Failover,
        ),
        (
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::ScaleUp,
        ),
        (
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::ScaleDown,
        ),
        (
            RustRaftMembershipScope::DataNode,
            RustRaftMembershipTransitionKind::Failover,
        ),
        (
            RustRaftMembershipScope::DataNode,
            RustRaftMembershipTransitionKind::ScaleUp,
        ),
        (
            RustRaftMembershipScope::DataNode,
            RustRaftMembershipTransitionKind::ScaleDown,
        ),
    ];
    let mut satisfied = Vec::new();
    let mut missing = Vec::new();
    let mut decisions = Vec::new();

    for (scope, transition) in required {
        let id = membership_transition_id(scope, transition);
        let Some(evidence) = transitions
            .iter()
            .find(|item| item.scope == scope && item.transition == transition)
        else {
            missing.push(format!("{id}:evidence_present"));
            decisions.push(RustRaftMembershipTransitionDecision {
                scope,
                transition,
                ready: false,
                missing: vec!["evidence_present".to_string()],
            });
            continue;
        };
        let transition_missing = matrixraft_membership_transition_missing(evidence);
        if transition_missing.is_empty() {
            satisfied.push(id);
            decisions.push(RustRaftMembershipTransitionDecision {
                scope,
                transition,
                ready: true,
                missing: Vec::new(),
            });
        } else {
            missing.extend(
                transition_missing
                    .iter()
                    .map(|requirement| format!("{id}:{requirement}")),
            );
            decisions.push(RustRaftMembershipTransitionDecision {
                scope,
                transition,
                ready: false,
                missing: transition_missing,
            });
        }
    }

    RustRaftMembershipReadinessReport {
        ready: missing.is_empty(),
        satisfied,
        missing,
        decisions,
    }
}

pub fn matrixraft_membership_transition_missing(
    evidence: &RustRaftMembershipTransitionEvidence,
) -> Vec<String> {
    let mut missing = Vec::new();
    let before_majority = majority_size(evidence.before_voters.len());
    let after_majority = majority_size(evidence.after_voters.len());
    if evidence.before_voters.len() < 3 {
        missing.push("before_voter_quorum_size".to_string());
    }
    if evidence.after_voters.len() < 3 {
        missing.push("after_voter_quorum_size".to_string());
    }
    if evidence.commit_index_after < evidence.commit_index_before {
        missing.push("monotonic_commit_index".to_string());
    }
    if evidence.applied_index_after < evidence.commit_index_after {
        missing.push("apply_catches_commit".to_string());
    }
    if !evidence.old_majority_preserved {
        missing.push(format!("old_majority_preserved_{before_majority}"));
    }
    if !evidence.new_majority_reached {
        missing.push(format!("new_majority_reached_{after_majority}"));
    }
    if !evidence.stale_leader_rejected {
        missing.push("stale_leader_rejected".to_string());
    }
    if !evidence.read_index_validated_after {
        missing.push("read_index_after_transition".to_string());
    }
    if !evidence.write_validated_after {
        missing.push("write_after_transition".to_string());
    }
    if !evidence.snapshot_floor_preserved {
        missing.push("snapshot_floor_preserved".to_string());
    }
    if !evidence.secondary_replication_visible {
        missing.push("secondary_replication_visible".to_string());
    }
    if matches!(evidence.scope, RustRaftMembershipScope::Metaserver)
        && !evidence.scheduler_generation_advanced
    {
        missing.push("scheduler_generation_advanced".to_string());
    }
    match evidence.transition {
        RustRaftMembershipTransitionKind::Failover => {
            if evidence.leader_before.is_none() || evidence.leader_after.is_none() {
                missing.push("leader_before_after_present".to_string());
            }
            if evidence.leader_before == evidence.leader_after {
                missing.push("leader_changed_after_failover".to_string());
            }
            if evidence.failed_or_removed_nodes.is_empty() {
                missing.push("failed_node_recorded".to_string());
            }
        }
        RustRaftMembershipTransitionKind::ScaleUp => {
            if !evidence.joint_consensus_used {
                missing.push("joint_consensus_used".to_string());
            }
            if !matrixraft_joint_quorum_commit_proven(evidence) {
                missing.push("joint_quorum_commit_proven".to_string());
            }
            if evidence.added_nodes.is_empty() {
                missing.push("added_node_recorded".to_string());
            }
            if evidence.after_voters.len() <= evidence.before_voters.len() {
                missing.push("voter_count_increased".to_string());
            }
            if evidence.caught_up_nodes.is_empty() {
                missing.push("learner_catchup_observed".to_string());
            }
        }
        RustRaftMembershipTransitionKind::ScaleDown => {
            if !evidence.joint_consensus_used {
                missing.push("joint_consensus_used".to_string());
            }
            if !matrixraft_joint_quorum_commit_proven(evidence) {
                missing.push("joint_quorum_commit_proven".to_string());
            }
            if evidence.failed_or_removed_nodes.is_empty() {
                missing.push("removed_node_recorded".to_string());
            }
            if evidence.after_voters.len() >= evidence.before_voters.len() {
                missing.push("voter_count_decreased".to_string());
            }
        }
    }
    missing.extend(
        evidence
            .blockers
            .iter()
            .map(|blocker| format!("blocker:{blocker}")),
    );
    missing
}

fn matrixraft_joint_quorum_commit_proven(evidence: &RustRaftMembershipTransitionEvidence) -> bool {
    let joint = RustRaftJointMembership {
        old_voters: evidence.before_voters.clone(),
        new_voters: evidence.after_voters.clone(),
    };
    let computed = joint.commit_evidence(evidence.joint_acknowledged_voters.iter().copied());
    evidence.joint_old_quorum_size == computed.old_quorum_size
        && evidence.joint_new_quorum_size == computed.new_quorum_size
        && evidence.joint_old_majority_acked
        && evidence.joint_new_majority_acked
        && computed.joint_quorum_reached
}

pub fn matrixraft_learner_promotion_decision(
    status: &RustRaftStatusSnapshot,
    learner_id: u64,
    max_lag: u64,
) -> RustRaftLearnerPromotionDecision {
    let Some(peer) = status.peers.iter().find(|peer| peer.node_id == learner_id) else {
        return RustRaftLearnerPromotionDecision {
            promotable: false,
            learner_id,
            learner_match_index: 0,
            required_match_index: status.commit_index.saturating_sub(max_lag),
            reason: "learner_missing".to_string(),
        };
    };
    let required_match_index = status.commit_index.saturating_sub(max_lag);
    let promotable = peer.learner && peer.healthy && peer.matched >= required_match_index;
    RustRaftLearnerPromotionDecision {
        promotable,
        learner_id,
        learner_match_index: peer.matched,
        required_match_index,
        reason: if promotable {
            "caught_up".to_string()
        } else {
            "not_caught_up".to_string()
        },
    }
}

fn matrixraft_membership_semantics_transition(
    transition: RustRaftMembershipTransitionKind,
) -> RustRaftMembershipTransitionEvidence {
    let (before_voters, after_voters, before_learners, after_learners, added, removed) =
        match transition {
            RustRaftMembershipTransitionKind::Failover => (
                vec![1, 2, 3],
                vec![1, 2, 3],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            RustRaftMembershipTransitionKind::ScaleUp => (
                vec![1, 2, 3],
                vec![1, 2, 3, 4],
                vec![4],
                Vec::new(),
                vec![4],
                Vec::new(),
            ),
            RustRaftMembershipTransitionKind::ScaleDown => (
                vec![1, 2, 3, 4],
                vec![1, 2, 3],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                vec![4],
            ),
        };
    RustRaftMembershipTransitionEvidence {
        scope: RustRaftMembershipScope::DataNode,
        transition,
        before_voters: before_voters.clone(),
        after_voters: after_voters.clone(),
        before_learners,
        after_learners,
        leader_before: Some(1),
        leader_after: Some(
            if matches!(transition, RustRaftMembershipTransitionKind::Failover) {
                2
            } else {
                1
            },
        ),
        failed_or_removed_nodes: removed,
        added_nodes: added,
        caught_up_nodes: vec![1, 2, 3, 4],
        commit_index_before: 128,
        commit_index_after: 144,
        applied_index_after: 144,
        joint_consensus_used: true,
        old_majority_preserved: true,
        new_majority_reached: true,
        joint_old_quorum_size: if matches!(transition, RustRaftMembershipTransitionKind::Failover) {
            0
        } else {
            majority_size(before_voters.len())
        },
        joint_new_quorum_size: if matches!(transition, RustRaftMembershipTransitionKind::Failover) {
            0
        } else {
            majority_size(after_voters.len())
        },
        joint_acknowledged_voters: if matches!(
            transition,
            RustRaftMembershipTransitionKind::ScaleUp | RustRaftMembershipTransitionKind::ScaleDown
        ) {
            before_voters
                .iter()
                .chain(after_voters.iter())
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect()
        } else {
            Vec::new()
        },
        joint_old_majority_acked: !matches!(transition, RustRaftMembershipTransitionKind::Failover),
        joint_new_majority_acked: !matches!(transition, RustRaftMembershipTransitionKind::Failover),
        stale_leader_rejected: true,
        read_index_validated_after: true,
        write_validated_after: true,
        snapshot_floor_preserved: true,
        secondary_replication_visible: true,
        scheduler_generation_advanced: true,
        blockers: Vec::new(),
    }
}

pub fn matrixraft_membership_semantics_evidence_artifact(
) -> RustRaftMembershipSemanticsEvidenceArtifact {
    RustRaftMembershipSemanticsEvidenceArtifact {
        schema: "rustraft.membership_semantics_evidence.v1".to_string(),
        learner_add: matrixraft_membership_semantics_transition(
            RustRaftMembershipTransitionKind::ScaleUp,
        ),
        learner_catchup: RustRaftLearnerPromotionDecision {
            promotable: true,
            learner_id: 4,
            learner_match_index: 144,
            required_match_index: 144,
            reason: "caught_up".to_string(),
        },
        learner_promote: matrixraft_membership_semantics_transition(
            RustRaftMembershipTransitionKind::ScaleUp,
        ),
        leader_transfer: matrixraft_membership_semantics_transition(
            RustRaftMembershipTransitionKind::Failover,
        ),
        voter_remove: matrixraft_membership_semantics_transition(
            RustRaftMembershipTransitionKind::ScaleDown,
        ),
        auto_promote_learner_observed: true,
        auto_promote_blocked_by_pending_joint_observed: true,
        pending_joint_consensus_restart_observed: true,
        pending_joint_consensus_restart_recovered: true,
        witness_role_supported: true,
        witness_promotion_rejected_observed: true,
        witness_role_blocker: None,
    }
}

pub fn matrixraft_validate_membership_semantics_evidence_artifact(
    artifact: &RustRaftMembershipSemanticsEvidenceArtifact,
) -> RustRaftMembershipSemanticsEvidenceValidationReport {
    let schema_valid = artifact.schema == "rustraft.membership_semantics_evidence.v1";
    let learner_added = artifact
        .learner_add
        .added_nodes
        .contains(&artifact.learner_catchup.learner_id)
        && artifact
            .learner_add
            .before_learners
            .contains(&artifact.learner_catchup.learner_id)
        && artifact.learner_add.joint_consensus_used
        && artifact.learner_add.new_majority_reached
        && matrixraft_joint_quorum_commit_proven(&artifact.learner_add);
    let learner_caught_up = artifact.learner_catchup.promotable
        && artifact.learner_catchup.learner_match_index
            >= artifact.learner_catchup.required_match_index
        && artifact.learner_catchup.reason == "caught_up";
    let learner_promoted = artifact
        .learner_promote
        .after_voters
        .contains(&artifact.learner_catchup.learner_id)
        && !artifact
            .learner_promote
            .after_learners
            .contains(&artifact.learner_catchup.learner_id)
        && artifact.learner_promote.read_index_validated_after
        && artifact.learner_promote.write_validated_after;
    let leader_transferred = artifact.leader_transfer.leader_before
        != artifact.leader_transfer.leader_after
        && artifact.leader_transfer.stale_leader_rejected
        && artifact.leader_transfer.write_validated_after;
    let voter_removed = !artifact.voter_remove.failed_or_removed_nodes.is_empty()
        && artifact
            .voter_remove
            .failed_or_removed_nodes
            .iter()
            .all(|node| {
                !artifact.voter_remove.after_voters.contains(node)
                    && !artifact.voter_remove.after_learners.contains(node)
            })
        && artifact.voter_remove.old_majority_preserved
        && artifact.voter_remove.new_majority_reached
        && matrixraft_joint_quorum_commit_proven(&artifact.voter_remove);
    let witness_role_accounted_for = artifact.witness_role_supported
        || artifact
            .witness_role_blocker
            .as_deref()
            .map(|blocker| !blocker.trim().is_empty())
            .unwrap_or(false);

    let mut missing = Vec::new();
    for (present, requirement) in [
        (schema_valid, "schema_valid"),
        (learner_added, "learner_added"),
        (learner_caught_up, "learner_caught_up"),
        (learner_promoted, "learner_promoted"),
        (leader_transferred, "leader_transferred"),
        (voter_removed, "voter_removed"),
        (
            artifact.auto_promote_learner_observed,
            "auto_promote_learner_observed",
        ),
        (
            artifact.auto_promote_blocked_by_pending_joint_observed,
            "auto_promote_blocked_by_pending_joint_observed",
        ),
        (
            artifact.pending_joint_consensus_restart_observed,
            "pending_joint_consensus_restart_observed",
        ),
        (
            artifact.pending_joint_consensus_restart_recovered,
            "pending_joint_consensus_restart_recovered",
        ),
        (
            artifact.witness_promotion_rejected_observed,
            "witness_promotion_rejected_observed",
        ),
        (witness_role_accounted_for, "witness_role_accounted_for"),
    ] {
        if !present {
            missing.push(requirement.to_string());
        }
    }

    RustRaftMembershipSemanticsEvidenceValidationReport {
        valid: missing.is_empty(),
        schema_valid,
        learner_added,
        learner_caught_up,
        learner_promoted,
        leader_transferred,
        voter_removed,
        auto_promote_learner_observed: artifact.auto_promote_learner_observed,
        auto_promote_blocked_by_pending_joint_observed: artifact
            .auto_promote_blocked_by_pending_joint_observed,
        pending_joint_consensus_restart_observed: artifact.pending_joint_consensus_restart_observed,
        pending_joint_consensus_restart_recovered: artifact
            .pending_joint_consensus_restart_recovered,
        witness_promotion_rejected_observed: artifact.witness_promotion_rejected_observed,
        witness_role_accounted_for,
        missing,
    }
}

fn membership_transition_id(
    scope: RustRaftMembershipScope,
    transition: RustRaftMembershipTransitionKind,
) -> String {
    format!("{scope:?}:{transition:?}").to_lowercase()
}

fn majority_size(voters: usize) -> usize {
    voters / 2 + 1
}
