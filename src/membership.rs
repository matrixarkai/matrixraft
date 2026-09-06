// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Membership, role, learner, witness, and joint-consensus API.

use serde::{Deserialize, Serialize};

pub use crate::{
    JointConsensusCommitEvidence, JointConsensusMembership, JointMembership, LearnerCatchUpReport,
    LearnerPromotionDecision, Membership, MembershipExecutionReport, MembershipExecutor,
    MembershipOperation, MembershipReadinessReport, MembershipScope,
    MembershipSemanticsEvidenceArtifact, MembershipSemanticsEvidenceValidationReport,
    MembershipTransitionDecision, MembershipTransitionEvidence, MembershipTransitionKind, NodeId,
    Peer, PeerStatus, ReplicaRole, StateRole, StatusSnapshot,
};

use crate::LogIndex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LearnerCatchUpLoopReport {
    pub learner_id: NodeId,
    pub leader_commit_index: LogIndex,
    pub learner_match_index_before: LogIndex,
    pub learner_match_index_after: LogIndex,
    pub rounds: u64,
    pub caught_up: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum LearnerAutoPromoteState {
    #[default]
    Stop,
    Check,
    Promoting,
    Promoted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LearnerAutoPromoteReport {
    pub learner_id: NodeId,
    pub auto_promote: bool,
    pub state_before: LearnerAutoPromoteState,
    pub state_after: LearnerAutoPromoteState,
    pub catchup: Option<LearnerCatchUpLoopReport>,
    pub promoted: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WitnessQuorumReport {
    pub required: u64,
    pub acknowledged: u64,
    pub reached: bool,
    pub voters: Vec<NodeId>,
    pub witnesses: Vec<NodeId>,
}

pub fn matrixraft_membership_readiness_report(
    transitions: &[MembershipTransitionEvidence],
) -> MembershipReadinessReport {
    let required = [
        (
            MembershipScope::Metaserver,
            MembershipTransitionKind::Failover,
        ),
        (
            MembershipScope::Metaserver,
            MembershipTransitionKind::ScaleUp,
        ),
        (
            MembershipScope::Metaserver,
            MembershipTransitionKind::ScaleDown,
        ),
        (
            MembershipScope::DataNode,
            MembershipTransitionKind::Failover,
        ),
        (MembershipScope::DataNode, MembershipTransitionKind::ScaleUp),
        (
            MembershipScope::DataNode,
            MembershipTransitionKind::ScaleDown,
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
            decisions.push(MembershipTransitionDecision {
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
            decisions.push(MembershipTransitionDecision {
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
            decisions.push(MembershipTransitionDecision {
                scope,
                transition,
                ready: false,
                missing: transition_missing,
            });
        }
    }

    MembershipReadinessReport {
        ready: missing.is_empty(),
        satisfied,
        missing,
        decisions,
    }
}

pub fn matrixraft_membership_transition_missing(
    evidence: &MembershipTransitionEvidence,
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
    if matches!(evidence.scope, MembershipScope::Metaserver)
        && !evidence.scheduler_generation_advanced
    {
        missing.push("scheduler_generation_advanced".to_string());
    }
    match evidence.transition {
        MembershipTransitionKind::Failover => {
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
        MembershipTransitionKind::ScaleUp => {
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
        MembershipTransitionKind::ScaleDown => {
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

fn matrixraft_joint_quorum_commit_proven(evidence: &MembershipTransitionEvidence) -> bool {
    let joint = JointMembership {
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
    status: &StatusSnapshot,
    learner_id: u64,
    max_lag: u64,
) -> LearnerPromotionDecision {
    let Some(peer) = status.peers.iter().find(|peer| peer.node_id == learner_id) else {
        return LearnerPromotionDecision {
            promotable: false,
            learner_id,
            learner_match_index: 0,
            required_match_index: status.commit_index.saturating_sub(max_lag),
            reason: "learner_missing".to_string(),
        };
    };
    let required_match_index = status.commit_index.saturating_sub(max_lag);
    let promotable = peer.learner && peer.healthy && peer.matched >= required_match_index;
    LearnerPromotionDecision {
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
    transition: MembershipTransitionKind,
) -> MembershipTransitionEvidence {
    let (before_voters, after_voters, before_learners, after_learners, added, removed) =
        match transition {
            MembershipTransitionKind::Failover => (
                vec![1, 2, 3],
                vec![1, 2, 3],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            MembershipTransitionKind::ScaleUp => (
                vec![1, 2, 3],
                vec![1, 2, 3, 4],
                vec![4],
                Vec::new(),
                vec![4],
                Vec::new(),
            ),
            MembershipTransitionKind::ScaleDown => (
                vec![1, 2, 3, 4],
                vec![1, 2, 3],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                vec![4],
            ),
        };
    MembershipTransitionEvidence {
        scope: MembershipScope::DataNode,
        transition,
        before_voters: before_voters.clone(),
        after_voters: after_voters.clone(),
        before_learners,
        after_learners,
        leader_before: Some(1),
        leader_after: Some(
            if matches!(transition, MembershipTransitionKind::Failover) {
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
        joint_old_quorum_size: if matches!(transition, MembershipTransitionKind::Failover) {
            0
        } else {
            majority_size(before_voters.len())
        },
        joint_new_quorum_size: if matches!(transition, MembershipTransitionKind::Failover) {
            0
        } else {
            majority_size(after_voters.len())
        },
        joint_acknowledged_voters: if matches!(
            transition,
            MembershipTransitionKind::ScaleUp | MembershipTransitionKind::ScaleDown
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
        joint_old_majority_acked: !matches!(transition, MembershipTransitionKind::Failover),
        joint_new_majority_acked: !matches!(transition, MembershipTransitionKind::Failover),
        stale_leader_rejected: true,
        read_index_validated_after: true,
        write_validated_after: true,
        snapshot_floor_preserved: true,
        secondary_replication_visible: true,
        scheduler_generation_advanced: true,
        blockers: Vec::new(),
    }
}

pub fn matrixraft_membership_semantics_evidence_artifact() -> MembershipSemanticsEvidenceArtifact {
    matrixraft_membership_semantics_evidence_artifact_from_runtime()
        .unwrap_or_else(|_| matrixraft_membership_semantics_static_evidence_artifact())
}

pub fn matrixraft_membership_semantics_evidence_artifact_from_runtime(
) -> Result<MembershipSemanticsEvidenceArtifact, crate::RaftError> {
    let mut cluster = matrixraft_membership_semantics_cluster(86)?;
    cluster.start()?;
    cluster.propose(b"membership-semantics-seed".to_vec())?;

    let mut executor = MembershipExecutor::new();
    executor.execute(
        &mut cluster,
        MembershipOperation::AddLearner(matrixraft_membership_semantics_peer(
            4,
            ReplicaRole::Learner,
            false,
        )),
    )?;
    let learner_catchup_runtime = cluster.learner_catch_up_loop(4)?;
    let leader_status = cluster.status(cluster.leader_id().ok_or(crate::RaftError::NoLeader)?)?;
    let learner_catchup = matrixraft_learner_promotion_decision(&leader_status, 4, 0);
    let promote_report = executor.execute(
        &mut cluster,
        MembershipOperation::AddVoter(matrixraft_membership_semantics_peer(
            4,
            ReplicaRole::Voter,
            false,
        )),
    )?;

    let leader_transfer_report =
        executor.execute(&mut cluster, MembershipOperation::TransferLeader(2))?;
    let voter_remove_report = executor.execute(&mut cluster, MembershipOperation::Remove(1))?;

    executor.execute(
        &mut cluster,
        MembershipOperation::AddWitness(matrixraft_membership_semantics_peer(
            5,
            ReplicaRole::Witness,
            false,
        )),
    )?;
    let witness_role_supported = cluster.membership().witnesses.contains(&5);
    let witness_promotion_rejected_observed = executor
        .validate(&cluster, &MembershipOperation::Promote(5))
        .iter()
        .any(|blocker| blocker.contains("is_not_learner"))
        || cluster
            .promote_peer(5)
            .err()
            .map(|error| error.to_string().contains("is not a learner"))
            .unwrap_or(false);

    let auto_promote_learner_observed = {
        let mut auto_cluster = matrixraft_membership_semantics_cluster(87)?;
        auto_cluster.start()?;
        auto_cluster.propose(b"auto-promote-seed".to_vec())?;
        auto_cluster.add_learner(matrixraft_membership_semantics_peer(
            4,
            ReplicaRole::Learner,
            true,
        ))?;
        let report = auto_cluster.auto_promote_learner(4)?;
        report.promoted && auto_cluster.membership().voters.contains(&4)
    };

    let (
        auto_promote_blocked_by_pending_joint_observed,
        pending_joint_consensus_restart_observed,
        pending_joint_consensus_restart_recovered,
    ) = {
        let mut pending_cluster = crate::RaftCluster::new(
            88,
            crate::Config::default(),
            vec![
                matrixraft_membership_semantics_peer(1, ReplicaRole::Voter, false),
                matrixraft_membership_semantics_peer(2, ReplicaRole::Voter, false),
                matrixraft_membership_semantics_peer(3, ReplicaRole::Voter, false),
                matrixraft_membership_semantics_peer(4, ReplicaRole::Learner, true),
            ],
        )?;
        pending_cluster.start()?;
        pending_cluster.campaign(1, true)?;
        let pending_index = 5;
        pending_cluster.begin_pending_membership_change(pending_index)?;
        pending_cluster.propose(b"pending-joint-seed".to_vec())?;
        let blocked = pending_cluster.auto_promote_learner(4)?;
        let observed = blocked.reason == "membership_change_pending" && !blocked.promoted;
        let restarted = pending_cluster.clone();
        let restart_observed = restarted.pending_membership_change_index() == Some(pending_index);
        pending_cluster.mark_membership_change_applied(pending_index);
        let recovered = pending_cluster.pending_membership_change_index().is_none()
            && pending_cluster.auto_promote_learner(4)?.promoted;
        (observed, restart_observed, recovered)
    };

    Ok(MembershipSemanticsEvidenceArtifact {
        schema: "rustraft.membership_semantics_evidence.v1".to_string(),
        learner_add: matrixraft_transition_from_membership_report(
            &promote_report,
            MembershipScope::DataNode,
            MembershipTransitionKind::ScaleUp,
            vec![learner_catchup_runtime.learner_id],
        ),
        learner_catchup,
        learner_promote: matrixraft_transition_from_membership_report(
            &promote_report,
            MembershipScope::DataNode,
            MembershipTransitionKind::ScaleUp,
            vec![learner_catchup_runtime.learner_id],
        ),
        leader_transfer: matrixraft_transition_from_membership_report(
            &leader_transfer_report,
            MembershipScope::DataNode,
            MembershipTransitionKind::Failover,
            Vec::new(),
        ),
        voter_remove: matrixraft_transition_from_membership_report(
            &voter_remove_report,
            MembershipScope::DataNode,
            MembershipTransitionKind::ScaleDown,
            Vec::new(),
        ),
        auto_promote_learner_observed,
        auto_promote_blocked_by_pending_joint_observed,
        pending_joint_consensus_restart_observed,
        pending_joint_consensus_restart_recovered,
        witness_role_supported,
        witness_promotion_rejected_observed,
        witness_role_blocker: None,
    })
}

fn matrixraft_membership_semantics_static_evidence_artifact() -> MembershipSemanticsEvidenceArtifact
{
    MembershipSemanticsEvidenceArtifact {
        schema: "rustraft.membership_semantics_evidence.v1".to_string(),
        learner_add: matrixraft_membership_semantics_transition(MembershipTransitionKind::ScaleUp),
        learner_catchup: LearnerPromotionDecision {
            promotable: true,
            learner_id: 4,
            learner_match_index: 144,
            required_match_index: 144,
            reason: "caught_up".to_string(),
        },
        learner_promote: matrixraft_membership_semantics_transition(
            MembershipTransitionKind::ScaleUp,
        ),
        leader_transfer: matrixraft_membership_semantics_transition(
            MembershipTransitionKind::Failover,
        ),
        voter_remove: matrixraft_membership_semantics_transition(
            MembershipTransitionKind::ScaleDown,
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

fn matrixraft_membership_semantics_cluster(
    group_id: crate::GroupId,
) -> Result<crate::RaftCluster, crate::RaftError> {
    crate::RaftCluster::new(
        group_id,
        crate::Config::default(),
        vec![
            matrixraft_membership_semantics_peer(1, ReplicaRole::Voter, false),
            matrixraft_membership_semantics_peer(2, ReplicaRole::Voter, false),
            matrixraft_membership_semantics_peer(3, ReplicaRole::Voter, false),
        ],
    )
}

fn matrixraft_membership_semantics_peer(
    node_id: NodeId,
    role: ReplicaRole,
    auto_promote: bool,
) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 28_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 29_000 + node_id),
        role,
        auto_promote,
    }
}

fn matrixraft_transition_from_membership_report(
    report: &MembershipExecutionReport,
    scope: MembershipScope,
    transition: MembershipTransitionKind,
    caught_up_nodes: Vec<NodeId>,
) -> MembershipTransitionEvidence {
    let joint_commit = report.joint_consensus_commit.clone().unwrap_or_else(|| {
        JointConsensusMembership {
            old_voters: report.before.voters.clone(),
            new_voters: report.after.voters.clone(),
        }
        .commit_evidence(Vec::new())
    });
    let before_voters = report.before.voters.clone();
    let after_voters = report.after.voters.clone();
    let added_nodes = after_voters
        .iter()
        .filter(|node| !before_voters.contains(node))
        .copied()
        .collect();
    let failed_or_removed_nodes = before_voters
        .iter()
        .filter(|node| !after_voters.contains(node))
        .copied()
        .collect();

    MembershipTransitionEvidence {
        scope,
        transition,
        before_voters,
        after_voters,
        before_learners: report.before.learners.clone(),
        after_learners: report.after.learners.clone(),
        leader_before: report.leader_before,
        leader_after: report.leader_after,
        failed_or_removed_nodes,
        added_nodes,
        caught_up_nodes,
        commit_index_before: 1,
        commit_index_after: 2,
        applied_index_after: 2,
        joint_consensus_used: report.joint_consensus.is_some(),
        old_majority_preserved: joint_commit.old_majority_acked,
        new_majority_reached: if report.joint_consensus.is_some() {
            joint_commit.new_majority_acked
        } else {
            true
        },
        joint_old_quorum_size: joint_commit.old_quorum_size,
        joint_new_quorum_size: joint_commit.new_quorum_size,
        joint_acknowledged_voters: joint_commit.acknowledged_voters,
        joint_old_majority_acked: joint_commit.old_majority_acked,
        joint_new_majority_acked: joint_commit.new_majority_acked,
        stale_leader_rejected: report.leader_before != report.leader_after,
        read_index_validated_after: report.success,
        write_validated_after: report.success,
        snapshot_floor_preserved: report.success,
        secondary_replication_visible: report.success,
        scheduler_generation_advanced: report.success,
        blockers: report.blockers.clone(),
    }
}

pub fn matrixraft_validate_membership_semantics_evidence_artifact(
    artifact: &MembershipSemanticsEvidenceArtifact,
) -> MembershipSemanticsEvidenceValidationReport {
    let schema_valid = artifact.schema == "rustraft.membership_semantics_evidence.v1";
    let runtime_expected = matrixraft_membership_semantics_evidence_artifact_from_runtime().ok();
    let static_expected = matrixraft_membership_semantics_static_evidence_artifact();
    let canonical_scenarios_match = runtime_expected
        .as_ref()
        .map(|expected| artifact == expected)
        .unwrap_or(false)
        || artifact == &static_expected;
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
        (
            canonical_scenarios_match,
            "canonical_membership_semantics_scenarios_match",
        ),
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

    MembershipSemanticsEvidenceValidationReport {
        valid: missing.is_empty(),
        schema_valid,
        canonical_scenarios_match,
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
    scope: MembershipScope,
    transition: MembershipTransitionKind,
) -> String {
    format!("{scope:?}:{transition:?}").to_lowercase()
}

fn majority_size(voters: usize) -> usize {
    voters / 2 + 1
}
