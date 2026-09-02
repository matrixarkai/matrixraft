// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Runtime health, cluster status, capability evidence, and admin report API.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    matrixraft_parity_report, matrixraft_public_api_contract, matrixraft_readiness_evidence,
    GroupId, LogIndex, NodeId, ParityReport, PeerProgress, PublicApiContract, ReadinessSnapshot,
    ReplicaRole, StateRole, StatusSnapshot,
};

pub use crate::{
    matrixraft_baseline_raft_operational_evidence_bundle,
    matrixraft_baseline_raft_runtime_capability_report,
    matrixraft_cross_plane_process_evidence_artifact,
    matrixraft_cross_plane_process_evidence_prometheus,
    matrixraft_cross_plane_process_evidence_summary,
    matrixraft_cross_plane_process_readiness_blocker_report,
    matrixraft_cross_plane_process_readiness_report, matrixraft_data_node_process_rollout_blockers,
    matrixraft_data_node_strict_process_rollout_validated,
    matrixraft_meta_process_rollout_blockers, matrixraft_meta_strict_process_rollout_validated,
    matrixraft_named_readiness_blockers, matrixraft_pipeline_evidence,
    matrixraft_process_readiness_blocker, matrixraft_process_readiness_field_detail,
    matrixraft_require_production_ready,
    matrixraft_validate_baseline_raft_operational_evidence_bundle,
    matrixraft_validate_cross_plane_process_evidence_artifact, matrixraft_validate_deployment_mode,
    matrixraft_validate_deployment_readiness,
    matrixraft_validate_membership_semantics_evidence_artifact,
    matrixraft_validate_read_safety_evidence_artifact,
    matrixraft_validate_replication_pipeline_evidence_artifact,
    matrixraft_validate_snapshot_lifecycle_evidence_artifact,
    matrixraft_validate_wal_lifecycle_evidence_artifact,
    matrixraft_wal_lifecycle_evidence_artifact, BaselineRaftOperationalEvidenceBundle,
    BaselineRaftOperationalEvidenceBundleValidationReport, CrossPlaneProcessEvidenceArtifact,
    CrossPlaneProcessEvidenceArtifactValidationReport, CrossPlaneProcessEvidenceSummary,
    CrossPlaneProcessReadinessBlockerReport, CrossPlaneProcessReadinessReport, DeploymentMode,
    MembershipSemanticsEvidenceArtifact, MembershipSemanticsEvidenceValidationReport,
    PipelineEvidence, PipelineLimits, ProductionReadinessError, ProductionReadinessReport,
    ReadSafetyEvidenceArtifact, ReadSafetyEvidenceValidationReport,
    ReplicationPipelineEvidenceArtifact, ReplicationPipelineEvidenceValidationReport,
    SnapshotLifecycleEvidenceArtifact, SnapshotLifecycleEvidenceValidationReport,
    WalLifecycleEvidenceArtifact, WalLifecycleEvidenceValidationReport,
};

pub use crate::fault::{
    matrixraft_baseline_raft_fault_scenarios, matrixraft_fault_harness_readiness_report,
    FaultHarnessReadinessReport, FaultScenario, FaultScenarioEvidence, FaultScenarioRequirement,
    FaultScenarioResult,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeTimerStatus {
    pub heartbeat_interval_ms: u64,
    pub election_timeout_ms: u64,
    pub leader_lease_timeout_ms: u64,
    pub leader_lease_elapsed_ms: u64,
    pub leader_lease_valid: bool,
    pub heartbeat_ticks: u64,
    pub election_ticks: u64,
    pub pre_vote_executions: u64,
    pub campaign_executions: u64,
    pub leader_transfer_executions: u64,
    pub last_tick_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerRuntimeState {
    pub node_id: NodeId,
    pub role: StateRole,
    pub replica_role: ReplicaRole,
    pub healthy: bool,
    pub matched: LogIndex,
    pub lag: LogIndex,
    pub heartbeat_due: bool,
    pub election_elapsed_ms: u64,
    pub pre_vote_sent: bool,
    pub transfer_leader_target: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderTransferState {
    pub transferee_id: NodeId,
    pub elapsed_ticks: u64,
    pub timeout_ticks: u64,
    pub aborted_transfers: u64,
    pub duplicate_requests: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LeaderTransferAdmissionKind {
    Accepted,
    IgnoredSelf,
    IgnoredUnknownPeer,
    IgnoredIneligiblePeer,
    Duplicate,
    Replaced,
}

/// What a leader-transfer request actually did.
///
/// `transfer_leader` returns `Ok` for all three, because none of them is an
/// error: an unknown or ineligible transferee is *ignored* (as etcd/raft drops
/// a `MsgTransferLeader` naming a peer it does not know), and a transferee that
/// is still catching up leaves the transfer queued. Only `Transferred` means
/// leadership moved.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LeaderTransferOutcome {
    /// The admission declined the request; nothing changed.
    Ignored,
    /// Accepted, but the transferee has not caught up yet, so it is queued.
    Pending,
    /// Leadership moved to the transferee.
    Transferred,
}

impl LeaderTransferOutcome {
    /// True only when leadership actually moved.
    pub fn is_transferred(self) -> bool {
        matches!(self, Self::Transferred)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderTransferAdmission {
    pub kind: LeaderTransferAdmissionKind,
    pub transferee_id: NodeId,
    pub previous_transferee_id: Option<NodeId>,
    pub reason: String,
}

impl LeaderTransferAdmission {
    pub fn is_accepted(&self) -> bool {
        matches!(
            self.kind,
            LeaderTransferAdmissionKind::Accepted | LeaderTransferAdmissionKind::Replaced
        )
    }

    pub fn is_duplicate(&self) -> bool {
        self.kind == LeaderTransferAdmissionKind::Duplicate
    }
}

pub fn matrixraft_leader_transfer_admission(
    local_id: NodeId,
    transferee_id: NodeId,
    current_transferee_id: Option<NodeId>,
    transferee_role: Option<ReplicaRole>,
) -> LeaderTransferAdmission {
    if transferee_id == local_id {
        return LeaderTransferAdmission {
            kind: LeaderTransferAdmissionKind::IgnoredSelf,
            transferee_id,
            previous_transferee_id: current_transferee_id,
            reason: "ignored_self_transfer".to_string(),
        };
    }
    let Some(role) = transferee_role else {
        return LeaderTransferAdmission {
            kind: LeaderTransferAdmissionKind::IgnoredUnknownPeer,
            transferee_id,
            previous_transferee_id: current_transferee_id,
            reason: "ignored_unknown_transferee".to_string(),
        };
    };
    if !role.can_be_leader() {
        return LeaderTransferAdmission {
            kind: LeaderTransferAdmissionKind::IgnoredIneligiblePeer,
            transferee_id,
            previous_transferee_id: current_transferee_id,
            reason: format!("ignored_ineligible_transferee_{role:?}"),
        };
    }
    if current_transferee_id == Some(transferee_id) {
        return LeaderTransferAdmission {
            kind: LeaderTransferAdmissionKind::Duplicate,
            transferee_id,
            previous_transferee_id: current_transferee_id,
            reason: "duplicate_transfer_in_progress".to_string(),
        };
    }
    LeaderTransferAdmission {
        kind: if current_transferee_id.is_some() {
            LeaderTransferAdmissionKind::Replaced
        } else {
            LeaderTransferAdmissionKind::Accepted
        },
        transferee_id,
        previous_transferee_id: current_transferee_id,
        reason: if current_transferee_id.is_some() {
            "replaced_transfer_in_progress".to_string()
        } else {
            "accepted_transfer".to_string()
        },
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessNodeEvidence {
    pub node_id: u64,
    pub addr: String,
    pub wal_dir: String,
    #[serde(default)]
    pub snapshot_dir: String,
    pub commit_index: u64,
    pub applied_index: u64,
    pub snapshot_id: Option<String>,
    pub restarted: bool,
    pub log_store_validated: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessOperationalSemanticsEvidence {
    #[serde(default)]
    pub api_presence_only_rejected: bool,
    #[serde(default)]
    pub process_path_validated: bool,
    #[serde(default)]
    pub read_index_validated: bool,
    #[serde(default)]
    pub leader_lease_validated: bool,
    #[serde(default)]
    pub stale_leader_lease_rejection_observed: bool,
    #[serde(default)]
    pub follower_lease_expiration_observed: bool,
    #[serde(default)]
    pub lagging_follower_read_rejected: bool,
    #[serde(default)]
    pub bounded_stale_read_acceptance_observed: bool,
    #[serde(default)]
    pub bounded_stale_read_rejection_observed: bool,
    #[serde(default)]
    pub minority_partition_read_rejection_observed: bool,
    #[serde(default)]
    pub healed_follower_catchup_observed: bool,
    #[serde(default)]
    pub stale_follower_write_rejected: bool,
    #[serde(default)]
    pub leader_transfer_exact_once_validated: bool,
    #[serde(default)]
    pub leader_transfer_under_load_validated: bool,
    #[serde(default)]
    pub snapshot_bootstrap_validated: bool,
    #[serde(default)]
    pub snapshot_install_restart_validated: bool,
    #[serde(default)]
    pub membership_rescale_validated: bool,
    #[serde(default)]
    pub membership_add_promote_remove_validated: bool,
    #[serde(default)]
    pub follower_rejoin_after_compaction_validated: bool,
    #[serde(default)]
    pub secondary_read_eligibility_validated: bool,
    #[serde(default)]
    pub apply_pipeline_converged: bool,
    #[serde(default)]
    pub wal_persistence_observed: bool,
    #[serde(default)]
    pub fsm_apply_idempotent_replay_observed: bool,
    #[serde(default)]
    pub storage_mutation_wal_fence_atomicity_observed: bool,
    #[serde(default)]
    pub snapshot_install_apply_fence_atomicity_observed: bool,
    #[serde(default)]
    pub process_restart_after_apply_crash_recovered: bool,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
}

impl ProcessOperationalSemanticsEvidence {
    pub fn proves_runtime_semantics(&self) -> bool {
        self.ready
            && self.blockers.is_empty()
            && self.api_presence_only_rejected
            && self.process_path_validated
            && self.read_index_validated
            && self.leader_lease_validated
            && self.stale_leader_lease_rejection_observed
            && self.follower_lease_expiration_observed
            && self.lagging_follower_read_rejected
            && self.bounded_stale_read_acceptance_observed
            && self.bounded_stale_read_rejection_observed
            && self.minority_partition_read_rejection_observed
            && self.healed_follower_catchup_observed
            && self.stale_follower_write_rejected
            && self.leader_transfer_exact_once_validated
            && self.leader_transfer_under_load_validated
            && self.snapshot_bootstrap_validated
            && self.snapshot_install_restart_validated
            && self.membership_rescale_validated
            && self.membership_add_promote_remove_validated
            && self.follower_rejoin_after_compaction_validated
            && self.secondary_read_eligibility_validated
            && self.apply_pipeline_converged
            && self.wal_persistence_observed
            && self.fsm_apply_idempotent_replay_observed
            && self.storage_mutation_wal_fence_atomicity_observed
            && self.snapshot_install_apply_fence_atomicity_observed
            && self.process_restart_after_apply_crash_recovered
    }

    pub fn missing_requirements(&self) -> Vec<String> {
        let mut missing = Vec::new();
        for (present, requirement) in [
            (self.ready, "operational_semantics_ready"),
            (
                self.api_presence_only_rejected,
                "api_presence_only_rejected",
            ),
            (self.process_path_validated, "process_path_validated"),
            (self.read_index_validated, "read_index_validated"),
            (self.leader_lease_validated, "leader_lease_validated"),
            (
                self.stale_leader_lease_rejection_observed,
                "stale_leader_lease_rejection_observed",
            ),
            (
                self.follower_lease_expiration_observed,
                "follower_lease_expiration_observed",
            ),
            (
                self.lagging_follower_read_rejected,
                "lagging_follower_read_rejected",
            ),
            (
                self.bounded_stale_read_acceptance_observed,
                "bounded_stale_read_acceptance_observed",
            ),
            (
                self.bounded_stale_read_rejection_observed,
                "bounded_stale_read_rejection_observed",
            ),
            (
                self.minority_partition_read_rejection_observed,
                "minority_partition_read_rejection_observed",
            ),
            (
                self.healed_follower_catchup_observed,
                "healed_follower_catchup_observed",
            ),
            (
                self.stale_follower_write_rejected,
                "stale_follower_write_rejected",
            ),
            (
                self.leader_transfer_exact_once_validated,
                "leader_transfer_exact_once_validated",
            ),
            (
                self.leader_transfer_under_load_validated,
                "leader_transfer_under_load_validated",
            ),
            (
                self.snapshot_bootstrap_validated,
                "snapshot_bootstrap_validated",
            ),
            (
                self.snapshot_install_restart_validated,
                "snapshot_install_restart_validated",
            ),
            (
                self.membership_rescale_validated,
                "membership_rescale_validated",
            ),
            (
                self.membership_add_promote_remove_validated,
                "membership_add_promote_remove_validated",
            ),
            (
                self.follower_rejoin_after_compaction_validated,
                "follower_rejoin_after_compaction_validated",
            ),
            (
                self.secondary_read_eligibility_validated,
                "secondary_read_eligibility_validated",
            ),
            (self.apply_pipeline_converged, "apply_pipeline_converged"),
            (self.wal_persistence_observed, "wal_persistence_observed"),
            (
                self.fsm_apply_idempotent_replay_observed,
                "fsm_apply_idempotent_replay_observed",
            ),
            (
                self.storage_mutation_wal_fence_atomicity_observed,
                "storage_mutation_wal_fence_atomicity_observed",
            ),
            (
                self.snapshot_install_apply_fence_atomicity_observed,
                "snapshot_install_apply_fence_atomicity_observed",
            ),
            (
                self.process_restart_after_apply_crash_recovered,
                "process_restart_after_apply_crash_recovered",
            ),
        ] {
            if !present {
                missing.push(requirement.to_string());
            }
        }
        missing.extend(
            self.blockers
                .iter()
                .map(|blocker| format!("blocker:{blocker}")),
        );
        missing
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationHealth {
    pub status: HealthStatus,
    pub leader_id: Option<NodeId>,
    pub commit_index: LogIndex,
    pub replicated_peer_count: u64,
    pub lagging_peer_count: u64,
    pub max_peer_lag: LogIndex,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplyHealth {
    pub status: HealthStatus,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub apply_lag: LogIndex,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityEvidence {
    pub capability: String,
    pub present: bool,
    pub evidence: Vec<String>,
    pub source_reference: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeLocalStatusReport {
    pub node_status: StatusSnapshot,
    pub peer_pipeline: Vec<PeerProgress>,
    pub replication_health: ReplicationHealth,
    pub apply_health: ApplyHealth,
    pub readiness: ReadinessSnapshot,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterStatusReport {
    pub group_id: GroupId,
    pub leader_id: Option<NodeId>,
    pub leader_transfer: Option<LeaderTransferState>,
    pub nodes: Vec<StatusSnapshot>,
    pub replication_health: ReplicationHealth,
    pub apply_health: ApplyHealth,
    pub ready: bool,
    pub health: HealthStatus,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeAdminReport {
    pub cluster_status: ClusterStatusReport,
    pub readiness: ReadinessSnapshot,
    pub parity: ParityReport,
    pub public_api: PublicApiContract,
    pub capability_evidence: Vec<CapabilityEvidence>,
    pub ready: bool,
    pub health: HealthStatus,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaselineRaftRuntimeCapabilityReport {
    pub ready: bool,
    pub capability_evidence: Vec<CapabilityEvidence>,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BlockerSeverity {
    Blocker,
    Fatal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Blocker {
    pub id: String,
    pub source: String,
    pub severity: BlockerSeverity,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessReadinessBlocker {
    pub blocker: String,
    pub evidence_field: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FatalBlockerReport {
    pub ready: bool,
    pub fatal: bool,
    pub source: String,
    pub blockers: Vec<Blocker>,
    pub blocker_count: u64,
    pub fatal_count: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiagnosticLogEntry {
    pub target: String,
    pub severity: DiagnosticSeverity,
    pub message: String,
    pub fields: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminStatusSurfaceInput {
    pub commit_index: LogIndex,
    pub max_observed_node_commit_index: LogIndex,
    pub quorum_size: u64,
    pub quorum_peer_ids: Vec<NodeId>,
    pub peer_pipeline: Vec<PeerProgress>,
    pub wal_last_log_index: LogIndex,
    pub wal_segment_lifecycle_present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminStatusSurfaceEvidence {
    pub complete: bool,
    pub peer_rows: u64,
    pub quorum_size: u64,
    pub quorum_peer_progress_observed: bool,
    pub peer_pipeline_runtime_activity_observed: bool,
    pub peer_pipeline_limits_observed: bool,
    pub wal_segment_lifecycle_present: bool,
    pub wal_log_range_covers_commit: bool,
    pub peer_next_index_present: bool,
    pub majority_configured: bool,
    pub cluster_commit_index_consistent: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum OptimizationHintSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OptimizationHint {
    pub id: String,
    pub severity: OptimizationHintSeverity,
    pub component: String,
    pub recommendation: String,
    pub observed_value: u64,
    pub threshold: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OptimizationReport {
    pub ready: bool,
    pub hint_count: u64,
    pub critical_count: u64,
    pub warning_count: u64,
    pub hints: Vec<OptimizationHint>,
}

pub fn matrixraft_fatal_blocker_report(
    source: impl Into<String>,
    blockers: Vec<String>,
    fatal_blockers: Vec<String>,
) -> FatalBlockerReport {
    let source = source.into();
    let blockers = blockers
        .into_iter()
        .map(|id| {
            let severity = if fatal_blockers.iter().any(|fatal| fatal == &id) {
                BlockerSeverity::Fatal
            } else {
                BlockerSeverity::Blocker
            };
            Blocker {
                detail: format!("{source}:{id}"),
                id,
                source: source.clone(),
                severity,
            }
        })
        .collect::<Vec<_>>();
    let fatal_count = blockers
        .iter()
        .filter(|blocker| blocker.severity == BlockerSeverity::Fatal)
        .count() as u64;
    FatalBlockerReport {
        ready: blockers.is_empty(),
        fatal: fatal_count > 0,
        source,
        blocker_count: blockers.len() as u64,
        fatal_count,
        blockers,
    }
}

pub fn matrixraft_admin_fatal_blocker_report(
    report: &RuntimeAdminReport,
    fatal_blockers: Vec<String>,
) -> FatalBlockerReport {
    matrixraft_fatal_blocker_report(
        "rustraft_admin_report",
        report.blockers.clone(),
        fatal_blockers,
    )
}

pub fn matrixraft_admin_diagnostic_log_entries(
    report: &RuntimeAdminReport,
) -> Vec<DiagnosticLogEntry> {
    let cluster = &report.cluster_status;
    let mut entries = Vec::new();
    entries.push(DiagnosticLogEntry {
        target: "rustraft.admin".to_string(),
        severity: if report.ready {
            DiagnosticSeverity::Info
        } else {
            DiagnosticSeverity::Warn
        },
        message: if report.ready {
            "rustraft admin report ready".to_string()
        } else {
            "rustraft admin report blocked".to_string()
        },
        fields: vec![
            ("group_id".to_string(), cluster.group_id.to_string()),
            ("health".to_string(), format!("{:?}", report.health)),
            ("ready".to_string(), report.ready.to_string()),
            (
                "production_status".to_string(),
                format!("{:?}", report.parity.production_status),
            ),
            (
                "blocker_count".to_string(),
                report.blockers.len().to_string(),
            ),
        ],
    });
    entries.push(DiagnosticLogEntry {
        target: "rustraft.replication".to_string(),
        severity: health_severity(cluster.replication_health.status),
        message: cluster.replication_health.reason.clone(),
        fields: vec![
            ("group_id".to_string(), cluster.group_id.to_string()),
            (
                "leader_id".to_string(),
                cluster
                    .replication_health
                    .leader_id
                    .map(|leader_id| leader_id.to_string())
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "commit_index".to_string(),
                cluster.replication_health.commit_index.to_string(),
            ),
            (
                "replicated_peer_count".to_string(),
                cluster.replication_health.replicated_peer_count.to_string(),
            ),
            (
                "lagging_peer_count".to_string(),
                cluster.replication_health.lagging_peer_count.to_string(),
            ),
            (
                "max_peer_lag".to_string(),
                cluster.replication_health.max_peer_lag.to_string(),
            ),
        ],
    });
    entries.push(DiagnosticLogEntry {
        target: "rustraft.apply".to_string(),
        severity: health_severity(cluster.apply_health.status),
        message: cluster.apply_health.reason.clone(),
        fields: vec![
            ("group_id".to_string(), cluster.group_id.to_string()),
            (
                "commit_index".to_string(),
                cluster.apply_health.commit_index.to_string(),
            ),
            (
                "applied_index".to_string(),
                cluster.apply_health.applied_index.to_string(),
            ),
            (
                "apply_lag".to_string(),
                cluster.apply_health.apply_lag.to_string(),
            ),
        ],
    });
    entries.extend(report.blockers.iter().map(|blocker| DiagnosticLogEntry {
        target: "rustraft.blocker".to_string(),
        severity: DiagnosticSeverity::Error,
        message: blocker.clone(),
        fields: vec![("group_id".to_string(), cluster.group_id.to_string())],
    }));
    entries
}

pub fn matrixraft_admin_diagnostic_json_lines(report: &RuntimeAdminReport) -> String {
    matrixraft_admin_diagnostic_log_entries(report)
        .into_iter()
        .map(|entry| {
            serde_json::to_string(&entry).expect("RustRaft diagnostic entry must serialize")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn health_severity(status: HealthStatus) -> DiagnosticSeverity {
    match status {
        HealthStatus::Healthy => DiagnosticSeverity::Info,
        HealthStatus::Degraded => DiagnosticSeverity::Warn,
        HealthStatus::Unavailable => DiagnosticSeverity::Error,
    }
}

pub fn matrixraft_replication_health(
    status: &StatusSnapshot,
    peer_pipeline: &[PeerProgress],
) -> ReplicationHealth {
    let max_status_lag = status
        .peers
        .iter()
        .map(|peer| peer.lag)
        .max()
        .unwrap_or_default();
    let max_pipeline_lag = peer_pipeline
        .iter()
        .map(|peer| status.commit_index.saturating_sub(peer.match_index))
        .max()
        .unwrap_or_default();
    let max_peer_lag = max_status_lag.max(max_pipeline_lag);
    let lagging_peer_count = status.peers.iter().filter(|peer| peer.lag > 0).count() as u64
        + peer_pipeline
            .iter()
            .filter(|peer| peer.match_index < status.commit_index)
            .count() as u64;
    let replicated_peer_count = status.peers.iter().filter(|peer| peer.healthy).count() as u64;
    let status_value = if status.leader_id.is_none() {
        HealthStatus::Unavailable
    } else if lagging_peer_count > 0 {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };
    ReplicationHealth {
        status: status_value,
        leader_id: status.leader_id,
        commit_index: status.commit_index,
        replicated_peer_count,
        lagging_peer_count,
        max_peer_lag,
        reason: match status_value {
            HealthStatus::Healthy => "replication_healthy".to_string(),
            HealthStatus::Degraded => "replication_lagging".to_string(),
            HealthStatus::Unavailable => "leader_unavailable".to_string(),
        },
    }
}

pub fn matrixraft_apply_health(status: &StatusSnapshot) -> ApplyHealth {
    let apply_lag = status.commit_index.saturating_sub(status.applied_index);
    let status_value = if status.leader_id.is_none() {
        HealthStatus::Unavailable
    } else if apply_lag > 0 {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };
    ApplyHealth {
        status: status_value,
        commit_index: status.commit_index,
        applied_index: status.applied_index,
        apply_lag,
        reason: match status_value {
            HealthStatus::Healthy => "apply_healthy".to_string(),
            HealthStatus::Degraded => "apply_lagging".to_string(),
            HealthStatus::Unavailable => "leader_unavailable".to_string(),
        },
    }
}

pub fn matrixraft_runtime_local_status_report(
    node_status: StatusSnapshot,
    peer_pipeline: Vec<PeerProgress>,
    readiness: ReadinessSnapshot,
) -> RuntimeLocalStatusReport {
    let replication_health = matrixraft_replication_health(&node_status, &peer_pipeline);
    let apply_health = matrixraft_apply_health(&node_status);
    let mut blockers = Vec::new();
    if replication_health.status != HealthStatus::Healthy {
        blockers.push(replication_health.reason.clone());
    }
    if apply_health.status != HealthStatus::Healthy {
        blockers.push(apply_health.reason.clone());
    }
    if !readiness.matrixraft_operator_observability_present {
        blockers.push("operator_observability_missing".to_string());
    }
    let ready = blockers.is_empty();
    RuntimeLocalStatusReport {
        node_status,
        peer_pipeline,
        replication_health,
        apply_health,
        readiness,
        ready,
        blockers,
    }
}

pub fn matrixraft_admin_status_surface_evidence(
    input: &AdminStatusSurfaceInput,
) -> AdminStatusSurfaceEvidence {
    let quorum_peer_ids = input
        .quorum_peer_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let peer_pipeline_runtime_activity_observed = input.peer_pipeline.iter().any(|peer| {
        peer.append_requests > 0
            || peer.append_accepted > 0
            || peer.append_rejected > 0
            || peer.match_index > 0
            || peer.next_index > 1
            || peer.append_queue_max_depth > 0
            || peer.apply_queue_max_depth > 0
            || peer.inflight_entries > 0
            || peer.inflight_bytes > 0
            || peer.snapshot_installed_index > 0
            || peer.snapshot_send_attempts > 0
            || peer.snapshot_install_progress_per_mille > 0
            || peer.transfer_leader_target
            || peer.transfer_leader_timeouts > 0
            || peer.pre_vote_rejections > 0
            || peer.election_rejections > 0
    });
    let peer_pipeline_limits_observed = !input.peer_pipeline.is_empty()
        && input.peer_pipeline.iter().all(|peer| {
            peer.next_index > 0
                && peer.append_queue_limit > 0
                && peer.inflight_bytes_limit > 0
                && peer.apply_inflight_limit > 0
                && peer.apply_batch_bytes_limit > 0
                && peer.snapshot_install_progress_per_mille <= 1_000
        });
    let quorum_peer_progress_observed = input.commit_index > 0
        && input
            .peer_pipeline
            .iter()
            .filter(|peer| {
                quorum_peer_ids.contains(&peer.peer_id)
                    && peer.match_index >= input.commit_index
                    && peer.next_index >= peer.match_index.saturating_add(1)
            })
            .count() as u64
            >= input.quorum_size;
    let wal_log_range_covers_commit = input.wal_last_log_index >= input.commit_index;
    let peer_next_index_present = input.peer_pipeline.iter().all(|peer| peer.next_index > 0);
    let majority_configured = input.quorum_size > 0;
    let cluster_commit_index_consistent =
        input.commit_index >= input.max_observed_node_commit_index;
    let mut blockers = Vec::new();
    for (present, blocker) in [
        (!input.peer_pipeline.is_empty(), "peer_pipeline_missing"),
        (
            peer_pipeline_limits_observed,
            "peer_pipeline_limits_missing",
        ),
        (
            quorum_peer_progress_observed,
            "quorum_peer_progress_missing",
        ),
        (
            peer_pipeline_runtime_activity_observed,
            "peer_pipeline_runtime_activity_missing",
        ),
        (
            input.wal_segment_lifecycle_present,
            "wal_segment_lifecycle_missing",
        ),
        (wal_log_range_covers_commit, "wal_commit_range_missing"),
        (peer_next_index_present, "peer_next_index_missing"),
        (majority_configured, "quorum_size_missing"),
        (
            cluster_commit_index_consistent,
            "cluster_commit_index_inconsistent",
        ),
    ] {
        if !present {
            blockers.push(blocker.to_string());
        }
    }
    AdminStatusSurfaceEvidence {
        complete: blockers.is_empty(),
        peer_rows: input.peer_pipeline.len() as u64,
        quorum_size: input.quorum_size,
        quorum_peer_progress_observed,
        peer_pipeline_runtime_activity_observed,
        peer_pipeline_limits_observed,
        wal_segment_lifecycle_present: input.wal_segment_lifecycle_present,
        wal_log_range_covers_commit,
        peer_next_index_present,
        majority_configured,
        cluster_commit_index_consistent,
        blockers,
    }
}

pub fn matrixraft_optimization_report(input: &AdminStatusSurfaceInput) -> OptimizationReport {
    let mut hints = Vec::new();
    if input.peer_pipeline.is_empty() {
        hints.push(optimization_hint(
            "peer_pipeline_missing",
            OptimizationHintSeverity::Critical,
            "replication_pipeline",
            "export peer pipeline rows before tuning queue or inflight limits",
            0,
            1,
        ));
    }
    if input.quorum_size == 0 {
        hints.push(optimization_hint(
            "quorum_size_missing",
            OptimizationHintSeverity::Critical,
            "membership",
            "configure a nonzero quorum size before evaluating replication health",
            0,
            1,
        ));
    }
    if input.max_observed_node_commit_index > input.commit_index {
        hints.push(optimization_hint(
            "cluster_commit_index_inconsistent",
            OptimizationHintSeverity::Critical,
            "replication",
            "investigate nodes reporting commit indexes beyond the cluster commit index",
            input.max_observed_node_commit_index,
            input.commit_index,
        ));
    }
    if input.wal_last_log_index < input.commit_index {
        hints.push(optimization_hint(
            "wal_commit_range_missing",
            OptimizationHintSeverity::Critical,
            "wal",
            "hold or recover WAL segments until the WAL range covers the committed index",
            input.wal_last_log_index,
            input.commit_index,
        ));
    }
    if !input.wal_segment_lifecycle_present {
        hints.push(optimization_hint(
            "wal_segment_lifecycle_missing",
            OptimizationHintSeverity::Warning,
            "wal",
            "enable WAL segment lifecycle evidence before tuning compaction thresholds",
            0,
            1,
        ));
    }

    let saturated_append_peers = input
        .peer_pipeline
        .iter()
        .filter(|peer| {
            peer.append_queue_limit > 0 && peer.append_queue_depth >= peer.append_queue_limit
        })
        .count() as u64;
    if saturated_append_peers > 0 {
        hints.push(optimization_hint(
            "append_queue_saturated",
            OptimizationHintSeverity::Warning,
            "replication_pipeline",
            "increase append queue capacity or reduce per-peer append burst size",
            saturated_append_peers,
            1,
        ));
    }

    let saturated_apply_peers = input
        .peer_pipeline
        .iter()
        .filter(|peer| {
            peer.apply_inflight_limit > 0 && peer.apply_inflight_tasks >= peer.apply_inflight_limit
        })
        .count() as u64;
    if saturated_apply_peers > 0 {
        hints.push(optimization_hint(
            "apply_inflight_saturated",
            OptimizationHintSeverity::Warning,
            "apply_pipeline",
            "raise apply inflight limits or lower apply batch cost",
            saturated_apply_peers,
            1,
        ));
    }

    let memory_pressure_peers = input
        .peer_pipeline
        .iter()
        .filter(|peer| {
            peer.inflight_bytes_limit > 0 && peer.inflight_bytes >= peer.inflight_bytes_limit
        })
        .count() as u64;
    if memory_pressure_peers > 0 {
        hints.push(optimization_hint(
            "inflight_bytes_saturated",
            OptimizationHintSeverity::Warning,
            "replication_pipeline",
            "raise inflight byte limits or reduce append batch bytes",
            memory_pressure_peers,
            1,
        ));
    }

    let reorder_pressure_peers = input
        .peer_pipeline
        .iter()
        .filter(|peer| peer.reorder_queue_depth > 0 || peer.reorder_dropped_packages > 0)
        .count() as u64;
    if reorder_pressure_peers > 0 {
        hints.push(optimization_hint(
            "reorder_queue_pressure",
            OptimizationHintSeverity::Info,
            "transport",
            "inspect transport ordering and reorder queue timeout settings",
            reorder_pressure_peers,
            1,
        ));
    }

    hints.sort_by(|left, right| {
        right
            .severity
            .cmp(&left.severity)
            .then_with(|| left.id.cmp(&right.id))
    });
    let critical_count = hints
        .iter()
        .filter(|hint| hint.severity == OptimizationHintSeverity::Critical)
        .count() as u64;
    let warning_count = hints
        .iter()
        .filter(|hint| hint.severity == OptimizationHintSeverity::Warning)
        .count() as u64;
    OptimizationReport {
        ready: critical_count == 0,
        hint_count: hints.len() as u64,
        critical_count,
        warning_count,
        hints,
    }
}

fn optimization_hint(
    id: &str,
    severity: OptimizationHintSeverity,
    component: &str,
    recommendation: &str,
    observed_value: u64,
    threshold: u64,
) -> OptimizationHint {
    OptimizationHint {
        id: id.to_string(),
        severity,
        component: component.to_string(),
        recommendation: recommendation.to_string(),
        observed_value,
        threshold,
    }
}

pub fn matrixraft_cluster_status_report(
    group_id: GroupId,
    leader_id: Option<NodeId>,
    leader_transfer: Option<LeaderTransferState>,
    nodes: Vec<StatusSnapshot>,
) -> ClusterStatusReport {
    let representative = nodes
        .iter()
        .find(|node| Some(node.node_id) == leader_id)
        .or_else(|| nodes.first());
    let (replication_health, apply_health) = if let Some(status) = representative {
        (
            matrixraft_replication_health(status, &[]),
            matrixraft_apply_health(status),
        )
    } else {
        (
            ReplicationHealth {
                status: HealthStatus::Unavailable,
                leader_id,
                commit_index: 0,
                replicated_peer_count: 0,
                lagging_peer_count: 0,
                max_peer_lag: 0,
                reason: "cluster_has_no_nodes".to_string(),
            },
            ApplyHealth {
                status: HealthStatus::Unavailable,
                commit_index: 0,
                applied_index: 0,
                apply_lag: 0,
                reason: "cluster_has_no_nodes".to_string(),
            },
        )
    };
    let mut blockers = Vec::new();
    if leader_id.is_none() {
        blockers.push("leader_unavailable".to_string());
    }
    if replication_health.status != HealthStatus::Healthy {
        blockers.push(replication_health.reason.clone());
    }
    if apply_health.status != HealthStatus::Healthy {
        blockers.push(apply_health.reason.clone());
    }
    let health = if blockers.is_empty() {
        HealthStatus::Healthy
    } else if leader_id.is_some() {
        HealthStatus::Degraded
    } else {
        HealthStatus::Unavailable
    };
    ClusterStatusReport {
        group_id,
        leader_id,
        leader_transfer,
        nodes,
        replication_health,
        apply_health,
        ready: blockers.is_empty(),
        health,
        blockers,
    }
}

pub fn matrixraft_capability_evidence(readiness: &ReadinessSnapshot) -> Vec<CapabilityEvidence> {
    matrixraft_readiness_evidence(readiness)
        .into_iter()
        .map(|evidence| CapabilityEvidence {
            capability: evidence.requirement_id,
            present: evidence.present,
            evidence: vec![evidence.readiness_field],
            source_reference: "rustraft_readiness_snapshot".to_string(),
        })
        .collect()
}

pub fn matrixraft_capability_evidence_from_fields<C, R, I, S>(
    capability: C,
    source_reference: R,
    fields: I,
) -> CapabilityEvidence
where
    C: Into<String>,
    R: Into<String>,
    I: IntoIterator<Item = (bool, S)>,
    S: AsRef<str>,
{
    let evidence = fields
        .into_iter()
        .map(|(present, field)| {
            if present {
                format!("present:{}", field.as_ref())
            } else {
                format!("missing:{}", field.as_ref())
            }
        })
        .collect::<Vec<_>>();
    CapabilityEvidence {
        capability: capability.into(),
        present: evidence.iter().all(|field| field.starts_with("present:")),
        evidence,
        source_reference: source_reference.into(),
    }
}

pub fn matrixraft_runtime_capability_report_from_evidence<I, S>(
    capability_evidence: Vec<CapabilityEvidence>,
    product_blockers: I,
) -> BaselineRaftRuntimeCapabilityReport
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let satisfied = capability_evidence
        .iter()
        .filter(|capability| capability.present)
        .map(|capability| capability.capability.clone())
        .collect::<Vec<_>>();
    let missing = capability_evidence
        .iter()
        .filter(|capability| !capability.present)
        .map(|capability| capability.capability.clone())
        .collect::<Vec<_>>();
    let mut blockers = capability_evidence
        .iter()
        .filter(|capability| !capability.present)
        .flat_map(|capability| {
            capability.evidence.iter().map(move |field| {
                let field = field.trim();
                if field.starts_with("missing:") {
                    format!("{}:{field}", capability.capability)
                } else {
                    format!("{}:missing:{field}", capability.capability)
                }
            })
        })
        .chain(product_blockers.into_iter().map(Into::into))
        .collect::<Vec<_>>();
    blockers.sort();
    blockers.dedup();
    BaselineRaftRuntimeCapabilityReport {
        ready: missing.is_empty() && blockers.is_empty(),
        capability_evidence,
        satisfied,
        missing,
        blockers,
    }
}

pub fn matrixraft_runtime_admin_report(
    cluster_status: ClusterStatusReport,
    readiness: ReadinessSnapshot,
    capability_evidence: Vec<CapabilityEvidence>,
) -> RuntimeAdminReport {
    let parity = matrixraft_parity_report(&readiness);
    let public_api = matrixraft_public_api_contract();
    let mut blockers = cluster_status.blockers.clone();
    blockers.extend(
        capability_evidence
            .iter()
            .filter(|evidence| !evidence.present)
            .map(|evidence| format!("capability_missing:{}", evidence.capability)),
    );
    blockers.extend(parity.production_blockers.iter().cloned());
    blockers.sort();
    blockers.dedup();
    let ready = cluster_status.ready
        && parity.ready
        && capability_evidence.iter().all(|evidence| evidence.present)
        && blockers.is_empty();
    let health = if ready {
        HealthStatus::Healthy
    } else if cluster_status.leader_id.is_some() {
        HealthStatus::Degraded
    } else {
        HealthStatus::Unavailable
    };
    RuntimeAdminReport {
        cluster_status,
        readiness,
        parity,
        public_api,
        capability_evidence,
        ready,
        health,
        blockers,
    }
}
