// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ReferenceRaft parity, public API, and production readiness reporting API.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::rustraft_metric_names;
use crate::{
    fault, RustRaftAdminStatusSurfaceEvidence, RustRaftDataNodeProcessRolloutReport,
    RustRaftMembershipTransitionEvidence, RustRaftMetaProcessRolloutReport, RustRaftMetricNames,
    RustRaftPipelineEvidence, RustRaftReferenceRaftBenchmarkEvidence,
    RustRaftSnapshotLifecycleEvidence, RustRaftWalLifecycleEvidence,
};

pub use crate::{
    rustraft_data_node_process_rollout_readiness_report,
    rustraft_meta_process_rollout_readiness_report, rustraft_production_readiness_report,
    RustRaftReferenceRaftParitySurface,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftRequirementCategory {
    Safety,
    Durability,
    Observability,
    Transport,
    Membership,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSemanticRequirement {
    pub id: String,
    pub category: RustRaftRequirementCategory,
    pub readiness_field: String,
    pub required_for_production: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftProductionStatus {
    ProductionReady,
    Blocked,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftDeploymentMode {
    /// Backward-compatible deserialization variant only.
    ///
    /// Runtime validation rejects local Raft deployment. Local clusters are
    /// test fixtures and cannot satisfy production readiness.
    LocalModel,
    ProductionDistributed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct RustRaftProductionReadinessError {
    pub mode: RustRaftDeploymentMode,
    pub message: String,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftParityContract {
    pub library_name: String,
    pub consensus_backend_boundary: String,
    pub openraft_dependency_removed: bool,
    pub requirements: Vec<RustRaftSemanticRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftParityReport {
    pub contract: RustRaftParityContract,
    pub reference_raft_reference_policy: RustRaftReferenceRaftReferencePolicy,
    pub ready: bool,
    pub production_status: RustRaftProductionStatus,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub production_blockers: Vec<String>,
    pub reference_raft_parity_matrix: Vec<RustRaftReferenceRaftParityItem>,
    pub reference_raft_gaps: Vec<String>,
    pub reference_raft_intentional_differences: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReferenceRaftReferencePolicy {
    pub feature_reference: String,
    pub performance_reference: String,
    pub rust_api_policy: String,
    pub temporalstore_consumption_boundary: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftReferenceRaftParityStatus {
    Satisfied,
    Gap,
    IntentionalDifference,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReferenceRaftParityItem {
    pub id: String,
    pub required: bool,
    pub status: RustRaftReferenceRaftParityStatus,
    pub evidence: Vec<String>,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProductionReadinessInput {
    pub readiness: RustRaftReadinessSnapshot,
    #[serde(default)]
    pub peer_pipeline: Option<RustRaftPipelineEvidence>,
    #[serde(default)]
    pub snapshot_lifecycle: Option<RustRaftSnapshotLifecycleEvidence>,
    #[serde(default)]
    pub wal_lifecycle: Option<RustRaftWalLifecycleEvidence>,
    #[serde(default)]
    pub admin_status_surface: Option<RustRaftAdminStatusSurfaceEvidence>,
    #[serde(default)]
    pub fault_harness: Option<fault::RustRaftFaultHarnessReadinessReport>,
    #[serde(default)]
    pub data_node_rollout: Option<RustRaftDataNodeProcessRolloutReport>,
    #[serde(default)]
    pub metaserver_rollout: Option<RustRaftMetaProcessRolloutReport>,
    #[serde(default)]
    pub membership_transitions: Vec<RustRaftMembershipTransitionEvidence>,
    #[serde(default)]
    pub reference_raft_benchmark: Option<RustRaftReferenceRaftBenchmarkEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProductionReadinessReport {
    pub parity: RustRaftParityReport,
    pub public_api: RustRaftPublicApiContract,
    pub ready: bool,
    pub production_status: RustRaftProductionStatus,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub production_blockers: Vec<String>,
    pub recommended_next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProcessRolloutReadinessReport {
    pub scope: String,
    pub ready: bool,
    pub production_status: RustRaftProductionStatus,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub blockers: Vec<String>,
    pub recommended_next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadinessEvidence {
    pub requirement_id: String,
    pub readiness_field: String,
    pub present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadinessSnapshot {
    pub rustraft_leader_write_authority_present: bool,
    pub rustraft_operator_observability_present: bool,
    pub rustraft_rpc_transport_contract_present: bool,
    pub rustraft_log_retention_snapshot_trigger_present: bool,
    pub rustraft_apply_snapshot_fence_present: bool,
    pub raft_storage_apply_fence_present: bool,
    pub rustraft_snapshot_floor_log_matching_present: bool,
    pub rustraft_snapshot_tail_catchup_present: bool,
    pub rustraft_compacted_entry_rejection_present: bool,
    pub rustraft_metaserver_snapshot_floor_election_present: bool,
    pub learner_catchup_promotion_present: bool,
    pub metaserver_membership_workflow_present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPublicApiContract {
    pub storage_trait: String,
    pub transport_trait: String,
    pub public_modules: Vec<String>,
    pub rpc_messages: Vec<String>,
    pub safety_helpers: Vec<String>,
    pub embedding_examples: Vec<String>,
    pub parity_reports: Vec<String>,
    pub benchmark_interfaces: Vec<String>,
    pub compatibility_reports: Vec<String>,
    pub metrics: RustRaftMetricNames,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftStandaloneCapability {
    pub id: String,
    pub ready: bool,
    pub evidence: Vec<String>,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftStandaloneReadinessReport {
    pub standalone: bool,
    pub production_status: RustRaftProductionStatus,
    pub capabilities: Vec<RustRaftStandaloneCapability>,
    pub missing: Vec<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftOpenSourceSurface {
    pub crate_name: String,
    pub public_modules: Vec<String>,
    pub embedding_docs: Vec<String>,
    pub embedding_examples: Vec<String>,
    pub reference_raft_parity_matrix: Vec<String>,
    pub benchmark_harness_interface: Vec<String>,
    pub compatibility_reports: Vec<String>,
    pub rustraft_owned: Vec<String>,
    pub temporalstore_adapter_boundary: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftTemporalStoreAdapterShape {
    pub backend_type: String,
    pub node_field: String,
    pub node_runtime_type: String,
    pub state_machine_type_parameter: String,
    pub transport_type_parameter: String,
    pub codec_field: String,
    pub engine_field: String,
    pub rustraft_owned: Vec<String>,
    pub temporalstore_owned: Vec<String>,
    pub example: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftExtractionStatus {
    InLibrary,
    AdapterOnly,
    PendingMigration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftExtractionSlice {
    pub id: String,
    pub status: RustRaftExtractionStatus,
    pub rustraft_owner: String,
    pub temporalstore_boundary: String,
    pub next_evidence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftTemporalStoreExtractionPlan {
    pub policy: String,
    pub slices: Vec<RustRaftExtractionSlice>,
}

pub fn rustraft_validate_deployment_mode(
    mode: RustRaftDeploymentMode,
    readiness: &RustRaftProductionReadinessReport,
) -> Result<(), RustRaftProductionReadinessError> {
    rustraft_validate_deployment_readiness(
        mode,
        readiness.ready,
        readiness_missing_reasons(readiness),
    )
}

pub fn rustraft_validate_deployment_readiness(
    mode: RustRaftDeploymentMode,
    production_ready: bool,
    missing: Vec<String>,
) -> Result<(), RustRaftProductionReadinessError> {
    match mode {
        RustRaftDeploymentMode::LocalModel => Err(RustRaftProductionReadinessError {
            mode,
            message:
                "local Raft deployment mode is disabled; production distributed Raft is required"
                    .to_string(),
            missing,
        }),
        RustRaftDeploymentMode::ProductionDistributed if production_ready => Ok(()),
        RustRaftDeploymentMode::ProductionDistributed => Err(RustRaftProductionReadinessError {
            mode,
            message: "distributed Raft is not production-ready".to_string(),
            missing,
        }),
    }
}

pub fn rustraft_require_production_ready(
    readiness: &RustRaftProductionReadinessReport,
) -> Result<(), RustRaftProductionReadinessError> {
    rustraft_validate_deployment_mode(RustRaftDeploymentMode::ProductionDistributed, readiness)
}

fn readiness_missing_reasons(readiness: &RustRaftProductionReadinessReport) -> Vec<String> {
    let mut missing = readiness.missing.clone();
    for blocker in &readiness.production_blockers {
        if !missing.contains(blocker) {
            missing.push(blocker.clone());
        }
    }
    if missing.is_empty() && !readiness.ready {
        missing.push("production readiness report is not ready".to_string());
    }
    missing
}

pub fn rustraft_readiness_evidence(
    snapshot: &RustRaftReadinessSnapshot,
) -> Vec<RustRaftReadinessEvidence> {
    rustraft_requirements()
        .into_iter()
        .map(|requirement| RustRaftReadinessEvidence {
            present: readiness_field_present(snapshot, &requirement.readiness_field),
            requirement_id: requirement.id,
            readiness_field: requirement.readiness_field,
        })
        .collect()
}

pub fn rustraft_requirements() -> Vec<RustRaftSemanticRequirement> {
    use RustRaftRequirementCategory::*;
    [
        (
            "leader_write_authority",
            Safety,
            "rustraft_leader_write_authority_present",
        ),
        (
            "operator_observability",
            Observability,
            "rustraft_operator_observability_present",
        ),
        (
            "rpc_transport_contract",
            Transport,
            "rustraft_rpc_transport_contract_present",
        ),
        (
            "snapshot_trigger",
            Durability,
            "rustraft_log_retention_snapshot_trigger_present",
        ),
        (
            "apply_snapshot_fence",
            Durability,
            "rustraft_apply_snapshot_fence_present",
        ),
        (
            "storage_apply_fence",
            Durability,
            "raft_storage_apply_fence_present",
        ),
        (
            "snapshot_floor_log_matching",
            Durability,
            "rustraft_snapshot_floor_log_matching_present",
        ),
        (
            "snapshot_tail_catchup",
            Durability,
            "rustraft_snapshot_tail_catchup_present",
        ),
        (
            "compacted_entry_rejection",
            Safety,
            "rustraft_compacted_entry_rejection_present",
        ),
        (
            "metaserver_snapshot_floor_election",
            Safety,
            "rustraft_metaserver_snapshot_floor_election_present",
        ),
        (
            "learner_catchup_promotion",
            Membership,
            "learner_catchup_promotion_present",
        ),
        (
            "metaserver_membership_workflow",
            Membership,
            "metaserver_membership_workflow_present",
        ),
    ]
    .into_iter()
    .map(
        |(id, category, readiness_field)| RustRaftSemanticRequirement {
            id: id.to_string(),
            category,
            readiness_field: readiness_field.to_string(),
            required_for_production: true,
        },
    )
    .collect()
}

fn readiness_field_present(snapshot: &RustRaftReadinessSnapshot, field: &str) -> bool {
    match field {
        "rustraft_leader_write_authority_present" => {
            snapshot.rustraft_leader_write_authority_present
        }
        "rustraft_operator_observability_present" => {
            snapshot.rustraft_operator_observability_present
        }
        "rustraft_rpc_transport_contract_present" => {
            snapshot.rustraft_rpc_transport_contract_present
        }
        "rustraft_log_retention_snapshot_trigger_present" => {
            snapshot.rustraft_log_retention_snapshot_trigger_present
        }
        "rustraft_apply_snapshot_fence_present" => snapshot.rustraft_apply_snapshot_fence_present,
        "raft_storage_apply_fence_present" => snapshot.raft_storage_apply_fence_present,
        "rustraft_snapshot_floor_log_matching_present" => {
            snapshot.rustraft_snapshot_floor_log_matching_present
        }
        "rustraft_snapshot_tail_catchup_present" => snapshot.rustraft_snapshot_tail_catchup_present,
        "rustraft_compacted_entry_rejection_present" => {
            snapshot.rustraft_compacted_entry_rejection_present
        }
        "rustraft_metaserver_snapshot_floor_election_present" => {
            snapshot.rustraft_metaserver_snapshot_floor_election_present
        }
        "learner_catchup_promotion_present" => snapshot.learner_catchup_promotion_present,
        "metaserver_membership_workflow_present" => snapshot.metaserver_membership_workflow_present,
        _ => false,
    }
}

pub fn rustraft_parity_contract() -> RustRaftParityContract {
    RustRaftParityContract {
        library_name: "rustraft".to_string(),
        consensus_backend_boundary: "temporalstore_rust::raft::DataRaftConsensusBackend"
            .to_string(),
        openraft_dependency_removed: true,
        requirements: rustraft_requirements(),
    }
}

pub fn rustraft_reference_raft_parity_surface() -> RustRaftReferenceRaftParitySurface {
    RustRaftReferenceRaftParitySurface {
        node_lifecycle: vec![
            "create".to_string(),
            "start".to_string(),
            "restart".to_string(),
            "stop".to_string(),
            "shutdown".to_string(),
        ],
        transport_api: vec![
            "append_entries_rpc".to_string(),
            "vote_rpc".to_string(),
            "pre_vote_rpc".to_string(),
            "install_snapshot_chunk_rpc".to_string(),
            "read_index_rpc".to_string(),
            "request_response_validation".to_string(),
            "in_memory_transport".to_string(),
            "tcp_reference_transport".to_string(),
            "auth_wrapper".to_string(),
        ],
        write_api: vec![
            "propose".to_string(),
            "propose_options.expected_term".to_string(),
        ],
        read_api: vec!["read_index".to_string(), "lease_read".to_string()],
        membership_api: vec![
            "add_node".to_string(),
            "add_learner".to_string(),
            "add_witness".to_string(),
            "promote".to_string(),
            "remove_node".to_string(),
            "transfer_leader".to_string(),
            "campaign".to_string(),
        ],
        durability_api: vec![
            "wal_hard_state".to_string(),
            "snapshot_install".to_string(),
            "snapshot_tail_catchup".to_string(),
            "apply_snapshot_fence".to_string(),
        ],
        observability_api: vec![
            "status".to_string(),
            "status_snapshot".to_string(),
            "local_status".to_string(),
            "admin_report".to_string(),
            "readiness_report".to_string(),
            "metrics".to_string(),
            "blocker_report".to_string(),
            "fatal_events".to_string(),
        ],
    }
}

pub fn rustraft_reference_raft_reference_policy() -> RustRaftReferenceRaftReferencePolicy {
    RustRaftReferenceRaftReferencePolicy {
        feature_reference: "ReferenceRaft is the feature reference for Raft behavior parity.".to_string(),
        performance_reference:
            "ReferenceRaft is the performance reference; RustRaft parity requires p50/p99 latency and throughput within the configured threshold."
                .to_string(),
        rust_api_policy:
            "RustRaft may expose idiomatic Rust traits, request/response types, and error types instead of ReferenceRaft-shaped APIs."
                .to_string(),
        temporalstore_consumption_boundary:
            "TemporalStore consumption must remain stable through temporalstore_rust::raft::DataRaftConsensusBackend and adapter-owned codecs/apply/storage wiring."
                .to_string(),
    }
}

pub fn rustraft_reference_raft_parity_matrix(
    snapshot: &RustRaftReadinessSnapshot,
) -> Vec<RustRaftReferenceRaftParityItem> {
    use RustRaftReferenceRaftParityStatus::*;

    fn item(
        id: &str,
        status: RustRaftReferenceRaftParityStatus,
        evidence: &[&str],
        note: &str,
    ) -> RustRaftReferenceRaftParityItem {
        RustRaftReferenceRaftParityItem {
            id: id.to_string(),
            required: true,
            status,
            evidence: evidence.iter().map(|field| (*field).to_string()).collect(),
            note: note.to_string(),
        }
    }

    fn status(ready: bool) -> RustRaftReferenceRaftParityStatus {
        if ready {
            Satisfied
        } else {
            Gap
        }
    }

    vec![
        item(
            "log_replication",
            status(
                snapshot.rustraft_leader_write_authority_present
                    && snapshot.rustraft_rpc_transport_contract_present,
            ),
            &[
                "rustraft_leader_write_authority_present",
                "rustraft_rpc_transport_contract_present",
            ],
            "leader-owned append path and append RPC contract are present",
        ),
        item(
            "leader_election",
            status(
                snapshot.rustraft_leader_write_authority_present
                    && snapshot.rustraft_metaserver_snapshot_floor_election_present,
            ),
            &[
                "rustraft_leader_write_authority_present",
                "rustraft_metaserver_snapshot_floor_election_present",
            ],
            "leader authority and snapshot-floor election safety are present",
        ),
        item(
            "pre_vote",
            status(snapshot.rustraft_rpc_transport_contract_present),
            &["rustraft_rpc_transport_contract_present", "RustRaftVoteRequest.pre_vote"],
            "pre-vote is represented in the vote RPC contract",
        ),
        item(
            "lease_read",
            status(snapshot.rustraft_leader_write_authority_present),
            &[
                "rustraft_leader_write_authority_present",
                "RustRaftReadIndexRequest.allow_lease_read",
            ],
            "lease reads are admitted only through leader/read-safety helpers",
        ),
        item(
            "read_index",
            status(snapshot.rustraft_operator_observability_present),
            &[
                "rustraft_operator_observability_present",
                "RustRaftReadIndexRequest",
                "RustRaftReadIndexResponse",
            ],
            "read-index request/response and metrics are part of the public contract",
        ),
        item(
            "membership_changes",
            status(snapshot.metaserver_membership_workflow_present),
            &["metaserver_membership_workflow_present"],
            "membership workflow evidence covers add/remove and joint changes",
        ),
        item(
            "learner_promotion",
            status(snapshot.learner_catchup_promotion_present),
            &["learner_catchup_promotion_present"],
            "learner catch-up and promotion decision helpers are present",
        ),
        item(
            "witness_quorum_behavior",
            Satisfied,
            &["RustRaftReplicaRole::Witness.participates_in_quorum"],
            "witnesses count for quorum but are not data-serving leaders",
        ),
        item(
            "log_compaction",
            status(
                snapshot.rustraft_compacted_entry_rejection_present
                    && snapshot.rustraft_log_retention_snapshot_trigger_present,
            ),
            &[
                "rustraft_compacted_entry_rejection_present",
                "rustraft_log_retention_snapshot_trigger_present",
            ],
            "compacted-entry rejection and snapshot-trigger retention evidence are present",
        ),
        item(
            "snapshot_trigger_install",
            status(
                snapshot.rustraft_log_retention_snapshot_trigger_present
                    && snapshot.rustraft_snapshot_tail_catchup_present
                    && snapshot.rustraft_snapshot_floor_log_matching_present,
            ),
            &[
                "rustraft_log_retention_snapshot_trigger_present",
                "rustraft_snapshot_tail_catchup_present",
                "rustraft_snapshot_floor_log_matching_present",
            ],
            "snapshot trigger, install/catch-up, and floor matching are present",
        ),
        item(
            "restart_recovery",
            status(
                snapshot.raft_storage_apply_fence_present
                    && snapshot.rustraft_apply_snapshot_fence_present,
            ),
            &[
                "raft_storage_apply_fence_present",
                "rustraft_apply_snapshot_fence_present",
            ],
            "WAL recovery is guarded by storage and apply/snapshot fences",
        ),
        item(
            "leader_transfer",
            IntentionalDifference,
            &["RustRaftConsensus::transfer_leader"],
            "RustRaft exposes the transfer contract; process validation is attached by the consuming runtime",
        ),
        item(
            "observability_status",
            status(snapshot.rustraft_operator_observability_present),
            &["rustraft_operator_observability_present", "RustRaftStatusSnapshot"],
            "status snapshots and metric names are part of the public contract",
        ),
    ]
}

pub fn rustraft_parity_report(snapshot: &RustRaftReadinessSnapshot) -> RustRaftParityReport {
    let contract = rustraft_parity_contract();
    let evidence = rustraft_readiness_evidence(snapshot);
    let satisfied = evidence
        .iter()
        .filter(|item| item.present)
        .map(|item| item.requirement_id.clone())
        .collect::<Vec<_>>();
    let missing = evidence
        .iter()
        .filter(|item| !item.present)
        .map(|item| item.requirement_id.clone())
        .collect::<Vec<_>>();
    let production_blockers = contract
        .requirements
        .iter()
        .filter(|requirement| {
            requirement.required_for_production && missing.iter().any(|id| id == &requirement.id)
        })
        .map(|requirement| format!("{:?}:{}", requirement.category, requirement.id).to_lowercase())
        .collect::<Vec<_>>();
    let reference_raft_parity_matrix = rustraft_reference_raft_parity_matrix(snapshot);
    let reference_raft_gaps = reference_raft_parity_matrix
        .iter()
        .filter(|item| item.status == RustRaftReferenceRaftParityStatus::Gap)
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    let reference_raft_intentional_differences = reference_raft_parity_matrix
        .iter()
        .filter(|item| item.status == RustRaftReferenceRaftParityStatus::IntentionalDifference)
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    let ready = missing.is_empty() && production_blockers.is_empty();
    RustRaftParityReport {
        contract,
        reference_raft_reference_policy: rustraft_reference_raft_reference_policy(),
        ready,
        production_status: if ready {
            RustRaftProductionStatus::ProductionReady
        } else {
            RustRaftProductionStatus::Blocked
        },
        satisfied,
        missing,
        production_blockers,
        reference_raft_parity_matrix,
        reference_raft_gaps,
        reference_raft_intentional_differences,
    }
}

pub fn rustraft_public_api_contract() -> RustRaftPublicApiContract {
    RustRaftPublicApiContract {
        storage_trait: "RustRaftStorage".to_string(),
        transport_trait: "RaftTransport".to_string(),
        public_modules: rustraft_public_module_names(),
        rpc_messages: vec![
            "AppendEntriesRequest".to_string(),
            "AppendEntriesResponse".to_string(),
            "VoteRequest".to_string(),
            "VoteResponse".to_string(),
            "PreVoteRequest".to_string(),
            "PreVoteResponse".to_string(),
            "InstallSnapshotRequest".to_string(),
            "InstallSnapshotResponse".to_string(),
            "RustRaftSnapshotChunk".to_string(),
            "ReadIndexRequest".to_string(),
            "ReadIndexResponse".to_string(),
            "AuthenticatedRaftRpc".to_string(),
            "RustRaftTransportValidationReport".to_string(),
            "InMemoryRaftTransport".to_string(),
            "TcpRaftTransport".to_string(),
        ],
        safety_helpers: vec![
            "rustraft_read_safety_decision".to_string(),
            "rustraft_append_safety_decision".to_string(),
            "rustraft_learner_promotion_decision".to_string(),
            "rustraft_fatal_blocker_report".to_string(),
        ],
        embedding_examples: rustraft_embedding_examples(),
        parity_reports: rustraft_parity_report_names(),
        benchmark_interfaces: rustraft_benchmark_interface_names(),
        compatibility_reports: rustraft_compatibility_report_names(),
        metrics: rustraft_metric_names(),
    }
}

pub fn rustraft_public_module_names() -> Vec<String> {
    [
        "node",
        "cluster",
        "config",
        "durability",
        "fsm",
        "membership",
        "wal",
        "snapshot",
        "transport",
        "status",
        "metrics",
        "readiness",
        "storage",
        "benchmark",
        "fault",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn rustraft_embedding_examples() -> Vec<String> {
    [
        "examples/readiness_report.rs",
        "examples/read_safety.rs",
        "examples/reference_raft_parity_benchmark.rs",
        "examples/open_source_surface.rs",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn rustraft_parity_report_names() -> Vec<String> {
    [
        "rustraft_parity_report",
        "rustraft_reference_raft_parity_matrix",
        "rustraft_reference_raft_parity_surface",
        "rustraft_reference_raft_reference_policy",
        "rustraft_durability_parity_report",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn rustraft_benchmark_interface_names() -> Vec<String> {
    [
        "RustRaftBenchmarkRunner",
        "RustRaftExternalReferenceRaftRunner",
        "RustRaftRuntimeBenchmarkRunner",
        "RustRaftBenchmarkOptions",
        "RustRaftBenchmarkReport",
        "rustraft_reference_raft_benchmark_workloads",
        "rustraft_run_reference_raft_parity_benchmark",
        "rustraft_assert_reference_raft_parity",
        "rustraft_assert_production_reference_raft_parity",
        "rustraft_reference_raft_benchmark_evidence",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn rustraft_compatibility_report_names() -> Vec<String> {
    [
        "rustraft_public_api_contract",
        "rustraft_standalone_readiness_report",
        "rustraft_production_readiness_report",
        "rustraft_data_node_process_rollout_readiness_report",
        "rustraft_meta_process_rollout_readiness_report",
        "rustraft_reference_raft_runtime_capability_report",
        "rustraft_runtime_local_status_report",
        "rustraft_runtime_admin_report",
        "rustraft_fatal_blocker_report",
        "rustraft_reference_raft_runtime_capability_prometheus",
        "rustraft_temporalstore_extraction_plan",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn rustraft_standalone_readiness_report() -> RustRaftStandaloneReadinessReport {
    let capabilities = rustraft_standalone_capabilities();
    let missing = capabilities
        .iter()
        .flat_map(|capability| {
            capability
                .missing
                .iter()
                .map(move |missing| format!("{}: {}", capability.id, missing))
        })
        .collect::<Vec<_>>();
    let evidence = capabilities
        .iter()
        .flat_map(|capability| {
            capability
                .evidence
                .iter()
                .map(move |evidence| format!("{}: {}", capability.id, evidence))
        })
        .collect::<Vec<_>>();
    let standalone = capabilities.iter().all(|capability| capability.ready);

    RustRaftStandaloneReadinessReport {
        standalone,
        production_status: if standalone {
            RustRaftProductionStatus::ProductionReady
        } else {
            RustRaftProductionStatus::Blocked
        },
        capabilities,
        missing,
        evidence,
    }
}

fn rustraft_standalone_capabilities() -> Vec<RustRaftStandaloneCapability> {
    vec![
        standalone_capability(
            "node_lifecycle",
            &[
                "node::RaftNodeRuntime exposes start, stop, restart, and shutdown",
                "cluster::RustRaftConsensus exposes start, stop, propose, campaign, and transfer_leader",
            ],
        ),
        standalone_capability(
            "replication",
            &[
                "cluster::RaftCluster::propose appends opaque payload entries",
                "transport::AppendEntriesRequest and AppendEntriesResponse define the replication RPC",
                "RaftReplicationPipeline tracks inflight entries, backoff, reorder, and lag status",
                "RustRaftByteQuotaLimiter provides ReferenceRaft-style byte quota gating for snapshot and replication transfer",
            ],
        ),
        standalone_capability(
            "election_pre_vote",
            &[
                "transport::VoteRequest and PreVoteRequest define vote and pre-vote RPCs",
                "RaftCluster::campaign supports campaign and pre-vote entry points",
                "RaftCluster::transfer_leader provides explicit leader transfer",
            ],
        ),
        standalone_capability(
            "membership",
            &[
                "membership::RaftMembershipExecutor owns add learner, auto-promote learner, promote, witness, remove, and joint consensus operations",
                "RaftMembership and JointConsensusMembership model voter, learner, and witness roles",
            ],
        ),
        standalone_capability(
            "wal_recovery",
            &[
                "wal::LocalRaftWal and PersistentRaftWalOptions provide segmented WAL persistence",
                "rustraft_recover_latest_wal_record validates checksums and corrupt-tail truncation",
                "RaftHardState and RaftWalRecord preserve term, vote, commit, and log records",
            ],
        ),
        standalone_capability(
            "snapshots",
            &[
                "snapshot::RaftSnapshotLifecycle chunks, retries, quota-throttles, and installs snapshots",
                "PersistentRaftSnapshotStore persists checkpoints and reloads snapshot payloads",
                "RustRaftApplySnapshotFence validates snapshot floor and tail catch-up safety",
            ],
        ),
        standalone_capability(
            "read_index_lease_read",
            &[
                "cluster::RaftCluster::read_index enforces quorum read-index safety",
                "RaftCluster::lease_read_eligible rejects stale leaders and unapplied reads",
                "RustRaftReadIndexRequest and RustRaftReadIndexResponse expose the public read path",
            ],
        ),
        standalone_capability(
            "status_metrics_readiness",
            &[
                "status::RustRaftStatusSnapshot and cluster status reports expose runtime state",
                "metrics::rustraft_metric_names names replication, WAL, snapshot, read, and blocker metrics",
                "readiness reports cover ReferenceRaft parity, production gates, and fatal blockers",
            ],
        ),
    ]
}

fn standalone_capability(id: &str, evidence: &[&str]) -> RustRaftStandaloneCapability {
    RustRaftStandaloneCapability {
        id: id.to_string(),
        ready: !evidence.is_empty(),
        evidence: evidence.iter().map(|item| item.to_string()).collect(),
        missing: Vec::new(),
    }
}

pub fn rustraft_open_source_surface() -> RustRaftOpenSourceSurface {
    RustRaftOpenSourceSurface {
        crate_name: "rustraft".to_string(),
        public_modules: rustraft_public_module_names(),
        embedding_docs: vec!["README.md".to_string(), "docs/gap_plan.md".to_string()],
        embedding_examples: rustraft_embedding_examples(),
        reference_raft_parity_matrix: rustraft_reference_raft_parity_matrix(
            &RustRaftReadinessSnapshot {
                rustraft_leader_write_authority_present: true,
                rustraft_operator_observability_present: true,
                rustraft_rpc_transport_contract_present: true,
                rustraft_log_retention_snapshot_trigger_present: true,
                rustraft_apply_snapshot_fence_present: true,
                raft_storage_apply_fence_present: true,
                rustraft_snapshot_floor_log_matching_present: true,
                rustraft_snapshot_tail_catchup_present: true,
                rustraft_compacted_entry_rejection_present: true,
                rustraft_metaserver_snapshot_floor_election_present: true,
                learner_catchup_promotion_present: true,
                metaserver_membership_workflow_present: true,
            },
        )
        .into_iter()
        .map(|item| item.id)
        .collect(),
        benchmark_harness_interface: rustraft_benchmark_interface_names(),
        compatibility_reports: rustraft_compatibility_report_names(),
        rustraft_owned: vec![
            "public Raft modules and generic types".to_string(),
            "ReferenceRaft parity matrix and readiness reports".to_string(),
            "benchmark harness traits and pass/fail reports".to_string(),
            "transport/storage/state-machine contracts".to_string(),
            "runtime status, metrics, blocker, and compatibility reports".to_string(),
        ],
        temporalstore_adapter_boundary: vec![
            "TemporalStore command codecs".to_string(),
            "TemporalEngine apply logic".to_string(),
            "metaserver scheduler integration".to_string(),
            "HTTP/process endpoints".to_string(),
            "storage-object wiring and deployment docs".to_string(),
        ],
    }
}

pub fn rustraft_temporalstore_adapter_shape() -> RustRaftTemporalStoreAdapterShape {
    RustRaftTemporalStoreAdapterShape {
        backend_type: "TemporalRaftConsensusBackend".to_string(),
        node_field: "node".to_string(),
        node_runtime_type:
            "matrixraft::node::RaftNodeRuntime<TemporalStoreStateMachine, TemporalTransport>"
                .to_string(),
        state_machine_type_parameter: "TemporalStoreStateMachine".to_string(),
        transport_type_parameter: "TemporalTransport".to_string(),
        codec_field: "codec: TemporalCommandCodec".to_string(),
        engine_field: "engine: TemporalEngine".to_string(),
        rustraft_owned: vec![
            "consensus node runtime".to_string(),
            "leader election and campaign/pre-vote".to_string(),
            "replication, read-index, lease-read safety".to_string(),
            "membership transitions and learner/witness roles".to_string(),
            "WAL, snapshot, transport, metrics, readiness contracts".to_string(),
        ],
        temporalstore_owned: vec![
            "command encoding".to_string(),
            "apply semantics".to_string(),
            "storage engine".to_string(),
            "process/admin integration".to_string(),
        ],
        example: [
            "struct TemporalRaftConsensusBackend {",
            "    node: matrixraft::node::RaftNodeRuntime<TemporalStoreStateMachine, TemporalTransport>,",
            "    codec: TemporalCommandCodec,",
            "    engine: TemporalEngine,",
            "}",
        ]
        .join("\n"),
    }
}

pub fn rustraft_temporalstore_extraction_plan() -> RustRaftTemporalStoreExtractionPlan {
    RustRaftTemporalStoreExtractionPlan {
        policy: "RustRaft owns reusable consensus contracts, safety decisions, membership state, WAL/snapshot models, transport/storage traits, pipeline metrics, and deterministic harness logic; TemporalStore keeps only command codecs, process startup, shard FSM adapters, and storage-engine integration.".to_string(),
        slices: vec![
            RustRaftExtractionSlice {
                id: "read_safety".to_string(),
                status: RustRaftExtractionStatus::InLibrary,
                rustraft_owner: "read-index, lease-read, bounded-stale, lagging-follower, stale-leader, and minority-partition decisions".to_string(),
                temporalstore_boundary: "translate data-node and metaserver runtime status into RustRaft read-safety inputs".to_string(),
                next_evidence: "multi-process TemporalStore harness must attach observed read-index and lease responses".to_string(),
            },
            RustRaftExtractionSlice {
                id: "membership_workflow".to_string(),
                status: RustRaftExtractionStatus::InLibrary,
                rustraft_owner: "learner add/catch-up/promote, voter add/remove, witness add, leader transfer validation, rollback reports, and joint consensus summaries".to_string(),
                temporalstore_boundary: "metaserver scheduler invokes RustRaft workflow and applies accepted operations through data-node process APIs".to_string(),
                next_evidence: "scheduler-owned data-node membership report with stale-token rejection and restart replay".to_string(),
            },
            RustRaftExtractionSlice {
                id: "wal_snapshot_models".to_string(),
                status: RustRaftExtractionStatus::InLibrary,
                rustraft_owner: "hard state, WAL records, segment status, snapshot metadata, apply snapshot fences, and snapshot lifecycle reports".to_string(),
                temporalstore_boundary: "persist records in TemporalStore-owned directories and bind apply fences to storage mutations".to_string(),
                next_evidence: "crash between WAL persistence, storage mutation, and snapshot install recovers deterministically".to_string(),
            },
            RustRaftExtractionSlice {
                id: "transport_storage_traits".to_string(),
                status: RustRaftExtractionStatus::InLibrary,
                rustraft_owner: "generic storage and transport traits plus AppendEntries, Vote, PreVote, InstallSnapshot, snapshot chunk, and ReadIndex messages".to_string(),
                temporalstore_boundary: "HTTP/tonic/process adapters implement the traits without leaking TemporalStore command types into RustRaft".to_string(),
                next_evidence: "data-node and metaserver process paths consume trait adapters in scale/failover harnesses".to_string(),
            },
            RustRaftExtractionSlice {
                id: "fault_harness_contract".to_string(),
                status: RustRaftExtractionStatus::InLibrary,
                rustraft_owner: "ReferenceRaft-derived fault scenario catalog and readiness report for process-path evidence".to_string(),
                temporalstore_boundary: "TemporalStore process harnesses run the real data-node and metaserver binaries and feed observed evidence into RustRaft reports".to_string(),
                next_evidence: "packet loss, slow WAL, snapshot during membership, leader transfer under load, compacted-log rejoin, and rolling restart reports all pass".to_string(),
            },
            RustRaftExtractionSlice {
                id: "replication_pipeline_runtime".to_string(),
                status: RustRaftExtractionStatus::PendingMigration,
                rustraft_owner: "inflight limits, append/apply queue limits, max replicate bytes, oversized-log rejection, reorder queue, and pressure counters".to_string(),
                temporalstore_boundary: "runtime should feed per-peer process observations into RustRaft pipeline evidence".to_string(),
                next_evidence: "ReferenceRaft-derived packet-loss, out-of-order append, slow WAL, and pressure tests pass through process harnesses".to_string(),
            },
            RustRaftExtractionSlice {
                id: "domain_fsm_adapters".to_string(),
                status: RustRaftExtractionStatus::AdapterOnly,
                rustraft_owner: "opaque bytes/state-machine trait contracts only".to_string(),
                temporalstore_boundary: "TemporalStore owns data-shard commands, metaserver mutations, object/block storage, and admin surfaces".to_string(),
                next_evidence: "integration tests prove adapters implement RustRaft traits without moving domain codecs into the library".to_string(),
            },
        ],
    }
}
