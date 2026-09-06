// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! BaselineRaft parity, public API, and production readiness reporting API.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::matrixraft_metric_names;
use crate::metrics::PrometheusMetricSet;
use crate::{
    fault, AdminStatusSurfaceEvidence, BaselineRaftBenchmarkEvidence, DataNodeProcessRolloutReport,
    MembershipTransitionEvidence, MetaProcessRolloutReport, MetricNames, PipelineEvidence,
    RuntimePressureAdmission, SnapshotLifecycleEvidence, WalLifecycleEvidence,
};

pub use crate::{
    matrixraft_data_node_process_rollout_readiness_report,
    matrixraft_meta_process_rollout_readiness_report, matrixraft_production_readiness_report,
    BaselineRaftParitySurface,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RequirementCategory {
    Safety,
    Durability,
    Observability,
    Transport,
    Membership,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SemanticRequirement {
    pub id: String,
    pub category: RequirementCategory,
    pub readiness_field: String,
    pub required_for_production: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProductionStatus {
    ProductionReady,
    Blocked,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentMode {
    /// Backward-compatible deserialization variant only.
    ///
    /// Runtime validation rejects local Raft deployment. Local clusters are
    /// test fixtures and cannot satisfy production readiness.
    LocalModel,
    ProductionDistributed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct ProductionReadinessError {
    pub mode: DeploymentMode,
    pub message: String,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParityContract {
    pub library_name: String,
    pub consensus_backend_boundary: String,
    pub openraft_dependency_removed: bool,
    pub requirements: Vec<SemanticRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParityReport {
    pub contract: ParityContract,
    pub baseline_raft_reference_policy: BaselineRaftReferencePolicy,
    pub ready: bool,
    pub production_status: ProductionStatus,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub production_blockers: Vec<String>,
    pub baseline_raft_parity_matrix: Vec<BaselineRaftParityItem>,
    pub baseline_raft_gaps: Vec<String>,
    pub baseline_raft_intentional_differences: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaselineRaftReferencePolicy {
    pub feature_reference: String,
    pub performance_reference: String,
    pub rust_api_policy: String,
    pub temporalstore_consumption_boundary: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BaselineRaftParityStatus {
    Satisfied,
    Gap,
    IntentionalDifference,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaselineRaftParityItem {
    pub id: String,
    pub required: bool,
    pub status: BaselineRaftParityStatus,
    pub evidence: Vec<String>,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionReadinessInput {
    pub readiness: ReadinessSnapshot,
    #[serde(default)]
    pub peer_pipeline: Option<PipelineEvidence>,
    #[serde(default)]
    pub runtime_pressure_admission: Option<RuntimePressureAdmission>,
    #[serde(default)]
    pub snapshot_lifecycle: Option<SnapshotLifecycleEvidence>,
    #[serde(default)]
    pub wal_lifecycle: Option<WalLifecycleEvidence>,
    #[serde(default)]
    pub admin_status_surface: Option<AdminStatusSurfaceEvidence>,
    #[serde(default)]
    pub fault_harness: Option<fault::FaultHarnessReadinessReport>,
    #[serde(default)]
    pub data_node_rollout: Option<DataNodeProcessRolloutReport>,
    #[serde(default)]
    pub metaserver_rollout: Option<MetaProcessRolloutReport>,
    #[serde(default)]
    pub membership_transitions: Vec<MembershipTransitionEvidence>,
    #[serde(default)]
    pub baseline_raft_benchmark: Option<BaselineRaftBenchmarkEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionReadinessReport {
    pub parity: ParityReport,
    pub public_api: PublicApiContract,
    pub ready: bool,
    pub production_status: ProductionStatus,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub production_blockers: Vec<String>,
    pub recommended_next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessRolloutReadinessReport {
    pub scope: String,
    pub ready: bool,
    pub production_status: ProductionStatus,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub blockers: Vec<String>,
    pub recommended_next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessEvidence {
    pub requirement_id: String,
    pub readiness_field: String,
    pub present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessSnapshot {
    pub matrixraft_leader_write_authority_present: bool,
    pub matrixraft_operator_observability_present: bool,
    pub matrixraft_rpc_transport_contract_present: bool,
    pub matrixraft_log_retention_snapshot_trigger_present: bool,
    pub matrixraft_apply_snapshot_fence_present: bool,
    pub raft_storage_apply_fence_present: bool,
    pub matrixraft_snapshot_floor_log_matching_present: bool,
    pub matrixraft_snapshot_tail_catchup_present: bool,
    pub matrixraft_compacted_entry_rejection_present: bool,
    pub matrixraft_metaserver_snapshot_floor_election_present: bool,
    pub learner_catchup_promotion_present: bool,
    pub metaserver_membership_workflow_present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicApiContract {
    pub storage_trait: String,
    pub transport_trait: String,
    #[serde(default)]
    pub core_interfaces: Vec<String>,
    pub api_name_mappings: Vec<ApiNameMapping>,
    pub public_modules: Vec<String>,
    pub rpc_messages: Vec<String>,
    pub safety_helpers: Vec<String>,
    pub embedding_examples: Vec<String>,
    pub parity_reports: Vec<String>,
    pub benchmark_interfaces: Vec<String>,
    pub observability_interfaces: Vec<String>,
    pub diagnostic_interfaces: Vec<String>,
    #[serde(default)]
    pub evidence_interfaces: Vec<String>,
    pub compatibility_reports: Vec<String>,
    pub metrics: MetricNames,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicApiContractValidationReport {
    pub ready: bool,
    pub mapped_canonical_names: Vec<String>,
    pub unmapped_advertised_names: Vec<String>,
    pub unmapped_reference_required_names: Vec<String>,
    pub api_mapping_coverage_percent: usize,
    pub mapping_coverage_by_category: Vec<PublicApiMappingCoverage>,
    pub reference_required_names: Vec<String>,
    pub interface_name_count: usize,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicApiMappingCoverage {
    pub category: String,
    pub advertised_name_count: usize,
    pub mapped_name_count: usize,
    pub coverage_percent: usize,
    pub mapped_names: Vec<String>,
    pub unmapped_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiNameMapping {
    pub canonical: String,
    pub matrixraft_facade: String,
    pub raft_rs_or_tikv_reference: String,
    pub byteraft_or_baseline_reference: String,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StandaloneCapability {
    pub id: String,
    pub ready: bool,
    pub evidence: Vec<String>,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StandaloneReadinessReport {
    pub standalone: bool,
    pub production_status: ProductionStatus,
    pub capabilities: Vec<StandaloneCapability>,
    pub missing: Vec<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenSourceSurface {
    pub crate_name: String,
    pub public_modules: Vec<String>,
    pub embedding_docs: Vec<String>,
    pub embedding_examples: Vec<String>,
    pub baseline_raft_parity_matrix: Vec<String>,
    pub benchmark_harness_interface: Vec<String>,
    pub compatibility_reports: Vec<String>,
    pub matrixraft_owned: Vec<String>,
    pub temporalstore_adapter_boundary: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporalStoreAdapterShape {
    pub backend_type: String,
    pub node_field: String,
    pub node_runtime_type: String,
    pub state_machine_type_parameter: String,
    pub transport_type_parameter: String,
    pub codec_field: String,
    pub engine_field: String,
    pub matrixraft_owned: Vec<String>,
    pub temporalstore_owned: Vec<String>,
    pub example: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtractionStatus {
    InLibrary,
    AdapterOnly,
    PendingMigration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractionSlice {
    pub id: String,
    pub status: ExtractionStatus,
    pub matrixraft_owner: String,
    pub temporalstore_boundary: String,
    pub next_evidence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporalStoreExtractionPlan {
    pub policy: String,
    pub slices: Vec<ExtractionSlice>,
}

pub fn matrixraft_validate_deployment_mode(
    mode: DeploymentMode,
    readiness: &ProductionReadinessReport,
) -> Result<(), ProductionReadinessError> {
    matrixraft_validate_deployment_readiness(
        mode,
        readiness.ready,
        readiness_missing_reasons(readiness),
    )
}

pub fn matrixraft_validate_deployment_readiness(
    mode: DeploymentMode,
    production_ready: bool,
    missing: Vec<String>,
) -> Result<(), ProductionReadinessError> {
    match mode {
        DeploymentMode::LocalModel => Err(ProductionReadinessError {
            mode,
            message:
                "local Raft deployment mode is disabled; production distributed Raft is required"
                    .to_string(),
            missing,
        }),
        DeploymentMode::ProductionDistributed if production_ready => Ok(()),
        DeploymentMode::ProductionDistributed => Err(ProductionReadinessError {
            mode,
            message: "distributed Raft is not production-ready".to_string(),
            missing,
        }),
    }
}

pub fn matrixraft_require_production_ready(
    readiness: &ProductionReadinessReport,
) -> Result<(), ProductionReadinessError> {
    matrixraft_validate_deployment_mode(DeploymentMode::ProductionDistributed, readiness)
}

fn readiness_missing_reasons(readiness: &ProductionReadinessReport) -> Vec<String> {
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

pub fn matrixraft_readiness_evidence(snapshot: &ReadinessSnapshot) -> Vec<ReadinessEvidence> {
    matrixraft_requirements()
        .into_iter()
        .map(|requirement| ReadinessEvidence {
            present: readiness_field_present(snapshot, &requirement.readiness_field),
            requirement_id: requirement.id,
            readiness_field: requirement.readiness_field,
        })
        .collect()
}

pub fn matrixraft_requirements() -> Vec<SemanticRequirement> {
    use RequirementCategory::*;
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
    .map(|(id, category, readiness_field)| SemanticRequirement {
        id: id.to_string(),
        category,
        readiness_field: readiness_field.to_string(),
        required_for_production: true,
    })
    .collect()
}

fn readiness_field_present(snapshot: &ReadinessSnapshot, field: &str) -> bool {
    match field {
        "rustraft_leader_write_authority_present" => {
            snapshot.matrixraft_leader_write_authority_present
        }
        "rustraft_operator_observability_present" => {
            snapshot.matrixraft_operator_observability_present
        }
        "rustraft_rpc_transport_contract_present" => {
            snapshot.matrixraft_rpc_transport_contract_present
        }
        "rustraft_log_retention_snapshot_trigger_present" => {
            snapshot.matrixraft_log_retention_snapshot_trigger_present
        }
        "rustraft_apply_snapshot_fence_present" => snapshot.matrixraft_apply_snapshot_fence_present,
        "raft_storage_apply_fence_present" => snapshot.raft_storage_apply_fence_present,
        "rustraft_snapshot_floor_log_matching_present" => {
            snapshot.matrixraft_snapshot_floor_log_matching_present
        }
        "rustraft_snapshot_tail_catchup_present" => {
            snapshot.matrixraft_snapshot_tail_catchup_present
        }
        "rustraft_compacted_entry_rejection_present" => {
            snapshot.matrixraft_compacted_entry_rejection_present
        }
        "rustraft_metaserver_snapshot_floor_election_present" => {
            snapshot.matrixraft_metaserver_snapshot_floor_election_present
        }
        "learner_catchup_promotion_present" => snapshot.learner_catchup_promotion_present,
        "metaserver_membership_workflow_present" => snapshot.metaserver_membership_workflow_present,
        _ => false,
    }
}

pub fn matrixraft_parity_contract() -> ParityContract {
    ParityContract {
        library_name: "rustraft".to_string(),
        consensus_backend_boundary: "temporalstore_rust::raft::DataRaftConsensusBackend"
            .to_string(),
        openraft_dependency_removed: true,
        requirements: matrixraft_requirements(),
    }
}

pub fn matrixraft_baseline_raft_parity_surface() -> BaselineRaftParitySurface {
    BaselineRaftParitySurface {
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

pub fn matrixraft_baseline_raft_reference_policy() -> BaselineRaftReferencePolicy {
    BaselineRaftReferencePolicy {
        feature_reference: "BaselineRaft is the feature reference for Raft behavior parity.".to_string(),
        performance_reference:
            "BaselineRaft is the performance reference; RustRaft parity requires p50/p99 latency and throughput within the configured threshold."
                .to_string(),
        rust_api_policy:
            "RustRaft may expose idiomatic Rust traits, request/response types, and error types instead of BaselineRaft-shaped APIs."
                .to_string(),
        temporalstore_consumption_boundary:
            "TemporalStore consumption must remain stable through temporalstore_rust::raft::DataRaftConsensusBackend and adapter-owned codecs/apply/storage wiring."
                .to_string(),
    }
}

pub fn matrixraft_baseline_raft_parity_matrix(
    snapshot: &ReadinessSnapshot,
) -> Vec<BaselineRaftParityItem> {
    use BaselineRaftParityStatus::*;

    fn item(
        id: &str,
        status: BaselineRaftParityStatus,
        evidence: &[&str],
        note: &str,
    ) -> BaselineRaftParityItem {
        BaselineRaftParityItem {
            id: id.to_string(),
            required: true,
            status,
            evidence: evidence.iter().map(|field| (*field).to_string()).collect(),
            note: note.to_string(),
        }
    }

    fn status(ready: bool) -> BaselineRaftParityStatus {
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
                snapshot.matrixraft_leader_write_authority_present
                    && snapshot.matrixraft_rpc_transport_contract_present,
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
                snapshot.matrixraft_leader_write_authority_present
                    && snapshot.matrixraft_metaserver_snapshot_floor_election_present,
            ),
            &[
                "rustraft_leader_write_authority_present",
                "rustraft_metaserver_snapshot_floor_election_present",
            ],
            "leader authority and snapshot-floor election safety are present",
        ),
        item(
            "pre_vote",
            status(snapshot.matrixraft_rpc_transport_contract_present),
            &["rustraft_rpc_transport_contract_present", "VoteRequest.pre_vote"],
            "pre-vote is represented in the vote RPC contract",
        ),
        item(
            "lease_read",
            status(snapshot.matrixraft_leader_write_authority_present),
            &[
                "rustraft_leader_write_authority_present",
                "ReadIndexRequest.allow_lease_read",
            ],
            "lease reads are admitted only through leader/read-safety helpers",
        ),
        item(
            "read_index",
            status(snapshot.matrixraft_operator_observability_present),
            &[
                "rustraft_operator_observability_present",
                "ReadIndexRequest",
                "ReadIndexResponse",
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
            &["ReplicaRole::Witness.participates_in_quorum"],
            "witnesses count for quorum but are not data-serving leaders",
        ),
        item(
            "log_compaction",
            status(
                snapshot.matrixraft_compacted_entry_rejection_present
                    && snapshot.matrixraft_log_retention_snapshot_trigger_present,
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
                snapshot.matrixraft_log_retention_snapshot_trigger_present
                    && snapshot.matrixraft_snapshot_tail_catchup_present
                    && snapshot.matrixraft_snapshot_floor_log_matching_present,
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
                    && snapshot.matrixraft_apply_snapshot_fence_present,
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
            &["Consensus::transfer_leader"],
            "RustRaft exposes the transfer contract; process validation is attached by the consuming runtime",
        ),
        item(
            "observability_status",
            status(snapshot.matrixraft_operator_observability_present),
            &["rustraft_operator_observability_present", "StatusSnapshot"],
            "status snapshots and metric names are part of the public contract",
        ),
    ]
}

pub fn matrixraft_parity_report(snapshot: &ReadinessSnapshot) -> ParityReport {
    let contract = matrixraft_parity_contract();
    let evidence = matrixraft_readiness_evidence(snapshot);
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
    let baseline_raft_parity_matrix = matrixraft_baseline_raft_parity_matrix(snapshot);
    let baseline_raft_gaps = baseline_raft_parity_matrix
        .iter()
        .filter(|item| item.status == BaselineRaftParityStatus::Gap)
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    let baseline_raft_intentional_differences = baseline_raft_parity_matrix
        .iter()
        .filter(|item| item.status == BaselineRaftParityStatus::IntentionalDifference)
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    let ready = missing.is_empty() && production_blockers.is_empty();
    ParityReport {
        contract,
        baseline_raft_reference_policy: matrixraft_baseline_raft_reference_policy(),
        ready,
        production_status: if ready {
            ProductionStatus::ProductionReady
        } else {
            ProductionStatus::Blocked
        },
        satisfied,
        missing,
        production_blockers,
        baseline_raft_parity_matrix,
        baseline_raft_gaps,
        baseline_raft_intentional_differences,
    }
}

pub fn matrixraft_public_api_contract() -> PublicApiContract {
    PublicApiContract {
        storage_trait: "Storage".to_string(),
        transport_trait: "Transport".to_string(),
        core_interfaces: matrixraft_core_interface_names(),
        api_name_mappings: matrixraft_api_name_mappings(),
        public_modules: matrixraft_public_module_names(),
        rpc_messages: vec![
            "AppendEntriesRequest".to_string(),
            "AppendEntriesResponse".to_string(),
            "VoteRequest".to_string(),
            "VoteResponse".to_string(),
            "PreVoteRequest".to_string(),
            "PreVoteResponse".to_string(),
            "InstallSnapshotRequest".to_string(),
            "InstallSnapshotResponse".to_string(),
            "SnapshotChunk".to_string(),
            "ReadIndexRequest".to_string(),
            "ReadIndexResponse".to_string(),
            "AuthenticatedRaftRpc".to_string(),
            "TransportValidationReport".to_string(),
            "InMemoryRaftTransport".to_string(),
            "TcpRaftTransport".to_string(),
        ],
        safety_helpers: vec![
            "matrixraft_read_safety_decision".to_string(),
            "matrixraft_append_safety_decision".to_string(),
            "matrixraft_learner_promotion_decision".to_string(),
            "matrixraft_fatal_blocker_report".to_string(),
        ],
        embedding_examples: matrixraft_embedding_examples(),
        parity_reports: matrixraft_parity_report_names(),
        benchmark_interfaces: matrixraft_benchmark_interface_names(),
        observability_interfaces: matrixraft_observability_interface_names(),
        diagnostic_interfaces: matrixraft_diagnostic_interface_names(),
        evidence_interfaces: matrixraft_evidence_interface_names(),
        compatibility_reports: matrixraft_compatibility_report_names(),
        metrics: matrixraft_metric_names(),
    }
}

pub fn matrixraft_validate_public_api_contract(
    contract: &PublicApiContract,
) -> PublicApiContractValidationReport {
    let mut blockers = Vec::new();
    if contract.storage_trait != "Storage" {
        blockers.push(format!(
            "storage_trait:non_canonical:{}:expected:Storage",
            contract.storage_trait
        ));
    }
    if contract.transport_trait != "Transport" {
        blockers.push(format!(
            "transport_trait:non_canonical:{}:expected:Transport",
            contract.transport_trait
        ));
    }
    push_duplicate_name_blockers(
        &mut blockers,
        "core_interfaces",
        contract.core_interfaces.iter().map(String::as_str),
    );
    push_duplicate_name_blockers(
        &mut blockers,
        "public_modules",
        contract.public_modules.iter().map(String::as_str),
    );
    push_duplicate_name_blockers(
        &mut blockers,
        "rpc_messages",
        contract.rpc_messages.iter().map(String::as_str),
    );
    push_duplicate_name_blockers(
        &mut blockers,
        "safety_helpers",
        contract.safety_helpers.iter().map(String::as_str),
    );
    push_duplicate_name_blockers(
        &mut blockers,
        "embedding_examples",
        contract.embedding_examples.iter().map(String::as_str),
    );
    push_duplicate_name_blockers(
        &mut blockers,
        "parity_reports",
        contract.parity_reports.iter().map(String::as_str),
    );
    push_duplicate_name_blockers(
        &mut blockers,
        "benchmark_interfaces",
        contract.benchmark_interfaces.iter().map(String::as_str),
    );
    push_duplicate_name_blockers(
        &mut blockers,
        "observability_interfaces",
        contract.observability_interfaces.iter().map(String::as_str),
    );
    push_duplicate_name_blockers(
        &mut blockers,
        "diagnostic_interfaces",
        contract.diagnostic_interfaces.iter().map(String::as_str),
    );
    push_duplicate_name_blockers(
        &mut blockers,
        "evidence_interfaces",
        contract.evidence_interfaces.iter().map(String::as_str),
    );
    push_duplicate_name_blockers(
        &mut blockers,
        "compatibility_reports",
        contract.compatibility_reports.iter().map(String::as_str),
    );

    let mut canonical_names = BTreeSet::new();
    let mut facade_names = BTreeSet::new();
    let advertised_names = matrixraft_advertised_public_api_names(contract);
    for mapping in &contract.api_name_mappings {
        if mapping.canonical.trim().is_empty() {
            blockers.push("api_mapping:empty_canonical".to_string());
        } else if !canonical_names.insert(mapping.canonical.as_str()) {
            blockers.push(format!(
                "api_mapping:duplicate_canonical:{}",
                mapping.canonical
            ));
        } else if !advertised_names.contains(mapping.canonical.as_str()) {
            blockers.push(format!(
                "api_mapping:unadvertised_canonical:{}",
                mapping.canonical
            ));
        }
        if mapping.matrixraft_facade.trim().is_empty() {
            blockers.push(format!(
                "api_mapping:{}:empty_matrixraft_facade",
                mapping.canonical
            ));
        } else if !facade_names.insert(mapping.matrixraft_facade.as_str()) {
            blockers.push(format!(
                "api_mapping:duplicate_matrixraft_facade:{}",
                mapping.matrixraft_facade
            ));
        }
        if mapping.raft_rs_or_tikv_reference.trim().is_empty() {
            blockers.push(format!(
                "api_mapping:{}:missing_raft_rs_or_tikv_reference",
                mapping.canonical
            ));
        }
        if mapping.byteraft_or_baseline_reference.trim().is_empty() {
            blockers.push(format!(
                "api_mapping:{}:missing_byteraft_or_baseline_reference",
                mapping.canonical
            ));
        }
        if mapping.note.trim().is_empty() {
            blockers.push(format!("api_mapping:{}:missing_note", mapping.canonical));
        }
    }

    let reference_required_names = matrixraft_reference_mapped_interface_names();
    let mut unmapped_reference_required_names = Vec::new();
    for required in &reference_required_names {
        if !canonical_names.contains(required.as_str()) {
            unmapped_reference_required_names.push(required.clone());
            blockers.push(format!("api_mapping:missing_required_canonical:{required}"));
        }
    }

    let interface_name_count = contract.public_modules.len()
        + contract.core_interfaces.len()
        + contract.rpc_messages.len()
        + contract.safety_helpers.len()
        + contract.embedding_examples.len()
        + contract.parity_reports.len()
        + contract.benchmark_interfaces.len()
        + contract.observability_interfaces.len()
        + contract.diagnostic_interfaces.len()
        + contract.evidence_interfaces.len()
        + contract.compatibility_reports.len();
    let mapped_advertised_name_count = advertised_names
        .iter()
        .filter(|name| canonical_names.contains(**name))
        .count();
    let api_mapping_coverage_percent = if advertised_names.is_empty() {
        100
    } else {
        mapped_advertised_name_count * 100 / advertised_names.len()
    };
    let unmapped_advertised_names = advertised_names
        .iter()
        .filter(|name| !canonical_names.contains(**name))
        .map(|name| (*name).to_string())
        .collect();
    let mapping_coverage_by_category =
        matrixraft_api_mapping_coverage_by_category(contract, &canonical_names);
    for coverage in &mapping_coverage_by_category {
        if coverage.advertised_name_count > 0 && coverage.mapped_name_count == 0 {
            blockers.push(format!(
                "api_mapping:category_without_reference_mapping:{}",
                coverage.category
            ));
        }
    }
    let mapped_canonical_names = canonical_names.into_iter().map(str::to_string).collect();

    PublicApiContractValidationReport {
        ready: blockers.is_empty(),
        mapped_canonical_names,
        unmapped_advertised_names,
        unmapped_reference_required_names,
        api_mapping_coverage_percent,
        mapping_coverage_by_category,
        reference_required_names,
        interface_name_count,
        blockers,
    }
}

pub fn matrixraft_public_api_contract_validation_prometheus(
    report: &PublicApiContractValidationReport,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let mut text = String::new();
    let mut metric_count = 0_u64;
    matrixraft_push_public_api_metric(
        &mut text,
        "rustraft_public_api_contract_ready",
        labels,
        u64::from(report.ready),
    );
    metric_count += 1;
    matrixraft_push_public_api_metric(
        &mut text,
        "rustraft_public_api_mapping_coverage_percent",
        labels,
        report.api_mapping_coverage_percent as u64,
    );
    metric_count += 1;
    matrixraft_push_public_api_metric(
        &mut text,
        "rustraft_public_api_interface_name_total",
        labels,
        report.interface_name_count as u64,
    );
    metric_count += 1;
    matrixraft_push_public_api_metric(
        &mut text,
        "rustraft_public_api_reference_required_total",
        labels,
        report.reference_required_names.len() as u64,
    );
    metric_count += 1;
    matrixraft_push_public_api_metric(
        &mut text,
        "rustraft_public_api_unmapped_reference_required_total",
        labels,
        report.unmapped_reference_required_names.len() as u64,
    );
    metric_count += 1;
    matrixraft_push_public_api_metric(
        &mut text,
        "rustraft_public_api_unmapped_advertised_total",
        labels,
        report.unmapped_advertised_names.len() as u64,
    );
    metric_count += 1;

    for coverage in &report.mapping_coverage_by_category {
        let mut category_labels = labels.to_vec();
        category_labels.push(("category", coverage.category.as_str()));
        matrixraft_push_public_api_metric(
            &mut text,
            "rustraft_public_api_mapping_category_coverage_percent",
            &category_labels,
            coverage.coverage_percent as u64,
        );
        matrixraft_push_public_api_metric(
            &mut text,
            "rustraft_public_api_mapping_category_unmapped_total",
            &category_labels,
            coverage.unmapped_names.len() as u64,
        );
        metric_count += 2;
    }

    for blocker in &report.blockers {
        let mut blocker_labels = labels.to_vec();
        blocker_labels.push(("blocker", blocker.as_str()));
        matrixraft_push_public_api_metric(
            &mut text,
            "rustraft_public_api_blocker_present",
            &blocker_labels,
            1,
        );
        metric_count += 1;
    }

    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count,
        text,
    }
}

fn matrixraft_push_public_api_metric(
    out: &mut String,
    name: &str,
    labels: &[(&str, &str)],
    value: u64,
) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (idx, (label_name, label_value)) in labels.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            out.push_str(label_name);
            out.push_str("=\"");
            out.push_str(&matrixraft_escape_public_api_prometheus_label(label_value));
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn matrixraft_escape_public_api_prometheus_label(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('\n', r"\n")
        .replace('"', r#"\""#)
}

fn matrixraft_api_mapping_coverage_by_category(
    contract: &PublicApiContract,
    canonical_names: &BTreeSet<&str>,
) -> Vec<PublicApiMappingCoverage> {
    let categories = [
        ("storage_trait", vec![contract.storage_trait.as_str()]),
        ("transport_trait", vec![contract.transport_trait.as_str()]),
        (
            "core_interfaces",
            contract
                .core_interfaces
                .iter()
                .map(String::as_str)
                .collect(),
        ),
        (
            "rpc_messages",
            contract.rpc_messages.iter().map(String::as_str).collect(),
        ),
        (
            "safety_helpers",
            contract.safety_helpers.iter().map(String::as_str).collect(),
        ),
        (
            "embedding_examples",
            contract
                .embedding_examples
                .iter()
                .map(String::as_str)
                .collect(),
        ),
        (
            "parity_reports",
            contract.parity_reports.iter().map(String::as_str).collect(),
        ),
        (
            "benchmark_interfaces",
            contract
                .benchmark_interfaces
                .iter()
                .map(String::as_str)
                .collect(),
        ),
        (
            "observability_interfaces",
            contract
                .observability_interfaces
                .iter()
                .map(String::as_str)
                .collect(),
        ),
        (
            "diagnostic_interfaces",
            contract
                .diagnostic_interfaces
                .iter()
                .map(String::as_str)
                .collect(),
        ),
        (
            "evidence_interfaces",
            contract
                .evidence_interfaces
                .iter()
                .map(String::as_str)
                .collect(),
        ),
        (
            "compatibility_reports",
            contract
                .compatibility_reports
                .iter()
                .map(String::as_str)
                .collect(),
        ),
    ];

    categories
        .into_iter()
        .map(|(category, names)| {
            let advertised_names = names.into_iter().collect::<BTreeSet<_>>();
            let advertised_name_count = advertised_names.len();
            let mapped_name_count = advertised_names
                .iter()
                .filter(|name| canonical_names.contains(**name))
                .count();
            let mapped_names = advertised_names
                .iter()
                .filter(|name| canonical_names.contains(**name))
                .map(|name| (*name).to_string())
                .collect();
            let coverage_percent = if advertised_name_count == 0 {
                100
            } else {
                mapped_name_count * 100 / advertised_name_count
            };
            let unmapped_names = advertised_names
                .iter()
                .filter(|name| !canonical_names.contains(**name))
                .map(|name| (*name).to_string())
                .collect();

            PublicApiMappingCoverage {
                category: category.to_string(),
                advertised_name_count,
                mapped_name_count,
                coverage_percent,
                mapped_names,
                unmapped_names,
            }
        })
        .collect()
}

pub fn matrixraft_reference_mapped_interface_names() -> Vec<String> {
    [
        "Storage",
        "Transport",
        "Config",
        "AppendEntriesRequest",
        "AppendEntriesResponse",
        "VoteRequest",
        "VoteResponse",
        "PreVoteRequest",
        "PreVoteResponse",
        "InstallSnapshotRequest",
        "InstallSnapshotResponse",
        "ReadIndexRequest",
        "ReadIndexResponse",
        "SnapshotMetadata",
        "SnapshotLifecycleEvidence",
        "PipelineEvidence",
        "PipelineEvidence::packet_loss_reorder_faulted_peer_count",
        "PipelineEvidence::packet_loss_reorder_recovered_peer_count",
        "PipelineEvidence::packet_loss_reorder_all_faulted_peers_recovered",
        "PeerProgress",
        "NodeRuntime",
        "RuntimeTimerStatus",
        "RuntimeAdminReport",
        "matrixraft_production_readiness_report",
        "matrixraft_production_readiness_report_with_runtime_pressure_policy",
        "matrixraft_production_readiness_report_with_runtime_pressure_policy_and_freshness",
        "matrixraft_runtime_local_status_report",
        "matrixraft_runtime_admin_report",
        "AdminCommand::ReleaseMemory",
        "PersistentRaftWal",
        "DebugSnapshot",
        "DiagnosticLogEntry",
        "matrixraft_admin_diagnostic_log_entries",
        "matrixraft_admin_diagnostic_json_lines",
        "matrixraft_local_status_diagnostic_log_entries",
        "matrixraft_local_status_diagnostic_json_lines",
        "matrixraft_node_runtime_status_diagnostic_log_entries",
        "matrixraft_node_runtime_status_diagnostic_json_lines",
        "matrixraft_diagnostic_log_prometheus",
        "RuntimePressureAdmission",
        "LatencyPressureDetail",
        "NodeRuntimeTimerPressureDetail",
        "NodeRuntimeTimerThresholds",
        "matrixraft_validate_runtime_pressure_admission_evidence",
        "matrixraft_validate_runtime_pressure_admission_evidence_with_policy",
        "matrixraft_runtime_pressure_admission_with_scale_targets",
        "matrixraft_runtime_pressure_admission_with_pipeline_pressure",
        "matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure",
        "matrixraft_runtime_pressure_admission_with_scale_pipeline_read_backlog_and_node_runtime_timer_pressure",
        "matrixraft_runtime_pressure_bottleneck_summary",
        "matrixraft_runtime_pressure_freshness_report",
        "matrixraft_runtime_pressure_freshness_prometheus",
        "matrixraft_runtime_pressure_freshness_diagnostic_log_entries",
        "matrixraft_runtime_pressure_freshness_diagnostic_json_lines",
        "MailBox",
        "MailBox::try_send_checked",
        "MailBox::fetch_checked",
        "MailChannel",
        "MailChannel::try_send_checked",
        "ChannelSelector",
        "ChannelSelector::select_checked",
        "matrixraft_public_api_contract_validation_prometheus",
        "matrixraft_snapshot_lifecycle_evidence_prometheus",
        "matrixraft_wal_lifecycle_evidence_prometheus",
        "matrixraft_membership_readiness_prometheus",
        "matrixraft_release_benchmark_runtime_timer_status",
        "matrixraft_benchmark_runtime_pressure_readiness_artifact",
        "matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog",
        "matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer",
        "matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact",
        "matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog",
        "matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer",
        "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact",
        "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog",
        "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer",
        "matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact",
        "matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog",
        "matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer",
        "matrixraft_production_readiness_input_with_benchmark_artifacts",
        "matrixraft_production_readiness_input_with_asserted_benchmark_artifacts",
        "matrixraft_production_readiness_report_with_benchmark_artifacts",
        "matrixraft_production_readiness_report_with_asserted_benchmark_artifacts",
        "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts",
        "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts",
        "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts",
        "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_artifacts",
        "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "matrixraft_production_readiness_input_with_runtime_pressure_evidence",
        "matrixraft_production_readiness_input_with_runtime_pressure_read_backlog_and_node_runtime_timer_evidence",
        "matrixraft_debug_snapshot_with_runtime_pressure_evidence",
        "matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence",
        "matrixraft_debug_snapshot_with_runtime_pressure_read_backlog_and_node_runtime_timer_evidence",
        "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts",
        "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "RuntimePressureAdmissionPolicy",
        "BenchmarkRunner",
        "matrixraft_grafana_dashboard",
        "matrixraft_grafana_dashboard_json",
        "matrixraft_alert_rules",
        "matrixraft_alert_rules_json",
        "matrixraft_observability_provisioning",
        "matrixraft_observability_provisioning_runbook_steps",
        "matrixraft_operator_runbook_steps_with_diagnostics",
        "matrixraft_operator_runbook_prometheus",
        "matrixraft_observability_provisioning_json",
        "matrixraft_observability_required_metric_names",
        "matrixraft_validate_required_metric_scrape_texts",
        "matrixraft_validate_observability_provisioning",
        "matrixraft_validate_observability_provisioning_json",
        "matrixraft_observability_provisioning_validation_prometheus",
        "matrixraft_operator_runbook_steps",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn push_duplicate_name_blockers<'a>(
    blockers: &mut Vec<String>,
    scope: &str,
    names: impl Iterator<Item = &'a str>,
) {
    let mut seen = BTreeSet::new();
    for name in names {
        if name.trim().is_empty() {
            blockers.push(format!("{scope}:empty_name"));
        } else if !seen.insert(name) {
            blockers.push(format!("{scope}:duplicate_name:{name}"));
        }
    }
}

fn matrixraft_advertised_public_api_names(contract: &PublicApiContract) -> BTreeSet<&str> {
    let mut names = BTreeSet::new();
    names.insert(contract.storage_trait.as_str());
    names.insert(contract.transport_trait.as_str());
    names.extend(contract.core_interfaces.iter().map(String::as_str));
    names.extend(contract.rpc_messages.iter().map(String::as_str));
    names.extend(contract.safety_helpers.iter().map(String::as_str));
    names.extend(contract.embedding_examples.iter().map(String::as_str));
    names.extend(contract.parity_reports.iter().map(String::as_str));
    names.extend(contract.benchmark_interfaces.iter().map(String::as_str));
    names.extend(contract.observability_interfaces.iter().map(String::as_str));
    names.extend(contract.diagnostic_interfaces.iter().map(String::as_str));
    names.extend(contract.evidence_interfaces.iter().map(String::as_str));
    names.extend(contract.compatibility_reports.iter().map(String::as_str));
    names
}

pub fn matrixraft_api_name_mappings() -> Vec<ApiNameMapping> {
    vec![
        ApiNameMapping {
            canonical: "Storage".to_string(),
            matrixraft_facade: "MatrixRaftStorage".to_string(),
            raft_rs_or_tikv_reference: "raft::Storage".to_string(),
            byteraft_or_baseline_reference: "Raft log/snapshot storage".to_string(),
            note: "Use the unprefixed trait in native Rust; the facade name is reserved for compatibility adapters."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "Transport".to_string(),
            matrixraft_facade: "MatrixRaftTransport".to_string(),
            raft_rs_or_tikv_reference: "Raft message router / RaftStore transport".to_string(),
            byteraft_or_baseline_reference: "AppendEntries/Vote/Snapshot RPC transport"
                .to_string(),
            note: "Transport owns RPC dispatch; production services provide the network implementation."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "Config".to_string(),
            matrixraft_facade: "MatrixRaftOptions".to_string(),
            raft_rs_or_tikv_reference: "raft::Config / TiKV raftstore tuning".to_string(),
            byteraft_or_baseline_reference: "raft group options".to_string(),
            note: "Config owns timing, payload, log-buffer, and election-safety limits; facade options adapt product-level defaults into the canonical runtime."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "AppendEntriesRequest".to_string(),
            matrixraft_facade: "MatrixRaftAppendEntriesRequest".to_string(),
            raft_rs_or_tikv_reference: "eraftpb::MessageType::MsgAppend".to_string(),
            byteraft_or_baseline_reference: "append_entries".to_string(),
            note: "Canonical RustRaft request fields stay typed; facade messages preserve MatrixRaft wire-shape naming."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "AppendEntriesResponse".to_string(),
            matrixraft_facade: "MatrixRaftAppendEntriesResponse".to_string(),
            raft_rs_or_tikv_reference: "eraftpb::MessageType::MsgAppendResponse".to_string(),
            byteraft_or_baseline_reference: "append_entries_response".to_string(),
            note: "Rejected index, rejection hint, and snapshot-required state map to replication pipeline backoff."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "VoteRequest".to_string(),
            matrixraft_facade: "MatrixRaftVoteRequest".to_string(),
            raft_rs_or_tikv_reference: "eraftpb::MessageType::MsgRequestVote".to_string(),
            byteraft_or_baseline_reference: "request_vote".to_string(),
            note: "VoteRequest is the canonical election request; facade naming keeps MatrixRaft wire compatibility."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "VoteResponse".to_string(),
            matrixraft_facade: "MatrixRaftVoteResponse".to_string(),
            raft_rs_or_tikv_reference: "eraftpb::MessageType::MsgRequestVoteResponse"
                .to_string(),
            byteraft_or_baseline_reference: "request_vote_response".to_string(),
            note: "VoteResponse carries election grant/reject evidence for parity checks and operator diagnostics."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "PreVoteRequest".to_string(),
            matrixraft_facade: "MatrixRaftPreVoteRequest".to_string(),
            raft_rs_or_tikv_reference: "eraftpb::MessageType::MsgRequestPreVote".to_string(),
            byteraft_or_baseline_reference: "pre_vote".to_string(),
            note: "PreVoteRequest keeps disruptive election prevention explicit for TiKV-style raftstore readiness."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "PreVoteResponse".to_string(),
            matrixraft_facade: "MatrixRaftPreVoteResponse".to_string(),
            raft_rs_or_tikv_reference: "eraftpb::MessageType::MsgRequestPreVoteResponse"
                .to_string(),
            byteraft_or_baseline_reference: "pre_vote_response".to_string(),
            note: "PreVoteResponse exposes pre-election quorum evidence without overloading the regular vote path."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "InstallSnapshotRequest".to_string(),
            matrixraft_facade: "MatrixRaftInstallSnapshotRequest".to_string(),
            raft_rs_or_tikv_reference: "eraftpb::MessageType::MsgSnapshot".to_string(),
            byteraft_or_baseline_reference: "install_snapshot".to_string(),
            note: "InstallSnapshotRequest is the canonical sender boundary for compacted-log recovery."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "InstallSnapshotResponse".to_string(),
            matrixraft_facade: "MatrixRaftInstallSnapshotResponse".to_string(),
            raft_rs_or_tikv_reference: "snapshot apply response / raftstore snapshot status"
                .to_string(),
            byteraft_or_baseline_reference: "install_snapshot_response".to_string(),
            note: "InstallSnapshotResponse gives snapshot sender/downloader lifecycle tests a stable completion vocabulary."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "ReadIndexRequest".to_string(),
            matrixraft_facade: "MatrixRaftReadIndexOptions".to_string(),
            raft_rs_or_tikv_reference: "ReadIndex / MsgReadIndex".to_string(),
            byteraft_or_baseline_reference: "read_index / lease_read".to_string(),
            note: "Lease reads and quorum reads are explicit options so followers can fail closed or serve bounded stale reads."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "ReadIndexResponse".to_string(),
            matrixraft_facade: "MatrixRaftReadIndexStatus".to_string(),
            raft_rs_or_tikv_reference: "ReadState / MsgReadIndexResp".to_string(),
            byteraft_or_baseline_reference: "read_index_response / lease_read_result"
                .to_string(),
            note: "ReadIndexResponse separates safe-read proof, lease-read eligibility, and bounded-stale fallback status."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "MailBox".to_string(),
            matrixraft_facade: "MatrixRaftMailBox".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore scheduler mailbox".to_string(),
            byteraft_or_baseline_reference: "BaselineRaft event queue".to_string(),
            note: "MailBox is the priority queue boundary for scheduler and transport work that embedders can monitor for pressure."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "MailBox::try_send_checked".to_string(),
            matrixraft_facade: "MatrixRaftMailBox::TrySendChecked".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore non-blocking mailbox send with backpressure".to_string(),
            byteraft_or_baseline_reference: "BaselineRaft checked event enqueue".to_string(),
            note: "The checked send path returns queue saturation and lock failures as values so production runtimes can shed or retry work without process aborts."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "MailBox::fetch_checked".to_string(),
            matrixraft_facade: "MatrixRaftMailBox::FetchChecked".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore bounded mailbox drain".to_string(),
            byteraft_or_baseline_reference: "BaselineRaft checked event dequeue".to_string(),
            note: "The checked fetch path preserves priority ordering and deadline behavior while surfacing queue runtime failures as RaftError."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "MailChannel".to_string(),
            matrixraft_facade: "MatrixRaftMailChannel".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore per-peer ready queue".to_string(),
            byteraft_or_baseline_reference: "BaselineRaft per-replica event lane".to_string(),
            note: "MailChannel names the per-replica queue used by the selector to track burst pressure and preserve per-peer scheduling."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "MailChannel::try_send_checked".to_string(),
            matrixraft_facade: "MatrixRaftMailChannel::TrySendChecked".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore per-peer enqueue with flow-control feedback".to_string(),
            byteraft_or_baseline_reference: "BaselineRaft checked per-peer enqueue".to_string(),
            note: "The checked channel send keeps overflow as a recoverable result that returns the caller's mail while reporting runtime failures as RaftError."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "ChannelSelector".to_string(),
            matrixraft_facade: "MatrixRaftChannelSelector".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore ready peer selector".to_string(),
            byteraft_or_baseline_reference: "BaselineRaft replica scheduler selector".to_string(),
            note: "ChannelSelector is the fairness and fanout boundary for selecting active peer queues under release-scale workloads."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "ChannelSelector::select_checked".to_string(),
            matrixraft_facade: "MatrixRaftChannelSelector::SelectChecked".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore bounded ready-poll loop".to_string(),
            byteraft_or_baseline_reference: "BaselineRaft checked ready-queue selector".to_string(),
            note: "The checked selector keeps deadline-bound polling and global-mail draining available without converting poisoned queue state into a process panic."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_read_safety_decision".to_string(),
            matrixraft_facade: "MatrixRaftReadSafetyDecision".to_string(),
            raft_rs_or_tikv_reference: "TiKV ReadIndex and lease-read safety check"
                .to_string(),
            byteraft_or_baseline_reference: "ByteRaft safe read / lease read gate".to_string(),
            note: "The helper maps live leader, quorum, applied-index, and bounded-stale evidence into a fail-closed read decision."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_append_safety_decision".to_string(),
            matrixraft_facade: "MatrixRaftAppendSafetyDecision".to_string(),
            raft_rs_or_tikv_reference: "raft-rs append log matching and rejection hint"
                .to_string(),
            byteraft_or_baseline_reference: "AppendEntries safety and backoff gate".to_string(),
            note: "The helper names the append acceptance contract that protects log matching, compacted-entry rejection, and pipeline retry behavior."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_learner_promotion_decision".to_string(),
            matrixraft_facade: "MatrixRaftLearnerPromotionDecision".to_string(),
            raft_rs_or_tikv_reference: "TiKV learner catch-up and promote-peer workflow"
                .to_string(),
            byteraft_or_baseline_reference: "learner catch-up / auto-promote gate".to_string(),
            note: "The helper keeps learner promotion tied to observed match index, lag, and membership readiness rather than a caller-only flag."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "SnapshotMetadata".to_string(),
            matrixraft_facade: "MatrixRaftSnapshotDesc".to_string(),
            raft_rs_or_tikv_reference: "Snapshot metadata".to_string(),
            byteraft_or_baseline_reference: "snapshot descriptor".to_string(),
            note: "The canonical type carries snapshot id, last log id, and membership; facade conversion keeps MatrixRaft field names."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "SnapshotLifecycleEvidence".to_string(),
            matrixraft_facade: "MatrixRaftSnapshotLifecycleEvidence".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore snapshot send/apply progress and lifecycle metrics".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft snapshot sender/downloader lifecycle evidence".to_string(),
            note: "SnapshotLifecycleEvidence is the canonical production gate for retry, timeout, rate-limit, sustained load, coherent transfer completion, rollback, membership-change, and compacted-log rejoin proof."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "PeerProgress".to_string(),
            matrixraft_facade: "MatrixRaftAsyncStatus".to_string(),
            raft_rs_or_tikv_reference: "Progress / ProgressTracker".to_string(),
            byteraft_or_baseline_reference: "per-peer replication pipeline status".to_string(),
            note: "Use PeerProgress for QPS, lag, inflight bytes, snapshot transfer, and packet-loss/reorder evidence."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "PipelineEvidence".to_string(),
            matrixraft_facade: "MatrixRaftPipelineEvidence".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore ProgressTracker and transport fault evidence".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft replication pipeline readiness evidence".to_string(),
            note: "PipelineEvidence is the production gate for append/apply backpressure, memory limits, stale terms, packet-loss probes, reorder convergence, and per-peer fault recovery."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "PipelineEvidence::packet_loss_reorder_faulted_peer_count".to_string(),
            matrixraft_facade: "MatrixRaftPipelineEvidence.packetLossReorderFaultedPeerCount"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV per-peer ProgressTracker transport fault coverage".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft packet-loss/reorder faulted peer count".to_string(),
            note: "Counts every peer that observed both packet loss and reordered append evidence so production parity cannot rely on a single sampled peer."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "PipelineEvidence::packet_loss_reorder_recovered_peer_count".to_string(),
            matrixraft_facade: "MatrixRaftPipelineEvidence.packetLossReorderRecoveredPeerCount"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV per-peer Progress recovery after transport fault and reorder".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft packet-loss/reorder recovered peer count".to_string(),
            note: "Counts peers that both saw the combined fault and reached clean append progress afterward."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "PipelineEvidence::packet_loss_reorder_all_faulted_peers_recovered"
                .to_string(),
            matrixraft_facade:
                "MatrixRaftPipelineEvidence.packetLossReorderAllFaultedPeersRecovered".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore all-peer Progress recovery gate after transport/reorder faults"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft all faulted peers recovered release gate".to_string(),
            note: "This is the fail-closed production signal required before QPS/latency parity can claim deeper per-peer replication-pipeline behavior."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "NodeRuntime".to_string(),
            matrixraft_facade: "MatrixRaftNodeRuntime".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore peer worker / RaftRouter event loop"
                .to_string(),
            byteraft_or_baseline_reference: "BaselineRaft node runtime and scheduler worker"
                .to_string(),
            note: "NodeRuntime is the canonical production event-loop boundary for lifecycle commands, ReadIndex routing, peer pipeline pressure, and timer backpressure."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "RuntimeTimerStatus".to_string(),
            matrixraft_facade: "MatrixRaftRuntimeTimerStatus".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore tick metrics and scheduler delay telemetry"
                .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft heartbeat/election timer status and scheduler pressure".to_string(),
            note: "RuntimeTimerStatus keeps heartbeat, election, lease, pending tick, rejected tick, and completed tick counters stable for Grafana, logs, and latency/QPS readiness triage."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "RuntimeAdminReport".to_string(),
            matrixraft_facade: "MatrixRaftAdminStatus".to_string(),
            raft_rs_or_tikv_reference: "RaftStore admin/status view".to_string(),
            byteraft_or_baseline_reference: "GetInfo/admin status".to_string(),
            note: "Admin reports are the stable status boundary for dashboards, logs, and production readiness gates."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_production_readiness_report".to_string(),
            matrixraft_facade: "MatrixRaftProductionReadinessReport".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore release gate with benchmark, telemetry, and operational evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft production readiness and ByteRaft release gate".to_string(),
            note: "Production-readiness reports are the canonical deploy gate for release blockers, QPS/latency/memory parity, and ranked runtime-pressure bottlenecks."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_production_readiness_report_with_runtime_pressure_policy"
                .to_string(),
            matrixraft_facade: "MatrixRaftPolicyAwareProductionReadinessReport".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore flow-control policy deployment gate".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft fail-closed runtime-pressure deployment gate".to_string(),
            note: "Policy-aware production readiness reports keep the stable production gate while rejecting runtime-pressure evidence whose rejected_component does not match the configured fail-closed admission policy."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_report_with_runtime_pressure_policy_and_freshness"
                    .to_string(),
            matrixraft_facade: "MatrixRaftFreshPolicyProductionReadinessReport".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore release gates require fresh flow-control and scheduler telemetry"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft fail-closed runtime-pressure freshness deployment gate".to_string(),
            note: "Freshness-aware production readiness reports reject stale, invalid, missing, or future-dated runtime-pressure evidence before QPS, latency, or memory parity can be claimed."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_runtime_local_status_report".to_string(),
            matrixraft_facade: "MatrixRaftRuntimeLocalStatusReport".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore local peer status and ProgressTracker snapshot".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft local status/GetInfo replica progress view".to_string(),
            note: "Local runtime status binds node state, peer replication progress, WAL/snapshot health, and readiness evidence for bounded-stale read and latency debugging."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_runtime_admin_report".to_string(),
            matrixraft_facade: "MatrixRaftRuntimeAdminReport".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore admin/status aggregation across peers".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft GetInfo/admin status aggregation".to_string(),
            note: "Runtime admin reports aggregate local status, release blockers, memory pressure, QPS/latency readiness, and peer-pipeline lag for operator triage."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "AdminCommand::ReleaseMemory".to_string(),
            matrixraft_facade: "MatrixRaftAdminCommandType::ReleaseMemory".to_string(),
            raft_rs_or_tikv_reference: "RaftStore admin command / memory pressure control".to_string(),
            byteraft_or_baseline_reference: "release_memory admin action".to_string(),
            note: "ReleaseMemory is the explicit operator and runtime-pressure path for bounded cache/log-buffer cleanup under memory pressure."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "PersistentRaftWal".to_string(),
            matrixraft_facade: "PersistentRaftWalOptions".to_string(),
            raft_rs_or_tikv_reference: "RaftEngine/WAL".to_string(),
            byteraft_or_baseline_reference: "persistent log segment lifecycle".to_string(),
            note: "WAL naming stays explicit because production readiness depends on fsync, recovery, and compaction evidence."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "DebugSnapshot".to_string(),
            matrixraft_facade: "matrixraft_debug_snapshot_json".to_string(),
            raft_rs_or_tikv_reference: "debug/metrics bundle".to_string(),
            byteraft_or_baseline_reference: "support envelope".to_string(),
            note: "DebugSnapshot is the handoff bundle for logs, Grafana, alerts, benchmark parity, memory, and latency evidence."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "DiagnosticLogEntry".to_string(),
            matrixraft_facade: "matrixraft_*_diagnostic_json_lines".to_string(),
            raft_rs_or_tikv_reference: "TiKV structured log / slog fields".to_string(),
            byteraft_or_baseline_reference: "diagnostic log line".to_string(),
            note: "DiagnosticLogEntry is the canonical logging record before rendering JSON lines or Prometheus counters for release triage."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_admin_diagnostic_log_entries".to_string(),
            matrixraft_facade: "MatrixRaftAdminDiagnosticLogs".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore admin structured logs and status fields".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft admin/GetInfo diagnostic log lines".to_string(),
            note: "Admin diagnostic log entries keep release blockers, unhealthy peers, memory pressure, WAL lag, and QPS/latency failures readable before JSON or Prometheus rendering."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_admin_diagnostic_json_lines".to_string(),
            matrixraft_facade: "MatrixRaftAdminDiagnosticJsonLines".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore JSON structured log export".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft admin/GetInfo diagnostic JSON lines".to_string(),
            note: "Admin diagnostic JSON lines provide machine-stable operator evidence for release automation, Grafana log panels, and support bundle triage."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_local_status_diagnostic_log_entries".to_string(),
            matrixraft_facade: "MatrixRaftLocalStatusDiagnosticLogs".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore local peer structured status logs".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft local replica status diagnostic log lines".to_string(),
            note: "Local-status diagnostic log entries expose per-replica progress, read readiness, WAL/snapshot health, and scheduler pressure for follower-read debugging."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_local_status_diagnostic_json_lines".to_string(),
            matrixraft_facade: "MatrixRaftLocalStatusDiagnosticJsonLines".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore local status JSON log export".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft local replica diagnostic JSON lines".to_string(),
            note: "Local-status diagnostic JSON lines keep random-replica read lag, bounded-stale readiness, and peer pipeline evidence parseable in support bundles."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_node_runtime_status_diagnostic_log_entries".to_string(),
            matrixraft_facade: "MatrixRaftNodeRuntimeStatusDiagnosticLogs".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore scheduler and peer worker structured logs".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft node-runtime diagnostic log lines".to_string(),
            note: "Node-runtime diagnostic logs expose event-loop timers, pending ticks, dropped work, memory pressure, and replication scheduler saturation for scale debugging."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_node_runtime_status_diagnostic_json_lines".to_string(),
            matrixraft_facade: "MatrixRaftNodeRuntimeStatusDiagnosticJsonLines".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore scheduler JSON structured log export".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft node-runtime diagnostic JSON lines".to_string(),
            note: "Node-runtime diagnostic JSON lines make timer backlog, queue saturation, and scale-readiness failures stable inputs for dashboards and release verifiers."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_diagnostic_log_prometheus".to_string(),
            matrixraft_facade: "MatrixRaftDiagnosticLogPrometheus".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore diagnostic log counters exported to Prometheus".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft diagnostic log severity/source metrics".to_string(),
            note: "Diagnostic-log Prometheus output turns structured log entries into release-scale counters without losing source, severity, QPS, latency, memory, WAL, or peer-pipeline labels."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "RuntimePressureAdmission".to_string(),
            matrixraft_facade: "matrixraft_runtime_pressure_admission".to_string(),
            raft_rs_or_tikv_reference: "raftstore flow control / backpressure".to_string(),
            byteraft_or_baseline_reference: "admission and throttle decision".to_string(),
            note: "RuntimePressureAdmission connects memory, latency, scale, and peer pipeline telemetry to observe-only or fail-closed admission decisions."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "LatencyPressureDetail".to_string(),
            matrixraft_facade: "MatrixRaftRuntimeLatencyPressureDetail".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore latency histogram and flow-control detail"
                .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release p95/p99 latency threshold evidence".to_string(),
            note: "LatencyPressureDetail is the stable tail-latency evidence record for sample count, p95, p99, configured threshold, and excess used by dashboards, logs, and production gates."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "NodeRuntimeTimerPressureDetail".to_string(),
            matrixraft_facade: "MatrixRaftRuntimeNodeTimerPressureDetail".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore tick scheduler saturation detail".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft event-loop timer queue pressure evidence".to_string(),
            note: "NodeRuntimeTimerPressureDetail records timer utilization percent, threshold, and excess so release-scale QPS and latency evidence can reject scheduler saturation before ticks are dropped."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "NodeRuntimeTimerThresholds".to_string(),
            matrixraft_facade: "MatrixRaftRuntimeNodeTimerThresholds".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore scheduler flow-control thresholds".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft runtime timer saturation threshold".to_string(),
            note: "NodeRuntimeTimerThresholds pins the proactive timer-utilization warning threshold used by runtime pressure admission and production observability."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_validate_runtime_pressure_admission_evidence".to_string(),
            matrixraft_facade: "MatrixRaftRuntimePressureAdmissionEvidenceValidator".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore release telemetry consistency checks".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft runtime-pressure evidence validation".to_string(),
            note: "Runtime-pressure admission evidence validation rejects malformed memory, latency, scale, pipeline, and read-backlog detail before Grafana, logs, or production gates can advertise impossible pressure samples."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_validate_runtime_pressure_admission_evidence_with_policy"
                .to_string(),
            matrixraft_facade:
                "MatrixRaftRuntimePressureAdmissionPolicyAwareEvidenceValidator".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore flow-control policy consistency checks".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft fail-closed runtime-pressure rejection validation".to_string(),
            note: "Policy-aware runtime-pressure validation rejects admissions whose rejected_component does not match the configured fail-closed pressure priority, so production gates cannot advertise a lower-priority blocker while read backlog, timer, or latency pressure is active."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_runtime_pressure_admission_with_scale_targets".to_string(),
            matrixraft_facade: "MatrixRaftRuntimeScalePressureAdmission".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore QPS and apply throughput flow control"
                .to_string(),
            byteraft_or_baseline_reference: "release QPS/throughput target admission"
                .to_string(),
            note: "Scale-target admission keeps release QPS, append/read/apply throughput, and replication bandwidth misses visible before production parity is claimed."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_runtime_pressure_admission_with_pipeline_pressure".to_string(),
            matrixraft_facade: "MatrixRaftRuntimePeerPipelinePressureAdmission".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore per-peer Progress and raftstore flow control".to_string(),
            byteraft_or_baseline_reference: "ByteRaft per-peer pipeline saturation admission"
                .to_string(),
            note: "Peer-pipeline admission maps append/apply/reorder queues and backpressure rejections into a fail-closed production guard."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure"
                .to_string(),
            matrixraft_facade: "MatrixRaftRuntimeNodeTimerPressureAdmission".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore scheduler backpressure guard".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft node event-loop timer pressure admission".to_string(),
            note: "Node-runtime timer admission turns pending-tick queue utilization into observe-only or fail-closed pressure evidence before release-scale latency or QPS parity is claimed."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure"
                    .to_string(),
            matrixraft_facade: "MatrixRaftRuntimeFullPressureAdmission".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore QPS, Progress, read-index, and flow-control gates".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft release-scale QPS, per-peer pipeline, and read backlog admission"
                    .to_string(),
            note: "Full runtime-pressure admission keeps scale targets, peer pipeline pressure, and read backlog pressure in one fail-closed production decision."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_runtime_pressure_admission_with_scale_pipeline_read_backlog_and_node_runtime_timer_pressure"
                    .to_string(),
            matrixraft_facade: "MatrixRaftRuntimeCompletePressureAdmission".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore QPS, Progress, read-index, scheduler, and flow-control gates"
                    .to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft release-scale QPS, per-peer pipeline, read backlog, and timer pressure admission"
                    .to_string(),
            note: "Complete runtime-pressure admission keeps scale targets, peer pipeline pressure, read backlog pressure, and node-runtime timer saturation in one fail-closed production decision."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_runtime_pressure_bottleneck_summary".to_string(),
            matrixraft_facade: "MatrixRaftRuntimePressureBottleneckSummary".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore flow-control bottleneck and dashboard triage signal".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft release-scale QPS/latency bottleneck ranking".to_string(),
            note: "Runtime-pressure bottleneck summaries rank memory, latency, scale, peer-pipeline, read-backlog, and timer pressure by excess or deficit percent so release automation can explain failed QPS/latency parity without parsing raw detail arrays."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_runtime_pressure_freshness_report".to_string(),
            matrixraft_facade: "MatrixRaftRuntimePressureFreshnessReport".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore release-dashboard freshness window".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft release-scale QPS/latency/memory evidence freshness gate".to_string(),
            note: "Runtime-pressure freshness reports classify fresh, low-fresh, stale, and invalid telemetry windows so memory, latency, and QPS evidence cannot be reused after its release gate age budget expires."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_runtime_pressure_freshness_prometheus".to_string(),
            matrixraft_facade: "MatrixRaftRuntimePressureFreshnessPrometheus".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore Prometheus freshness scrape for release dashboards".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft release-scale QPS/latency/memory freshness telemetry".to_string(),
            note: "Runtime-pressure freshness Prometheus exports generated time, age, stale boundary, remaining freshness, status, and issue counters so Grafana can reject stale QPS, latency, or memory evidence before parity is claimed."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_runtime_pressure_freshness_diagnostic_log_entries".to_string(),
            matrixraft_facade: "MatrixRaftRuntimePressureFreshnessDiagnosticLogEntries"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore structured telemetry freshness diagnostics".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft release-scale QPS/latency/memory freshness logs".to_string(),
            note: "Runtime-pressure freshness diagnostic entries make stale, low-fresh, invalid, and future-dated benchmark evidence visible to log-only release gates without parsing Prometheus text."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_runtime_pressure_freshness_diagnostic_json_lines".to_string(),
            matrixraft_facade: "MatrixRaftRuntimePressureFreshnessDiagnosticJsonLines".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore JSON log freshness diagnostics".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft release-scale freshness diagnostic JSON lines".to_string(),
            note: "Runtime-pressure freshness JSON lines preserve generated time, age, stale boundary, remaining freshness, status, and issue fields for release automation and centralized log queries."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_public_api_contract_validation_prometheus".to_string(),
            matrixraft_facade: "MatrixRaftPublicApiContractValidationPrometheus".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore public API compatibility dashboard scrape".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft API mapping and release-contract drift telemetry".to_string(),
            note: "Public API contract validation Prometheus exports API mapping readiness, coverage, category drift, and blocker metrics so release dashboards can fail closed on unmapped TiKV or ByteRaft reference vocabulary."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_snapshot_lifecycle_evidence_prometheus".to_string(),
            matrixraft_facade: "MatrixRaftSnapshotLifecyclePrometheus".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore snapshot send/apply progress Prometheus scrape".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft snapshot sender/downloader lifecycle Prometheus evidence"
                    .to_string(),
            note: "Snapshot lifecycle Prometheus turns sender, downloader, retry, timeout, rate-limit, install, rollback, and compacted-log rejoin proof into release-dashboard metrics."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_wal_lifecycle_evidence_prometheus".to_string(),
            matrixraft_facade: "MatrixRaftWalLifecyclePrometheus".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV RaftEngine/WAL segment lifecycle Prometheus scrape".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft WAL segment compaction and slow-fsync Prometheus evidence".to_string(),
            note: "WAL lifecycle Prometheus exposes retained ranges, log-index ranges, released segments, slow fsync, and compaction-after-pressure proof before durability or QPS parity is accepted."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_membership_readiness_prometheus".to_string(),
            matrixraft_facade: "MatrixRaftMembershipReadinessPrometheus".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore joint-consensus and learner/witness readiness scrape"
                    .to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft membership transition readiness Prometheus evidence".to_string(),
            note: "Membership readiness Prometheus keeps failover, scale-up, scale-down, joint quorum, learner catch-up, witness, and scheduler-generation gaps visible in release dashboards."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_production_readiness_input_with_runtime_pressure_evidence"
                .to_string(),
            matrixraft_facade: "MatrixRaftProductionReadinessRuntimePressureEvidence".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release dashboard evidence feeding raftstore flow-control gates".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale QPS, latency, memory, and per-peer pipeline evidence"
                    .to_string(),
            note: "Production-readiness runtime-pressure evidence wires release-scale telemetry into the fail-closed deployment gate instead of requiring callers to hand-assemble admission records."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_input_with_runtime_pressure_read_backlog_and_node_runtime_timer_evidence"
                    .to_string(),
            matrixraft_facade: "MatrixRaftProductionReadinessCompletePressureEvidence"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release dashboard evidence feeding read-index, scheduler, and raftstore flow-control gates"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale QPS, latency, memory, pipeline, read backlog, and timer evidence"
                    .to_string(),
            note: "Timer-aware production-readiness runtime-pressure evidence wires live read backlog and node-runtime timer utilization into the same fail-closed deployment gate as release-scale QPS and latency telemetry."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_production_readiness_input_with_benchmark_artifacts"
                .to_string(),
            matrixraft_facade: "MatrixRaftProductionReadinessBenchmarkInput".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark evidence attached to readiness input".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft diagnostic benchmark artifacts attached to a release gate"
                    .to_string(),
            note: "The diagnostic helper accepts matched benchmark report/summary artifacts and carries their blockers into the production-readiness report so failed QPS, latency, throughput, correctness, CPU, or memory evidence remains explainable."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_production_readiness_input_with_asserted_benchmark_artifacts"
                .to_string(),
            matrixraft_facade: "MatrixRaftAssertedProductionReadinessBenchmarkInput"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark fail-closed readiness input".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft production-clean benchmark artifacts attached to a release gate"
                    .to_string(),
            note: "The asserted helper rejects any matched-but-failing benchmark artifacts before readiness input is trusted as release evidence."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts"
                    .to_string(),
            matrixraft_facade: "MatrixRaftProductionReadinessBenchmarkRuntimePressureEvidence"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark evidence feeding raftstore flow-control gates".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale benchmark artifacts feeding runtime admission"
                    .to_string(),
            note: "Production-readiness benchmark runtime-pressure input validates the benchmark report/summary pair, derives BaselineRaft-backed scale targets, and attaches fail-closed admission evidence in one call."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftAssertedProductionReadinessBenchmarkRuntimePressureEvidence"
                    .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark fail-closed scale and raftstore flow-control evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft production-clean benchmark artifacts feeding runtime admission"
                    .to_string(),
            note: "The asserted runtime-pressure helper rejects matched-but-failing benchmark artifacts before deriving QPS scale targets or accepting runtime pressure evidence."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftProductionReadinessBenchmarkFullPressureEvidence".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, ReadIndex backlog, and raftstore flow-control evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale benchmark artifacts feeding runtime and read backlog admission"
                    .to_string(),
            note: "Read-backlog benchmark runtime-pressure input binds benchmark-derived QPS targets, live read backlog, and fail-closed production admission in one call."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftAssertedProductionReadinessBenchmarkFullPressureEvidence"
                    .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark fail-closed ReadIndex backlog and raftstore flow-control evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft production-clean benchmark artifacts feeding runtime and read backlog admission"
                    .to_string(),
            note: "The asserted read-backlog helper rejects matched-but-failing benchmark artifacts before QPS scale targets, peer-pipeline pressure, or pending-read backlog evidence are trusted."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftProductionReadinessBenchmarkCompletePressureEvidence".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, ReadIndex backlog, scheduler, and raftstore flow-control evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale benchmark artifacts feeding runtime, read backlog, and timer admission"
                    .to_string(),
            note: "Timer-aware benchmark runtime-pressure input binds benchmark-derived QPS targets, live read backlog, node-runtime timer saturation, and fail-closed production admission in one call."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftAssertedProductionReadinessBenchmarkCompletePressureEvidence"
                    .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark fail-closed ReadIndex, scheduler, and flow-control evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft production-clean benchmark artifacts feeding runtime, read backlog, and timer admission"
                    .to_string(),
            note: "The asserted complete-pressure helper rejects matched-but-failing benchmark artifacts before QPS scale targets, read backlog, peer-pipeline pressure, or node-runtime timer evidence are trusted."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_production_readiness_report_with_benchmark_artifacts"
                .to_string(),
            matrixraft_facade: "MatrixRaftProductionReadinessBenchmarkReport".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark readiness report with diagnostic blockers".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft benchmark parity artifacts rendered as a readiness report"
                    .to_string(),
            note: "The diagnostic report helper preserves failing benchmark blockers for dashboards, runbooks, and operator triage."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_production_readiness_report_with_asserted_benchmark_artifacts"
                .to_string(),
            matrixraft_facade: "MatrixRaftAssertedProductionReadinessBenchmarkReport"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark fail-closed readiness report".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft production-clean benchmark parity readiness report".to_string(),
            note: "The asserted report helper is the public release-gate API for callers that require QPS, latency, throughput, correctness, CPU, and memory parity to be proven before a readiness report is built."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts"
                    .to_string(),
            matrixraft_facade: "MatrixRaftProductionReadinessBenchmarkRuntimePressureReport"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark and raftstore flow-control readiness report".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale benchmark artifacts producing a gated readiness report"
                    .to_string(),
            note: "Production-readiness benchmark runtime-pressure reports validate benchmark artifacts, derive scale pressure, attach runtime admission evidence, and run the fail-closed deployment gate in one release call."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_artifacts"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftAssertedProductionReadinessBenchmarkRuntimePressureReport"
                    .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark fail-closed scale and flow-control readiness report"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft production-clean benchmark artifacts producing a runtime-pressure gated readiness report"
                    .to_string(),
            note: "The asserted runtime-pressure report helper is the release automation path when QPS, latency, throughput, correctness, CPU, and memory parity must be proven before scale-target admission is trusted."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
                    .to_string(),
            matrixraft_facade: "MatrixRaftProductionReadinessBenchmarkFullPressureReport"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark and ReadIndex flow-control readiness report".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale benchmark artifacts producing a read backlog gated readiness report"
                    .to_string(),
            note: "Read-backlog benchmark runtime-pressure reports fail closed when benchmark parity is clean but read serving queues are already over budget."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftAssertedProductionReadinessBenchmarkFullPressureReport"
                    .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark fail-closed ReadIndex backlog readiness report"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft production-clean benchmark artifacts producing a read backlog gated readiness report"
                    .to_string(),
            note: "The asserted read-backlog report helper is the release automation path when benchmark parity and live read-serving backlog both need hard-gate semantics."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts"
                    .to_string(),
            matrixraft_facade: "MatrixRaftProductionReadinessBenchmarkCompletePressureReport"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, ReadIndex, scheduler, and flow-control readiness report"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale benchmark artifacts producing a read backlog and timer gated readiness report"
                    .to_string(),
            note: "Timer-aware benchmark runtime-pressure reports fail closed when benchmark parity is clean but read serving queues or node-runtime timer queues are already over budget."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftAssertedProductionReadinessBenchmarkCompletePressureReport"
                    .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark fail-closed ReadIndex and scheduler readiness report"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft production-clean benchmark artifacts producing a read backlog and timer gated readiness report"
                    .to_string(),
            note: "The asserted complete-pressure report helper is the release automation path when benchmark parity, read backlog, and node-runtime timer pressure all need hard-gate semantics."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_release_benchmark_runtime_timer_status".to_string(),
            matrixraft_facade: "MatrixRaftReleaseBenchmarkRuntimeTimerStatus".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark scheduler/timer pressure baseline".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release promotion no-pressure timer evidence fixture".to_string(),
            note: "Release benchmark verifier examples use this canonical timer-status baseline so readiness artifacts cannot drift between producer and consumer."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_benchmark_runtime_pressure_readiness_artifact".to_string(),
            matrixraft_facade: "MatrixRaftBenchmarkRuntimePressureReadinessArtifact".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, raftstore flow-control, Prometheus, and structured logs"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale readiness artifact with metrics and diagnostic logs"
                    .to_string(),
            note: "Benchmark runtime-pressure readiness artifacts package the fail-closed production report, Prometheus readiness metrics, and diagnostic JSON lines so release automation cannot split QPS evidence from operator logs."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
                .to_string(),
            matrixraft_facade: "MatrixRaftBenchmarkFullPressureReadinessArtifact".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, ReadIndex backlog, Prometheus, and structured logs"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale readiness artifact with read backlog metrics and diagnostic logs"
                    .to_string(),
            note: "Read-backlog benchmark readiness artifacts serialize the same fail-closed report, Prometheus text, and diagnostic JSON lines after read backlog pressure is included."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
                    .to_string(),
            matrixraft_facade: "MatrixRaftBenchmarkCompletePressureReadinessArtifact"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, ReadIndex backlog, scheduler pressure, Prometheus, and structured logs"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale readiness artifact with read backlog and timer metrics"
                    .to_string(),
            note: "Timer-aware benchmark readiness artifacts serialize the same fail-closed report, Prometheus text, and diagnostic JSON lines after read backlog and node-runtime timer pressure are included."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact"
                .to_string(),
            matrixraft_facade:
                "MatrixRaftAssertedBenchmarkRuntimePressureReadinessArtifact".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark gate that rejects non-production-clean QPS evidence before dashboard export"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale readiness artifact requiring production-clean benchmark evidence"
                    .to_string(),
            note: "Asserted benchmark runtime-pressure artifacts refuse matched-but-failing benchmark artifacts before Prometheus, Grafana, or diagnostic release payloads are emitted."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftAssertedBenchmarkFullPressureReadinessArtifact".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark and ReadIndex backlog gate that rejects non-production-clean QPS evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale readiness artifact requiring clean benchmark and read-backlog evidence"
                    .to_string(),
            note: "Asserted read-backlog artifacts keep release dashboards from publishing pending-read pressure evidence derived from a failing benchmark comparison."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftAssertedBenchmarkCompletePressureReadinessArtifact".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, ReadIndex backlog, and scheduler pressure gate with production-clean QPS evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale readiness artifact requiring clean benchmark, backlog, and timer evidence"
                    .to_string(),
            note: "Asserted complete-pressure artifacts require production-clean benchmark evidence before read backlog, peer pipeline, and node-runtime timer pressure are exported as release evidence."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact"
                .to_string(),
            matrixraft_facade: "MatrixRaftValidateBenchmarkRuntimePressureReadinessArtifact"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark artifact validation and Prometheus/log consistency checks"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release artifact validation with schema, freshness, metrics, and diagnostic logs"
                    .to_string(),
            note: "Benchmark runtime-pressure readiness artifact validation rejects stale schemas, stale timestamps, report drift, Prometheus drift, and diagnostic JSON-line drift before release automation trusts production-readiness evidence."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftValidateBenchmarkFullPressureReadinessArtifact".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark validation, ReadIndex backlog, Prometheus, and structured log consistency checks"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release artifact validation with read backlog pressure evidence"
                    .to_string(),
            note: "Read-backlog readiness artifact validation recomputes the full-pressure release artifact so promotion checks cannot accidentally compare a read-heavy artifact against the zero-backlog compatibility path."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftValidateBenchmarkCompletePressureReadinessArtifact".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark validation, ReadIndex backlog, scheduler pressure, Prometheus, and structured log consistency checks"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release artifact validation with read backlog and timer pressure evidence"
                    .to_string(),
            note: "Timer-aware readiness artifact validation recomputes the complete release artifact so promotion checks cannot drop node-runtime timer pressure while validating QPS and read-backlog evidence."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftValidateAssertedBenchmarkRuntimePressureReadinessArtifact".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark artifact validation that recomputes from production-clean QPS evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft asserted release artifact validation with schema, freshness, metrics, and diagnostic logs"
                    .to_string(),
            note: "Asserted readiness artifact validation recomputes the release artifact through the production-clean benchmark gate, so a stale or failing benchmark cannot validate an otherwise well-formed artifact."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftValidateAssertedBenchmarkFullPressureReadinessArtifact".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark and ReadIndex backlog artifact validation from production-clean QPS evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft asserted release artifact validation with read-backlog pressure evidence"
                    .to_string(),
            note: "Asserted read-backlog validation rejects artifacts whose source benchmark no longer satisfies production-clean QPS, latency, memory, and correctness gates."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
                    .to_string(),
            matrixraft_facade:
                "MatrixRaftValidateAssertedBenchmarkCompletePressureReadinessArtifact".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, ReadIndex backlog, and scheduler pressure artifact validation from production-clean evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft asserted release artifact validation with read-backlog and timer pressure evidence"
                    .to_string(),
            note: "Asserted complete-pressure validation recomputes the artifact through the strongest release gate before trusting dashboard, Prometheus, or diagnostic payloads."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_debug_snapshot_with_runtime_pressure_evidence".to_string(),
            matrixraft_facade: "MatrixRaftRuntimePressureDebugSnapshot".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release dashboard, structured log, and raftstore flow-control evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft support bundle carrying QPS, latency, memory, and pipeline pressure"
                    .to_string(),
            note: "Runtime-pressure debug snapshots keep admission metrics and diagnostic log entries in the same support bundle used by release-scale QPS and latency triage."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence"
                    .to_string(),
            matrixraft_facade: "MatrixRaftFullRuntimePressureDebugSnapshot".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release dashboard with QPS, read-index, Progress, and flow-control evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft support bundle carrying QPS, latency, memory, pipeline, and read backlog pressure"
                    .to_string(),
            note: "Read-backlog-aware debug snapshots keep release-scale admission evidence from dropping pending ReadIndex or bounded-stale read pressure."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_debug_snapshot_with_runtime_pressure_read_backlog_and_node_runtime_timer_evidence"
                    .to_string(),
            matrixraft_facade: "MatrixRaftCompleteRuntimePressureDebugSnapshot"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release dashboard with QPS, read-index, scheduler, Progress, and flow-control evidence"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft support bundle carrying QPS, latency, memory, pipeline, read backlog, and timer pressure"
                    .to_string(),
            note: "Timer-aware debug snapshots keep release-scale admission evidence from dropping node-runtime timer saturation beside QPS, pipeline, and read-backlog pressure."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts"
                .to_string(),
            matrixraft_facade: "MatrixRaftBenchmarkRuntimePressureSupportBundle".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, raftstore flow-control, dashboard, and structured log bundle"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale benchmark artifacts plus runtime pressure evidence"
                    .to_string(),
            note: "Benchmark runtime-pressure support bundles combine BaselineRaft-derived QPS targets, benchmark parity metrics, runtime admission metrics, and diagnostic logs in one release artifact."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
                    .to_string(),
            matrixraft_facade: "MatrixRaftBenchmarkFullPressureSupportBundle".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, ReadIndex backlog, dashboard, and structured log bundle"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale benchmark artifacts plus runtime and read backlog pressure evidence"
                    .to_string(),
            note: "Read-backlog benchmark support bundles keep benchmark parity, QPS targets, pending read queues, admission metrics, and diagnostics together."
                .to_string(),
        },
        ApiNameMapping {
            canonical:
                "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts"
                    .to_string(),
            matrixraft_facade: "MatrixRaftBenchmarkCompletePressureSupportBundle"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV release benchmark, ReadIndex backlog, scheduler pressure, dashboard, and structured log bundle"
                    .to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-scale benchmark artifacts plus runtime, read backlog, and timer pressure evidence"
                    .to_string(),
            note: "Timer-aware benchmark support bundles keep benchmark parity, QPS targets, pending read queues, timer utilization, admission metrics, and diagnostics together."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "RuntimePressureAdmissionPolicy".to_string(),
            matrixraft_facade: "MatrixRaftRuntimePressurePolicy".to_string(),
            raft_rs_or_tikv_reference: "raftstore flow-control policy".to_string(),
            byteraft_or_baseline_reference: "admission policy / throttle mode".to_string(),
            note: "RuntimePressureAdmissionPolicy pins observe-only, reject, and throttle behavior to a stable operator-facing name."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_parity_report".to_string(),
            matrixraft_facade: "MatrixRaftParityReport".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore semantic readiness report".to_string(),
            byteraft_or_baseline_reference: "BaselineRaft compatibility and release gate report"
                .to_string(),
            note: "The parity report is the canonical API for checking semantic readiness before RustRaft claims production compatibility."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_baseline_raft_parity_matrix".to_string(),
            matrixraft_facade: "MatrixRaftBaselineParityMatrix".to_string(),
            raft_rs_or_tikv_reference: "raft-rs/TiKV feature parity matrix".to_string(),
            byteraft_or_baseline_reference: "ByteRaft/BaselineRaft parity matrix"
                .to_string(),
            note: "The matrix makes compatibility, intentional differences, and remaining production gaps auditable by release reviewers."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "BenchmarkRunner".to_string(),
            matrixraft_facade: "MatrixRaftBenchmarkRunner".to_string(),
            raft_rs_or_tikv_reference: "raftstore benchmark harness".to_string(),
            byteraft_or_baseline_reference: "release QPS/latency parity runner".to_string(),
            note: "BenchmarkRunner is the stable release-parity harness for C++ baseline comparisons."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_grafana_dashboard".to_string(),
            matrixraft_facade: "MatrixRaftGrafanaDashboard".to_string(),
            raft_rs_or_tikv_reference: "TiKV Grafana raftstore dashboard".to_string(),
            byteraft_or_baseline_reference: "ByteRaft operational dashboard".to_string(),
            note: "The dashboard export keeps QPS, latency, memory, WAL, snapshot, and peer-pipeline panels reviewable."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_grafana_dashboard_json".to_string(),
            matrixraft_facade: "MatrixRaftGrafanaDashboardJson".to_string(),
            raft_rs_or_tikv_reference: "TiKV Grafana dashboard JSON provisioning".to_string(),
            byteraft_or_baseline_reference: "ByteRaft dashboard JSON import".to_string(),
            note: "The JSON export pins the exact dashboard artifact consumed by Grafana provisioning and release review."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_alert_rules".to_string(),
            matrixraft_facade: "MatrixRaftAlertRules".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore alert rules".to_string(),
            byteraft_or_baseline_reference: "ByteRaft release and runtime alerts".to_string(),
            note: "Alert rules are mapped so production readiness gates remain tied to reference operational vocabulary."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_alert_rules_json".to_string(),
            matrixraft_facade: "MatrixRaftAlertRulesJson".to_string(),
            raft_rs_or_tikv_reference: "TiKV alertmanager rule JSON provisioning".to_string(),
            byteraft_or_baseline_reference: "ByteRaft alert rule JSON import".to_string(),
            note: "The JSON export keeps alert provisioning tied to the same readiness, QPS, latency, memory, WAL, and snapshot thresholds as the typed rules."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_observability_provisioning".to_string(),
            matrixraft_facade: "MatrixRaftObservabilityProvisioning".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore observability bundle".to_string(),
            byteraft_or_baseline_reference: "ByteRaft production observability bundle"
                .to_string(),
            note: "The provisioning bundle groups dashboard, alert, debug-artifact, Prometheus, and runbook contracts for production rollout."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_observability_provisioning_runbook_steps".to_string(),
            matrixraft_facade: "MatrixRaftObservabilityProvisioningRunbook".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore observability provisioning runbook checklist".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft release-observability runbook checklist".to_string(),
            note: "The provisioning runbook helper gives CI and operators the static remediation checklist that must accompany alert, dashboard, metric, debug bundle, runtime-pressure, and benchmark freshness provisioning."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_observability_required_metric_names".to_string(),
            matrixraft_facade: "MatrixRaftObservabilityRequiredMetricNames".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore required metric catalog".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft production benchmark and runtime metric catalog".to_string(),
            note: "The flattened catalog gives CI, Grafana provisioning, and release automation one stable list covering readiness, QPS, latency, memory, lifecycle, runtime-pressure, and BaselineRaft parity metrics."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_validate_required_metric_scrape_texts".to_string(),
            matrixraft_facade: "MatrixRaftValidateRequiredMetricScrapeTexts".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore Prometheus scrape contract validation"
                .to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft production metric scrape completeness gate".to_string(),
            note: "The scrape validator compares emitted Prometheus payloads with the required QPS, latency, memory, runtime-pressure, lifecycle, and benchmark metric catalog before release evidence is trusted."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_observability_provisioning_json".to_string(),
            matrixraft_facade: "MatrixRaftObservabilityProvisioningJson".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore observability bundle JSON"
                .to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft production observability bundle JSON import".to_string(),
            note: "The JSON export is the stable artifact for CI and deployment systems that cannot link the Rust types directly."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_validate_observability_provisioning".to_string(),
            matrixraft_facade: "MatrixRaftValidateObservabilityProvisioning".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore dashboard and alert provisioning validation".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft operational provisioning drift validation".to_string(),
            note: "The validator fails closed when dashboard panels, alert expressions, required metrics, or runbook artifacts drift from the production contract."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_validate_observability_provisioning_json".to_string(),
            matrixraft_facade: "MatrixRaftValidateObservabilityProvisioningJson".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore JSON observability provisioning validation".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft JSON provisioning drift validation".to_string(),
            note: "The JSON validator lets CI check imported Grafana, alert, metric, and runbook bundles before release promotion."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_observability_provisioning_validation_prometheus".to_string(),
            matrixraft_facade: "MatrixRaftObservabilityProvisioningValidationPrometheus"
                .to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore provisioning validation Prometheus metric".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft provisioning validation readiness metric".to_string(),
            note: "The validation Prometheus export makes provisioning drift visible beside runtime readiness, QPS, latency, memory, and WAL panels."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_operator_runbook_steps_with_diagnostics".to_string(),
            matrixraft_facade: "MatrixRaftDiagnosticOperatorRunbook".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore diagnostic runbook fed by structured logs and alerts".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft diagnostic runbook with production-readiness blockers".to_string(),
            note: "The diagnostic runbook helper joins structured diagnostic targets, production-readiness evidence, and alert rules so release support gets the first corrective action instead of a raw blocker list."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_operator_runbook_prometheus".to_string(),
            matrixraft_facade: "MatrixRaftOperatorRunbookPrometheus".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore runbook step Prometheus scrape for Grafana triage".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft runbook-step telemetry for release dashboards".to_string(),
            note: "The Prometheus exporter turns active operator steps into grouped step totals, per-step presence, and first-step signals that dashboards can route without parsing JSON."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "examples/readiness_report.rs".to_string(),
            matrixraft_facade: "MatrixRaftReadinessReportExample".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore release-readiness and status-report example".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft production readiness example wiring".to_string(),
            note: "The example is part of the open-source embedding contract for constructing readiness evidence without TemporalStore adapter code."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "examples/read_safety.rs".to_string(),
            matrixraft_facade: "MatrixRaftReadSafetyExample".to_string(),
            raft_rs_or_tikv_reference: "TiKV ReadIndex and lease-read example".to_string(),
            byteraft_or_baseline_reference: "ByteRaft safe-read example flow".to_string(),
            note: "The example maps quorum ReadIndex, lease-read, and bounded-stale follower-read checks to the canonical RustRaft read-safety helpers."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "examples/debug_artifacts.rs".to_string(),
            matrixraft_facade: "MatrixRaftDebugArtifactsExample".to_string(),
            raft_rs_or_tikv_reference: "TiKV raftstore debug bundle and Prometheus example"
                .to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft support bundle and dashboard artifact example".to_string(),
            note: "The example keeps debug snapshot, Prometheus, Grafana, alert, and diagnostic-log rendering discoverable for release triage."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "examples/baseline_raft_parity_benchmark.rs".to_string(),
            matrixraft_facade: "MatrixRaftBaselineRaftParityBenchmarkExample".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV raftstore benchmark and pressure-snapshot example".to_string(),
            byteraft_or_baseline_reference:
                "BaselineRaft-vs-RustRaft parity benchmark example".to_string(),
            note: "The example shows the release-scale QPS, latency, throughput, CPU, and memory parity path expected before production claims."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "examples/open_source_surface.rs".to_string(),
            matrixraft_facade: "MatrixRaftOpenSourceSurfaceExample".to_string(),
            raft_rs_or_tikv_reference:
                "TiKV-style public raftstore module surface example".to_string(),
            byteraft_or_baseline_reference:
                "ByteRaft-compatible standalone crate surface example".to_string(),
            note: "The example proves the standalone module, adapter-boundary, parity-matrix, and compatibility-report surface for open-source consumers."
                .to_string(),
        },
        ApiNameMapping {
            canonical: "matrixraft_operator_runbook_steps".to_string(),
            matrixraft_facade: "MatrixRaftOperatorRunbook".to_string(),
            raft_rs_or_tikv_reference: "TiKV operator runbook / raftstore triage".to_string(),
            byteraft_or_baseline_reference: "ByteRaft operational triage".to_string(),
            note: "Runbook steps give every alert a deterministic remediation route for production support."
                .to_string(),
        },
    ]
}

pub fn matrixraft_core_interface_names() -> Vec<String> {
    [
        "Config",
        "SnapshotMetadata",
        "PeerProgress",
        "NodeRuntime",
        "RuntimeTimerStatus",
        "RuntimeAdminReport",
        "AdminCommand::ReleaseMemory",
        "MailBox",
        "MailBox::try_send_checked",
        "MailBox::fetch_checked",
        "MailChannel",
        "MailChannel::try_send_checked",
        "ChannelSelector",
        "ChannelSelector::select_checked",
        "PersistentRaftWal",
        "DebugSnapshot",
        "DiagnosticLogEntry",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn matrixraft_public_module_names() -> Vec<String> {
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
        "mailbox",
        "channel_selector",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn matrixraft_embedding_examples() -> Vec<String> {
    [
        "examples/readiness_report.rs",
        "examples/read_safety.rs",
        "examples/debug_artifacts.rs",
        "examples/baseline_raft_parity_benchmark.rs",
        "examples/open_source_surface.rs",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn matrixraft_parity_report_names() -> Vec<String> {
    [
        "matrixraft_parity_report",
        "matrixraft_baseline_raft_parity_matrix",
        "matrixraft_baseline_raft_parity_surface",
        "matrixraft_baseline_raft_reference_policy",
        "matrixraft_durability_parity_report",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn matrixraft_benchmark_interface_names() -> Vec<String> {
    [
        "BenchmarkRunner",
        "ExternalBaselineRaftRunner",
        "RuntimeBenchmarkRunner",
        "BenchmarkOptions",
        "BenchmarkReport",
        "ReleasePressureSnapshot",
        "matrixraft_release_pressure_snapshot_json",
        "matrixraft_release_pressure_snapshot_from_json",
        "matrixraft_release_pressure_snapshot_from_json_bytes",
        "matrixraft_validate_release_pressure_snapshot",
        "matrixraft_read_release_pressure_snapshot",
        "matrixraft_write_release_pressure_snapshot_atomic",
        "matrixraft_baseline_raft_benchmark_workloads",
        "matrixraft_run_baseline_raft_parity_benchmark",
        "matrixraft_assert_baseline_raft_parity",
        "matrixraft_assert_production_baseline_raft_parity",
        "matrixraft_baseline_raft_benchmark_evidence",
        "matrixraft_baseline_raft_benchmark_grafana_panels",
        "matrixraft_baseline_raft_benchmark_metric_names",
        "matrixraft_baseline_raft_benchmark_summary_prometheus",
        "matrixraft_benchmark_runbook_steps",
        "matrixraft_scale_rate_metrics_from_benchmark_report",
        "matrixraft_scale_optimization_targets_from_baseline_raft_report",
        "matrixraft_scale_optimization_inputs_from_benchmark_report",
        "matrixraft_validate_benchmark_scale_optimization_inputs",
        "matrixraft_release_benchmark_runtime_timer_status",
        "matrixraft_benchmark_runtime_pressure_readiness_artifact",
        "matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog",
        "matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer",
        "matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact",
        "matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog",
        "matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer",
        "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact",
        "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog",
        "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer",
        "matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact",
        "matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog",
        "matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer",
        "matrixraft_debug_snapshot_with_benchmark_scale_inputs",
        "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts",
        "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "matrixraft_production_readiness_input_with_benchmark_artifacts",
        "matrixraft_production_readiness_input_with_asserted_benchmark_artifacts",
        "matrixraft_production_readiness_report_with_benchmark_artifacts",
        "matrixraft_production_readiness_report_with_asserted_benchmark_artifacts",
        "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts",
        "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts",
        "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts",
        "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_artifacts",
        "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "matrixraft_production_readiness_input_with_runtime_pressure_evidence",
        "matrixraft_production_readiness_input_with_runtime_pressure_read_backlog_and_node_runtime_timer_evidence",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn matrixraft_observability_interface_names() -> Vec<String> {
    [
        "matrixraft_metric_names",
        "matrixraft_scale_metric_names",
        "matrixraft_scale_metrics_prometheus",
        "matrixraft_scale_target_metric_names",
        "matrixraft_scale_target_metrics_prometheus",
        "matrixraft_scale_grafana_panels",
        "matrixraft_scale_target_grafana_panels",
        "matrixraft_memory_metric_names",
        "matrixraft_memory_metrics_prometheus",
        "matrixraft_memory_grafana_panels",
        "matrixraft_latency_metrics_prometheus",
        "matrixraft_runtime_pressure_admission",
        "matrixraft_runtime_pressure_admission_with_scale_targets",
        "matrixraft_runtime_pressure_admission_with_pipeline_pressure",
        "matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure",
        "matrixraft_runtime_pressure_admission_with_scale_and_pipeline_pressure",
        "matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure",
        "matrixraft_runtime_pressure_admission_with_scale_pipeline_read_backlog_and_node_runtime_timer_pressure",
        "matrixraft_runtime_pressure_bottleneck_summary",
        "matrixraft_runtime_pressure_freshness_report",
        "matrixraft_runtime_pressure_freshness_prometheus",
        "matrixraft_runtime_pressure_freshness_diagnostic_log_entries",
        "matrixraft_runtime_pressure_freshness_diagnostic_json_lines",
        "matrixraft_public_api_contract_validation_prometheus",
        "matrixraft_runtime_pressure_admission_prometheus",
        "matrixraft_validate_runtime_pressure_admission_evidence",
        "matrixraft_validate_runtime_pressure_admission_evidence_with_policy",
        "matrixraft_runtime_pressure_metric_names",
        "matrixraft_runtime_pressure_grafana_panels",
        "matrixraft_public_api_contract_validation_grafana_panels",
        "matrixraft_debug_snapshot_with_runtime_pressure_evidence",
        "matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence",
        "matrixraft_debug_snapshot_with_runtime_pressure_read_backlog_and_node_runtime_timer_evidence",
        "matrixraft_node_runtime_status_prometheus",
        "matrixraft_node_runtime_grafana_panels",
        "RuntimePressureAdmission",
        "LatencyPressureDetail",
        "NodeRuntimeTimerPressureDetail",
        "NodeRuntimeTimerThresholds",
        "RuntimePressureAdmissionPolicy",
        "RuntimePressureMetricNames",
        "SnapshotLifecycleEvidence",
        "matrixraft_snapshot_lifecycle_metric_names",
        "matrixraft_snapshot_lifecycle_evidence_prometheus",
        "matrixraft_snapshot_lifecycle_grafana_panels",
        "SnapshotLifecycleMetricNames",
        "matrixraft_wal_lifecycle_metric_names",
        "matrixraft_wal_lifecycle_evidence_prometheus",
        "matrixraft_wal_lifecycle_grafana_panels",
        "WalLifecycleMetricNames",
        "matrixraft_membership_readiness_metric_names",
        "matrixraft_membership_readiness_prometheus",
        "matrixraft_membership_readiness_grafana_panels",
        "MembershipReadinessMetricNames",
        "matrixraft_production_readiness_metric_names",
        "matrixraft_production_readiness_grafana_panels",
        "ProductionReadinessMetricNames",
        "matrixraft_grafana_dashboard",
        "matrixraft_grafana_dashboard_json",
        "matrixraft_alert_rules",
        "matrixraft_alert_rules_json",
        "matrixraft_debug_snapshot_with_benchmark_summary",
        "matrixraft_debug_snapshot_with_benchmark_artifacts",
        "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts",
        "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts",
        "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts",
        "matrixraft_observability_provisioning",
        "matrixraft_observability_provisioning_json",
        "matrixraft_observability_provisioning_runbook_steps",
        "matrixraft_observability_required_metric_names",
        "matrixraft_validate_required_metric_scrape_texts",
        "matrixraft_validate_observability_provisioning",
        "matrixraft_validate_observability_provisioning_json",
        "matrixraft_observability_provisioning_validation_prometheus",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn matrixraft_diagnostic_interface_names() -> Vec<String> {
    [
        "matrixraft_admin_diagnostic_log_entries",
        "matrixraft_admin_diagnostic_json_lines",
        "matrixraft_local_status_diagnostic_log_entries",
        "matrixraft_local_status_diagnostic_json_lines",
        "matrixraft_node_runtime_status_diagnostic_log_entries",
        "matrixraft_node_runtime_status_diagnostic_json_lines",
        "matrixraft_diagnostic_log_prometheus",
        "matrixraft_optimization_report",
        "matrixraft_optimization_report_prometheus",
        "matrixraft_optimization_diagnostic_log_entries",
        "matrixraft_optimization_diagnostic_json_lines",
        "matrixraft_runtime_pressure_diagnostic_log_entries",
        "matrixraft_runtime_pressure_diagnostic_json_lines",
        "matrixraft_runtime_pressure_freshness_diagnostic_log_entries",
        "matrixraft_runtime_pressure_freshness_diagnostic_json_lines",
        "matrixraft_membership_readiness_diagnostic_log_entries",
        "matrixraft_membership_readiness_diagnostic_json_lines",
        "matrixraft_production_readiness_diagnostic_log_entries",
        "matrixraft_production_readiness_diagnostic_json_lines",
        "matrixraft_scale_optimization_hints",
        "matrixraft_memory_optimization_hints",
        "matrixraft_operator_triage_summary",
        "matrixraft_operator_triage_prometheus",
        "matrixraft_operator_runbook_steps",
        "matrixraft_operator_runbook_steps_with_diagnostics",
        "matrixraft_operator_runbook_prometheus",
        "matrixraft_debug_bundle_contract",
        "matrixraft_debug_snapshot_json",
        "matrixraft_debug_snapshot_with_scale_metrics",
        "matrixraft_debug_snapshot_with_runtime_metrics",
        "matrixraft_debug_snapshot_with_observability_metrics",
        "matrixraft_debug_snapshot_with_performance_targets",
        "matrixraft_validate_debug_snapshot",
        "matrixraft_validate_debug_snapshot_json",
        "matrixraft_debug_bundle_validation_prometheus",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn matrixraft_evidence_interface_names() -> Vec<String> {
    [
        "PipelineEvidence",
        "PipelineEvidence::packet_loss_reorder_faulted_peer_count",
        "PipelineEvidence::packet_loss_reorder_recovered_peer_count",
        "PipelineEvidence::packet_loss_reorder_all_faulted_peers_recovered",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn matrixraft_compatibility_report_names() -> Vec<String> {
    [
        "matrixraft_public_api_contract",
        "matrixraft_validate_public_api_contract",
        "matrixraft_api_name_mappings",
        "matrixraft_standalone_readiness_report",
        "matrixraft_production_readiness_report",
        "matrixraft_production_readiness_report_with_runtime_pressure_policy",
        "matrixraft_production_readiness_report_with_runtime_pressure_policy_and_freshness",
        "matrixraft_production_readiness_report_prometheus",
        "matrixraft_data_node_process_rollout_readiness_report",
        "matrixraft_meta_process_rollout_readiness_report",
        "matrixraft_baseline_raft_runtime_capability_report",
        "matrixraft_runtime_local_status_report",
        "matrixraft_runtime_admin_report",
        "matrixraft_fatal_blocker_report",
        "matrixraft_baseline_raft_runtime_capability_prometheus",
        "matrixraft_temporalstore_extraction_plan",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn matrixraft_standalone_readiness_report() -> StandaloneReadinessReport {
    let capabilities = matrixraft_standalone_capabilities();
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

    StandaloneReadinessReport {
        standalone,
        production_status: if standalone {
            ProductionStatus::ProductionReady
        } else {
            ProductionStatus::Blocked
        },
        capabilities,
        missing,
        evidence,
    }
}

fn matrixraft_standalone_capabilities() -> Vec<StandaloneCapability> {
    vec![
        standalone_capability(
            "node_lifecycle",
            &[
                "node::NodeRuntime exposes start, stop, restart, and shutdown",
                "cluster::Consensus exposes start, stop, propose, campaign, and transfer_leader",
            ],
        ),
        standalone_capability(
            "replication",
            &[
                "cluster::RaftCluster::propose appends opaque payload entries",
                "transport::AppendEntriesRequest and AppendEntriesResponse define the replication RPC",
                "ReplicationPipeline tracks inflight entries, backoff, reorder, and lag status",
                "ByteQuotaLimiter provides BaselineRaft-style byte quota gating for snapshot and replication transfer",
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
                "membership::MembershipExecutor owns add learner, auto-promote learner, promote, witness, remove, and joint consensus operations",
                "Membership and JointConsensusMembership model voter, learner, and witness roles",
            ],
        ),
        standalone_capability(
            "wal_recovery",
            &[
                "wal::LocalRaftWal and PersistentRaftWalOptions provide segmented WAL persistence",
                "matrixraft_recover_latest_wal_record validates checksums and corrupt-tail truncation",
                "HardState and WalRecord preserve term, vote, commit, and log records",
            ],
        ),
        standalone_capability(
            "snapshots",
            &[
                "snapshot::SnapshotLifecycle chunks, retries, quota-throttles, and installs snapshots",
                "PersistentRaftSnapshotStore persists checkpoints and reloads snapshot payloads",
                "ApplySnapshotFence validates snapshot floor and tail catch-up safety",
            ],
        ),
        standalone_capability(
            "read_index_lease_read",
            &[
                "cluster::RaftCluster::read_index enforces quorum read-index safety",
                "RaftCluster::lease_read_eligible rejects stale leaders and unapplied reads",
                "ReadIndexRequest and ReadIndexResponse expose the public read path",
            ],
        ),
        standalone_capability(
            "status_metrics_readiness",
            &[
                "status::StatusSnapshot and cluster status reports expose runtime state",
                "metrics::matrixraft_metric_names names replication, WAL, snapshot, read, and blocker metrics",
                "readiness reports cover BaselineRaft parity, production gates, and fatal blockers",
            ],
        ),
    ]
}

fn standalone_capability(id: &str, evidence: &[&str]) -> StandaloneCapability {
    StandaloneCapability {
        id: id.to_string(),
        ready: !evidence.is_empty(),
        evidence: evidence.iter().map(|item| item.to_string()).collect(),
        missing: Vec::new(),
    }
}

pub fn matrixraft_open_source_surface() -> OpenSourceSurface {
    OpenSourceSurface {
        crate_name: "rustraft".to_string(),
        public_modules: matrixraft_public_module_names(),
        embedding_docs: vec!["README.md".to_string(), "docs/gap_plan.md".to_string()],
        embedding_examples: matrixraft_embedding_examples(),
        baseline_raft_parity_matrix: matrixraft_baseline_raft_parity_matrix(&ReadinessSnapshot {
            matrixraft_leader_write_authority_present: true,
            matrixraft_operator_observability_present: true,
            matrixraft_rpc_transport_contract_present: true,
            matrixraft_log_retention_snapshot_trigger_present: true,
            matrixraft_apply_snapshot_fence_present: true,
            raft_storage_apply_fence_present: true,
            matrixraft_snapshot_floor_log_matching_present: true,
            matrixraft_snapshot_tail_catchup_present: true,
            matrixraft_compacted_entry_rejection_present: true,
            matrixraft_metaserver_snapshot_floor_election_present: true,
            learner_catchup_promotion_present: true,
            metaserver_membership_workflow_present: true,
        })
        .into_iter()
        .map(|item| item.id)
        .collect(),
        benchmark_harness_interface: matrixraft_benchmark_interface_names(),
        compatibility_reports: matrixraft_compatibility_report_names(),
        matrixraft_owned: vec![
            "public Raft modules and generic types".to_string(),
            "BaselineRaft parity matrix and readiness reports".to_string(),
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

pub fn matrixraft_temporalstore_adapter_shape() -> TemporalStoreAdapterShape {
    TemporalStoreAdapterShape {
        backend_type: "TemporalRaftConsensusBackend".to_string(),
        node_field: "node".to_string(),
        node_runtime_type:
            "matrixraft::node::NodeRuntime<TemporalStoreStateMachine, TemporalTransport>"
                .to_string(),
        state_machine_type_parameter: "TemporalStoreStateMachine".to_string(),
        transport_type_parameter: "TemporalTransport".to_string(),
        codec_field: "codec: TemporalCommandCodec".to_string(),
        engine_field: "engine: TemporalEngine".to_string(),
        matrixraft_owned: vec![
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
            "    node: matrixraft::node::NodeRuntime<TemporalStoreStateMachine, TemporalTransport>,",
            "    codec: TemporalCommandCodec,",
            "    engine: TemporalEngine,",
            "}",
        ]
        .join("\n"),
    }
}

pub fn matrixraft_temporalstore_extraction_plan() -> TemporalStoreExtractionPlan {
    TemporalStoreExtractionPlan {
        policy: "RustRaft owns reusable consensus contracts, safety decisions, membership state, WAL/snapshot models, transport/storage traits, pipeline metrics, and deterministic harness logic; TemporalStore keeps only command codecs, process startup, shard FSM adapters, and storage-engine integration.".to_string(),
        slices: vec![
            ExtractionSlice {
                id: "read_safety".to_string(),
                status: ExtractionStatus::InLibrary,
                matrixraft_owner: "read-index, lease-read, bounded-stale, lagging-follower, stale-leader, and minority-partition decisions".to_string(),
                temporalstore_boundary: "translate data-node and metaserver runtime status into RustRaft read-safety inputs".to_string(),
                next_evidence: "multi-process TemporalStore harness must attach observed read-index and lease responses".to_string(),
            },
            ExtractionSlice {
                id: "membership_workflow".to_string(),
                status: ExtractionStatus::InLibrary,
                matrixraft_owner: "learner add/catch-up/promote, voter add/remove, witness add, leader transfer validation, rollback reports, and joint consensus summaries".to_string(),
                temporalstore_boundary: "metaserver scheduler invokes RustRaft workflow and applies accepted operations through data-node process APIs".to_string(),
                next_evidence: "scheduler-owned data-node membership report with stale-token rejection and restart replay".to_string(),
            },
            ExtractionSlice {
                id: "wal_snapshot_models".to_string(),
                status: ExtractionStatus::InLibrary,
                matrixraft_owner: "hard state, WAL records, segment status, snapshot metadata, apply snapshot fences, and snapshot lifecycle reports".to_string(),
                temporalstore_boundary: "persist records in TemporalStore-owned directories and bind apply fences to storage mutations".to_string(),
                next_evidence: "crash between WAL persistence, storage mutation, and snapshot install recovers deterministically".to_string(),
            },
            ExtractionSlice {
                id: "transport_storage_traits".to_string(),
                status: ExtractionStatus::InLibrary,
                matrixraft_owner: "generic storage and transport traits plus AppendEntries, Vote, PreVote, InstallSnapshot, snapshot chunk, and ReadIndex messages".to_string(),
                temporalstore_boundary: "HTTP/tonic/process adapters implement the traits without leaking TemporalStore command types into RustRaft".to_string(),
                next_evidence: "data-node and metaserver process paths consume trait adapters in scale/failover harnesses".to_string(),
            },
            ExtractionSlice {
                id: "fault_harness_contract".to_string(),
                status: ExtractionStatus::InLibrary,
                matrixraft_owner: "BaselineRaft-derived fault scenario catalog and readiness report for process-path evidence".to_string(),
                temporalstore_boundary: "TemporalStore process harnesses run the real data-node and metaserver binaries and feed observed evidence into RustRaft reports".to_string(),
                next_evidence: "packet loss, slow WAL, snapshot during membership, leader transfer under load, compacted-log rejoin, and rolling restart reports all pass".to_string(),
            },
            ExtractionSlice {
                id: "replication_pipeline_runtime".to_string(),
                status: ExtractionStatus::PendingMigration,
                matrixraft_owner: "inflight limits, append/apply queue limits, max replicate bytes, oversized-log rejection, reorder queue, and pressure counters".to_string(),
                temporalstore_boundary: "runtime should feed per-peer process observations into RustRaft pipeline evidence".to_string(),
                next_evidence: "BaselineRaft-derived packet-loss, out-of-order append, slow WAL, and pressure tests pass through process harnesses".to_string(),
            },
            ExtractionSlice {
                id: "domain_fsm_adapters".to_string(),
                status: ExtractionStatus::AdapterOnly,
                matrixraft_owner: "opaque bytes/state-machine trait contracts only".to_string(),
                temporalstore_boundary: "TemporalStore owns data-shard commands, metaserver mutations, object/block storage, and admin surfaces".to_string(),
                next_evidence: "integration tests prove adapters implement RustRaft traits without moving domain codecs into the library".to_string(),
            },
        ],
    }
}
