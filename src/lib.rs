// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![forbid(unsafe_code)]
// Lint configuration (the `type_complexity` / `too_many_arguments` allows and the
// rustdoc broken-intra-doc-links deny) lives in the `[lints]` table of Cargo.toml.
//! RustRaft is the TemporalStore-owned Raft contract and readiness library.
//!
//! The crate intentionally focuses on portable consensus-facing contracts:
//! request/response types, storage and transport traits, safety decisions,
//! metrics names, and fail-closed production readiness reports. It does not run
//! the TemporalStore data-node or metaserver by itself. Those runtimes consume
//! this crate and attach live evidence for pipeline, WAL, snapshot, membership,
//! failover, and process-rollout behavior.
//!
//! Typical integration flow:
//!
//! 1. Build a [`RustRaftReadinessSnapshot`] from the serving runtime.
//! 2. Call [`rustraft_parity_report`] for semantic contract readiness.
//! 3. Attach live runtime evidence to [`RustRaftProductionReadinessInput`].
//! 4. Call [`rustraft_production_readiness_report`] and block production claims
//!    unless the report is ready.
//!
//! The public API is OpenRaft-free by design. Compatibility with existing
//! TemporalStore deployment semantics is expressed through RustRaft-owned types
//! and tests instead of upstream-specific type aliases.
//! BaselineRaft remains the feature and performance reference; RustRaft may expose
//! more idiomatic Rust traits and error types as long as TemporalStore consumes
//! it through a stable adapter boundary.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};
use thiserror::Error;

pub mod benchmark;
pub mod channel_selector;
pub mod checksum;
pub mod cluster;
pub mod config;
pub mod durability;
pub mod fault;
pub mod fsm;
pub mod heartbeat_merge;
pub mod lease;
pub mod log_buffer;
pub mod mailbox;
pub mod membership;
pub mod metrics;
pub mod node;
pub mod operational_evidence;
pub mod pipeline;
pub mod rate_limit;
pub mod read_safety;
pub mod readiness;
pub mod scheduler;
pub mod snapshot;
pub mod status;
pub mod storage;
pub mod transport;
pub mod unique_id;
pub mod wal;

pub use benchmark::{
    rustraft_production_readiness_input_with_benchmark_artifacts,
    rustraft_production_readiness_input_with_benchmark_summary,
    rustraft_production_readiness_report_with_benchmark_artifacts,
    RustRaftBaselineRaftBenchmarkEvidence,
};
pub use channel_selector::{
    RustRaftChannelSelection, RustRaftChannelSelector, RustRaftChannelSelectorPolicy,
    RustRaftMailChannel, RUSTRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS,
};
pub use checksum::{
    rustraft_checksum_file_list, rustraft_crc32c, rustraft_murmur32, RustRaftChecksumContext,
    RustRaftChecksumResult, RustRaftChecksumType, RustRaftFileChecksumContext,
    RustRaftFileChecksumResult,
};
pub use config::{RaftConfig, RaftConfigError, RustRaftConfig};
pub use durability::rustraft_durability_parity_report;
pub use fsm::{
    matrixraft_flexible_apply_with_store, matrixraft_flexible_apply_with_store_report,
    rustraft_apply_entry, rustraft_fsm_entry_kind, MatrixRaftBatchId, MatrixRaftCheckpoint,
    MatrixRaftConfigurationApplied, MatrixRaftFlexibleApplyReport, MatrixRaftFsm,
    MatrixRaftFsmEntry, MatrixRaftFsmEntryKind, MatrixRaftFsmIterator, MatrixRaftFsmRuntimeBinding,
    MatrixRaftFsmRuntimeHookReport, MatrixRaftStoreFsm, RaftApply, RaftFsmAdapter,
    RaftFsmApplyEntryKind, RaftFsmApplyOutcome, RaftFsmBatchApplyReport, RaftFsmCheckpoint,
    RaftFsmReplayReport, RaftStateMachine, RustRaftStateMachine, MATRIXRAFT_NON_BATCH,
};
pub use heartbeat_merge::{
    RustRaftHeartbeatAddressResolver, RustRaftHeartbeatMergeMessage, RustRaftHeartbeatMergeStats,
    RustRaftHeartbeatMerger, RustRaftMergedHeartbeatBatch, RUSTRAFT_HEARTBEAT_MERGE_BUCKETS,
};
pub use lease::{
    RustRaftFollowerLease, RustRaftLeaderLease, RustRaftLeaderLeaseStatus, RustRaftLeaseEpochId,
    RustRaftLeasePeer,
};
pub use log_buffer::{RustRaftLogBuffer, RustRaftLogBufferFlush, RustRaftLogBufferRelease};
pub use mailbox::{
    RustRaftMailBox, RustRaftMailBoxFetchPolicy, RustRaftMailPriority,
    RUSTRAFT_MAILBOX_MAX_TIMEOUT_MS,
};
pub use membership::{
    rustraft_learner_promotion_decision, rustraft_membership_readiness_report,
    rustraft_membership_semantics_evidence_artifact, rustraft_membership_transition_missing,
    rustraft_validate_membership_semantics_evidence_artifact, RaftLearnerAutoPromoteReport,
    RaftLearnerAutoPromoteState, RaftLearnerCatchUpLoopReport, RaftWitnessQuorumReport,
};
pub use metrics::{
    rustraft_alert_rules, rustraft_alert_rules_json, rustraft_debug_bundle_contract,
    rustraft_debug_bundle_validation_prometheus, rustraft_debug_snapshot,
    rustraft_debug_snapshot_json, rustraft_debug_snapshot_metadata_prometheus,
    rustraft_diagnostic_log_prometheus, rustraft_grafana_dashboard,
    rustraft_grafana_dashboard_json, rustraft_metric_names, rustraft_observability_provisioning,
    rustraft_observability_provisioning_json, rustraft_observability_provisioning_runbook_steps,
    rustraft_observability_provisioning_validation_prometheus,
    rustraft_operator_runbook_prometheus, rustraft_operator_runbook_steps,
    rustraft_operator_triage_prometheus, rustraft_operator_triage_summary,
    rustraft_optimization_report_prometheus, rustraft_validate_debug_snapshot,
    rustraft_validate_debug_snapshot_json, rustraft_validate_observability_provisioning,
    rustraft_validate_observability_provisioning_json, RustRaftAlertRule,
    RustRaftDebugBundleContract, RustRaftDebugBundleValidationReport, RustRaftDebugSnapshot,
    RustRaftGrafanaDashboard, RustRaftGrafanaPanel, RustRaftMetricNames,
    RustRaftObservabilityProvisioning, RustRaftOperatorRunbookStep, RustRaftOperatorTriageSummary,
    RustRaftPrometheusMetricSet,
};
pub use node::{
    RustRaftConsensus, RustRaftRequestTimer, RustRaftTickAdmission, RustRaftTickBackpressure,
    RustRaftTimerTask, RUSTRAFT_REQUEST_TIMER_MAX_TIMEOUT_MS,
};
pub use operational_evidence::{
    rustraft_baseline_raft_operational_evidence_bundle,
    rustraft_validate_baseline_raft_operational_evidence_bundle,
    RustRaftBaselineRaftOperationalEvidenceBundle,
    RustRaftBaselineRaftOperationalEvidenceBundleValidationReport,
};
pub use pipeline::{
    rustraft_apply_batch_outcome_like_matrixraft, rustraft_peer_pipeline_status_from_observed,
    rustraft_pipeline_evidence, rustraft_replication_pipeline_evidence_artifact,
    rustraft_validate_replication_pipeline_evidence_artifact, RaftInflightAppend,
    RaftPeerPipelineState, RaftReplicationPipeline, RaftSnapshotTransferState,
    RustRaftApplyBatchOutcome, RustRaftApplyBatchStatus, RustRaftObservedPeerPipeline,
    RustRaftPeerPipelineStatus, RustRaftPeerProgressState, RustRaftPipelineEvidence,
    RustRaftPipelineLimits, RustRaftReplicationPipelineEvidenceArtifact,
    RustRaftReplicationPipelineEvidenceValidationReport,
};
pub use rate_limit::{
    RustRaftByteQuotaLimiter, RustRaftRateLimitDecision, RustRaftRateLimiter,
    RustRaftRateLimiterStats,
};
pub use read_safety::{
    rustraft_append_safety_decision, rustraft_applied_index_fence_report,
    rustraft_bounded_stale_read_report, rustraft_lease_read_eligibility_report,
    rustraft_read_safety_decision, rustraft_read_safety_evidence_artifact,
    rustraft_read_safety_runtime_decision, rustraft_validate_read_safety_evidence_artifact,
    RustRaftPendingReadIndex, RustRaftPendingReadIndexQueue, RustRaftPendingReadIndexResult,
};
pub use readiness::{
    rustraft_baseline_raft_parity_matrix, rustraft_baseline_raft_parity_surface,
    rustraft_baseline_raft_reference_policy, rustraft_benchmark_interface_names,
    rustraft_compatibility_report_names, rustraft_embedding_examples, rustraft_open_source_surface,
    rustraft_parity_contract, rustraft_parity_report, rustraft_parity_report_names,
    rustraft_public_api_contract, rustraft_public_module_names, rustraft_readiness_evidence,
    rustraft_require_production_ready, rustraft_requirements, rustraft_standalone_readiness_report,
    rustraft_temporalstore_adapter_shape, rustraft_temporalstore_extraction_plan,
    rustraft_validate_deployment_mode, rustraft_validate_deployment_readiness,
    RustRaftBaselineRaftParityItem, RustRaftBaselineRaftParityStatus,
    RustRaftBaselineRaftReferencePolicy, RustRaftDeploymentMode, RustRaftExtractionSlice,
    RustRaftExtractionStatus, RustRaftOpenSourceSurface, RustRaftParityContract,
    RustRaftParityReport, RustRaftProcessRolloutReadinessReport, RustRaftProductionReadinessError,
    RustRaftProductionReadinessInput, RustRaftProductionReadinessReport, RustRaftProductionStatus,
    RustRaftPublicApiContract, RustRaftReadinessEvidence, RustRaftReadinessSnapshot,
    RustRaftRequirementCategory, RustRaftSemanticRequirement, RustRaftStandaloneCapability,
    RustRaftStandaloneReadinessReport, RustRaftTemporalStoreAdapterShape,
    RustRaftTemporalStoreExtractionPlan,
};
pub use scheduler::{
    RustRaftApplyResult, RustRaftApplySnapshotTask, RustRaftApplyTask, RustRaftFlushTask,
    RustRaftFlushTaskDesc, RustRaftReadTask, RustRaftResetTask, RustRaftScheduler,
    RustRaftSchedulerTask, RustRaftStepDownSignal, RustRaftTriggerSnapshotTask,
};
pub use snapshot::{
    rustraft_snapshot_lifecycle_evidence, rustraft_snapshot_lifecycle_evidence_artifact,
    rustraft_validate_snapshot_floor_log_matching, rustraft_validate_snapshot_install,
    rustraft_validate_snapshot_lifecycle_evidence_artifact,
    rustraft_validate_snapshot_tail_catchup, RustRaftSnapshotLifecycleEvidence,
    RustRaftSnapshotLifecycleEvidenceArtifact, RustRaftSnapshotLifecycleEvidenceValidationReport,
};
pub use status::{
    rustraft_admin_diagnostic_json_lines, rustraft_admin_diagnostic_log_entries,
    rustraft_admin_fatal_blocker_report, rustraft_admin_status_surface_evidence,
    rustraft_apply_health, rustraft_capability_evidence, rustraft_capability_evidence_from_fields,
    rustraft_cluster_status_report, rustraft_fatal_blocker_report,
    rustraft_leader_transfer_admission, rustraft_optimization_report, rustraft_replication_health,
    rustraft_runtime_admin_report, rustraft_runtime_capability_report_from_evidence,
    rustraft_runtime_local_status_report, RaftApplyHealth, RaftCapabilityEvidence,
    RaftClusterStatusReport, RaftHealthStatus, RaftLeaderTransferAdmission,
    RaftLeaderTransferAdmissionKind, RaftLeaderTransferState, RaftPeerRuntimeState,
    RaftReplicationHealth, RaftRuntimeAdminReport, RaftRuntimeLocalStatusReport,
    RaftRuntimeTimerStatus, RustRaftAdminStatusSurfaceEvidence, RustRaftAdminStatusSurfaceInput,
    RustRaftBaselineRaftRuntimeCapabilityReport, RustRaftBlocker, RustRaftBlockerSeverity,
    RustRaftDiagnosticLogEntry, RustRaftDiagnosticSeverity, RustRaftFatalBlockerReport,
    RustRaftOptimizationHint, RustRaftOptimizationHintSeverity, RustRaftOptimizationReport,
    RustRaftProcessNodeEvidence, RustRaftProcessOperationalSemanticsEvidence,
    RustRaftProcessReadinessBlocker,
};
pub use storage::{
    rustraft_validate_storage_apply_fence, MatrixRaftGroupStorage, MatrixRaftLogCompactionReport,
    MatrixRaftLogRange, MatrixRaftLogSegment, MatrixRaftLogSegmentEvent,
    MatrixRaftLogSegmentEventKind, MatrixRaftLogStorage, MatrixRaftLogStorageOptions,
    MatrixRaftLogStoragePrepareOptions, MatrixRaftLogStorageWriteTask,
    MatrixRaftMemoryGroupStorage, MatrixRaftMemoryLogStorage, RustRaftStorage,
};
use transport::require_transport_validation;
pub use transport::{
    rustraft_validate_append_entries_request, rustraft_validate_append_entries_response,
    rustraft_validate_install_snapshot_request, rustraft_validate_install_snapshot_response,
    rustraft_validate_read_index_request, rustraft_validate_read_index_response,
    rustraft_validate_tcp_transport_request, rustraft_validate_vote_request,
    rustraft_validate_vote_response, RaftTransport, RustRaftTransport,
};
pub use unique_id::{
    RustRaftUniqueIdGenerator, RustRaftUniqueIdParts, RUSTRAFT_UNIQUE_ID_COUNTER_BITS,
    RUSTRAFT_UNIQUE_ID_COUNTER_MASK, RUSTRAFT_UNIQUE_ID_MEMBER_BITS,
    RUSTRAFT_UNIQUE_ID_MEMBER_MASK, RUSTRAFT_UNIQUE_ID_TIMESTAMP_BITS,
    RUSTRAFT_UNIQUE_ID_TIMESTAMP_MASK,
};
pub use wal::{
    rustraft_fold_wal_records, rustraft_recover_latest_wal_record,
    rustraft_validate_apply_snapshot_fence, rustraft_validate_hard_state_persistence,
    rustraft_validate_wal_lifecycle_evidence_artifact, rustraft_wal_checksum,
    rustraft_wal_checksum_format, rustraft_wal_checksum_valid, rustraft_wal_delta_base,
    rustraft_wal_lifecycle_evidence, rustraft_wal_lifecycle_evidence_artifact,
    RaftLogRetainedRange, RaftWalChecksumFormat, RaftWalCompactionReport, RaftWalRecord,
    RaftWalRecoveryReport, RaftWalSegment, RaftWalSegmentIndex, RaftWalWriteReport,
    RustRaftWalLifecycleEvidence, RustRaftWalLifecycleEvidenceArtifact,
    RustRaftWalLifecycleEvidenceValidationReport, RustRaftWalLifecycleStatus, RustRaftWalRecord,
};
// process rollout and cross-plane evidence report structs.
include!("facade/process_reports.rs");

// core membership roles, peers, learners, and joint membership helpers.
include!("facade/membership_core.rs");

// WAL persistence/runtime structs and segmented WAL helpers.
include!("facade/wal_runtime.rs");

// public API request/response/message/admin command contracts.
include!("facade/api_messages.rs");

// snapshot metadata, lifecycle, stores, and install/read-index messages.
include!("facade/snapshot_runtime.rs");

// read-safety evidence and runtime decision types.
include!("facade/read_safety_runtime.rs");

// in-process cluster runtime and consensus behavior.
include!("facade/cluster_runtime.rs");

// membership operation executor and validation helpers.
include!("facade/membership_executor.rs");

// stoppable node runtime worker and command loop.
include!("facade/node_runtime.rs");

// MatrixRaft-compatible public facade over the native MatrixRaft runtime.
include!("facade/matrixraft_compat.rs");

// authenticated, in-memory, TCP, and cluster transport runtime.
include!("facade/transport_runtime.rs");

// production readiness, status/admin reports, and harness-facing evidence.
include!("facade/status_admin_runtime.rs");

// crate-level regression tests.
include!("facade/tests.rs");
