// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![forbid(unsafe_code)]
// Lint configuration (the `type_complexity` / `too_many_arguments` allows and the
// rustdoc broken-intra-doc-links deny) lives in the `[lints]` table of Cargo.toml.
//! MatrixRaft is the TemporalStore-owned Raft contract and readiness library.
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
//! 2. Call [`matrixraft_parity_report`] for semantic contract readiness.
//! 3. Attach live runtime evidence to [`RustRaftProductionReadinessInput`].
//! 4. Call [`matrixraft_production_readiness_report`] and block production claims
//!    unless the report is ready.
//!
//! The public API is OpenRaft-free by design. Compatibility with existing
//! TemporalStore deployment semantics is expressed through MatrixRaft-owned
//! types and tests instead of upstream-specific type aliases. MatrixRaft is free
//! to expose idiomatic Rust traits and error types as long as TemporalStore
//! consumes it through a stable adapter boundary.
//!
//! Many public types still carry a `RustRaft` prefix from an earlier name for
//! this crate. Renaming them is a separate change: seven of them would collide
//! with distinct types of the same name in the compatibility facade.

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
    matrixraft_production_readiness_input_with_benchmark_artifacts,
    matrixraft_production_readiness_input_with_benchmark_summary,
    matrixraft_production_readiness_report_with_benchmark_artifacts,
    RustRaftBaselineRaftBenchmarkEvidence,
};
pub use channel_selector::{
    RustRaftChannelSelection, RustRaftChannelSelector, RustRaftChannelSelectorPolicy,
    RustRaftMailChannel, MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS,
};
pub use checksum::{
    matrixraft_checksum_file_list, matrixraft_crc32c, matrixraft_murmur32, RustRaftChecksumContext,
    RustRaftChecksumResult, RustRaftChecksumType, RustRaftFileChecksumContext,
    RustRaftFileChecksumResult,
};
pub use config::{RaftConfig, RaftConfigError, RustRaftConfig};
pub use durability::matrixraft_durability_parity_report;
pub use fsm::{
    matrixraft_apply_entry, matrixraft_flexible_apply_with_store,
    matrixraft_flexible_apply_with_store_report, matrixraft_fsm_entry_kind, MatrixRaftBatchId,
    MatrixRaftCheckpoint, MatrixRaftConfigurationApplied, MatrixRaftFlexibleApplyReport,
    MatrixRaftFsm, MatrixRaftFsmEntry, MatrixRaftFsmEntryKind, MatrixRaftFsmIterator,
    MatrixRaftFsmRuntimeBinding, MatrixRaftFsmRuntimeHookReport, MatrixRaftStoreFsm, RaftApply,
    RaftFsmAdapter, RaftFsmApplyEntryKind, RaftFsmApplyOutcome, RaftFsmBatchApplyReport,
    RaftFsmCheckpoint, RaftFsmReplayReport, RaftStateMachine, RustRaftStateMachine,
    MATRIXRAFT_NON_BATCH,
};
pub use heartbeat_merge::{
    RustRaftHeartbeatAddressResolver, RustRaftHeartbeatMergeMessage, RustRaftHeartbeatMergeStats,
    RustRaftHeartbeatMerger, RustRaftMergedHeartbeatBatch, MATRIXRAFT_HEARTBEAT_MERGE_BUCKETS,
};
pub use lease::{
    RustRaftFollowerLease, RustRaftLeaderLease, RustRaftLeaderLeaseStatus, RustRaftLeaseEpochId,
    RustRaftLeasePeer,
};
pub use log_buffer::{RustRaftLogBuffer, RustRaftLogBufferFlush, RustRaftLogBufferRelease};
pub use mailbox::{
    RustRaftMailBox, RustRaftMailBoxFetchPolicy, RustRaftMailPriority,
    MATRIXRAFT_MAILBOX_MAX_TIMEOUT_MS,
};
pub use membership::{
    matrixraft_learner_promotion_decision, matrixraft_membership_readiness_report,
    matrixraft_membership_semantics_evidence_artifact, matrixraft_membership_transition_missing,
    matrixraft_validate_membership_semantics_evidence_artifact, RaftLearnerAutoPromoteReport,
    RaftLearnerAutoPromoteState, RaftLearnerCatchUpLoopReport, RaftWitnessQuorumReport,
};
pub use metrics::{
    matrixraft_alert_rules, matrixraft_alert_rules_json, matrixraft_debug_bundle_contract,
    matrixraft_debug_bundle_validation_prometheus, matrixraft_debug_snapshot,
    matrixraft_debug_snapshot_json, matrixraft_debug_snapshot_metadata_prometheus,
    matrixraft_diagnostic_log_prometheus, matrixraft_grafana_dashboard,
    matrixraft_grafana_dashboard_json, matrixraft_metric_names,
    matrixraft_observability_provisioning, matrixraft_observability_provisioning_json,
    matrixraft_observability_provisioning_runbook_steps,
    matrixraft_observability_provisioning_validation_prometheus,
    matrixraft_operator_runbook_prometheus, matrixraft_operator_runbook_steps,
    matrixraft_operator_triage_prometheus, matrixraft_operator_triage_summary,
    matrixraft_optimization_report_prometheus, matrixraft_validate_debug_snapshot,
    matrixraft_validate_debug_snapshot_json, matrixraft_validate_observability_provisioning,
    matrixraft_validate_observability_provisioning_json, RustRaftAlertRule,
    RustRaftDebugBundleContract, RustRaftDebugBundleValidationReport, RustRaftDebugSnapshot,
    RustRaftGrafanaDashboard, RustRaftGrafanaPanel, RustRaftMetricNames,
    RustRaftObservabilityProvisioning, RustRaftOperatorRunbookStep, RustRaftOperatorTriageSummary,
    RustRaftPrometheusMetricSet,
};
pub use node::{
    RustRaftConsensus, RustRaftRequestTimer, RustRaftTickAdmission, RustRaftTickBackpressure,
    RustRaftTimerTask, MATRIXRAFT_REQUEST_TIMER_MAX_TIMEOUT_MS,
};
pub use operational_evidence::{
    matrixraft_baseline_raft_operational_evidence_bundle,
    matrixraft_validate_baseline_raft_operational_evidence_bundle,
    RustRaftBaselineRaftOperationalEvidenceBundle,
    RustRaftBaselineRaftOperationalEvidenceBundleValidationReport,
};
pub use pipeline::{
    matrixraft_apply_batch_outcome, matrixraft_peer_pipeline_status_from_observed,
    matrixraft_pipeline_evidence, matrixraft_replication_pipeline_evidence_artifact,
    matrixraft_validate_replication_pipeline_evidence_artifact, RaftInflightAppend,
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
    matrixraft_append_safety_decision, matrixraft_applied_index_fence_report,
    matrixraft_bounded_stale_read_report, matrixraft_lease_read_eligibility_report,
    matrixraft_read_safety_decision, matrixraft_read_safety_evidence_artifact,
    matrixraft_read_safety_runtime_decision, matrixraft_validate_read_safety_evidence_artifact,
    RustRaftPendingReadIndex, RustRaftPendingReadIndexQueue, RustRaftPendingReadIndexResult,
};
pub use readiness::{
    matrixraft_baseline_raft_parity_matrix, matrixraft_baseline_raft_parity_surface,
    matrixraft_baseline_raft_reference_policy, matrixraft_benchmark_interface_names,
    matrixraft_compatibility_report_names, matrixraft_embedding_examples,
    matrixraft_open_source_surface, matrixraft_parity_contract, matrixraft_parity_report,
    matrixraft_parity_report_names, matrixraft_public_api_contract, matrixraft_public_module_names,
    matrixraft_readiness_evidence, matrixraft_require_production_ready, matrixraft_requirements,
    matrixraft_standalone_readiness_report, matrixraft_temporalstore_adapter_shape,
    matrixraft_temporalstore_extraction_plan, matrixraft_validate_deployment_mode,
    matrixraft_validate_deployment_readiness, RustRaftBaselineRaftParityItem,
    RustRaftBaselineRaftParityStatus, RustRaftBaselineRaftReferencePolicy, RustRaftDeploymentMode,
    RustRaftExtractionSlice, RustRaftExtractionStatus, RustRaftOpenSourceSurface,
    RustRaftParityContract, RustRaftParityReport, RustRaftProcessRolloutReadinessReport,
    RustRaftProductionReadinessError, RustRaftProductionReadinessInput,
    RustRaftProductionReadinessReport, RustRaftProductionStatus, RustRaftPublicApiContract,
    RustRaftReadinessEvidence, RustRaftReadinessSnapshot, RustRaftRequirementCategory,
    RustRaftSemanticRequirement, RustRaftStandaloneCapability, RustRaftStandaloneReadinessReport,
    RustRaftTemporalStoreAdapterShape, RustRaftTemporalStoreExtractionPlan,
};
pub use scheduler::{
    RustRaftApplyResult, RustRaftApplySnapshotTask, RustRaftApplyTask, RustRaftFlushTask,
    RustRaftFlushTaskDesc, RustRaftReadTask, RustRaftResetTask, RustRaftScheduler,
    RustRaftSchedulerTask, RustRaftStepDownSignal, RustRaftTriggerSnapshotTask,
};
pub use snapshot::{
    matrixraft_snapshot_lifecycle_evidence, matrixraft_snapshot_lifecycle_evidence_artifact,
    matrixraft_validate_snapshot_floor_log_matching, matrixraft_validate_snapshot_install,
    matrixraft_validate_snapshot_lifecycle_evidence_artifact,
    matrixraft_validate_snapshot_tail_catchup, RustRaftSnapshotLifecycleEvidence,
    RustRaftSnapshotLifecycleEvidenceArtifact, RustRaftSnapshotLifecycleEvidenceValidationReport,
};
pub use status::{
    matrixraft_admin_diagnostic_json_lines, matrixraft_admin_diagnostic_log_entries,
    matrixraft_admin_fatal_blocker_report, matrixraft_admin_status_surface_evidence,
    matrixraft_apply_health, matrixraft_capability_evidence,
    matrixraft_capability_evidence_from_fields, matrixraft_cluster_status_report,
    matrixraft_fatal_blocker_report, matrixraft_leader_transfer_admission,
    matrixraft_optimization_report, matrixraft_replication_health, matrixraft_runtime_admin_report,
    matrixraft_runtime_capability_report_from_evidence, matrixraft_runtime_local_status_report,
    RaftApplyHealth, RaftCapabilityEvidence, RaftClusterStatusReport, RaftHealthStatus,
    RaftLeaderTransferAdmission, RaftLeaderTransferAdmissionKind, RaftLeaderTransferState,
    RaftPeerRuntimeState, RaftReplicationHealth, RaftRuntimeAdminReport,
    RaftRuntimeLocalStatusReport, RaftRuntimeTimerStatus, RustRaftAdminStatusSurfaceEvidence,
    RustRaftAdminStatusSurfaceInput, RustRaftBaselineRaftRuntimeCapabilityReport, RustRaftBlocker,
    RustRaftBlockerSeverity, RustRaftDiagnosticLogEntry, RustRaftDiagnosticSeverity,
    RustRaftFatalBlockerReport, RustRaftOptimizationHint, RustRaftOptimizationHintSeverity,
    RustRaftOptimizationReport, RustRaftProcessNodeEvidence,
    RustRaftProcessOperationalSemanticsEvidence, RustRaftProcessReadinessBlocker,
};
pub use storage::{
    matrixraft_validate_storage_apply_fence, MatrixRaftGroupStorage, MatrixRaftLogCompactionReport,
    MatrixRaftLogRange, MatrixRaftLogSegment, MatrixRaftLogSegmentEvent,
    MatrixRaftLogSegmentEventKind, MatrixRaftLogStorage, MatrixRaftLogStorageOptions,
    MatrixRaftLogStoragePrepareOptions, MatrixRaftLogStorageWriteTask,
    MatrixRaftMemoryGroupStorage, MatrixRaftMemoryLogStorage, RustRaftStorage,
};
use transport::require_transport_validation;
pub use transport::{
    matrixraft_validate_append_entries_request, matrixraft_validate_append_entries_response,
    matrixraft_validate_install_snapshot_request, matrixraft_validate_install_snapshot_response,
    matrixraft_validate_read_index_request, matrixraft_validate_read_index_response,
    matrixraft_validate_tcp_transport_request, matrixraft_validate_vote_request,
    matrixraft_validate_vote_response, RaftTransport, RustRaftTransport,
};
pub use unique_id::{
    RustRaftUniqueIdGenerator, RustRaftUniqueIdParts, MATRIXRAFT_UNIQUE_ID_COUNTER_BITS,
    MATRIXRAFT_UNIQUE_ID_COUNTER_MASK, MATRIXRAFT_UNIQUE_ID_MEMBER_BITS,
    MATRIXRAFT_UNIQUE_ID_MEMBER_MASK, MATRIXRAFT_UNIQUE_ID_TIMESTAMP_BITS,
    MATRIXRAFT_UNIQUE_ID_TIMESTAMP_MASK,
};
pub use wal::{
    matrixraft_fold_wal_records, matrixraft_recover_latest_wal_record,
    matrixraft_validate_apply_snapshot_fence, matrixraft_validate_hard_state_persistence,
    matrixraft_validate_wal_lifecycle_evidence_artifact, matrixraft_wal_checksum,
    matrixraft_wal_checksum_format, matrixraft_wal_checksum_valid, matrixraft_wal_delta_base,
    matrixraft_wal_lifecycle_evidence, matrixraft_wal_lifecycle_evidence_artifact,
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
