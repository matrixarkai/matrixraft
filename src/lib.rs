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
//! 1. Build a [`ReadinessSnapshot`] from the serving runtime.
//! 2. Call [`matrixraft_parity_report`] for semantic contract readiness.
//! 3. Attach live runtime evidence to [`ProductionReadinessInput`].
//! 4. Call [`matrixraft_production_readiness_report`] and block production claims
//!    unless the report is ready.
//!
//! The public API is OpenRaft-free by design. Compatibility with existing
//! TemporalStore deployment semantics is expressed through MatrixRaft-owned
//! types and tests instead of upstream-specific type aliases. MatrixRaft is free
//! to expose idiomatic Rust traits and error types as long as TemporalStore
//! consumes it through a stable adapter boundary.
//!
//! Naming conventions, so that additions stay consistent:
//!
//! * **Types are unprefixed.** The crate is the namespace, so it is
//!   [`Storage`] and `Message`, not `MatrixRaftStorage` -- the same shape
//!   `raft-rs` uses for `raft::Storage`. Where a concept has an established
//!   upstream name, that name is used: `StateRole`, `ProgressState`,
//!   `SnapshotMetadata`.
//! * **The compatibility facade keeps its `MatrixRaft` prefix**, because those
//!   types mirror the reference implementation's API rather than this crate's,
//!   and several are genuinely distinct from the like-named type here --
//!   `NodeId` is a `u64` alias while `MatrixRaftNodeId` is a struct of a peer
//!   id and two addresses.
//! * **A surviving `Raft` prefix marks the generic half of a pair.** Where a
//!   concept exists both as a concrete type and as a form generic over group
//!   and payload, the bare name is the concrete one and the `Raft`-prefixed
//!   name is the generic one: `LogEntry` is
//!   `GenericLogEntry<Payload>` while `RaftLogEntry<P>` is
//!   `GenericLogEntry<P>`, and `StateMachine` is the concrete apply/snapshot
//!   contract while `RaftStateMachine<G, P>` extends `RaftApply<G, P>`. The
//!   same holds for `ApplyRequest`/`RaftApplyRequest` and
//!   `ApplyResponse`/`RaftApplyResponse`. Do not "finish" the de-prefixing by
//!   collapsing these -- the prefix is carrying meaning, and collapsing them
//!   would still compile.
//! * **Emitted strings are not identifiers.** Prometheus metric names, alert
//!   rule names and evidence keys still spell `rustraft_*` / `RustRaft*`.
//!   They are a published interface; renaming them would break dashboards and
//!   alert rules, which is a breaking operational change, not a tidy-up.

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
    matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact,
    matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog,
    matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer,
    matrixraft_baseline_raft_benchmark_grafana_panels,
    matrixraft_baseline_raft_benchmark_metric_names,
    matrixraft_baseline_raft_benchmark_summary_prometheus, matrixraft_benchmark_runbook_steps,
    matrixraft_benchmark_runtime_pressure_readiness_artifact,
    matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog,
    matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer,
    matrixraft_debug_snapshot_with_benchmark_artifacts,
    matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts,
    matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts,
    matrixraft_debug_snapshot_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts,
    matrixraft_debug_snapshot_with_benchmark_scale_inputs,
    matrixraft_debug_snapshot_with_benchmark_summary,
    matrixraft_production_readiness_input_with_asserted_benchmark_artifacts,
    matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts,
    matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts,
    matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts,
    matrixraft_production_readiness_input_with_benchmark_artifacts,
    matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts,
    matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts,
    matrixraft_production_readiness_input_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts,
    matrixraft_production_readiness_input_with_benchmark_summary,
    matrixraft_production_readiness_input_with_runtime_pressure_and_read_backlog_evidence,
    matrixraft_production_readiness_input_with_runtime_pressure_evidence,
    matrixraft_production_readiness_input_with_runtime_pressure_read_backlog_and_node_runtime_timer_evidence,
    matrixraft_production_readiness_report_with_asserted_benchmark_artifacts,
    matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts,
    matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_artifacts,
    matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts,
    matrixraft_production_readiness_report_with_benchmark_artifacts,
    matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts,
    matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts,
    matrixraft_production_readiness_report_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts,
    matrixraft_read_release_pressure_snapshot, matrixraft_release_benchmark_runtime_timer_status,
    matrixraft_release_pressure_snapshot_from_json,
    matrixraft_release_pressure_snapshot_from_json_bytes,
    matrixraft_release_pressure_snapshot_json,
    matrixraft_scale_optimization_inputs_from_benchmark_report,
    matrixraft_scale_optimization_targets_from_baseline_raft_report,
    matrixraft_scale_rate_metrics_from_benchmark_report,
    matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact,
    matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog,
    matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer,
    matrixraft_validate_benchmark_runtime_pressure_readiness_artifact,
    matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog,
    matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer,
    matrixraft_validate_benchmark_scale_optimization_inputs,
    matrixraft_validate_release_pressure_snapshot,
    matrixraft_write_release_pressure_snapshot_atomic, BaselineRaftBenchmarkEvidence,
    BaselineRaftBenchmarkMetricNames, BenchmarkRuntimePressureReadinessArtifact,
    BenchmarkScaleOptimizationInputs, ReleasePressureSnapshot,
    MATRIXRAFT_BENCHMARK_RUNTIME_PRESSURE_READINESS_ARTIFACT_SCHEMA,
};
pub use channel_selector::{
    ChannelSelection, ChannelSelector, ChannelSelectorPolicy, MailChannel,
    MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS,
};
pub use checksum::{
    matrixraft_checksum_file_list, matrixraft_crc32c, matrixraft_murmur32, ChecksumContext,
    ChecksumResult, ChecksumType, FileChecksumContext, FileChecksumResult,
};
pub use config::{Config, ConfigError};
pub use durability::matrixraft_durability_parity_report;
pub use fsm::{
    matrixraft_apply_entry, matrixraft_flexible_apply_with_store,
    matrixraft_flexible_apply_with_store_report, matrixraft_fsm_entry_kind, FsmAdapter,
    FsmApplyEntryKind, FsmApplyOutcome, FsmBatchApplyReport, FsmCheckpoint, FsmReplayReport,
    MatrixRaftBatchId, MatrixRaftCheckpoint, MatrixRaftConfigurationApplied,
    MatrixRaftFlexibleApplyReport, MatrixRaftFsm, MatrixRaftFsmEntry, MatrixRaftFsmEntryKind,
    MatrixRaftFsmIterator, MatrixRaftFsmRuntimeBinding, MatrixRaftFsmRuntimeHookReport,
    MatrixRaftStoreFsm, RaftApply, RaftStateMachine, StateMachine, MATRIXRAFT_NON_BATCH,
};
pub use heartbeat_merge::{
    HeartbeatAddressResolver, HeartbeatMergeMessage, HeartbeatMergeStats, HeartbeatMerger,
    MergedHeartbeatBatch, MATRIXRAFT_HEARTBEAT_MERGE_BUCKETS,
};
pub use lease::{FollowerLease, LeaderLease, LeaderLeaseStatus, LeaseEpochId, LeasePeer};
pub use log_buffer::{LogBuffer, LogBufferFlush, LogBufferRelease};
pub use mailbox::{MailBox, MailBoxFetchPolicy, MailPriority, MATRIXRAFT_MAILBOX_MAX_TIMEOUT_MS};
pub use membership::{
    matrixraft_learner_promotion_decision, matrixraft_membership_readiness_report,
    matrixraft_membership_semantics_evidence_artifact,
    matrixraft_membership_semantics_evidence_artifact_from_runtime,
    matrixraft_membership_transition_missing,
    matrixraft_validate_membership_semantics_evidence_artifact, LearnerAutoPromoteReport,
    LearnerAutoPromoteState, LearnerCatchUpLoopReport, WitnessQuorumReport,
};
pub use metrics::{
    matrixraft_alert_rules, matrixraft_alert_rules_json, matrixraft_debug_bundle_contract,
    matrixraft_debug_bundle_validation_prometheus, matrixraft_debug_snapshot,
    matrixraft_debug_snapshot_json, matrixraft_debug_snapshot_metadata_prometheus,
    matrixraft_debug_snapshot_with_observability_metrics,
    matrixraft_debug_snapshot_with_performance_targets,
    matrixraft_debug_snapshot_with_runtime_metrics,
    matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence,
    matrixraft_debug_snapshot_with_runtime_pressure_evidence,
    matrixraft_debug_snapshot_with_runtime_pressure_read_backlog_and_node_runtime_timer_evidence,
    matrixraft_debug_snapshot_with_scale_metrics, matrixraft_diagnostic_log_prometheus,
    matrixraft_grafana_dashboard, matrixraft_grafana_dashboard_json,
    matrixraft_latency_metrics_prometheus, matrixraft_membership_readiness_diagnostic_json_lines,
    matrixraft_membership_readiness_diagnostic_log_entries,
    matrixraft_membership_readiness_grafana_panels, matrixraft_membership_readiness_metric_names,
    matrixraft_membership_readiness_prometheus, matrixraft_memory_grafana_panels,
    matrixraft_memory_metric_names, matrixraft_memory_metrics_prometheus,
    matrixraft_memory_optimization_hints, matrixraft_metric_names,
    matrixraft_node_runtime_grafana_panels, matrixraft_observability_provisioning,
    matrixraft_observability_provisioning_json,
    matrixraft_observability_provisioning_runbook_steps,
    matrixraft_observability_provisioning_validation_prometheus,
    matrixraft_observability_required_metric_names, matrixraft_operator_runbook_prometheus,
    matrixraft_operator_runbook_steps, matrixraft_operator_runbook_steps_with_diagnostics,
    matrixraft_operator_triage_prometheus, matrixraft_operator_triage_summary,
    matrixraft_optimization_report_prometheus, matrixraft_peer_pipeline_metrics_prometheus,
    matrixraft_production_readiness_grafana_panels, matrixraft_production_readiness_metric_names,
    matrixraft_runtime_pressure_admission, matrixraft_runtime_pressure_admission_prometheus,
    matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure,
    matrixraft_runtime_pressure_admission_with_pipeline_pressure,
    matrixraft_runtime_pressure_admission_with_read_backlog_pressure,
    matrixraft_runtime_pressure_admission_with_scale_and_pipeline_pressure,
    matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure,
    matrixraft_runtime_pressure_admission_with_scale_pipeline_read_backlog_and_node_runtime_timer_pressure,
    matrixraft_runtime_pressure_admission_with_scale_targets,
    matrixraft_runtime_pressure_bottleneck_summary,
    matrixraft_runtime_pressure_diagnostic_json_lines,
    matrixraft_runtime_pressure_diagnostic_log_entries,
    matrixraft_runtime_pressure_freshness_prometheus, matrixraft_runtime_pressure_freshness_report,
    matrixraft_runtime_pressure_grafana_panels, matrixraft_runtime_pressure_metric_names,
    matrixraft_scale_grafana_panels, matrixraft_scale_metric_names,
    matrixraft_scale_metrics_prometheus, matrixraft_scale_optimization_hints,
    matrixraft_scale_target_grafana_panels, matrixraft_scale_target_metric_names,
    matrixraft_scale_target_metrics_prometheus, matrixraft_snapshot_lifecycle_evidence_prometheus,
    matrixraft_snapshot_lifecycle_grafana_panels, matrixraft_snapshot_lifecycle_metric_names,
    matrixraft_validate_debug_snapshot, matrixraft_validate_debug_snapshot_json,
    matrixraft_validate_observability_provisioning,
    matrixraft_validate_observability_provisioning_json,
    matrixraft_validate_required_metric_scrape_texts,
    matrixraft_validate_runtime_pressure_admission_evidence,
    matrixraft_validate_runtime_pressure_admission_evidence_with_policy,
    matrixraft_wal_lifecycle_evidence_prometheus, matrixraft_wal_lifecycle_grafana_panels,
    matrixraft_wal_lifecycle_metric_names, AlertRule, DebugBundleContract,
    DebugBundleValidationReport, DebugSnapshot, GrafanaDashboard, GrafanaPanel, LatencyBucket,
    LatencyHistogram, LatencyMetrics, LatencyOptimizationThresholds, LatencyPressureDetail,
    MembershipReadinessMetricNames, MemoryMetricNames, MemoryMetrics, MemoryOptimizationThresholds,
    MemoryPressureDetail, MetricNames, NodeRuntimeTimerPressureDetail, NodeRuntimeTimerThresholds,
    ObservabilityProvisioning, OperatorRunbookStep, OperatorTriageSummary, PipelinePressureDetail,
    ProductionReadinessMetricNames, PrometheusMetricSet, ReadBacklogMetrics,
    ReadBacklogPressureDetail, ReadBacklogThresholds, RuntimePressureAdmission,
    RuntimePressureAdmissionPolicy, RuntimePressureBottleneck, RuntimePressureFreshnessReport,
    RuntimePressureMetricNames, ScaleMetricNames, ScaleMetrics, ScaleOptimizationTargets,
    ScalePressureDetail, ScaleRateMetrics, ScaleTargetMetricNames, SnapshotLifecycleMetricNames,
    WalLifecycleMetricNames,
};
pub use node::{
    Consensus, RequestTimer, TickAdmission, TickBackpressure, TimerTask,
    MATRIXRAFT_REQUEST_TIMER_MAX_TIMEOUT_MS,
};
pub use operational_evidence::{
    matrixraft_baseline_raft_operational_evidence_bundle,
    matrixraft_validate_baseline_raft_operational_evidence_bundle,
    BaselineRaftOperationalEvidenceBundle, BaselineRaftOperationalEvidenceBundleValidationReport,
};
pub use pipeline::{
    matrixraft_apply_batch_outcome, matrixraft_peer_pipeline_status_from_observed,
    matrixraft_pipeline_evidence, matrixraft_replication_pipeline_evidence_artifact,
    matrixraft_validate_replication_pipeline_evidence_artifact, ApplyBatchOutcome,
    ApplyBatchStatus, InflightAppend, ObservedPeerPipeline, PeerProgress, PipelineEvidence,
    PipelineLimits, ProgressState, ReplicationPipeline, ReplicationPipelineEvidenceArtifact,
    ReplicationPipelineEvidenceValidationReport, SnapshotTransferState,
};
pub use rate_limit::{ByteQuotaLimiter, RateLimitDecision, RateLimiter, RateLimiterStats};
pub use read_safety::{
    matrixraft_append_safety_decision, matrixraft_applied_index_fence_report,
    matrixraft_bounded_stale_read_report, matrixraft_lease_read_eligibility_report,
    matrixraft_read_safety_decision, matrixraft_read_safety_evidence_artifact,
    matrixraft_read_safety_runtime_decision, matrixraft_validate_read_safety_evidence_artifact,
    PendingReadIndex, PendingReadIndexQueue, PendingReadIndexResult,
};
pub use readiness::{
    matrixraft_api_name_mappings, matrixraft_baseline_raft_parity_matrix,
    matrixraft_baseline_raft_parity_surface, matrixraft_baseline_raft_reference_policy,
    matrixraft_benchmark_interface_names, matrixraft_compatibility_report_names,
    matrixraft_core_interface_names, matrixraft_diagnostic_interface_names,
    matrixraft_embedding_examples, matrixraft_evidence_interface_names,
    matrixraft_observability_interface_names, matrixraft_open_source_surface,
    matrixraft_parity_contract, matrixraft_parity_report, matrixraft_parity_report_names,
    matrixraft_public_api_contract, matrixraft_public_module_names, matrixraft_readiness_evidence,
    matrixraft_require_production_ready, matrixraft_requirements,
    matrixraft_standalone_readiness_report, matrixraft_temporalstore_adapter_shape,
    matrixraft_temporalstore_extraction_plan, matrixraft_validate_deployment_mode,
    matrixraft_validate_deployment_readiness, matrixraft_validate_public_api_contract,
    ApiNameMapping, BaselineRaftParityItem, BaselineRaftParityStatus, BaselineRaftReferencePolicy,
    DeploymentMode, ExtractionSlice, ExtractionStatus, OpenSourceSurface, ParityContract,
    ParityReport, ProcessRolloutReadinessReport, ProductionReadinessError,
    ProductionReadinessInput, ProductionReadinessReport, ProductionStatus, PublicApiContract,
    PublicApiContractValidationReport, PublicApiMappingCoverage, ReadinessEvidence,
    ReadinessSnapshot, RequirementCategory, SemanticRequirement, StandaloneCapability,
    StandaloneReadinessReport, TemporalStoreAdapterShape, TemporalStoreExtractionPlan,
};
pub use scheduler::{
    ApplyResult, ApplySnapshotTask, ApplyTask, FlushTask, FlushTaskDesc, ReadTask, ResetTask,
    Scheduler, SchedulerTask, StepDownSignal, TriggerSnapshotTask,
};
pub use snapshot::{
    matrixraft_snapshot_lifecycle_evidence, matrixraft_snapshot_lifecycle_evidence_artifact,
    matrixraft_validate_snapshot_floor_log_matching, matrixraft_validate_snapshot_install,
    matrixraft_validate_snapshot_lifecycle_evidence_artifact,
    matrixraft_validate_snapshot_tail_catchup, SnapshotLifecycleEvidence,
    SnapshotLifecycleEvidenceArtifact, SnapshotLifecycleEvidenceValidationReport,
};
pub use status::{
    matrixraft_admin_diagnostic_json_lines, matrixraft_admin_diagnostic_log_entries,
    matrixraft_admin_fatal_blocker_report, matrixraft_admin_status_surface_evidence,
    matrixraft_apply_health, matrixraft_capability_evidence,
    matrixraft_capability_evidence_from_fields, matrixraft_cluster_status_report,
    matrixraft_fatal_blocker_report, matrixraft_leader_transfer_admission,
    matrixraft_local_status_diagnostic_json_lines, matrixraft_local_status_diagnostic_log_entries,
    matrixraft_optimization_diagnostic_json_lines, matrixraft_optimization_diagnostic_log_entries,
    matrixraft_optimization_report, matrixraft_replication_health, matrixraft_runtime_admin_report,
    matrixraft_runtime_capability_report_from_evidence, matrixraft_runtime_local_status_report,
    AdminStatusSurfaceEvidence, AdminStatusSurfaceInput, ApplyHealth,
    BaselineRaftRuntimeCapabilityReport, Blocker, BlockerSeverity, CapabilityEvidence,
    ClusterStatusReport, DiagnosticLogEntry, DiagnosticSeverity, FatalBlockerReport, HealthStatus,
    LeaderTransferAdmission, LeaderTransferAdmissionKind, LeaderTransferState, OptimizationHint,
    OptimizationHintSeverity, OptimizationReport, PeerRuntimeState, ProcessNodeEvidence,
    ProcessOperationalSemanticsEvidence, ProcessReadinessBlocker, ReplicationHealth,
    RuntimeAdminReport, RuntimeLocalStatusReport, RuntimeTimerStatus,
};
pub use storage::{
    matrixraft_validate_storage_apply_fence, MatrixRaftGroupStorage, MatrixRaftLogCompactionReport,
    MatrixRaftLogRange, MatrixRaftLogSegment, MatrixRaftLogSegmentEvent,
    MatrixRaftLogSegmentEventKind, MatrixRaftLogStorage, MatrixRaftLogStorageOptions,
    MatrixRaftLogStoragePrepareOptions, MatrixRaftLogStorageWriteTask,
    MatrixRaftMemoryGroupStorage, MatrixRaftMemoryLogStorage, Storage,
};
use transport::require_transport_validation;
pub use transport::{
    matrixraft_validate_append_entries_request, matrixraft_validate_append_entries_response,
    matrixraft_validate_install_snapshot_request, matrixraft_validate_install_snapshot_response,
    matrixraft_validate_read_index_request, matrixraft_validate_read_index_response,
    matrixraft_validate_tcp_transport_request, matrixraft_validate_vote_request,
    matrixraft_validate_vote_response, Transport,
};
pub use unique_id::{
    UniqueIdGenerator, UniqueIdParts, MATRIXRAFT_UNIQUE_ID_COUNTER_BITS,
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
    LogRetainedRange, WalChecksumFormat, WalCompactionReport, WalLifecycleEvidence,
    WalLifecycleEvidenceArtifact, WalLifecycleEvidenceValidationReport, WalLifecycleStatus,
    WalRecord, WalRecoveryReport, WalSegment, WalSegmentIndex, WalWriteReport,
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
