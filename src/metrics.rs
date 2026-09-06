// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Metric-name contract for RustRaft observability.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::benchmark::{
    matrixraft_baseline_raft_benchmark_grafana_panels,
    matrixraft_baseline_raft_benchmark_metric_names,
};
pub use crate::matrixraft_baseline_raft_runtime_capability_prometheus;
use crate::membership::{MembershipReadinessReport, MembershipScope, MembershipTransitionKind};
use crate::pipeline::PeerProgress;
use crate::snapshot::SnapshotLifecycleEvidence;
use crate::status::{
    matrixraft_admin_diagnostic_log_entries, matrixraft_optimization_report,
    AdminStatusSurfaceInput, DiagnosticLogEntry, DiagnosticSeverity, OptimizationHint,
    OptimizationHintSeverity, OptimizationReport, RuntimeAdminReport, RuntimeLocalStatusReport,
    RuntimeTimerStatus,
};
use crate::wal::WalLifecycleEvidence;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetricNames {
    pub ready: String,
    pub append_latency_ms: String,
    pub vote_latency_ms: String,
    pub pre_vote_latency_ms: String,
    pub read_index_latency_ms: String,
    pub snapshot_install_latency_ms: String,
    pub peer_append_queue_depth: String,
    pub peer_reorder_queue_depth: String,
    pub peer_reorder_entries_converged_total: String,
    pub peer_snapshot_installed_index: String,
    pub wal_segment_count: String,
    pub blocker_total: String,
    pub fatal_total: String,
    pub diagnostic_log_total: String,
    pub diagnostic_log_entry_total: String,
    pub optimization_ready: String,
    pub optimization_critical_total: String,
    pub optimization_warning_total: String,
    pub optimization_hint_total: String,
    pub optimization_component_hint_total: String,
    pub operator_triage_status: String,
    pub operator_triage_diagnostic_error_total: String,
    pub operator_triage_diagnostic_warning_total: String,
    pub operator_triage_optimization_critical_total: String,
    pub operator_triage_optimization_warning_total: String,
    pub operator_triage_alert_rule_total: String,
    pub operator_triage_first_action: String,
    pub operator_triage_top_diagnostic: String,
    pub operator_triage_top_alert: String,
    pub operator_triage_top_optimization_hint: String,
    pub operator_runbook_step_total: String,
    pub operator_runbook_step_present: String,
    pub operator_runbook_first_step: String,
    pub debug_snapshot_generated_at_unix_ms: String,
    pub debug_snapshot_age_ms: String,
    pub debug_snapshot_max_age_ms: String,
    pub debug_snapshot_stale_after_unix_ms: String,
    pub debug_snapshot_remaining_fresh_ms: String,
    pub debug_snapshot_low_fresh_ms: String,
    pub debug_snapshot_low_fresh: String,
    pub debug_snapshot_fresh: String,
    pub debug_bundle_validation_ready: String,
    pub debug_bundle_validation_issue_total: String,
    pub debug_bundle_validation_issue: String,
    pub debug_bundle_validation_first_issue: String,
    pub observability_provisioning_validation_ready: String,
    pub observability_provisioning_validation_issue_total: String,
    pub observability_provisioning_validation_issue: String,
    pub observability_provisioning_validation_first_issue: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScaleMetricNames {
    pub proposal_qps_total: String,
    pub append_entries_qps_total: String,
    pub read_index_qps_total: String,
    pub apply_entries_qps_total: String,
    pub replication_bytes_total: String,
    pub apply_bytes_total: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScaleTargetMetricNames {
    pub min_proposal_qps: String,
    pub min_append_entries_qps: String,
    pub min_read_index_qps: String,
    pub min_apply_entries_qps: String,
    pub min_replication_mib_per_sec: String,
    pub min_apply_mib_per_sec: String,
    pub proposal_target_percent: String,
    pub append_entries_target_percent: String,
    pub read_index_target_percent: String,
    pub apply_entries_target_percent: String,
    pub replication_target_percent: String,
    pub apply_target_percent: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryMetricNames {
    pub process_resident_memory_bytes: String,
    pub heap_allocated_bytes: String,
    pub log_cache_bytes: String,
    pub snapshot_buffer_bytes: String,
    pub replication_buffer_bytes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimePressureMetricNames {
    pub admission_accepted: String,
    pub admission_rejected: String,
    pub freshness_generated_at_unix_ms: String,
    pub freshness_age_ms: String,
    pub freshness_max_age_ms: String,
    pub freshness_stale_after_unix_ms: String,
    pub freshness_remaining_fresh_ms: String,
    pub freshness_low_fresh_ms: String,
    pub freshness_low_fresh: String,
    pub freshness_fresh: String,
    pub freshness_status: String,
    pub freshness_issue_total: String,
    pub freshness_issue: String,
    pub bottleneck_score_percent: String,
    pub memory_pressure: String,
    pub memory_pressure_observed_value: String,
    pub memory_pressure_threshold_value: String,
    pub memory_pressure_excess: String,
    pub latency_pressure: String,
    pub latency_pressure_sample_count: String,
    pub latency_pressure_observed_p95_ms: String,
    pub latency_pressure_observed_p99_ms: String,
    pub latency_pressure_threshold_p99_ms: String,
    pub latency_pressure_excess_ms: String,
    pub scale_pressure: String,
    pub scale_pressure_observed_value: String,
    pub scale_pressure_target_value: String,
    pub scale_pressure_deficit: String,
    pub scale_pressure_target_percent: String,
    pub pipeline_pressure: String,
    pub pipeline_pressure_observed_value: String,
    pub pipeline_pressure_threshold_value: String,
    pub pipeline_pressure_excess: String,
    pub read_backlog_pressure: String,
    pub read_backlog_pressure_observed_value: String,
    pub read_backlog_pressure_threshold_value: String,
    pub read_backlog_pressure_excess: String,
    pub node_runtime_timer_pressure: String,
    pub node_runtime_timer_pressure_observed_percent: String,
    pub node_runtime_timer_pressure_threshold_percent: String,
    pub node_runtime_timer_pressure_excess_percent: String,
    pub action_total: String,
    pub action_source_total: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotLifecycleMetricNames {
    pub sender_lifecycle_present: String,
    pub downloader_lifecycle_present: String,
    pub retry_backpressure_present: String,
    pub chunk_retry_present: String,
    pub send_timeout_present: String,
    pub rate_limit_present: String,
    pub sustained_sender_load_present: String,
    pub sustained_downloader_load_present: String,
    pub sustained_sender_completion_present: String,
    pub sustained_downloader_completion_present: String,
    pub sustained_transfer_completion_present: String,
    pub snapshot_peer_count: String,
    pub sustained_transfer_completed_peer_count: String,
    pub install_progress_present: String,
    pub install_rollback_present: String,
    pub membership_change_present: String,
    pub rejoin_after_compacted_log_present: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalLifecycleMetricNames {
    pub segment_lifecycle_present: String,
    pub retained_range_present: String,
    pub sequence_range_present: String,
    pub log_index_range_present: String,
    pub compaction_observed: String,
    pub slow_fsync_backpressure_observed: String,
    pub compaction_after_slow_fsync_observed: String,
    pub released_segment_count: String,
    pub compacted_after_slow_fsync_count: String,
    pub slow_fsync_segment_count: String,
    pub compacted_slow_fsync_segment_count: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipReadinessMetricNames {
    pub ready: String,
    pub satisfied_total: String,
    pub missing_total: String,
    pub transition_ready: String,
    pub transition_missing_total: String,
    pub transition_missing: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionReadinessMetricNames {
    pub ready: String,
    pub satisfied_total: String,
    pub missing_total: String,
    pub blocker_total: String,
    pub next_action_total: String,
    pub missing_present: String,
    pub blocker_present: String,
    pub runtime_pressure_bottleneck_score_percent: String,
    pub next_action_present: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScaleMetrics {
    pub proposal_total: u64,
    pub append_entries_total: u64,
    pub read_index_total: u64,
    pub apply_entries_total: u64,
    pub replication_bytes_total: u64,
    pub apply_bytes_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScaleRateMetrics {
    pub proposal_qps: u64,
    pub append_entries_qps: u64,
    pub read_index_qps: u64,
    pub apply_entries_qps: u64,
    pub replication_mib_per_sec: u64,
    pub apply_mib_per_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScaleOptimizationTargets {
    pub min_proposal_qps: u64,
    pub min_append_entries_qps: u64,
    pub min_read_index_qps: u64,
    pub min_apply_entries_qps: u64,
    pub min_replication_mib_per_sec: u64,
    pub min_apply_mib_per_sec: u64,
}

impl ScaleMetrics {
    pub fn zero() -> Self {
        Self {
            proposal_total: 0,
            append_entries_total: 0,
            read_index_total: 0,
            apply_entries_total: 0,
            replication_bytes_total: 0,
            apply_bytes_total: 0,
        }
    }
}

impl ScaleRateMetrics {
    pub fn zero() -> Self {
        Self {
            proposal_qps: 0,
            append_entries_qps: 0,
            read_index_qps: 0,
            apply_entries_qps: 0,
            replication_mib_per_sec: 0,
            apply_mib_per_sec: 0,
        }
    }
}

impl Default for ScaleOptimizationTargets {
    fn default() -> Self {
        Self {
            min_proposal_qps: 0,
            min_append_entries_qps: 0,
            min_read_index_qps: 0,
            min_apply_entries_qps: 0,
            min_replication_mib_per_sec: 0,
            min_apply_mib_per_sec: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryMetrics {
    pub process_resident_memory_bytes: u64,
    pub heap_allocated_bytes: u64,
    pub log_cache_bytes: u64,
    pub snapshot_buffer_bytes: u64,
    pub replication_buffer_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryOptimizationThresholds {
    pub process_resident_warning_bytes: u64,
    pub heap_allocated_warning_bytes: u64,
    pub log_cache_warning_bytes: u64,
    pub snapshot_buffer_warning_bytes: u64,
    pub replication_buffer_warning_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatencyOptimizationThresholds {
    pub append_p99_warning_ms: u64,
    pub vote_p99_warning_ms: u64,
    pub pre_vote_p99_warning_ms: u64,
    pub read_index_p99_warning_ms: u64,
    pub snapshot_install_p99_warning_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadBacklogMetrics {
    pub pending_read_index_requests: u64,
    pub pending_bounded_stale_reads: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadBacklogThresholds {
    pub pending_read_index_warning: u64,
    pub pending_bounded_stale_read_warning: u64,
}

impl MemoryMetrics {
    pub fn zero() -> Self {
        Self {
            process_resident_memory_bytes: 0,
            heap_allocated_bytes: 0,
            log_cache_bytes: 0,
            snapshot_buffer_bytes: 0,
            replication_buffer_bytes: 0,
        }
    }
}

impl Default for MemoryOptimizationThresholds {
    fn default() -> Self {
        Self {
            process_resident_warning_bytes: 8 * 1024 * 1024 * 1024,
            heap_allocated_warning_bytes: 4 * 1024 * 1024 * 1024,
            log_cache_warning_bytes: 1024 * 1024 * 1024,
            snapshot_buffer_warning_bytes: 1024 * 1024 * 1024,
            replication_buffer_warning_bytes: 1024 * 1024 * 1024,
        }
    }
}

impl Default for LatencyOptimizationThresholds {
    fn default() -> Self {
        Self {
            append_p99_warning_ms: 100,
            vote_p99_warning_ms: 100,
            pre_vote_p99_warning_ms: 100,
            read_index_p99_warning_ms: 50,
            snapshot_install_p99_warning_ms: 5_000,
        }
    }
}

impl ReadBacklogMetrics {
    pub fn zero() -> Self {
        Self {
            pending_read_index_requests: 0,
            pending_bounded_stale_reads: 0,
        }
    }
}

impl Default for ReadBacklogThresholds {
    fn default() -> Self {
        Self {
            pending_read_index_warning: 1024,
            pending_bounded_stale_read_warning: 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatencyBucket {
    pub le_ms: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatencyHistogram {
    pub buckets: Vec<LatencyBucket>,
    pub sum_ms: u64,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatencyMetrics {
    pub append_latency_ms: LatencyHistogram,
    pub vote_latency_ms: LatencyHistogram,
    pub pre_vote_latency_ms: LatencyHistogram,
    pub read_index_latency_ms: LatencyHistogram,
    pub snapshot_install_latency_ms: LatencyHistogram,
}

impl LatencyHistogram {
    pub fn zero(default_buckets_ms: &[&str]) -> Self {
        Self {
            buckets: default_buckets_ms
                .iter()
                .map(|le_ms| LatencyBucket {
                    le_ms: (*le_ms).to_string(),
                    count: 0,
                })
                .collect(),
            sum_ms: 0,
            count: 0,
        }
    }
}

impl LatencyMetrics {
    pub fn zero() -> Self {
        const DEFAULT_BUCKETS_MS: &[&str] = &["1", "5", "10", "25", "50", "100", "250", "+Inf"];
        Self {
            append_latency_ms: LatencyHistogram::zero(DEFAULT_BUCKETS_MS),
            vote_latency_ms: LatencyHistogram::zero(DEFAULT_BUCKETS_MS),
            pre_vote_latency_ms: LatencyHistogram::zero(DEFAULT_BUCKETS_MS),
            read_index_latency_ms: LatencyHistogram::zero(DEFAULT_BUCKETS_MS),
            snapshot_install_latency_ms: LatencyHistogram::zero(DEFAULT_BUCKETS_MS),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimePressureAdmissionPolicy {
    pub reject_on_memory_pressure: bool,
    pub reject_on_latency_pressure: bool,
    #[serde(default)]
    pub reject_on_scale_pressure: bool,
    #[serde(default)]
    pub reject_on_pipeline_pressure: bool,
    #[serde(default)]
    pub reject_on_read_backlog_pressure: bool,
    #[serde(default)]
    pub reject_on_node_runtime_timer_pressure: bool,
}

impl RuntimePressureAdmissionPolicy {
    pub fn observe_only() -> Self {
        Self {
            reject_on_memory_pressure: false,
            reject_on_latency_pressure: false,
            reject_on_scale_pressure: false,
            reject_on_pipeline_pressure: false,
            reject_on_read_backlog_pressure: false,
            reject_on_node_runtime_timer_pressure: false,
        }
    }

    pub fn fail_closed() -> Self {
        Self {
            reject_on_memory_pressure: true,
            reject_on_latency_pressure: true,
            reject_on_scale_pressure: true,
            reject_on_pipeline_pressure: true,
            reject_on_read_backlog_pressure: true,
            reject_on_node_runtime_timer_pressure: true,
        }
    }
}

impl Default for RuntimePressureAdmissionPolicy {
    fn default() -> Self {
        Self::observe_only()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimePressureAdmission {
    pub accepted: bool,
    pub memory_pressure: bool,
    #[serde(default)]
    pub memory_pressure_details: Vec<MemoryPressureDetail>,
    pub latency_pressure: bool,
    #[serde(default)]
    pub latency_pressure_details: Vec<LatencyPressureDetail>,
    #[serde(default)]
    pub scale_pressure: bool,
    #[serde(default)]
    pub scale_pressure_details: Vec<ScalePressureDetail>,
    #[serde(default)]
    pub pipeline_pressure: bool,
    #[serde(default)]
    pub pipeline_pressure_details: Vec<PipelinePressureDetail>,
    #[serde(default)]
    pub read_backlog_pressure: bool,
    #[serde(default)]
    pub read_backlog_pressure_details: Vec<ReadBacklogPressureDetail>,
    #[serde(default)]
    pub node_runtime_timer_pressure: bool,
    #[serde(default)]
    pub node_runtime_timer_pressure_details: Vec<NodeRuntimeTimerPressureDetail>,
    pub reason: String,
    pub rejected_component: Option<String>,
    pub actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimePressureBottleneck {
    pub rank: usize,
    pub category: String,
    pub component: String,
    pub observed_value: u64,
    pub threshold_or_target_value: u64,
    pub excess_or_deficit: u64,
    pub score_percent: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimePressureFreshnessReport {
    pub generated_at_unix_ms: u64,
    pub now_unix_ms: u64,
    pub max_age_ms: u64,
    pub low_fresh_ms: u64,
    pub age_ms: u64,
    pub stale_after_unix_ms: u64,
    pub remaining_fresh_ms: u64,
    pub fresh: bool,
    pub low_fresh: bool,
    pub freshness_status: String,
    pub issues: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryPressureDetail {
    pub component: String,
    pub observed_value: u64,
    pub threshold_value: u64,
    pub excess: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatencyPressureDetail {
    pub component: String,
    #[serde(default)]
    pub sample_count: u64,
    #[serde(default)]
    pub observed_p95_ms: u64,
    pub observed_p99_ms: u64,
    pub threshold_p99_ms: u64,
    pub excess_ms: u64,
}

fn matrixraft_runtime_pressure_expected_actions(component: &str) -> &'static [&'static str] {
    match component {
        "memory.process_resident" | "memory.heap_allocated" => &["release_memory"],
        "memory.log_cache" => &["compact_applied_log_cache"],
        "memory.snapshot_buffer" | "latency.snapshot_install" => &["throttle_snapshot_transfer"],
        "memory.replication_buffer" | "pipeline.memory_backpressure_rejections" => {
            &["reduce_append_inflight_bytes"]
        }
        "latency.append" => &["reduce_append_batch_or_raise_replication_parallelism"],
        "latency.vote" | "latency.pre_vote" => {
            &["prioritize_election_rpc_and_inspect_network_backpressure"]
        }
        "latency.read_index" => {
            &["route_reads_to_healthy_leaders_or_reduce_read_index_quorum_latency"]
        }
        "scale.proposal_qps" => &["increase_proposal_pipeline_parallelism"],
        "scale.append_entries_qps" => &["raise_append_entries_pipeline_capacity"],
        "scale.read_index_qps" => &["increase_read_index_fast_path_capacity"],
        "scale.apply_entries_qps"
        | "pipeline.apply_queue"
        | "pipeline.apply_backpressure_rejections" => &["raise_apply_worker_capacity"],
        "scale.replication_mib_per_sec" => &["increase_replication_batching_or_network_capacity"],
        "scale.apply_mib_per_sec" => &["increase_apply_io_parallelism"],
        "pipeline.append_queue" => &["increase_append_queue_capacity_or_reduce_peer_append_burst"],
        "pipeline.reorder_queue" => &["inspect_transport_ordering_and_reorder_queue_timeouts"],
        "read_backlog.pending_read_index" => {
            &["shed_or_route_read_index_requests_to_healthy_leaders"]
        }
        "read_backlog.pending_bounded_stale" => {
            &["reduce_bounded_stale_read_fanout_or_tighten_replica_read_deadlines"]
        }
        "node_runtime.timer_utilization" => &["raise_timer_queue_capacity_or_reduce_tick_burst"],
        _ => &[],
    }
}

fn matrixraft_runtime_pressure_recommended_actions_field(component: &str) -> String {
    matrixraft_runtime_pressure_expected_actions(component).join(",")
}

fn matrixraft_runtime_pressure_action_sources(
    admission: &RuntimePressureAdmission,
) -> Vec<(&str, &'static str)> {
    let mut sources = BTreeSet::new();
    for component in admission
        .memory_pressure_details
        .iter()
        .map(|detail| detail.component.as_str())
        .chain(
            admission
                .latency_pressure_details
                .iter()
                .map(|detail| detail.component.as_str()),
        )
        .chain(
            admission
                .scale_pressure_details
                .iter()
                .map(|detail| detail.component.as_str()),
        )
        .chain(
            admission
                .pipeline_pressure_details
                .iter()
                .map(|detail| detail.component.as_str()),
        )
        .chain(
            admission
                .read_backlog_pressure_details
                .iter()
                .map(|detail| detail.component.as_str()),
        )
        .chain(
            admission
                .node_runtime_timer_pressure_details
                .iter()
                .map(|detail| detail.component.as_str()),
        )
    {
        for action in matrixraft_runtime_pressure_expected_actions(component) {
            sources.insert((component, *action));
        }
    }
    sources.into_iter().collect()
}

pub fn matrixraft_runtime_pressure_bottleneck_summary(
    admission: &RuntimePressureAdmission,
) -> Vec<RuntimePressureBottleneck> {
    let mut bottlenecks = Vec::new();
    bottlenecks.extend(admission.memory_pressure_details.iter().map(|detail| {
        runtime_pressure_bottleneck(
            "memory",
            &detail.component,
            detail.observed_value,
            detail.threshold_value,
            detail.excess,
        )
    }));
    bottlenecks.extend(admission.latency_pressure_details.iter().map(|detail| {
        runtime_pressure_bottleneck(
            "latency",
            &detail.component,
            detail.observed_p99_ms,
            detail.threshold_p99_ms,
            detail.excess_ms,
        )
    }));
    bottlenecks.extend(admission.scale_pressure_details.iter().map(|detail| {
        runtime_pressure_bottleneck(
            "scale",
            &detail.component,
            detail.observed_value,
            detail.target_value,
            detail.deficit,
        )
    }));
    bottlenecks.extend(admission.pipeline_pressure_details.iter().map(|detail| {
        runtime_pressure_bottleneck(
            "pipeline",
            &detail.component,
            detail.observed_value,
            detail.threshold_value,
            detail.excess,
        )
    }));
    bottlenecks.extend(
        admission
            .read_backlog_pressure_details
            .iter()
            .map(|detail| {
                runtime_pressure_bottleneck(
                    "read_backlog",
                    &detail.component,
                    detail.observed_value,
                    detail.threshold_value,
                    detail.excess,
                )
            }),
    );
    bottlenecks.extend(
        admission
            .node_runtime_timer_pressure_details
            .iter()
            .map(|detail| {
                runtime_pressure_bottleneck(
                    "node_runtime_timer",
                    &detail.component,
                    detail.observed_percent,
                    detail.threshold_percent,
                    detail.excess_percent,
                )
            }),
    );
    bottlenecks.sort_by(|left, right| {
        right
            .score_percent
            .cmp(&left.score_percent)
            .then_with(|| {
                runtime_pressure_category_priority(&left.category)
                    .cmp(&runtime_pressure_category_priority(&right.category))
            })
            .then_with(|| left.component.cmp(&right.component))
    });
    for (index, bottleneck) in bottlenecks.iter_mut().enumerate() {
        bottleneck.rank = index + 1;
    }
    bottlenecks
}

fn runtime_pressure_category_priority(category: &str) -> u8 {
    match category {
        "read_backlog" => 0,
        "node_runtime_timer" => 1,
        "pipeline" => 2,
        "latency" => 3,
        "scale" => 4,
        "memory" => 5,
        _ => u8::MAX,
    }
}

pub fn matrixraft_runtime_pressure_freshness_report(
    generated_at_unix_ms: u64,
    now_unix_ms: u64,
    max_age_ms: u64,
    low_fresh_ms: u64,
) -> RuntimePressureFreshnessReport {
    let mut issues = Vec::new();
    if generated_at_unix_ms == 0 {
        issues.push("runtime_pressure_generated_at_missing".to_string());
    }
    if max_age_ms == 0 {
        issues.push("runtime_pressure_max_age_zero".to_string());
    }
    if low_fresh_ms > max_age_ms {
        issues.push("runtime_pressure_low_fresh_exceeds_max_age".to_string());
    }
    if generated_at_unix_ms > now_unix_ms.saturating_add(60_000) {
        issues.push("runtime_pressure_generated_at_in_future".to_string());
    }

    let invalid = !issues.is_empty();
    let age_ms = now_unix_ms.saturating_sub(generated_at_unix_ms);
    let stale_after_unix_ms = generated_at_unix_ms.saturating_add(max_age_ms);
    let remaining_fresh_ms = max_age_ms.saturating_sub(age_ms);
    let fresh = issues.is_empty() && age_ms <= max_age_ms;
    let low_fresh = fresh && remaining_fresh_ms >= low_fresh_ms;
    if !invalid && !fresh {
        issues.push("runtime_pressure_generated_at_stale".to_string());
    }
    let freshness_status = if invalid {
        "invalid"
    } else if !fresh {
        "stale"
    } else if low_fresh {
        "fresh"
    } else {
        "low_fresh"
    }
    .to_string();

    RuntimePressureFreshnessReport {
        generated_at_unix_ms,
        now_unix_ms,
        max_age_ms,
        low_fresh_ms,
        age_ms,
        stale_after_unix_ms,
        remaining_fresh_ms,
        fresh,
        low_fresh,
        freshness_status,
        issues,
    }
}

pub fn matrixraft_runtime_pressure_freshness_prometheus(
    report: &RuntimePressureFreshnessReport,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let names = matrixraft_runtime_pressure_metric_names();
    let mut text = String::new();
    let mut status_labels = labels.to_vec();
    status_labels.push(("freshness_status", report.freshness_status.as_str()));

    push_metric(
        &mut text,
        &names.freshness_generated_at_unix_ms,
        labels,
        report.generated_at_unix_ms,
    );
    push_metric(
        &mut text,
        &names.freshness_age_ms,
        &status_labels,
        report.age_ms,
    );
    push_metric(
        &mut text,
        &names.freshness_max_age_ms,
        labels,
        report.max_age_ms,
    );
    push_metric(
        &mut text,
        &names.freshness_stale_after_unix_ms,
        labels,
        report.stale_after_unix_ms,
    );
    push_metric(
        &mut text,
        &names.freshness_remaining_fresh_ms,
        &status_labels,
        report.remaining_fresh_ms,
    );
    push_metric(
        &mut text,
        &names.freshness_low_fresh_ms,
        labels,
        report.low_fresh_ms,
    );
    push_metric(
        &mut text,
        &names.freshness_low_fresh,
        &status_labels,
        bool_metric(report.low_fresh),
    );
    push_metric(
        &mut text,
        &names.freshness_fresh,
        &status_labels,
        bool_metric(report.fresh),
    );
    push_metric(&mut text, &names.freshness_status, &status_labels, 1);
    push_metric(
        &mut text,
        &names.freshness_issue_total,
        labels,
        report.issues.len() as u64,
    );
    if report.issues.is_empty() {
        let mut issue_labels = labels.to_vec();
        issue_labels.push(("issue", "none"));
        push_metric(&mut text, &names.freshness_issue, &issue_labels, 0);
    } else {
        for issue in &report.issues {
            let mut issue_labels = labels.to_vec();
            issue_labels.push(("issue", issue.as_str()));
            push_metric(&mut text, &names.freshness_issue, &issue_labels, 1);
        }
    }

    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count: text.lines().count() as u64,
        text,
    }
}

fn runtime_pressure_bottleneck(
    category: &str,
    component: &str,
    observed_value: u64,
    threshold_or_target_value: u64,
    excess_or_deficit: u64,
) -> RuntimePressureBottleneck {
    RuntimePressureBottleneck {
        rank: 0,
        category: category.to_string(),
        component: component.to_string(),
        observed_value,
        threshold_or_target_value,
        excess_or_deficit,
        score_percent: pressure_score_percent(excess_or_deficit, threshold_or_target_value),
    }
}

fn pressure_score_percent(excess_or_deficit: u64, threshold_or_target_value: u64) -> u64 {
    if threshold_or_target_value == 0 {
        return 0;
    }
    excess_or_deficit.saturating_mul(100) / threshold_or_target_value
}

pub fn matrixraft_validate_runtime_pressure_admission_evidence(
    admission: &RuntimePressureAdmission,
) -> Result<(), Vec<String>> {
    let mut issues = BTreeSet::new();
    let pressure_present = admission.memory_pressure
        || admission.latency_pressure
        || admission.scale_pressure
        || admission.pipeline_pressure
        || admission.read_backlog_pressure
        || admission.node_runtime_timer_pressure;
    let mut pressure_components = BTreeSet::new();
    pressure_components.extend(
        admission
            .memory_pressure_details
            .iter()
            .map(|detail| detail.component.as_str()),
    );
    pressure_components.extend(
        admission
            .latency_pressure_details
            .iter()
            .map(|detail| detail.component.as_str()),
    );
    pressure_components.extend(
        admission
            .scale_pressure_details
            .iter()
            .map(|detail| detail.component.as_str()),
    );
    pressure_components.extend(
        admission
            .pipeline_pressure_details
            .iter()
            .map(|detail| detail.component.as_str()),
    );
    pressure_components.extend(
        admission
            .read_backlog_pressure_details
            .iter()
            .map(|detail| detail.component.as_str()),
    );
    pressure_components.extend(
        admission
            .node_runtime_timer_pressure_details
            .iter()
            .map(|detail| detail.component.as_str()),
    );
    let mut unique_actions = BTreeSet::new();
    for action in &admission.actions {
        if action.trim().is_empty() {
            issues.insert("runtime_pressure:action_empty".to_string());
        }
        if action.trim() != action {
            issues.insert(format!("runtime_pressure:action_not_canonical:{action}"));
        }
        if !unique_actions.insert(action.as_str()) {
            issues.insert(format!("runtime_pressure:action_duplicate:{action}"));
        }
    }
    let mut expected_actions = BTreeSet::new();
    for component in &pressure_components {
        for action in matrixraft_runtime_pressure_expected_actions(component) {
            expected_actions.insert(*action);
        }
    }

    if admission.memory_pressure && admission.memory_pressure_details.is_empty() {
        issues.insert("runtime_pressure:memory_pressure_details_missing".to_string());
    }
    if !admission.memory_pressure && !admission.memory_pressure_details.is_empty() {
        issues.insert("runtime_pressure:memory_pressure_details_without_signal".to_string());
    }
    if admission.latency_pressure && admission.latency_pressure_details.is_empty() {
        issues.insert("runtime_pressure:latency_pressure_details_missing".to_string());
    }
    if !admission.latency_pressure && !admission.latency_pressure_details.is_empty() {
        issues.insert("runtime_pressure:latency_pressure_details_without_signal".to_string());
    }
    if admission.scale_pressure && admission.scale_pressure_details.is_empty() {
        issues.insert("runtime_pressure:scale_pressure_details_missing".to_string());
    }
    if !admission.scale_pressure && !admission.scale_pressure_details.is_empty() {
        issues.insert("runtime_pressure:scale_pressure_details_without_signal".to_string());
    }
    if admission.pipeline_pressure && admission.pipeline_pressure_details.is_empty() {
        issues.insert("runtime_pressure:pipeline_pressure_details_missing".to_string());
    }
    if !admission.pipeline_pressure && !admission.pipeline_pressure_details.is_empty() {
        issues.insert("runtime_pressure:pipeline_pressure_details_without_signal".to_string());
    }
    if admission.read_backlog_pressure && admission.read_backlog_pressure_details.is_empty() {
        issues.insert("runtime_pressure:read_backlog_pressure_details_missing".to_string());
    }
    if !admission.read_backlog_pressure && !admission.read_backlog_pressure_details.is_empty() {
        issues.insert("runtime_pressure:read_backlog_pressure_details_without_signal".to_string());
    }
    if admission.node_runtime_timer_pressure
        && admission.node_runtime_timer_pressure_details.is_empty()
    {
        issues.insert("runtime_pressure:node_runtime_timer_pressure_details_missing".to_string());
    }
    if !admission.node_runtime_timer_pressure
        && !admission.node_runtime_timer_pressure_details.is_empty()
    {
        issues.insert(
            "runtime_pressure:node_runtime_timer_pressure_details_without_signal".to_string(),
        );
    }
    if !pressure_present {
        if !admission.accepted {
            issues.insert("runtime_pressure:rejected_without_pressure".to_string());
        }
        if admission.reason != "accepted_no_pressure" {
            issues.insert(format!(
                "runtime_pressure:reason_mismatch:{}:{}",
                admission.reason, "accepted_no_pressure"
            ));
        }
        if admission.rejected_component.is_some() {
            issues.insert("runtime_pressure:rejected_component_without_pressure".to_string());
        }
        if !admission.actions.is_empty() {
            issues.insert("runtime_pressure:actions_without_pressure".to_string());
        }
    } else {
        if admission.actions.is_empty() {
            issues.insert("runtime_pressure:actions_missing_for_pressure".to_string());
        }
        if !expected_actions.is_empty() {
            for action in &unique_actions {
                if !expected_actions.contains(*action) {
                    issues.insert(format!(
                        "runtime_pressure:action_without_pressure_signal:{action}"
                    ));
                }
            }
            for expected_action in &expected_actions {
                if !unique_actions.contains(expected_action) {
                    issues.insert(format!(
                        "runtime_pressure:action_missing_for_pressure_signal:{expected_action}"
                    ));
                }
            }
        }
        if admission.accepted {
            if admission.reason != "accepted_observe_only_pressure" {
                issues.insert(format!(
                    "runtime_pressure:reason_mismatch:{}:{}",
                    admission.reason, "accepted_observe_only_pressure"
                ));
            }
            if admission.rejected_component.is_some() {
                issues.insert(
                    "runtime_pressure:rejected_component_on_accepted_admission".to_string(),
                );
            }
        } else {
            if admission.reason != "rejected_runtime_pressure" {
                issues.insert(format!(
                    "runtime_pressure:reason_mismatch:{}:{}",
                    admission.reason, "rejected_runtime_pressure"
                ));
            }
            match admission.rejected_component.as_deref() {
                Some(component) if component.trim().is_empty() => {
                    issues.insert("runtime_pressure:rejected_component_empty".to_string());
                }
                Some(component) if pressure_components.contains(component) => {}
                Some(component) => {
                    issues.insert(format!(
                        "runtime_pressure:rejected_component_not_in_pressure_details:{component}"
                    ));
                }
                None => {
                    issues.insert("runtime_pressure:rejected_component_missing".to_string());
                }
            }
        }
    }

    for detail in &admission.memory_pressure_details {
        if detail.component.is_empty() {
            issues.insert("runtime_pressure:memory_pressure_detail_component_empty".to_string());
        }
        if detail.threshold_value == 0 {
            issues.insert(format!(
                "runtime_pressure:memory_pressure_detail_threshold_zero:{}",
                detail.component
            ));
        } else if detail.observed_value < detail.threshold_value {
            issues.insert(format!(
                "runtime_pressure:memory_pressure_detail_not_over_threshold:{}:{}:{}",
                detail.component, detail.observed_value, detail.threshold_value
            ));
        }
        let expected_excess = detail.observed_value.saturating_sub(detail.threshold_value);
        if detail.excess != expected_excess {
            issues.insert(format!(
                "runtime_pressure:memory_pressure_detail_excess_mismatch:{}:{}:{}",
                detail.component, detail.excess, expected_excess
            ));
        }
    }

    for detail in &admission.latency_pressure_details {
        if detail.component.is_empty() {
            issues.insert("runtime_pressure:latency_pressure_detail_component_empty".to_string());
        }
        if detail.sample_count == 0 {
            issues.insert(format!(
                "runtime_pressure:latency_pressure_detail_sample_count_zero:{}",
                detail.component
            ));
        }
        if detail.observed_p95_ms > detail.observed_p99_ms {
            issues.insert(format!(
                "runtime_pressure:latency_pressure_detail_quantile_order_invalid:{}:{}:{}",
                detail.component, detail.observed_p95_ms, detail.observed_p99_ms
            ));
        }
        if detail.threshold_p99_ms == 0 {
            issues.insert(format!(
                "runtime_pressure:latency_pressure_detail_threshold_zero:{}",
                detail.component
            ));
        } else if detail.observed_p99_ms <= detail.threshold_p99_ms {
            issues.insert(format!(
                "runtime_pressure:latency_pressure_detail_not_over_threshold:{}:{}:{}",
                detail.component, detail.observed_p99_ms, detail.threshold_p99_ms
            ));
        }
        let expected_excess = detail
            .observed_p99_ms
            .saturating_sub(detail.threshold_p99_ms);
        if detail.excess_ms != expected_excess {
            issues.insert(format!(
                "runtime_pressure:latency_pressure_detail_excess_mismatch:{}:{}:{}",
                detail.component, detail.excess_ms, expected_excess
            ));
        }
    }

    for detail in &admission.scale_pressure_details {
        if detail.component.is_empty() {
            issues.insert("runtime_pressure:scale_pressure_detail_component_empty".to_string());
        }
        if detail.target_value == 0 {
            issues.insert(format!(
                "runtime_pressure:scale_pressure_detail_target_zero:{}",
                detail.component
            ));
        } else {
            if detail.observed_value >= detail.target_value {
                issues.insert(format!(
                    "runtime_pressure:scale_pressure_detail_not_below_target:{}:{}:{}",
                    detail.component, detail.observed_value, detail.target_value
                ));
            }
            let expected_target_percent =
                target_percent(detail.observed_value, detail.target_value);
            if detail.target_percent != expected_target_percent {
                issues.insert(format!(
                    "runtime_pressure:scale_pressure_detail_target_percent_mismatch:{}:{}:{}",
                    detail.component, detail.target_percent, expected_target_percent
                ));
            }
        }
        let expected_deficit = detail.target_value.saturating_sub(detail.observed_value);
        if detail.deficit != expected_deficit {
            issues.insert(format!(
                "runtime_pressure:scale_pressure_detail_deficit_mismatch:{}:{}:{}",
                detail.component, detail.deficit, expected_deficit
            ));
        }
    }

    for detail in &admission.pipeline_pressure_details {
        if detail.peer_id == 0 {
            issues.insert(format!(
                "runtime_pressure:pipeline_pressure_detail_peer_id_zero:{}",
                detail.component
            ));
        }
        if detail.component.is_empty() {
            issues.insert("runtime_pressure:pipeline_pressure_detail_component_empty".to_string());
        }
        if detail.threshold_value == 0 {
            issues.insert(format!(
                "runtime_pressure:pipeline_pressure_detail_threshold_zero:{}",
                detail.component
            ));
        } else if detail.observed_value < detail.threshold_value {
            issues.insert(format!(
                "runtime_pressure:pipeline_pressure_detail_not_over_threshold:{}:{}:{}",
                detail.component, detail.observed_value, detail.threshold_value
            ));
        }
        let expected_excess = detail.observed_value.saturating_sub(detail.threshold_value);
        if detail.excess != expected_excess {
            issues.insert(format!(
                "runtime_pressure:pipeline_pressure_detail_excess_mismatch:{}:{}:{}",
                detail.component, detail.excess, expected_excess
            ));
        }
    }

    for detail in &admission.read_backlog_pressure_details {
        if detail.component.is_empty() {
            issues.insert(
                "runtime_pressure:read_backlog_pressure_detail_component_empty".to_string(),
            );
        }
        if detail.threshold_value == 0 {
            issues.insert(format!(
                "runtime_pressure:read_backlog_pressure_detail_threshold_zero:{}",
                detail.component
            ));
        } else if detail.observed_value < detail.threshold_value {
            issues.insert(format!(
                "runtime_pressure:read_backlog_pressure_detail_not_over_threshold:{}:{}:{}",
                detail.component, detail.observed_value, detail.threshold_value
            ));
        }
        let expected_excess = detail.observed_value.saturating_sub(detail.threshold_value);
        if detail.excess != expected_excess {
            issues.insert(format!(
                "runtime_pressure:read_backlog_pressure_detail_excess_mismatch:{}:{}:{}",
                detail.component, detail.excess, expected_excess
            ));
        }
    }

    for detail in &admission.node_runtime_timer_pressure_details {
        if detail.component.is_empty() {
            issues.insert(
                "runtime_pressure:node_runtime_timer_pressure_detail_component_empty".to_string(),
            );
        }
        if detail.threshold_percent == 0 {
            issues.insert(format!(
                "runtime_pressure:node_runtime_timer_pressure_detail_threshold_zero:{}",
                detail.component
            ));
        } else if detail.observed_percent < detail.threshold_percent {
            issues.insert(format!(
                "runtime_pressure:node_runtime_timer_pressure_detail_not_over_threshold:{}:{}:{}",
                detail.component, detail.observed_percent, detail.threshold_percent
            ));
        }
        let expected_excess = detail
            .observed_percent
            .saturating_sub(detail.threshold_percent);
        if detail.excess_percent != expected_excess {
            issues.insert(format!(
                "runtime_pressure:node_runtime_timer_pressure_detail_excess_mismatch:{}:{}:{}",
                detail.component, detail.excess_percent, expected_excess
            ));
        }
    }

    if issues.is_empty() {
        Ok(())
    } else {
        Err(issues.into_iter().collect())
    }
}

pub fn matrixraft_validate_runtime_pressure_admission_evidence_with_policy(
    admission: &RuntimePressureAdmission,
    policy: &RuntimePressureAdmissionPolicy,
) -> Result<(), Vec<String>> {
    let mut issues = match matrixraft_validate_runtime_pressure_admission_evidence(admission) {
        Ok(()) => Vec::new(),
        Err(issues) => issues,
    };
    let pressure_components = runtime_pressure_component_names(admission);
    let expected_rejected_component = matrixraft_runtime_pressure_rejected_component(
        &pressure_components,
        admission.memory_pressure,
        admission.latency_pressure,
        admission.scale_pressure,
        admission.pipeline_pressure,
        admission.read_backlog_pressure,
        admission.node_runtime_timer_pressure,
        policy,
    );

    match (
        admission.accepted,
        admission.rejected_component.as_deref(),
        expected_rejected_component.as_deref(),
    ) {
        (true, None, Some(expected)) => {
            issues.push(format!(
                "runtime_pressure:policy_rejection_missing:{expected}"
            ));
        }
        (false, Some(actual), Some(expected)) if actual != expected => {
            issues.push(format!(
                "runtime_pressure:policy_rejected_component_mismatch:{actual}:{expected}"
            ));
        }
        (false, _, None) => {
            issues.push("runtime_pressure:policy_rejection_unexpected".to_string());
        }
        _ => {}
    }

    issues.sort();
    issues.dedup();
    if issues.is_empty() {
        Ok(())
    } else {
        Err(issues)
    }
}

fn runtime_pressure_component_names(admission: &RuntimePressureAdmission) -> Vec<String> {
    admission
        .memory_pressure_details
        .iter()
        .map(|detail| detail.component.clone())
        .chain(
            admission
                .latency_pressure_details
                .iter()
                .map(|detail| detail.component.clone()),
        )
        .chain(
            admission
                .scale_pressure_details
                .iter()
                .map(|detail| detail.component.clone()),
        )
        .chain(
            admission
                .pipeline_pressure_details
                .iter()
                .map(|detail| detail.component.clone()),
        )
        .chain(
            admission
                .read_backlog_pressure_details
                .iter()
                .map(|detail| detail.component.clone()),
        )
        .chain(
            admission
                .node_runtime_timer_pressure_details
                .iter()
                .map(|detail| detail.component.clone()),
        )
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScalePressureDetail {
    pub component: String,
    pub observed_value: u64,
    pub target_value: u64,
    pub deficit: u64,
    pub target_percent: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelinePressureDetail {
    pub peer_id: u64,
    pub component: String,
    pub observed_value: u64,
    pub threshold_value: u64,
    pub excess: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadBacklogPressureDetail {
    pub component: String,
    pub observed_value: u64,
    pub threshold_value: u64,
    pub excess: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRuntimeTimerThresholds {
    pub utilization_warning_percent: u64,
}

impl Default for NodeRuntimeTimerThresholds {
    fn default() -> Self {
        Self {
            utilization_warning_percent: 80,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRuntimeTimerPressureDetail {
    pub component: String,
    pub observed_percent: u64,
    pub threshold_percent: u64,
    pub excess_percent: u64,
}

pub fn matrixraft_runtime_pressure_admission(
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    policy: &RuntimePressureAdmissionPolicy,
) -> RuntimePressureAdmission {
    matrixraft_runtime_pressure_admission_inner(
        memory_metrics,
        memory_thresholds,
        latency_metrics,
        latency_thresholds,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        policy,
    )
}

pub fn matrixraft_runtime_pressure_admission_with_scale_targets(
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    scale_rates: &ScaleRateMetrics,
    scale_targets: &ScaleOptimizationTargets,
    policy: &RuntimePressureAdmissionPolicy,
) -> RuntimePressureAdmission {
    matrixraft_runtime_pressure_admission_inner(
        memory_metrics,
        memory_thresholds,
        latency_metrics,
        latency_thresholds,
        Some(scale_rates),
        Some(scale_targets),
        None,
        None,
        None,
        None,
        None,
        policy,
    )
}

pub fn matrixraft_runtime_pressure_admission_with_pipeline_pressure(
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    peer_pipeline: &[PeerProgress],
    policy: &RuntimePressureAdmissionPolicy,
) -> RuntimePressureAdmission {
    matrixraft_runtime_pressure_admission_inner(
        memory_metrics,
        memory_thresholds,
        latency_metrics,
        latency_thresholds,
        None,
        None,
        Some(peer_pipeline),
        None,
        None,
        None,
        None,
        policy,
    )
}

pub fn matrixraft_runtime_pressure_admission_with_read_backlog_pressure(
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    read_backlog_metrics: &ReadBacklogMetrics,
    read_backlog_thresholds: &ReadBacklogThresholds,
    policy: &RuntimePressureAdmissionPolicy,
) -> RuntimePressureAdmission {
    matrixraft_runtime_pressure_admission_inner(
        memory_metrics,
        memory_thresholds,
        latency_metrics,
        latency_thresholds,
        None,
        None,
        None,
        Some(read_backlog_metrics),
        Some(read_backlog_thresholds),
        None,
        None,
        policy,
    )
}

pub fn matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure(
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    timer_status: &RuntimeTimerStatus,
    timer_thresholds: &NodeRuntimeTimerThresholds,
    policy: &RuntimePressureAdmissionPolicy,
) -> RuntimePressureAdmission {
    matrixraft_runtime_pressure_admission_inner(
        memory_metrics,
        memory_thresholds,
        latency_metrics,
        latency_thresholds,
        None,
        None,
        None,
        None,
        None,
        Some(timer_status),
        Some(timer_thresholds),
        policy,
    )
}

pub fn matrixraft_runtime_pressure_admission_with_scale_and_pipeline_pressure(
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    scale_rates: &ScaleRateMetrics,
    scale_targets: &ScaleOptimizationTargets,
    peer_pipeline: &[PeerProgress],
    policy: &RuntimePressureAdmissionPolicy,
) -> RuntimePressureAdmission {
    matrixraft_runtime_pressure_admission_inner(
        memory_metrics,
        memory_thresholds,
        latency_metrics,
        latency_thresholds,
        Some(scale_rates),
        Some(scale_targets),
        Some(peer_pipeline),
        None,
        None,
        None,
        None,
        policy,
    )
}

pub fn matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure(
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    scale_rates: &ScaleRateMetrics,
    scale_targets: &ScaleOptimizationTargets,
    peer_pipeline: &[PeerProgress],
    read_backlog_metrics: &ReadBacklogMetrics,
    read_backlog_thresholds: &ReadBacklogThresholds,
    policy: &RuntimePressureAdmissionPolicy,
) -> RuntimePressureAdmission {
    matrixraft_runtime_pressure_admission_inner(
        memory_metrics,
        memory_thresholds,
        latency_metrics,
        latency_thresholds,
        Some(scale_rates),
        Some(scale_targets),
        Some(peer_pipeline),
        Some(read_backlog_metrics),
        Some(read_backlog_thresholds),
        None,
        None,
        policy,
    )
}

pub fn matrixraft_runtime_pressure_admission_with_scale_pipeline_read_backlog_and_node_runtime_timer_pressure(
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    scale_rates: &ScaleRateMetrics,
    scale_targets: &ScaleOptimizationTargets,
    peer_pipeline: &[PeerProgress],
    read_backlog_metrics: &ReadBacklogMetrics,
    read_backlog_thresholds: &ReadBacklogThresholds,
    timer_status: &RuntimeTimerStatus,
    timer_thresholds: &NodeRuntimeTimerThresholds,
    policy: &RuntimePressureAdmissionPolicy,
) -> RuntimePressureAdmission {
    matrixraft_runtime_pressure_admission_inner(
        memory_metrics,
        memory_thresholds,
        latency_metrics,
        latency_thresholds,
        Some(scale_rates),
        Some(scale_targets),
        Some(peer_pipeline),
        Some(read_backlog_metrics),
        Some(read_backlog_thresholds),
        Some(timer_status),
        Some(timer_thresholds),
        policy,
    )
}

fn matrixraft_runtime_pressure_admission_inner(
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    scale_rates: Option<&ScaleRateMetrics>,
    scale_targets: Option<&ScaleOptimizationTargets>,
    peer_pipeline: Option<&[PeerProgress]>,
    read_backlog_metrics: Option<&ReadBacklogMetrics>,
    read_backlog_thresholds: Option<&ReadBacklogThresholds>,
    timer_status: Option<&RuntimeTimerStatus>,
    timer_thresholds: Option<&NodeRuntimeTimerThresholds>,
    policy: &RuntimePressureAdmissionPolicy,
) -> RuntimePressureAdmission {
    let mut pressure_components = Vec::new();
    let mut memory_pressure = false;
    let mut memory_pressure_details = Vec::new();
    let mut latency_pressure = false;
    let mut latency_pressure_details = Vec::new();
    let mut scale_pressure = false;
    let mut scale_pressure_details = Vec::new();
    let mut pipeline_pressure = false;
    let mut pipeline_pressure_details = Vec::new();
    let mut read_backlog_pressure = false;
    let mut read_backlog_pressure_details = Vec::new();
    let mut node_runtime_timer_pressure = false;
    let mut node_runtime_timer_pressure_details = Vec::new();
    let mut actions = BTreeSet::new();

    for signal in memory_pressure_signals(memory_metrics, memory_thresholds) {
        memory_pressure = true;
        pressure_components.push(signal.component.to_string());
        actions.insert(signal.action.to_string());
        if let Some(detail) = signal.memory_detail {
            memory_pressure_details.push(detail);
        }
    }

    for signal in latency_pressure_signals(latency_metrics, latency_thresholds) {
        latency_pressure = true;
        pressure_components.push(signal.component.to_string());
        actions.insert(signal.action.to_string());
        if let Some(detail) = signal.latency_detail {
            latency_pressure_details.push(detail);
        }
    }

    if let (Some(rates), Some(targets)) = (scale_rates, scale_targets) {
        for signal in scale_pressure_signals(rates, targets) {
            scale_pressure = true;
            pressure_components.push(signal.component.to_string());
            actions.insert(signal.action.to_string());
            if let Some(detail) = signal.scale_detail {
                scale_pressure_details.push(detail);
            }
        }
    }

    if let Some(peers) = peer_pipeline {
        for signal in pipeline_pressure_signals(peers) {
            pipeline_pressure = true;
            pressure_components.push(signal.component.to_string());
            actions.insert(signal.action.to_string());
            if let Some(detail) = signal.pipeline_detail {
                pipeline_pressure_details.push(detail);
            }
        }
    }

    if let (Some(metrics), Some(thresholds)) = (read_backlog_metrics, read_backlog_thresholds) {
        for signal in read_backlog_pressure_signals(metrics, thresholds) {
            read_backlog_pressure = true;
            pressure_components.push(signal.component.to_string());
            actions.insert(signal.action.to_string());
            if let Some(detail) = signal.read_backlog_detail {
                read_backlog_pressure_details.push(detail);
            }
        }
    }

    if let (Some(status), Some(thresholds)) = (timer_status, timer_thresholds) {
        for signal in node_runtime_timer_pressure_signals(status, thresholds) {
            node_runtime_timer_pressure = true;
            pressure_components.push(signal.component.to_string());
            actions.insert(signal.action.to_string());
            if let Some(detail) = signal.node_runtime_timer_detail {
                node_runtime_timer_pressure_details.push(detail);
            }
        }
    }

    let rejected_component = matrixraft_runtime_pressure_rejected_component(
        &pressure_components,
        memory_pressure,
        latency_pressure,
        scale_pressure,
        pipeline_pressure,
        read_backlog_pressure,
        node_runtime_timer_pressure,
        policy,
    );

    let accepted = rejected_component.is_none();
    let reason = match (
        accepted,
        memory_pressure
            || latency_pressure
            || scale_pressure
            || pipeline_pressure
            || read_backlog_pressure
            || node_runtime_timer_pressure,
    ) {
        (true, false) => "accepted_no_pressure",
        (true, true) => "accepted_observe_only_pressure",
        (false, _) => "rejected_runtime_pressure",
    }
    .to_string();

    RuntimePressureAdmission {
        accepted,
        memory_pressure,
        memory_pressure_details,
        latency_pressure,
        latency_pressure_details,
        scale_pressure,
        scale_pressure_details,
        pipeline_pressure,
        pipeline_pressure_details,
        read_backlog_pressure,
        read_backlog_pressure_details,
        node_runtime_timer_pressure,
        node_runtime_timer_pressure_details,
        reason,
        rejected_component,
        actions: actions.into_iter().collect(),
    }
}

#[derive(Debug, Clone)]
struct RuntimePressureSignal {
    component: &'static str,
    action: &'static str,
    memory_detail: Option<MemoryPressureDetail>,
    latency_detail: Option<LatencyPressureDetail>,
    scale_detail: Option<ScalePressureDetail>,
    pipeline_detail: Option<PipelinePressureDetail>,
    read_backlog_detail: Option<ReadBacklogPressureDetail>,
    node_runtime_timer_detail: Option<NodeRuntimeTimerPressureDetail>,
}

fn matrixraft_runtime_pressure_rejected_component(
    pressure_components: &[String],
    memory_pressure: bool,
    latency_pressure: bool,
    scale_pressure: bool,
    pipeline_pressure: bool,
    read_backlog_pressure: bool,
    node_runtime_timer_pressure: bool,
    policy: &RuntimePressureAdmissionPolicy,
) -> Option<String> {
    let priority = [
        (
            read_backlog_pressure && policy.reject_on_read_backlog_pressure,
            "read_backlog.",
        ),
        (
            node_runtime_timer_pressure && policy.reject_on_node_runtime_timer_pressure,
            "node_runtime.",
        ),
        (
            pipeline_pressure && policy.reject_on_pipeline_pressure,
            "pipeline.",
        ),
        (
            latency_pressure && policy.reject_on_latency_pressure,
            "latency.",
        ),
        (scale_pressure && policy.reject_on_scale_pressure, "scale."),
        (
            memory_pressure && policy.reject_on_memory_pressure,
            "memory.",
        ),
    ];

    priority.iter().find_map(|(rejectable, prefix)| {
        rejectable.then(|| {
            pressure_components
                .iter()
                .find(|component| component.starts_with(prefix))
                .cloned()
                .or_else(|| pressure_components.first().cloned())
        })?
    })
}

fn memory_pressure_signals(
    metrics: &MemoryMetrics,
    thresholds: &MemoryOptimizationThresholds,
) -> Vec<RuntimePressureSignal> {
    let mut signals = Vec::new();
    push_pressure_signal(
        &mut signals,
        "memory.process_resident",
        "release_memory",
        metrics.process_resident_memory_bytes,
        thresholds.process_resident_warning_bytes,
        true,
    );
    push_pressure_signal(
        &mut signals,
        "memory.heap_allocated",
        "release_memory",
        metrics.heap_allocated_bytes,
        thresholds.heap_allocated_warning_bytes,
        true,
    );
    push_pressure_signal(
        &mut signals,
        "memory.log_cache",
        "compact_applied_log_cache",
        metrics.log_cache_bytes,
        thresholds.log_cache_warning_bytes,
        true,
    );
    push_pressure_signal(
        &mut signals,
        "memory.snapshot_buffer",
        "throttle_snapshot_transfer",
        metrics.snapshot_buffer_bytes,
        thresholds.snapshot_buffer_warning_bytes,
        true,
    );
    push_pressure_signal(
        &mut signals,
        "memory.replication_buffer",
        "reduce_append_inflight_bytes",
        metrics.replication_buffer_bytes,
        thresholds.replication_buffer_warning_bytes,
        true,
    );
    signals
}

fn latency_pressure_signals(
    metrics: &LatencyMetrics,
    thresholds: &LatencyOptimizationThresholds,
) -> Vec<RuntimePressureSignal> {
    let mut signals = Vec::new();
    push_latency_pressure_signal(
        &mut signals,
        "latency.append",
        "reduce_append_batch_or_raise_replication_parallelism",
        &metrics.append_latency_ms,
        thresholds.append_p99_warning_ms,
    );
    push_latency_pressure_signal(
        &mut signals,
        "latency.vote",
        "prioritize_election_rpc_and_inspect_network_backpressure",
        &metrics.vote_latency_ms,
        thresholds.vote_p99_warning_ms,
    );
    push_latency_pressure_signal(
        &mut signals,
        "latency.pre_vote",
        "prioritize_election_rpc_and_inspect_network_backpressure",
        &metrics.pre_vote_latency_ms,
        thresholds.pre_vote_p99_warning_ms,
    );
    push_latency_pressure_signal(
        &mut signals,
        "latency.read_index",
        "route_reads_to_healthy_leaders_or_reduce_read_index_quorum_latency",
        &metrics.read_index_latency_ms,
        thresholds.read_index_p99_warning_ms,
    );
    push_latency_pressure_signal(
        &mut signals,
        "latency.snapshot_install",
        "throttle_snapshot_transfer",
        &metrics.snapshot_install_latency_ms,
        thresholds.snapshot_install_p99_warning_ms,
    );
    signals
}

fn push_latency_pressure_signal(
    signals: &mut Vec<RuntimePressureSignal>,
    component: &'static str,
    action: &'static str,
    histogram: &LatencyHistogram,
    threshold: u64,
) {
    if let Some(observed_p99_ms) = matrixraft_latency_histogram_p99_ms(histogram) {
        if threshold == 0 || observed_p99_ms <= threshold {
            return;
        }
        let observed_p95_ms = matrixraft_latency_histogram_p95_ms(histogram).unwrap_or(0);
        signals.push(RuntimePressureSignal {
            component,
            action,
            memory_detail: None,
            latency_detail: Some(LatencyPressureDetail {
                component: component.to_string(),
                sample_count: histogram.count,
                observed_p95_ms,
                observed_p99_ms,
                threshold_p99_ms: threshold,
                excess_ms: observed_p99_ms.saturating_sub(threshold),
            }),
            scale_detail: None,
            pipeline_detail: None,
            read_backlog_detail: None,
            node_runtime_timer_detail: None,
        });
    }
}

fn push_pressure_signal(
    signals: &mut Vec<RuntimePressureSignal>,
    component: &'static str,
    action: &'static str,
    observed_value: u64,
    threshold: u64,
    inclusive: bool,
) {
    if threshold == 0 {
        return;
    }
    let over_threshold = if inclusive {
        observed_value >= threshold
    } else {
        observed_value > threshold
    };
    if over_threshold {
        signals.push(RuntimePressureSignal {
            component,
            action,
            memory_detail: Some(MemoryPressureDetail {
                component: component.to_string(),
                observed_value,
                threshold_value: threshold,
                excess: observed_value.saturating_sub(threshold),
            }),
            latency_detail: None,
            scale_detail: None,
            pipeline_detail: None,
            read_backlog_detail: None,
            node_runtime_timer_detail: None,
        });
    }
}

fn scale_pressure_signals(
    rates: &ScaleRateMetrics,
    targets: &ScaleOptimizationTargets,
) -> Vec<RuntimePressureSignal> {
    let mut signals = Vec::new();
    push_scale_pressure_signal(
        &mut signals,
        "scale.proposal_qps",
        "increase_proposal_pipeline_parallelism",
        rates.proposal_qps,
        targets.min_proposal_qps,
    );
    push_scale_pressure_signal(
        &mut signals,
        "scale.append_entries_qps",
        "raise_append_entries_pipeline_capacity",
        rates.append_entries_qps,
        targets.min_append_entries_qps,
    );
    push_scale_pressure_signal(
        &mut signals,
        "scale.read_index_qps",
        "increase_read_index_fast_path_capacity",
        rates.read_index_qps,
        targets.min_read_index_qps,
    );
    push_scale_pressure_signal(
        &mut signals,
        "scale.apply_entries_qps",
        "raise_apply_worker_capacity",
        rates.apply_entries_qps,
        targets.min_apply_entries_qps,
    );
    push_scale_pressure_signal(
        &mut signals,
        "scale.replication_mib_per_sec",
        "increase_replication_batching_or_network_capacity",
        rates.replication_mib_per_sec,
        targets.min_replication_mib_per_sec,
    );
    push_scale_pressure_signal(
        &mut signals,
        "scale.apply_mib_per_sec",
        "increase_apply_io_parallelism",
        rates.apply_mib_per_sec,
        targets.min_apply_mib_per_sec,
    );
    signals
}

fn push_scale_pressure_signal(
    signals: &mut Vec<RuntimePressureSignal>,
    component: &'static str,
    action: &'static str,
    observed_value: u64,
    target_value: u64,
) {
    if target_value > 0 && observed_value < target_value {
        signals.push(RuntimePressureSignal {
            component,
            action,
            memory_detail: None,
            latency_detail: None,
            scale_detail: Some(ScalePressureDetail {
                component: component.to_string(),
                observed_value,
                target_value,
                deficit: target_value.saturating_sub(observed_value),
                target_percent: target_percent(observed_value, target_value),
            }),
            pipeline_detail: None,
            read_backlog_detail: None,
            node_runtime_timer_detail: None,
        });
    }
}

fn pipeline_pressure_signals(peers: &[PeerProgress]) -> Vec<RuntimePressureSignal> {
    let mut signals = Vec::new();
    for peer in peers {
        push_pipeline_pressure_signal(
            &mut signals,
            peer.peer_id,
            "pipeline.append_queue",
            "increase_append_queue_capacity_or_reduce_peer_append_burst",
            peer.append_queue_depth,
            peer.append_queue_limit,
            true,
        );
        push_pipeline_pressure_signal(
            &mut signals,
            peer.peer_id,
            "pipeline.apply_queue",
            "raise_apply_worker_capacity",
            peer.apply_queue_depth,
            peer.apply_inflight_limit,
            true,
        );
        push_pipeline_pressure_signal(
            &mut signals,
            peer.peer_id,
            "pipeline.apply_backpressure_rejections",
            "raise_apply_worker_capacity",
            peer.apply_backpressure_rejections,
            1,
            true,
        );
        push_pipeline_pressure_signal(
            &mut signals,
            peer.peer_id,
            "pipeline.memory_backpressure_rejections",
            "reduce_append_inflight_bytes",
            peer.memory_backpressure_rejections,
            1,
            true,
        );
        push_pipeline_pressure_signal(
            &mut signals,
            peer.peer_id,
            "pipeline.reorder_queue",
            "inspect_transport_ordering_and_reorder_queue_timeouts",
            peer.reorder_queue_depth,
            1,
            true,
        );
    }
    signals
}

fn push_pipeline_pressure_signal(
    signals: &mut Vec<RuntimePressureSignal>,
    peer_id: u64,
    component: &'static str,
    action: &'static str,
    observed_value: u64,
    threshold: u64,
    inclusive: bool,
) {
    if threshold == 0 {
        return;
    }
    let over_threshold = if inclusive {
        observed_value >= threshold
    } else {
        observed_value > threshold
    };
    if over_threshold {
        signals.push(RuntimePressureSignal {
            component,
            action,
            memory_detail: None,
            latency_detail: None,
            scale_detail: None,
            pipeline_detail: Some(PipelinePressureDetail {
                peer_id,
                component: component.to_string(),
                observed_value,
                threshold_value: threshold,
                excess: observed_value.saturating_sub(threshold),
            }),
            read_backlog_detail: None,
            node_runtime_timer_detail: None,
        });
    }
}

fn read_backlog_pressure_signals(
    metrics: &ReadBacklogMetrics,
    thresholds: &ReadBacklogThresholds,
) -> Vec<RuntimePressureSignal> {
    let mut signals = Vec::new();
    push_read_backlog_pressure_signal(
        &mut signals,
        "read_backlog.pending_read_index",
        "shed_or_route_read_index_requests_to_healthy_leaders",
        metrics.pending_read_index_requests,
        thresholds.pending_read_index_warning,
        true,
    );
    push_read_backlog_pressure_signal(
        &mut signals,
        "read_backlog.pending_bounded_stale",
        "reduce_bounded_stale_read_fanout_or_tighten_replica_read_deadlines",
        metrics.pending_bounded_stale_reads,
        thresholds.pending_bounded_stale_read_warning,
        true,
    );
    signals
}

fn push_read_backlog_pressure_signal(
    signals: &mut Vec<RuntimePressureSignal>,
    component: &'static str,
    action: &'static str,
    observed_value: u64,
    threshold: u64,
    inclusive: bool,
) {
    if threshold == 0 {
        return;
    }
    let over_threshold = if inclusive {
        observed_value >= threshold
    } else {
        observed_value > threshold
    };
    if over_threshold {
        signals.push(RuntimePressureSignal {
            component,
            action,
            memory_detail: None,
            latency_detail: None,
            scale_detail: None,
            pipeline_detail: None,
            read_backlog_detail: Some(ReadBacklogPressureDetail {
                component: component.to_string(),
                observed_value,
                threshold_value: threshold,
                excess: observed_value.saturating_sub(threshold),
            }),
            node_runtime_timer_detail: None,
        });
    }
}

fn node_runtime_timer_pressure_signals(
    status: &RuntimeTimerStatus,
    thresholds: &NodeRuntimeTimerThresholds,
) -> Vec<RuntimePressureSignal> {
    let mut signals = Vec::new();
    if status.max_pending_ticks == 0 || thresholds.utilization_warning_percent == 0 {
        return signals;
    }
    let utilization_percent = status.pending_ticks.saturating_mul(100) / status.max_pending_ticks;
    if utilization_percent >= thresholds.utilization_warning_percent {
        signals.push(RuntimePressureSignal {
            component: "node_runtime.timer_utilization",
            action: "raise_timer_queue_capacity_or_reduce_tick_burst",
            memory_detail: None,
            latency_detail: None,
            scale_detail: None,
            pipeline_detail: None,
            read_backlog_detail: None,
            node_runtime_timer_detail: Some(NodeRuntimeTimerPressureDetail {
                component: "node_runtime.timer_utilization".to_string(),
                observed_percent: utilization_percent,
                threshold_percent: thresholds.utilization_warning_percent,
                excess_percent: utilization_percent
                    .saturating_sub(thresholds.utilization_warning_percent),
            }),
        });
    }
    signals
}

fn matrixraft_latency_histogram_p99_ms(histogram: &LatencyHistogram) -> Option<u64> {
    matrixraft_latency_histogram_quantile_ms(histogram, 99)
}

fn matrixraft_latency_histogram_p95_ms(histogram: &LatencyHistogram) -> Option<u64> {
    matrixraft_latency_histogram_quantile_ms(histogram, 95)
}

fn matrixraft_latency_histogram_quantile_ms(
    histogram: &LatencyHistogram,
    percentile: u64,
) -> Option<u64> {
    if histogram.count == 0 {
        return None;
    }
    let rank = histogram.count.saturating_mul(percentile).div_ceil(100);
    histogram
        .buckets
        .iter()
        .find(|bucket| bucket.count >= rank)
        .and_then(|bucket| bucket.le_ms.parse::<u64>().ok())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrometheusMetricSet {
    pub format: String,
    pub metric_count: u64,
    pub text: String,
}

impl Default for PrometheusMetricSet {
    fn default() -> Self {
        Self {
            format: "prometheus_text_v0.0.4".to_string(),
            metric_count: 0,
            text: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GrafanaDashboard {
    pub title: String,
    pub uid: String,
    pub timezone: String,
    pub schema_version: u32,
    pub refresh: String,
    pub tags: Vec<String>,
    pub panels: Vec<GrafanaPanel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GrafanaPanel {
    pub id: u32,
    pub title: String,
    #[serde(rename = "type")]
    pub panel_type: String,
    pub expr: String,
    pub unit: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlertRule {
    pub alert: String,
    pub expr: String,
    pub duration: String,
    pub severity: String,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObservabilityProvisioning {
    pub service: String,
    pub prometheus_format: String,
    pub required_metric_names: Vec<String>,
    pub validation_metric_names: Vec<String>,
    pub debug_artifact_names: Vec<String>,
    pub prometheus_artifact_names: Vec<String>,
    pub dashboard: GrafanaDashboard,
    pub alert_rules: Vec<AlertRule>,
    pub runbook_steps: Vec<OperatorRunbookStep>,
    pub debug_bundle_contract: DebugBundleContract,
    pub sample_artifact_command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorTriageSummary {
    pub status: String,
    pub severity: String,
    pub first_action: String,
    pub diagnostic_error_count: usize,
    pub diagnostic_warning_count: usize,
    pub critical_optimization_count: u64,
    pub warning_optimization_count: u64,
    pub alert_rule_count: usize,
    pub top_diagnostic_target: Option<String>,
    pub top_diagnostic_message: Option<String>,
    pub top_alert: Option<String>,
    pub top_optimization_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorRunbookStep {
    pub id: String,
    pub severity: String,
    pub target: String,
    pub action: String,
    pub validation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebugBundleContract {
    pub name: String,
    pub version: u32,
    pub producer: String,
    pub schema: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebugBundleValidationReport {
    pub ready: bool,
    pub issue_count: usize,
    pub issues: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebugSnapshot {
    pub contract: DebugBundleContract,
    pub generated_at_unix_ms: u64,
    pub admin_report: RuntimeAdminReport,
    pub diagnostics: Vec<DiagnosticLogEntry>,
    pub diagnostic_prometheus: PrometheusMetricSet,
    pub latency_prometheus: PrometheusMetricSet,
    pub memory_prometheus: PrometheusMetricSet,
    pub scale_prometheus: PrometheusMetricSet,
    pub scale_target_prometheus: PrometheusMetricSet,
    #[serde(default)]
    pub benchmark_prometheus: PrometheusMetricSet,
    #[serde(default)]
    pub runtime_pressure_admission: Option<RuntimePressureAdmission>,
    #[serde(default)]
    pub runtime_pressure_diagnostics: Vec<DiagnosticLogEntry>,
    #[serde(default)]
    pub runtime_pressure_prometheus: PrometheusMetricSet,
    pub optimization: OptimizationReport,
    pub optimization_prometheus: PrometheusMetricSet,
    pub grafana: GrafanaDashboard,
    pub alerts: Vec<AlertRule>,
    pub triage: OperatorTriageSummary,
    pub runbook_prometheus: PrometheusMetricSet,
    pub runbook_steps: Vec<OperatorRunbookStep>,
}

pub fn matrixraft_metric_names() -> MetricNames {
    MetricNames {
        ready: "rustraft_ready".to_string(),
        append_latency_ms: "rustraft_append_latency_ms".to_string(),
        vote_latency_ms: "rustraft_vote_latency_ms".to_string(),
        pre_vote_latency_ms: "rustraft_pre_vote_latency_ms".to_string(),
        read_index_latency_ms: "rustraft_read_index_latency_ms".to_string(),
        snapshot_install_latency_ms: "rustraft_snapshot_install_latency_ms".to_string(),
        peer_append_queue_depth: "rustraft_peer_append_queue_depth".to_string(),
        peer_reorder_queue_depth: "rustraft_peer_reorder_queue_depth".to_string(),
        peer_reorder_entries_converged_total: "rustraft_peer_reorder_entries_converged_total"
            .to_string(),
        peer_snapshot_installed_index: "rustraft_peer_snapshot_installed_index".to_string(),
        wal_segment_count: "rustraft_wal_segment_count".to_string(),
        blocker_total: "rustraft_blocker_total".to_string(),
        fatal_total: "rustraft_fatal_total".to_string(),
        diagnostic_log_total: "rustraft_diagnostic_log_total".to_string(),
        diagnostic_log_entry_total: "rustraft_diagnostic_log_entry_total".to_string(),
        optimization_ready: "rustraft_optimization_ready".to_string(),
        optimization_critical_total: "rustraft_optimization_critical_total".to_string(),
        optimization_warning_total: "rustraft_optimization_warning_total".to_string(),
        optimization_hint_total: "rustraft_optimization_hint_total".to_string(),
        optimization_component_hint_total: "rustraft_optimization_component_hint_total".to_string(),
        operator_triage_status: "rustraft_operator_triage_status".to_string(),
        operator_triage_diagnostic_error_total: "rustraft_operator_triage_diagnostic_error_total"
            .to_string(),
        operator_triage_diagnostic_warning_total:
            "rustraft_operator_triage_diagnostic_warning_total".to_string(),
        operator_triage_optimization_critical_total:
            "rustraft_operator_triage_optimization_critical_total".to_string(),
        operator_triage_optimization_warning_total:
            "rustraft_operator_triage_optimization_warning_total".to_string(),
        operator_triage_alert_rule_total: "rustraft_operator_triage_alert_rule_total".to_string(),
        operator_triage_first_action: "rustraft_operator_triage_first_action".to_string(),
        operator_triage_top_diagnostic: "rustraft_operator_triage_top_diagnostic".to_string(),
        operator_triage_top_alert: "rustraft_operator_triage_top_alert".to_string(),
        operator_triage_top_optimization_hint: "rustraft_operator_triage_top_optimization_hint"
            .to_string(),
        operator_runbook_step_total: "rustraft_operator_runbook_step_total".to_string(),
        operator_runbook_step_present: "rustraft_operator_runbook_step_present".to_string(),
        operator_runbook_first_step: "rustraft_operator_runbook_first_step".to_string(),
        debug_snapshot_generated_at_unix_ms: "rustraft_debug_snapshot_generated_at_unix_ms"
            .to_string(),
        debug_snapshot_age_ms: "rustraft_debug_snapshot_age_ms".to_string(),
        debug_snapshot_max_age_ms: "rustraft_debug_snapshot_max_age_ms".to_string(),
        debug_snapshot_stale_after_unix_ms: "rustraft_debug_snapshot_stale_after_unix_ms"
            .to_string(),
        debug_snapshot_remaining_fresh_ms: "rustraft_debug_snapshot_remaining_fresh_ms".to_string(),
        debug_snapshot_low_fresh_ms: "rustraft_debug_snapshot_low_fresh_ms".to_string(),
        debug_snapshot_low_fresh: "rustraft_debug_snapshot_low_fresh".to_string(),
        debug_snapshot_fresh: "rustraft_debug_snapshot_fresh".to_string(),
        debug_bundle_validation_ready: "rustraft_debug_bundle_validation_ready".to_string(),
        debug_bundle_validation_issue_total: "rustraft_debug_bundle_validation_issue_total"
            .to_string(),
        debug_bundle_validation_issue: "rustraft_debug_bundle_validation_issue".to_string(),
        debug_bundle_validation_first_issue: "rustraft_debug_bundle_validation_first_issue"
            .to_string(),
        observability_provisioning_validation_ready:
            "rustraft_observability_provisioning_validation_ready".to_string(),
        observability_provisioning_validation_issue_total:
            "rustraft_observability_provisioning_validation_issue_total".to_string(),
        observability_provisioning_validation_issue:
            "rustraft_observability_provisioning_validation_issue".to_string(),
        observability_provisioning_validation_first_issue:
            "rustraft_observability_provisioning_validation_first_issue".to_string(),
    }
}

pub fn matrixraft_scale_metric_names() -> ScaleMetricNames {
    ScaleMetricNames {
        proposal_qps_total: "rustraft_proposal_total".to_string(),
        append_entries_qps_total: "rustraft_append_entries_total".to_string(),
        read_index_qps_total: "rustraft_read_index_total".to_string(),
        apply_entries_qps_total: "rustraft_apply_entries_total".to_string(),
        replication_bytes_total: "rustraft_replication_bytes_total".to_string(),
        apply_bytes_total: "rustraft_apply_bytes_total".to_string(),
    }
}

pub fn matrixraft_scale_target_metric_names() -> ScaleTargetMetricNames {
    ScaleTargetMetricNames {
        min_proposal_qps: "rustraft_scale_target_min_proposal_qps".to_string(),
        min_append_entries_qps: "rustraft_scale_target_min_append_entries_qps".to_string(),
        min_read_index_qps: "rustraft_scale_target_min_read_index_qps".to_string(),
        min_apply_entries_qps: "rustraft_scale_target_min_apply_entries_qps".to_string(),
        min_replication_mib_per_sec: "rustraft_scale_target_min_replication_mib_per_sec"
            .to_string(),
        min_apply_mib_per_sec: "rustraft_scale_target_min_apply_mib_per_sec".to_string(),
        proposal_target_percent: "rustraft_scale_observed_proposal_target_percent".to_string(),
        append_entries_target_percent: "rustraft_scale_observed_append_entries_target_percent"
            .to_string(),
        read_index_target_percent: "rustraft_scale_observed_read_index_target_percent".to_string(),
        apply_entries_target_percent: "rustraft_scale_observed_apply_entries_target_percent"
            .to_string(),
        replication_target_percent: "rustraft_scale_observed_replication_target_percent"
            .to_string(),
        apply_target_percent: "rustraft_scale_observed_apply_target_percent".to_string(),
    }
}

pub fn matrixraft_memory_metric_names() -> MemoryMetricNames {
    MemoryMetricNames {
        process_resident_memory_bytes: "rustraft_process_resident_memory_bytes".to_string(),
        heap_allocated_bytes: "rustraft_heap_allocated_bytes".to_string(),
        log_cache_bytes: "rustraft_log_cache_bytes".to_string(),
        snapshot_buffer_bytes: "rustraft_snapshot_buffer_bytes".to_string(),
        replication_buffer_bytes: "rustraft_replication_buffer_bytes".to_string(),
    }
}

pub fn matrixraft_runtime_pressure_metric_names() -> RuntimePressureMetricNames {
    RuntimePressureMetricNames {
        admission_accepted: "rustraft_runtime_pressure_admission_accepted".to_string(),
        admission_rejected: "rustraft_runtime_pressure_admission_rejected".to_string(),
        freshness_generated_at_unix_ms: "rustraft_runtime_pressure_freshness_generated_at_unix_ms"
            .to_string(),
        freshness_age_ms: "rustraft_runtime_pressure_freshness_age_ms".to_string(),
        freshness_max_age_ms: "rustraft_runtime_pressure_freshness_max_age_ms".to_string(),
        freshness_stale_after_unix_ms: "rustraft_runtime_pressure_freshness_stale_after_unix_ms"
            .to_string(),
        freshness_remaining_fresh_ms: "rustraft_runtime_pressure_freshness_remaining_fresh_ms"
            .to_string(),
        freshness_low_fresh_ms: "rustraft_runtime_pressure_freshness_low_fresh_ms".to_string(),
        freshness_low_fresh: "rustraft_runtime_pressure_freshness_low_fresh".to_string(),
        freshness_fresh: "rustraft_runtime_pressure_freshness_fresh".to_string(),
        freshness_status: "rustraft_runtime_pressure_freshness_status".to_string(),
        freshness_issue_total: "rustraft_runtime_pressure_freshness_issue_total".to_string(),
        freshness_issue: "rustraft_runtime_pressure_freshness_issue".to_string(),
        bottleneck_score_percent: "rustraft_runtime_pressure_bottleneck_score_percent".to_string(),
        memory_pressure: "rustraft_runtime_pressure_memory".to_string(),
        memory_pressure_observed_value: "rustraft_runtime_pressure_memory_observed_value"
            .to_string(),
        memory_pressure_threshold_value: "rustraft_runtime_pressure_memory_threshold_value"
            .to_string(),
        memory_pressure_excess: "rustraft_runtime_pressure_memory_excess".to_string(),
        latency_pressure: "rustraft_runtime_pressure_latency".to_string(),
        latency_pressure_sample_count: "rustraft_runtime_pressure_latency_sample_count".to_string(),
        latency_pressure_observed_p95_ms: "rustraft_runtime_pressure_latency_observed_p95_ms"
            .to_string(),
        latency_pressure_observed_p99_ms: "rustraft_runtime_pressure_latency_observed_p99_ms"
            .to_string(),
        latency_pressure_threshold_p99_ms: "rustraft_runtime_pressure_latency_threshold_p99_ms"
            .to_string(),
        latency_pressure_excess_ms: "rustraft_runtime_pressure_latency_excess_ms".to_string(),
        scale_pressure: "rustraft_runtime_pressure_scale".to_string(),
        scale_pressure_observed_value: "rustraft_runtime_pressure_scale_observed_value".to_string(),
        scale_pressure_target_value: "rustraft_runtime_pressure_scale_target_value".to_string(),
        scale_pressure_deficit: "rustraft_runtime_pressure_scale_deficit".to_string(),
        scale_pressure_target_percent: "rustraft_runtime_pressure_scale_target_percent".to_string(),
        pipeline_pressure: "rustraft_runtime_pressure_pipeline".to_string(),
        pipeline_pressure_observed_value: "rustraft_runtime_pressure_pipeline_observed_value"
            .to_string(),
        pipeline_pressure_threshold_value: "rustraft_runtime_pressure_pipeline_threshold_value"
            .to_string(),
        pipeline_pressure_excess: "rustraft_runtime_pressure_pipeline_excess".to_string(),
        read_backlog_pressure: "rustraft_runtime_pressure_read_backlog".to_string(),
        read_backlog_pressure_observed_value:
            "rustraft_runtime_pressure_read_backlog_observed_value".to_string(),
        read_backlog_pressure_threshold_value:
            "rustraft_runtime_pressure_read_backlog_threshold_value".to_string(),
        read_backlog_pressure_excess: "rustraft_runtime_pressure_read_backlog_excess".to_string(),
        node_runtime_timer_pressure: "rustraft_runtime_pressure_node_runtime_timer".to_string(),
        node_runtime_timer_pressure_observed_percent:
            "rustraft_runtime_pressure_node_runtime_timer_observed_percent".to_string(),
        node_runtime_timer_pressure_threshold_percent:
            "rustraft_runtime_pressure_node_runtime_timer_threshold_percent".to_string(),
        node_runtime_timer_pressure_excess_percent:
            "rustraft_runtime_pressure_node_runtime_timer_excess_percent".to_string(),
        action_total: "rustraft_runtime_pressure_action_total".to_string(),
        action_source_total: "rustraft_runtime_pressure_action_source_total".to_string(),
    }
}

pub fn matrixraft_snapshot_lifecycle_metric_names() -> SnapshotLifecycleMetricNames {
    SnapshotLifecycleMetricNames {
        sender_lifecycle_present: "rustraft_snapshot_lifecycle_sender_present".to_string(),
        downloader_lifecycle_present: "rustraft_snapshot_lifecycle_downloader_present".to_string(),
        retry_backpressure_present: "rustraft_snapshot_lifecycle_retry_backpressure_present"
            .to_string(),
        chunk_retry_present: "rustraft_snapshot_lifecycle_chunk_retry_present".to_string(),
        send_timeout_present: "rustraft_snapshot_lifecycle_send_timeout_present".to_string(),
        rate_limit_present: "rustraft_snapshot_lifecycle_rate_limit_present".to_string(),
        sustained_sender_load_present: "rustraft_snapshot_lifecycle_sustained_sender_load_present"
            .to_string(),
        sustained_downloader_load_present:
            "rustraft_snapshot_lifecycle_sustained_downloader_load_present".to_string(),
        sustained_sender_completion_present:
            "rustraft_snapshot_lifecycle_sustained_sender_completion_present".to_string(),
        sustained_downloader_completion_present:
            "rustraft_snapshot_lifecycle_sustained_downloader_completion_present".to_string(),
        sustained_transfer_completion_present:
            "rustraft_snapshot_lifecycle_sustained_transfer_completion_present".to_string(),
        snapshot_peer_count: "rustraft_snapshot_lifecycle_peer_count".to_string(),
        sustained_transfer_completed_peer_count:
            "rustraft_snapshot_lifecycle_sustained_transfer_completed_peer_count".to_string(),
        install_progress_present: "rustraft_snapshot_lifecycle_install_progress_present"
            .to_string(),
        install_rollback_present: "rustraft_snapshot_lifecycle_install_rollback_present"
            .to_string(),
        membership_change_present: "rustraft_snapshot_lifecycle_membership_change_present"
            .to_string(),
        rejoin_after_compacted_log_present:
            "rustraft_snapshot_lifecycle_rejoin_after_compacted_log_present".to_string(),
    }
}

pub fn matrixraft_wal_lifecycle_metric_names() -> WalLifecycleMetricNames {
    WalLifecycleMetricNames {
        segment_lifecycle_present: "rustraft_wal_lifecycle_segment_present".to_string(),
        retained_range_present: "rustraft_wal_lifecycle_retained_range_present".to_string(),
        sequence_range_present: "rustraft_wal_lifecycle_sequence_range_present".to_string(),
        log_index_range_present: "rustraft_wal_lifecycle_log_index_range_present".to_string(),
        compaction_observed: "rustraft_wal_lifecycle_compaction_observed".to_string(),
        slow_fsync_backpressure_observed: "rustraft_wal_lifecycle_slow_fsync_backpressure_observed"
            .to_string(),
        compaction_after_slow_fsync_observed:
            "rustraft_wal_lifecycle_compaction_after_slow_fsync_observed".to_string(),
        released_segment_count: "rustraft_wal_lifecycle_released_segment_count".to_string(),
        compacted_after_slow_fsync_count: "rustraft_wal_lifecycle_compacted_after_slow_fsync_count"
            .to_string(),
        slow_fsync_segment_count: "rustraft_wal_lifecycle_slow_fsync_segment_count".to_string(),
        compacted_slow_fsync_segment_count:
            "rustraft_wal_lifecycle_compacted_slow_fsync_segment_count".to_string(),
    }
}

pub fn matrixraft_membership_readiness_metric_names() -> MembershipReadinessMetricNames {
    MembershipReadinessMetricNames {
        ready: "rustraft_membership_readiness_ready".to_string(),
        satisfied_total: "rustraft_membership_readiness_satisfied_total".to_string(),
        missing_total: "rustraft_membership_readiness_missing_total".to_string(),
        transition_ready: "rustraft_membership_transition_ready".to_string(),
        transition_missing_total: "rustraft_membership_transition_missing_total".to_string(),
        transition_missing: "rustraft_membership_transition_missing".to_string(),
    }
}

pub fn matrixraft_production_readiness_metric_names() -> ProductionReadinessMetricNames {
    ProductionReadinessMetricNames {
        ready: "rustraft_production_readiness_ready".to_string(),
        satisfied_total: "rustraft_production_readiness_satisfied_total".to_string(),
        missing_total: "rustraft_production_readiness_missing_total".to_string(),
        blocker_total: "rustraft_production_readiness_blocker_total".to_string(),
        next_action_total: "rustraft_production_readiness_next_action_total".to_string(),
        missing_present: "rustraft_production_readiness_missing_present".to_string(),
        blocker_present: "rustraft_production_readiness_blocker_present".to_string(),
        runtime_pressure_bottleneck_score_percent:
            "rustraft_production_readiness_runtime_pressure_bottleneck_score_percent".to_string(),
        next_action_present: "rustraft_production_readiness_next_action_present".to_string(),
    }
}

pub fn matrixraft_scale_metrics_prometheus(
    metrics: &ScaleMetrics,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let names = matrixraft_scale_metric_names();
    let mut text = String::new();
    push_metric(
        &mut text,
        &names.proposal_qps_total,
        labels,
        metrics.proposal_total,
    );
    push_metric(
        &mut text,
        &names.append_entries_qps_total,
        labels,
        metrics.append_entries_total,
    );
    push_metric(
        &mut text,
        &names.read_index_qps_total,
        labels,
        metrics.read_index_total,
    );
    push_metric(
        &mut text,
        &names.apply_entries_qps_total,
        labels,
        metrics.apply_entries_total,
    );
    push_metric(
        &mut text,
        &names.replication_bytes_total,
        labels,
        metrics.replication_bytes_total,
    );
    push_metric(
        &mut text,
        &names.apply_bytes_total,
        labels,
        metrics.apply_bytes_total,
    );
    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count: 6,
        text,
    }
}

pub fn matrixraft_scale_target_metrics_prometheus(
    rates: &ScaleRateMetrics,
    targets: &ScaleOptimizationTargets,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let names = matrixraft_scale_target_metric_names();
    let mut text = String::new();
    push_metric(
        &mut text,
        &names.min_proposal_qps,
        labels,
        targets.min_proposal_qps,
    );
    push_metric(
        &mut text,
        &names.min_append_entries_qps,
        labels,
        targets.min_append_entries_qps,
    );
    push_metric(
        &mut text,
        &names.min_read_index_qps,
        labels,
        targets.min_read_index_qps,
    );
    push_metric(
        &mut text,
        &names.min_apply_entries_qps,
        labels,
        targets.min_apply_entries_qps,
    );
    push_metric(
        &mut text,
        &names.min_replication_mib_per_sec,
        labels,
        targets.min_replication_mib_per_sec,
    );
    push_metric(
        &mut text,
        &names.min_apply_mib_per_sec,
        labels,
        targets.min_apply_mib_per_sec,
    );
    push_metric(
        &mut text,
        &names.proposal_target_percent,
        labels,
        target_percent(rates.proposal_qps, targets.min_proposal_qps),
    );
    push_metric(
        &mut text,
        &names.append_entries_target_percent,
        labels,
        target_percent(rates.append_entries_qps, targets.min_append_entries_qps),
    );
    push_metric(
        &mut text,
        &names.read_index_target_percent,
        labels,
        target_percent(rates.read_index_qps, targets.min_read_index_qps),
    );
    push_metric(
        &mut text,
        &names.apply_entries_target_percent,
        labels,
        target_percent(rates.apply_entries_qps, targets.min_apply_entries_qps),
    );
    push_metric(
        &mut text,
        &names.replication_target_percent,
        labels,
        target_percent(
            rates.replication_mib_per_sec,
            targets.min_replication_mib_per_sec,
        ),
    );
    push_metric(
        &mut text,
        &names.apply_target_percent,
        labels,
        target_percent(rates.apply_mib_per_sec, targets.min_apply_mib_per_sec),
    );
    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count: 12,
        text,
    }
}

pub fn matrixraft_memory_metrics_prometheus(
    metrics: &MemoryMetrics,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let names = matrixraft_memory_metric_names();
    let mut text = String::new();
    push_metric(
        &mut text,
        &names.process_resident_memory_bytes,
        labels,
        metrics.process_resident_memory_bytes,
    );
    push_metric(
        &mut text,
        &names.heap_allocated_bytes,
        labels,
        metrics.heap_allocated_bytes,
    );
    push_metric(
        &mut text,
        &names.log_cache_bytes,
        labels,
        metrics.log_cache_bytes,
    );
    push_metric(
        &mut text,
        &names.snapshot_buffer_bytes,
        labels,
        metrics.snapshot_buffer_bytes,
    );
    push_metric(
        &mut text,
        &names.replication_buffer_bytes,
        labels,
        metrics.replication_buffer_bytes,
    );
    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count: 5,
        text,
    }
}

pub fn matrixraft_peer_pipeline_metrics_prometheus(
    report: &RuntimeLocalStatusReport,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let names = matrixraft_metric_names();
    let mut text = String::new();
    let mut metric_count = 0_u64;
    let group = report.node_status.group_id.to_string();
    let node = report.node_status.node_id.to_string();

    for peer in &report.peer_pipeline {
        let peer_id = peer.peer_id.to_string();
        let mut metric_labels = labels.to_vec();
        metric_labels.push(("group", group.as_str()));
        metric_labels.push(("node", node.as_str()));
        metric_labels.push(("peer", peer_id.as_str()));

        push_metric(
            &mut text,
            &names.peer_append_queue_depth,
            &metric_labels,
            peer.append_queue_depth,
        );
        metric_count += 1;
        push_metric(
            &mut text,
            &names.peer_reorder_queue_depth,
            &metric_labels,
            peer.reorder_queue_depth,
        );
        metric_count += 1;
        push_metric(
            &mut text,
            &names.peer_reorder_entries_converged_total,
            &metric_labels,
            peer.reorder_entries_converged,
        );
        metric_count += 1;
        push_metric(
            &mut text,
            &names.peer_snapshot_installed_index,
            &metric_labels,
            peer.snapshot_installed_index,
        );
        metric_count += 1;
    }

    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count,
        text,
    }
}

pub fn matrixraft_runtime_pressure_admission_prometheus(
    admission: &RuntimePressureAdmission,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let names = matrixraft_runtime_pressure_metric_names();
    let mut text = String::new();
    let rejected_component = admission.rejected_component.as_deref().unwrap_or("none");
    let mut decision_labels = labels.to_vec();
    decision_labels.push(("reason", admission.reason.as_str()));
    decision_labels.push(("rejected_component", rejected_component));

    push_metric(
        &mut text,
        &names.admission_accepted,
        &decision_labels,
        bool_metric(admission.accepted),
    );
    push_metric(
        &mut text,
        &names.admission_rejected,
        &decision_labels,
        bool_metric(!admission.accepted),
    );
    push_metric(
        &mut text,
        &names.memory_pressure,
        labels,
        bool_metric(admission.memory_pressure),
    );
    push_metric(
        &mut text,
        &names.latency_pressure,
        labels,
        bool_metric(admission.latency_pressure),
    );
    push_metric(
        &mut text,
        &names.scale_pressure,
        labels,
        bool_metric(admission.scale_pressure),
    );
    push_metric(
        &mut text,
        &names.pipeline_pressure,
        labels,
        bool_metric(admission.pipeline_pressure),
    );
    push_metric(
        &mut text,
        &names.read_backlog_pressure,
        labels,
        bool_metric(admission.read_backlog_pressure),
    );
    push_metric(
        &mut text,
        &names.node_runtime_timer_pressure,
        labels,
        bool_metric(admission.node_runtime_timer_pressure),
    );

    for detail in &admission.memory_pressure_details {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("component", detail.component.as_str()));
        push_metric(
            &mut text,
            &names.memory_pressure_observed_value,
            &detail_labels,
            detail.observed_value,
        );
        push_metric(
            &mut text,
            &names.memory_pressure_threshold_value,
            &detail_labels,
            detail.threshold_value,
        );
        push_metric(
            &mut text,
            &names.memory_pressure_excess,
            &detail_labels,
            detail.excess,
        );
    }
    if admission.memory_pressure_details.is_empty() {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("component", "none"));
        push_metric(
            &mut text,
            &names.memory_pressure_observed_value,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.memory_pressure_threshold_value,
            &detail_labels,
            0,
        );
        push_metric(&mut text, &names.memory_pressure_excess, &detail_labels, 0);
    }

    for detail in &admission.latency_pressure_details {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("component", detail.component.as_str()));
        push_metric(
            &mut text,
            &names.latency_pressure_sample_count,
            &detail_labels,
            detail.sample_count,
        );
        push_metric(
            &mut text,
            &names.latency_pressure_observed_p95_ms,
            &detail_labels,
            detail.observed_p95_ms,
        );
        push_metric(
            &mut text,
            &names.latency_pressure_observed_p99_ms,
            &detail_labels,
            detail.observed_p99_ms,
        );
        push_metric(
            &mut text,
            &names.latency_pressure_threshold_p99_ms,
            &detail_labels,
            detail.threshold_p99_ms,
        );
        push_metric(
            &mut text,
            &names.latency_pressure_excess_ms,
            &detail_labels,
            detail.excess_ms,
        );
    }
    if admission.latency_pressure_details.is_empty() {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("component", "none"));
        push_metric(
            &mut text,
            &names.latency_pressure_sample_count,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.latency_pressure_observed_p95_ms,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.latency_pressure_observed_p99_ms,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.latency_pressure_threshold_p99_ms,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.latency_pressure_excess_ms,
            &detail_labels,
            0,
        );
    }

    for detail in &admission.scale_pressure_details {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("component", detail.component.as_str()));
        push_metric(
            &mut text,
            &names.scale_pressure_observed_value,
            &detail_labels,
            detail.observed_value,
        );
        push_metric(
            &mut text,
            &names.scale_pressure_target_value,
            &detail_labels,
            detail.target_value,
        );
        push_metric(
            &mut text,
            &names.scale_pressure_deficit,
            &detail_labels,
            detail.deficit,
        );
        push_metric(
            &mut text,
            &names.scale_pressure_target_percent,
            &detail_labels,
            detail.target_percent,
        );
    }
    if admission.scale_pressure_details.is_empty() {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("component", "none"));
        push_metric(
            &mut text,
            &names.scale_pressure_observed_value,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.scale_pressure_target_value,
            &detail_labels,
            0,
        );
        push_metric(&mut text, &names.scale_pressure_deficit, &detail_labels, 0);
        push_metric(
            &mut text,
            &names.scale_pressure_target_percent,
            &detail_labels,
            0,
        );
    }

    for detail in &admission.pipeline_pressure_details {
        let peer_id = detail.peer_id.to_string();
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("peer_id", peer_id.as_str()));
        detail_labels.push(("component", detail.component.as_str()));
        push_metric(
            &mut text,
            &names.pipeline_pressure_observed_value,
            &detail_labels,
            detail.observed_value,
        );
        push_metric(
            &mut text,
            &names.pipeline_pressure_threshold_value,
            &detail_labels,
            detail.threshold_value,
        );
        push_metric(
            &mut text,
            &names.pipeline_pressure_excess,
            &detail_labels,
            detail.excess,
        );
    }
    if admission.pipeline_pressure_details.is_empty() {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("peer_id", "none"));
        detail_labels.push(("component", "none"));
        push_metric(
            &mut text,
            &names.pipeline_pressure_observed_value,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.pipeline_pressure_threshold_value,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.pipeline_pressure_excess,
            &detail_labels,
            0,
        );
    }

    for detail in &admission.read_backlog_pressure_details {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("component", detail.component.as_str()));
        push_metric(
            &mut text,
            &names.read_backlog_pressure_observed_value,
            &detail_labels,
            detail.observed_value,
        );
        push_metric(
            &mut text,
            &names.read_backlog_pressure_threshold_value,
            &detail_labels,
            detail.threshold_value,
        );
        push_metric(
            &mut text,
            &names.read_backlog_pressure_excess,
            &detail_labels,
            detail.excess,
        );
    }
    if admission.read_backlog_pressure_details.is_empty() {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("component", "none"));
        push_metric(
            &mut text,
            &names.read_backlog_pressure_observed_value,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.read_backlog_pressure_threshold_value,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.read_backlog_pressure_excess,
            &detail_labels,
            0,
        );
    }

    for detail in &admission.node_runtime_timer_pressure_details {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("component", detail.component.as_str()));
        push_metric(
            &mut text,
            &names.node_runtime_timer_pressure_observed_percent,
            &detail_labels,
            detail.observed_percent,
        );
        push_metric(
            &mut text,
            &names.node_runtime_timer_pressure_threshold_percent,
            &detail_labels,
            detail.threshold_percent,
        );
        push_metric(
            &mut text,
            &names.node_runtime_timer_pressure_excess_percent,
            &detail_labels,
            detail.excess_percent,
        );
    }
    if admission.node_runtime_timer_pressure_details.is_empty() {
        let mut detail_labels = labels.to_vec();
        detail_labels.push(("component", "none"));
        push_metric(
            &mut text,
            &names.node_runtime_timer_pressure_observed_percent,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.node_runtime_timer_pressure_threshold_percent,
            &detail_labels,
            0,
        );
        push_metric(
            &mut text,
            &names.node_runtime_timer_pressure_excess_percent,
            &detail_labels,
            0,
        );
    }

    for action in &admission.actions {
        let mut action_labels = labels.to_vec();
        action_labels.push(("action", action.as_str()));
        push_metric(&mut text, &names.action_total, &action_labels, 1);
    }
    if admission.actions.is_empty() {
        let mut action_labels = labels.to_vec();
        action_labels.push(("action", "none"));
        push_metric(&mut text, &names.action_total, &action_labels, 0);
    }
    let action_sources = matrixraft_runtime_pressure_action_sources(admission);
    for (component, action) in &action_sources {
        let mut action_labels = labels.to_vec();
        action_labels.push(("component", component));
        action_labels.push(("action", action));
        push_metric(&mut text, &names.action_source_total, &action_labels, 1);
    }
    if action_sources.is_empty() {
        let mut action_labels = labels.to_vec();
        action_labels.push(("component", "none"));
        action_labels.push(("action", "none"));
        push_metric(&mut text, &names.action_source_total, &action_labels, 0);
    }
    let bottlenecks = matrixraft_runtime_pressure_bottleneck_summary(admission);
    for bottleneck in &bottlenecks {
        let rank = bottleneck.rank.to_string();
        let mut bottleneck_labels = labels.to_vec();
        bottleneck_labels.push(("rank", rank.as_str()));
        bottleneck_labels.push(("category", bottleneck.category.as_str()));
        bottleneck_labels.push(("component", bottleneck.component.as_str()));
        push_metric(
            &mut text,
            &names.bottleneck_score_percent,
            &bottleneck_labels,
            bottleneck.score_percent,
        );
    }
    if bottlenecks.is_empty() {
        let mut bottleneck_labels = labels.to_vec();
        bottleneck_labels.push(("rank", "0"));
        bottleneck_labels.push(("category", "none"));
        bottleneck_labels.push(("component", "none"));
        push_metric(
            &mut text,
            &names.bottleneck_score_percent,
            &bottleneck_labels,
            0,
        );
    }

    let metric_count = text.lines().count() as u64;
    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count,
        text,
    }
}

pub fn matrixraft_snapshot_lifecycle_evidence_prometheus(
    evidence: &SnapshotLifecycleEvidence,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let names = matrixraft_snapshot_lifecycle_metric_names();
    let mut text = String::new();
    let signals = [
        (
            &names.sender_lifecycle_present,
            "sender_lifecycle",
            evidence.sender_lifecycle_present,
        ),
        (
            &names.downloader_lifecycle_present,
            "downloader_lifecycle",
            evidence.downloader_lifecycle_present,
        ),
        (
            &names.retry_backpressure_present,
            "retry_backpressure",
            evidence.retry_backpressure_present,
        ),
        (
            &names.chunk_retry_present,
            "chunk_retry",
            evidence.chunk_retry_present,
        ),
        (
            &names.send_timeout_present,
            "send_timeout",
            evidence.send_timeout_present,
        ),
        (
            &names.rate_limit_present,
            "rate_limit",
            evidence.rate_limit_present,
        ),
        (
            &names.sustained_sender_load_present,
            "sustained_sender_load",
            evidence.sustained_sender_load_present,
        ),
        (
            &names.sustained_downloader_load_present,
            "sustained_downloader_load",
            evidence.sustained_downloader_load_present,
        ),
        (
            &names.sustained_sender_completion_present,
            "sustained_sender_completion",
            evidence.sustained_sender_completion_present,
        ),
        (
            &names.sustained_downloader_completion_present,
            "sustained_downloader_completion",
            evidence.sustained_downloader_completion_present,
        ),
        (
            &names.sustained_transfer_completion_present,
            "sustained_transfer_completion",
            evidence.sustained_transfer_completion_present,
        ),
        (
            &names.install_progress_present,
            "install_progress",
            evidence.install_progress_present,
        ),
        (
            &names.install_rollback_present,
            "install_rollback",
            evidence.install_rollback_present,
        ),
        (
            &names.membership_change_present,
            "membership_change",
            evidence.membership_change_present,
        ),
        (
            &names.rejoin_after_compacted_log_present,
            "rejoin_after_compacted_log",
            evidence.rejoin_after_compacted_log_present,
        ),
    ];
    let metric_count = signals.len() as u64;
    for (metric_name, signal, present) in signals {
        let mut signal_labels = labels.to_vec();
        signal_labels.push(("signal", signal));
        push_metric(&mut text, metric_name, &signal_labels, bool_metric(present));
    }
    push_metric(
        &mut text,
        &names.snapshot_peer_count,
        labels,
        evidence.snapshot_peer_count,
    );
    push_metric(
        &mut text,
        &names.sustained_transfer_completed_peer_count,
        labels,
        evidence.sustained_transfer_completed_peer_count,
    );
    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count: metric_count + 2,
        text,
    }
}

pub fn matrixraft_wal_lifecycle_evidence_prometheus(
    evidence: &WalLifecycleEvidence,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let names = matrixraft_wal_lifecycle_metric_names();
    let mut text = String::new();
    let signals = [
        (
            &names.segment_lifecycle_present,
            "segment_lifecycle",
            evidence.segment_lifecycle_present,
        ),
        (
            &names.retained_range_present,
            "retained_range",
            evidence.retained_range_present,
        ),
        (
            &names.sequence_range_present,
            "sequence_range",
            evidence.sequence_range_present,
        ),
        (
            &names.log_index_range_present,
            "log_index_range",
            evidence.log_index_range_present,
        ),
        (
            &names.compaction_observed,
            "compaction",
            evidence.compaction_observed,
        ),
        (
            &names.slow_fsync_backpressure_observed,
            "slow_fsync_backpressure",
            evidence.slow_fsync_backpressure_observed,
        ),
        (
            &names.compaction_after_slow_fsync_observed,
            "compaction_after_slow_fsync",
            evidence.compaction_after_slow_fsync_observed,
        ),
    ];
    let metric_count = signals.len() as u64;
    for (metric_name, signal, present) in signals {
        let mut signal_labels = labels.to_vec();
        signal_labels.push(("signal", signal));
        push_metric(&mut text, metric_name, &signal_labels, bool_metric(present));
    }
    let count_signals = [
        (
            &names.released_segment_count,
            "released_segment_count",
            evidence.released_segment_count,
        ),
        (
            &names.compacted_after_slow_fsync_count,
            "compacted_after_slow_fsync_count",
            evidence.compacted_after_slow_fsync_count,
        ),
        (
            &names.slow_fsync_segment_count,
            "slow_fsync_segment_count",
            evidence.slow_fsync_segment_count,
        ),
        (
            &names.compacted_slow_fsync_segment_count,
            "compacted_slow_fsync_segment_count",
            evidence.compacted_slow_fsync_segment_count,
        ),
    ];
    let count_metric_count = count_signals.len() as u64;
    for (metric_name, signal, value) in count_signals {
        let mut signal_labels = labels.to_vec();
        signal_labels.push(("signal", signal));
        push_metric(&mut text, metric_name, &signal_labels, value);
    }
    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count: metric_count + count_metric_count,
        text,
    }
}

pub fn matrixraft_membership_readiness_prometheus(
    report: &MembershipReadinessReport,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let names = matrixraft_membership_readiness_metric_names();
    let mut text = String::new();
    push_metric(&mut text, &names.ready, labels, bool_metric(report.ready));
    push_metric(
        &mut text,
        &names.satisfied_total,
        labels,
        report.satisfied.len() as u64,
    );
    push_metric(
        &mut text,
        &names.missing_total,
        labels,
        report.missing.len() as u64,
    );

    let mut metric_count = 3;
    for decision in &report.decisions {
        let scope = membership_scope_label(decision.scope);
        let transition = membership_transition_label(decision.transition);
        let mut transition_labels = labels.to_vec();
        transition_labels.push(("scope", scope));
        transition_labels.push(("transition", transition));
        push_metric(
            &mut text,
            &names.transition_ready,
            &transition_labels,
            bool_metric(decision.ready),
        );
        push_metric(
            &mut text,
            &names.transition_missing_total,
            &transition_labels,
            decision.missing.len() as u64,
        );
        metric_count += 2;
        for missing in &decision.missing {
            let mut missing_labels = transition_labels.clone();
            missing_labels.push(("missing", missing.as_str()));
            push_metric(&mut text, &names.transition_missing, &missing_labels, 1);
            metric_count += 1;
        }
    }

    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count,
        text,
    }
}

pub fn matrixraft_membership_readiness_diagnostic_log_entries(
    report: &MembershipReadinessReport,
) -> Vec<DiagnosticLogEntry> {
    let mut entries = Vec::with_capacity(1 + report.decisions.len());
    entries.push(DiagnosticLogEntry {
        target: "rustraft.membership_readiness".to_string(),
        severity: if report.ready {
            DiagnosticSeverity::Info
        } else {
            DiagnosticSeverity::Warn
        },
        message: if report.ready {
            "membership readiness satisfied".to_string()
        } else {
            "membership readiness blocked".to_string()
        },
        fields: vec![
            ("ready".to_string(), report.ready.to_string()),
            (
                "satisfied_total".to_string(),
                report.satisfied.len().to_string(),
            ),
            (
                "missing_total".to_string(),
                report.missing.len().to_string(),
            ),
            (
                "transition_count".to_string(),
                report.decisions.len().to_string(),
            ),
        ],
    });
    entries.extend(report.decisions.iter().map(|decision| {
        let scope = membership_scope_label(decision.scope);
        let transition = membership_transition_label(decision.transition);
        DiagnosticLogEntry {
            target: format!("rustraft.membership_readiness.{scope}.{transition}"),
            severity: if decision.ready {
                DiagnosticSeverity::Info
            } else {
                DiagnosticSeverity::Warn
            },
            message: if decision.ready {
                "membership transition readiness satisfied".to_string()
            } else {
                "membership transition readiness missing evidence".to_string()
            },
            fields: vec![
                ("scope".to_string(), scope.to_string()),
                ("transition".to_string(), transition.to_string()),
                ("ready".to_string(), decision.ready.to_string()),
                (
                    "missing_count".to_string(),
                    decision.missing.len().to_string(),
                ),
                ("missing".to_string(), decision.missing.join(",")),
            ],
        }
    }));
    entries
}

pub fn matrixraft_membership_readiness_diagnostic_json_lines(
    report: &MembershipReadinessReport,
) -> String {
    matrixraft_membership_readiness_diagnostic_log_entries(report)
        .into_iter()
        .map(|entry| {
            serde_json::to_string(&entry)
                .expect("RustRaft membership readiness diagnostic entry must serialize")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn membership_scope_label(scope: MembershipScope) -> &'static str {
    match scope {
        MembershipScope::Metaserver => "metaserver",
        MembershipScope::DataNode => "data_node",
    }
}

fn membership_transition_label(transition: MembershipTransitionKind) -> &'static str {
    match transition {
        MembershipTransitionKind::Failover => "failover",
        MembershipTransitionKind::ScaleUp => "scale_up",
        MembershipTransitionKind::ScaleDown => "scale_down",
    }
}

pub fn matrixraft_runtime_pressure_diagnostic_log_entries(
    admission: &RuntimePressureAdmission,
) -> Vec<DiagnosticLogEntry> {
    let severity = if !admission.accepted {
        DiagnosticSeverity::Error
    } else if admission.memory_pressure
        || admission.latency_pressure
        || admission.scale_pressure
        || admission.pipeline_pressure
        || admission.read_backlog_pressure
        || admission.node_runtime_timer_pressure
    {
        DiagnosticSeverity::Warn
    } else {
        DiagnosticSeverity::Info
    };
    let mut action_provenance = BTreeSet::new();
    action_provenance.extend(
        admission
            .memory_pressure_details
            .iter()
            .filter_map(|detail| {
                let actions =
                    matrixraft_runtime_pressure_recommended_actions_field(&detail.component);
                (!actions.is_empty()).then(|| format!("{}=>{}", detail.component, actions))
            }),
    );
    action_provenance.extend(
        admission
            .latency_pressure_details
            .iter()
            .filter_map(|detail| {
                let actions =
                    matrixraft_runtime_pressure_recommended_actions_field(&detail.component);
                (!actions.is_empty()).then(|| format!("{}=>{}", detail.component, actions))
            }),
    );
    action_provenance.extend(
        admission
            .scale_pressure_details
            .iter()
            .filter_map(|detail| {
                let actions =
                    matrixraft_runtime_pressure_recommended_actions_field(&detail.component);
                (!actions.is_empty()).then(|| format!("{}=>{}", detail.component, actions))
            }),
    );
    action_provenance.extend(
        admission
            .pipeline_pressure_details
            .iter()
            .filter_map(|detail| {
                let actions =
                    matrixraft_runtime_pressure_recommended_actions_field(&detail.component);
                (!actions.is_empty()).then(|| format!("{}=>{}", detail.component, actions))
            }),
    );
    action_provenance.extend(
        admission
            .read_backlog_pressure_details
            .iter()
            .filter_map(|detail| {
                let actions =
                    matrixraft_runtime_pressure_recommended_actions_field(&detail.component);
                (!actions.is_empty()).then(|| format!("{}=>{}", detail.component, actions))
            }),
    );
    action_provenance.extend(
        admission
            .node_runtime_timer_pressure_details
            .iter()
            .filter_map(|detail| {
                let actions =
                    matrixraft_runtime_pressure_recommended_actions_field(&detail.component);
                (!actions.is_empty()).then(|| format!("{}=>{}", detail.component, actions))
            }),
    );
    let bottlenecks = matrixraft_runtime_pressure_bottleneck_summary(admission);
    let bottleneck_summary = bottlenecks
        .iter()
        .map(|bottleneck| {
            format!(
                "{}:{}:{}pct/{}/{}",
                bottleneck.rank,
                bottleneck.component,
                bottleneck.score_percent,
                bottleneck.excess_or_deficit,
                bottleneck.threshold_or_target_value
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let top_bottleneck = bottlenecks
        .first()
        .map(|bottleneck| bottleneck.component.as_str())
        .unwrap_or("none");
    let top_bottleneck_category = bottlenecks
        .first()
        .map(|bottleneck| bottleneck.category.as_str())
        .unwrap_or("none");
    let top_bottleneck_score_percent = bottlenecks
        .first()
        .map(|bottleneck| bottleneck.score_percent)
        .unwrap_or(0);
    let top_bottleneck_excess_or_deficit = bottlenecks
        .first()
        .map(|bottleneck| bottleneck.excess_or_deficit)
        .unwrap_or(0);
    let top_bottleneck_threshold_or_target = bottlenecks
        .first()
        .map(|bottleneck| bottleneck.threshold_or_target_value)
        .unwrap_or(0);
    let mut entries = Vec::with_capacity(
        1 + admission.memory_pressure_details.len()
            + admission.latency_pressure_details.len()
            + admission.scale_pressure_details.len()
            + admission.pipeline_pressure_details.len()
            + admission.read_backlog_pressure_details.len()
            + admission.node_runtime_timer_pressure_details.len(),
    );
    entries.push(DiagnosticLogEntry {
        target: "rustraft.runtime_pressure.admission".to_string(),
        severity,
        message: admission.reason.clone(),
        fields: vec![
            ("accepted".to_string(), admission.accepted.to_string()),
            (
                "memory_pressure".to_string(),
                admission.memory_pressure.to_string(),
            ),
            (
                "memory_pressure_detail_count".to_string(),
                admission.memory_pressure_details.len().to_string(),
            ),
            (
                "memory_pressure_details".to_string(),
                admission
                    .memory_pressure_details
                    .iter()
                    .map(|detail| {
                        format!(
                            "{}:{}/{}/excess={}",
                            detail.component,
                            detail.observed_value,
                            detail.threshold_value,
                            detail.excess
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "latency_pressure".to_string(),
                admission.latency_pressure.to_string(),
            ),
            (
                "latency_pressure_detail_count".to_string(),
                admission.latency_pressure_details.len().to_string(),
            ),
            (
                "latency_pressure_details".to_string(),
                admission
                    .latency_pressure_details
                    .iter()
                    .map(|detail| {
                        format!(
                            "{}:samples={}/p95={}ms/p99={}ms/threshold={}ms/excess={}",
                            detail.component,
                            detail.sample_count,
                            detail.observed_p95_ms,
                            detail.observed_p99_ms,
                            detail.threshold_p99_ms,
                            detail.excess_ms
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "scale_pressure".to_string(),
                admission.scale_pressure.to_string(),
            ),
            (
                "pipeline_pressure".to_string(),
                admission.pipeline_pressure.to_string(),
            ),
            (
                "read_backlog_pressure".to_string(),
                admission.read_backlog_pressure.to_string(),
            ),
            (
                "node_runtime_timer_pressure".to_string(),
                admission.node_runtime_timer_pressure.to_string(),
            ),
            (
                "scale_pressure_detail_count".to_string(),
                admission.scale_pressure_details.len().to_string(),
            ),
            (
                "scale_pressure_details".to_string(),
                admission
                    .scale_pressure_details
                    .iter()
                    .map(|detail| {
                        format!(
                            "{}:{}/{}/{}pct/deficit={}",
                            detail.component,
                            detail.observed_value,
                            detail.target_value,
                            detail.target_percent,
                            detail.deficit
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "read_backlog_pressure_detail_count".to_string(),
                admission.read_backlog_pressure_details.len().to_string(),
            ),
            (
                "read_backlog_pressure_details".to_string(),
                admission
                    .read_backlog_pressure_details
                    .iter()
                    .map(|detail| {
                        format!(
                            "{}:{}/{}/excess={}",
                            detail.component,
                            detail.observed_value,
                            detail.threshold_value,
                            detail.excess
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "node_runtime_timer_pressure_detail_count".to_string(),
                admission
                    .node_runtime_timer_pressure_details
                    .len()
                    .to_string(),
            ),
            (
                "node_runtime_timer_pressure_details".to_string(),
                admission
                    .node_runtime_timer_pressure_details
                    .iter()
                    .map(|detail| {
                        format!(
                            "{}:{}pct/{}pct/excess={}pct",
                            detail.component,
                            detail.observed_percent,
                            detail.threshold_percent,
                            detail.excess_percent
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "rejected_component".to_string(),
                admission
                    .rejected_component
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "action_count".to_string(),
                admission.actions.len().to_string(),
            ),
            ("actions".to_string(), admission.actions.join(",")),
            (
                "action_provenance".to_string(),
                action_provenance.into_iter().collect::<Vec<_>>().join(","),
            ),
            (
                "bottleneck_count".to_string(),
                bottlenecks.len().to_string(),
            ),
            ("top_bottleneck".to_string(), top_bottleneck.to_string()),
            (
                "top_bottleneck_category".to_string(),
                top_bottleneck_category.to_string(),
            ),
            (
                "top_bottleneck_score_percent".to_string(),
                top_bottleneck_score_percent.to_string(),
            ),
            (
                "top_bottleneck_excess_or_deficit".to_string(),
                top_bottleneck_excess_or_deficit.to_string(),
            ),
            (
                "top_bottleneck_threshold_or_target".to_string(),
                top_bottleneck_threshold_or_target.to_string(),
            ),
            ("bottlenecks".to_string(), bottleneck_summary),
        ],
    });
    entries.extend(
        admission
            .memory_pressure_details
            .iter()
            .map(|detail| DiagnosticLogEntry {
                target: "rustraft.runtime_pressure.memory".to_string(),
                severity,
                message: "memory pressure component".to_string(),
                fields: vec![
                    ("component".to_string(), detail.component.clone()),
                    (
                        "observed_value".to_string(),
                        detail.observed_value.to_string(),
                    ),
                    (
                        "threshold_value".to_string(),
                        detail.threshold_value.to_string(),
                    ),
                    ("excess".to_string(), detail.excess.to_string()),
                    (
                        "recommended_actions".to_string(),
                        matrixraft_runtime_pressure_recommended_actions_field(&detail.component),
                    ),
                    ("accepted".to_string(), admission.accepted.to_string()),
                ],
            }),
    );
    entries.extend(
        admission
            .latency_pressure_details
            .iter()
            .map(|detail| DiagnosticLogEntry {
                target: "rustraft.runtime_pressure.latency".to_string(),
                severity,
                message: "latency pressure component".to_string(),
                fields: vec![
                    ("component".to_string(), detail.component.clone()),
                    ("sample_count".to_string(), detail.sample_count.to_string()),
                    (
                        "observed_p95_ms".to_string(),
                        detail.observed_p95_ms.to_string(),
                    ),
                    (
                        "observed_p99_ms".to_string(),
                        detail.observed_p99_ms.to_string(),
                    ),
                    (
                        "threshold_p99_ms".to_string(),
                        detail.threshold_p99_ms.to_string(),
                    ),
                    ("excess_ms".to_string(), detail.excess_ms.to_string()),
                    (
                        "recommended_actions".to_string(),
                        matrixraft_runtime_pressure_recommended_actions_field(&detail.component),
                    ),
                    ("accepted".to_string(), admission.accepted.to_string()),
                ],
            }),
    );
    entries.extend(
        admission
            .scale_pressure_details
            .iter()
            .map(|detail| DiagnosticLogEntry {
                target: "rustraft.runtime_pressure.scale".to_string(),
                severity,
                message: "scale pressure component".to_string(),
                fields: vec![
                    ("component".to_string(), detail.component.clone()),
                    (
                        "observed_value".to_string(),
                        detail.observed_value.to_string(),
                    ),
                    ("target_value".to_string(), detail.target_value.to_string()),
                    (
                        "target_percent".to_string(),
                        detail.target_percent.to_string(),
                    ),
                    ("deficit".to_string(), detail.deficit.to_string()),
                    (
                        "recommended_actions".to_string(),
                        matrixraft_runtime_pressure_recommended_actions_field(&detail.component),
                    ),
                    ("accepted".to_string(), admission.accepted.to_string()),
                ],
            }),
    );
    entries.extend(
        admission
            .pipeline_pressure_details
            .iter()
            .map(|detail| DiagnosticLogEntry {
                target: "rustraft.runtime_pressure.pipeline".to_string(),
                severity,
                message: "pipeline pressure component".to_string(),
                fields: vec![
                    ("peer_id".to_string(), detail.peer_id.to_string()),
                    ("component".to_string(), detail.component.clone()),
                    (
                        "observed_value".to_string(),
                        detail.observed_value.to_string(),
                    ),
                    (
                        "threshold_value".to_string(),
                        detail.threshold_value.to_string(),
                    ),
                    ("excess".to_string(), detail.excess.to_string()),
                    (
                        "recommended_actions".to_string(),
                        matrixraft_runtime_pressure_recommended_actions_field(&detail.component),
                    ),
                    ("accepted".to_string(), admission.accepted.to_string()),
                ],
            }),
    );
    entries.extend(
        admission
            .read_backlog_pressure_details
            .iter()
            .map(|detail| DiagnosticLogEntry {
                target: "rustraft.runtime_pressure.read_backlog".to_string(),
                severity,
                message: "read backlog pressure component".to_string(),
                fields: vec![
                    ("component".to_string(), detail.component.clone()),
                    (
                        "observed_value".to_string(),
                        detail.observed_value.to_string(),
                    ),
                    (
                        "threshold_value".to_string(),
                        detail.threshold_value.to_string(),
                    ),
                    ("excess".to_string(), detail.excess.to_string()),
                    (
                        "recommended_actions".to_string(),
                        matrixraft_runtime_pressure_recommended_actions_field(&detail.component),
                    ),
                    ("accepted".to_string(), admission.accepted.to_string()),
                ],
            }),
    );
    entries.extend(
        admission
            .node_runtime_timer_pressure_details
            .iter()
            .map(|detail| DiagnosticLogEntry {
                target: "rustraft.runtime_pressure.node_runtime_timer".to_string(),
                severity,
                message: "node runtime timer pressure component".to_string(),
                fields: vec![
                    ("component".to_string(), detail.component.clone()),
                    (
                        "observed_percent".to_string(),
                        detail.observed_percent.to_string(),
                    ),
                    (
                        "threshold_percent".to_string(),
                        detail.threshold_percent.to_string(),
                    ),
                    (
                        "excess_percent".to_string(),
                        detail.excess_percent.to_string(),
                    ),
                    (
                        "recommended_actions".to_string(),
                        matrixraft_runtime_pressure_recommended_actions_field(&detail.component),
                    ),
                    ("accepted".to_string(), admission.accepted.to_string()),
                ],
            }),
    );
    entries
}

pub fn matrixraft_runtime_pressure_diagnostic_json_lines(
    admission: &RuntimePressureAdmission,
) -> String {
    matrixraft_runtime_pressure_diagnostic_log_entries(admission)
        .into_iter()
        .map(|entry| {
            serde_json::to_string(&entry)
                .expect("RustRaft runtime pressure diagnostic entry must serialize")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn matrixraft_scale_optimization_hints(
    metrics: &ScaleRateMetrics,
    targets: &ScaleOptimizationTargets,
) -> Vec<OptimizationHint> {
    let mut hints = Vec::new();
    push_scale_optimization_hint(
        &mut hints,
        "proposal_qps_below_target",
        "proposal_pipeline",
        "raise proposal batching, reduce fsync stalls, or scale leaders and Raft groups",
        metrics.proposal_qps,
        targets.min_proposal_qps,
    );
    push_scale_optimization_hint(
        &mut hints,
        "append_entries_qps_below_target",
        "replication_pipeline",
        "increase per-peer append concurrency, batch more entries, or inspect network backpressure",
        metrics.append_entries_qps,
        targets.min_append_entries_qps,
    );
    push_scale_optimization_hint(
        &mut hints,
        "read_index_qps_below_target",
        "read_path",
        "reduce read-index quorum latency or route read-heavy workloads across more healthy leaders",
        metrics.read_index_qps,
        targets.min_read_index_qps,
    );
    push_scale_optimization_hint(
        &mut hints,
        "apply_entries_qps_below_target",
        "apply_pipeline",
        "increase apply batching or remove state-machine stalls before raising write load",
        metrics.apply_entries_qps,
        targets.min_apply_entries_qps,
    );
    push_scale_optimization_hint(
        &mut hints,
        "replication_throughput_below_target",
        "replication_pipeline",
        "raise append batch bytes or inspect per-peer inflight-byte and snapshot-transfer limits",
        metrics.replication_mib_per_sec,
        targets.min_replication_mib_per_sec,
    );
    push_scale_optimization_hint(
        &mut hints,
        "apply_throughput_below_target",
        "apply_pipeline",
        "increase apply batch bytes or profile downstream state-machine write amplification",
        metrics.apply_mib_per_sec,
        targets.min_apply_mib_per_sec,
    );
    hints
}

fn push_scale_optimization_hint(
    hints: &mut Vec<OptimizationHint>,
    id: &str,
    component: &str,
    recommendation: &str,
    observed_value: u64,
    target: u64,
) {
    if target > 0 && observed_value < target {
        hints.push(OptimizationHint {
            id: id.to_string(),
            severity: OptimizationHintSeverity::Warning,
            component: component.to_string(),
            recommendation: recommendation.to_string(),
            observed_value,
            threshold: target,
        });
    }
}

pub fn matrixraft_memory_optimization_hints(
    metrics: &MemoryMetrics,
    thresholds: &MemoryOptimizationThresholds,
) -> Vec<OptimizationHint> {
    let mut hints = Vec::new();
    push_memory_optimization_hint(
        &mut hints,
        "process_resident_memory_high",
        "memory",
        "inspect allocator fragmentation, Raft log cache retention, snapshot buffers, and replication buffers",
        metrics.process_resident_memory_bytes,
        thresholds.process_resident_warning_bytes,
    );
    push_memory_optimization_hint(
        &mut hints,
        "heap_allocated_memory_high",
        "memory",
        "inspect heap allocation growth and reduce retained Raft message, entry, and callback state",
        metrics.heap_allocated_bytes,
        thresholds.heap_allocated_warning_bytes,
    );
    push_memory_optimization_hint(
        &mut hints,
        "log_cache_memory_high",
        "log_cache",
        "tighten log cache retention or compact applied entries sooner after snapshot fences advance",
        metrics.log_cache_bytes,
        thresholds.log_cache_warning_bytes,
    );
    push_memory_optimization_hint(
        &mut hints,
        "snapshot_buffer_memory_high",
        "snapshot",
        "reduce snapshot chunk concurrency or lower snapshot sender/downloader buffer limits",
        metrics.snapshot_buffer_bytes,
        thresholds.snapshot_buffer_warning_bytes,
    );
    push_memory_optimization_hint(
        &mut hints,
        "replication_buffer_memory_high",
        "replication_pipeline",
        "lower per-peer append inflight byte limits or reduce append batch bytes",
        metrics.replication_buffer_bytes,
        thresholds.replication_buffer_warning_bytes,
    );
    hints
}

fn push_memory_optimization_hint(
    hints: &mut Vec<OptimizationHint>,
    id: &str,
    component: &str,
    recommendation: &str,
    observed_value: u64,
    threshold: u64,
) {
    if threshold > 0 && observed_value >= threshold {
        hints.push(OptimizationHint {
            id: id.to_string(),
            severity: OptimizationHintSeverity::Warning,
            component: component.to_string(),
            recommendation: recommendation.to_string(),
            observed_value,
            threshold,
        });
    }
}

fn matrixraft_merge_memory_optimization_hints(
    mut report: OptimizationReport,
    memory_metrics: &MemoryMetrics,
) -> OptimizationReport {
    report.hints.extend(matrixraft_memory_optimization_hints(
        memory_metrics,
        &MemoryOptimizationThresholds::default(),
    ));
    report.hints.sort_by(|left, right| {
        right
            .severity
            .cmp(&left.severity)
            .then_with(|| left.id.cmp(&right.id))
    });
    report.hint_count = report.hints.len() as u64;
    report.critical_count = report
        .hints
        .iter()
        .filter(|hint| hint.severity == OptimizationHintSeverity::Critical)
        .count() as u64;
    report.warning_count = report
        .hints
        .iter()
        .filter(|hint| hint.severity == OptimizationHintSeverity::Warning)
        .count() as u64;
    report.ready = report.critical_count == 0;
    report
}

fn matrixraft_merge_scale_optimization_hints(
    mut report: OptimizationReport,
    scale_rates: &ScaleRateMetrics,
    scale_targets: &ScaleOptimizationTargets,
) -> OptimizationReport {
    report.hints.extend(matrixraft_scale_optimization_hints(
        scale_rates,
        scale_targets,
    ));
    report.hints.sort_by(|left, right| {
        right
            .severity
            .cmp(&left.severity)
            .then_with(|| left.id.cmp(&right.id))
    });
    report.hint_count = report.hints.len() as u64;
    report.critical_count = report
        .hints
        .iter()
        .filter(|hint| hint.severity == OptimizationHintSeverity::Critical)
        .count() as u64;
    report.warning_count = report
        .hints
        .iter()
        .filter(|hint| hint.severity == OptimizationHintSeverity::Warning)
        .count() as u64;
    report.ready = report.critical_count == 0;
    report
}

pub fn matrixraft_latency_metrics_prometheus(
    metrics: &LatencyMetrics,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let names = matrixraft_metric_names();
    let mut text = String::new();
    let mut metric_count = 0_u64;

    metric_count += push_latency_histogram(
        &mut text,
        &names.append_latency_ms,
        labels,
        &metrics.append_latency_ms,
    );
    metric_count += push_latency_histogram(
        &mut text,
        &names.vote_latency_ms,
        labels,
        &metrics.vote_latency_ms,
    );
    metric_count += push_latency_histogram(
        &mut text,
        &names.pre_vote_latency_ms,
        labels,
        &metrics.pre_vote_latency_ms,
    );
    metric_count += push_latency_histogram(
        &mut text,
        &names.read_index_latency_ms,
        labels,
        &metrics.read_index_latency_ms,
    );
    metric_count += push_latency_histogram(
        &mut text,
        &names.snapshot_install_latency_ms,
        labels,
        &metrics.snapshot_install_latency_ms,
    );

    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count,
        text,
    }
}

pub fn matrixraft_alert_rules() -> Vec<AlertRule> {
    let metrics = matrixraft_metric_names();
    let memory_metrics = matrixraft_memory_metric_names();
    let runtime_pressure_metrics = matrixraft_runtime_pressure_metric_names();
    let snapshot_lifecycle_metrics = matrixraft_snapshot_lifecycle_metric_names();
    let wal_lifecycle_metrics = matrixraft_wal_lifecycle_metric_names();
    let membership_readiness_metrics = matrixraft_membership_readiness_metric_names();
    let production_readiness_metrics = matrixraft_production_readiness_metric_names();
    let benchmark_metrics = matrixraft_baseline_raft_benchmark_metric_names();
    let memory_thresholds = MemoryOptimizationThresholds::default();
    let latency_thresholds = LatencyOptimizationThresholds::default();
    vec![
        AlertRule {
            alert: "RustRaftOptimizationNotReady".to_string(),
            expr: format!("{} == 0", metrics.optimization_ready),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft optimization readiness is not passing; follow resolve_critical_optimization_hints."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftOptimizationCriticalHints".to_string(),
            expr: format!("{} > 0", metrics.optimization_critical_total),
            duration: "5m".to_string(),
            severity: "critical".to_string(),
            summary:
                "RustRaft has critical optimization hints; follow resolve_critical_optimization_hints before rollout."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftOptimizationWarningHints".to_string(),
            expr: format!("{} > 0", metrics.optimization_warning_total),
            duration: "10m".to_string(),
            severity: "warning".to_string(),
            summary: "RustRaft has warning optimization hints to review before rollout."
                .to_string(),
        },
        AlertRule {
            alert: "RustRaftMemoryPressure".to_string(),
            expr: format!(
                "{} >= {} or {} >= {} or {} >= {} or {} >= {} or {} >= {}",
                memory_metrics.process_resident_memory_bytes,
                memory_thresholds.process_resident_warning_bytes,
                memory_metrics.heap_allocated_bytes,
                memory_thresholds.heap_allocated_warning_bytes,
                memory_metrics.log_cache_bytes,
                memory_thresholds.log_cache_warning_bytes,
                memory_metrics.snapshot_buffer_bytes,
                memory_thresholds.snapshot_buffer_warning_bytes,
                memory_metrics.replication_buffer_bytes,
                memory_thresholds.replication_buffer_warning_bytes
            ),
            duration: "10m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft memory pressure crossed the default warning threshold; inspect memory_prometheus and review warning optimization hints."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftLatencyPressure".to_string(),
            expr: format!(
                "{} > {} or {} > {} or {} > {} or {} > {} or {} > {}",
                percentile_expr(&metrics.append_latency_ms, "0.99"),
                latency_thresholds.append_p99_warning_ms,
                percentile_expr(&metrics.vote_latency_ms, "0.99"),
                latency_thresholds.vote_p99_warning_ms,
                percentile_expr(&metrics.pre_vote_latency_ms, "0.99"),
                latency_thresholds.pre_vote_p99_warning_ms,
                percentile_expr(&metrics.read_index_latency_ms, "0.99"),
                latency_thresholds.read_index_p99_warning_ms,
                percentile_expr(&metrics.snapshot_install_latency_ms, "0.99"),
                latency_thresholds.snapshot_install_p99_warning_ms
            ),
            duration: "10m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft p99 latency crossed the default warning threshold; inspect latency_prometheus and release-scale Grafana panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftRuntimeAdmissionRejected".to_string(),
            expr: format!("{} > 0", runtime_pressure_metrics.admission_rejected),
            duration: "1m".to_string(),
            severity: "critical".to_string(),
            summary:
                "RustRaft runtime pressure admission rejected work; inspect Runtime Admission Rejected, Runtime Pressure Bottlenecks, Runtime Pressure Actions, and Runtime Pressure Action Sources panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftRuntimePressureBottleneckActive".to_string(),
            expr: format!("{} > 0", runtime_pressure_metrics.bottleneck_score_percent),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft runtime pressure bottleneck score is active; inspect Runtime Pressure Bottlenecks and Runtime Pressure Action Sources before accepting QPS, latency, or memory parity."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftRuntimePressureFreshnessLow".to_string(),
            expr: format!("{} == 0", runtime_pressure_metrics.freshness_low_fresh),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft runtime-pressure evidence has little freshness runway left; refresh release-scale QPS, latency, and memory evidence before accepting parity."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftRuntimePressureFreshnessLost".to_string(),
            expr: format!("{} == 0", runtime_pressure_metrics.freshness_fresh),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft runtime-pressure evidence is stale or invalid; rerun release-scale QPS, latency, and memory evidence before trusting parity dashboards."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftRuntimePressureFreshnessInvalid".to_string(),
            expr: format!("{} > 0", runtime_pressure_metrics.freshness_issue_total),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft runtime-pressure freshness metadata is malformed; inspect freshness issues and regenerate release-scale runtime-pressure evidence."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftRuntimeScalePressure".to_string(),
            expr: format!("{} > 0", runtime_pressure_metrics.scale_pressure),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft release-scale admission observed QPS or throughput target pressure; inspect Runtime Scale Pressure and target-attainment panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftRuntimePipelinePressure".to_string(),
            expr: format!("{} > 0", runtime_pressure_metrics.pipeline_pressure),
            duration: "1m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft runtime admission observed peer pipeline pressure; inspect Runtime Pipeline Pressure and per-peer pipeline panels before trusting QPS and p99 latency."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftRuntimeReadBacklogPressure".to_string(),
            expr: format!("{} > 0", runtime_pressure_metrics.read_backlog_pressure),
            duration: "1m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft runtime admission observed pending read backlog pressure; inspect Runtime Read Backlog panels before trusting read-index QPS or tail latency."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftNodeRuntimeTimerBackpressure".to_string(),
            expr: "rustraft_node_runtime_timer_utilization_percent >= 80 or rustraft_node_runtime_timer_backpressure > 0 or rate(rustraft_node_runtime_timer_rejected_ticks_total[1m]) > 0"
                .to_string(),
            duration: "1m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft node-runtime timer loop is near capacity, backing up, or rejecting ticks; inspect Node Runtime Timer Utilization, Tick Backpressure, and Rejected Ticks panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftSnapshotRetryBackpressure".to_string(),
            expr: format!("{} > 0", snapshot_lifecycle_metrics.retry_backpressure_present),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft snapshot sender/downloader observed retry or max-inflight backpressure; inspect Snapshot Retry Backpressure and sustained lifecycle panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftWalSlowFsyncBackpressure".to_string(),
            expr: format!("{} > 0", wal_lifecycle_metrics.slow_fsync_backpressure_observed),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft WAL observed slow-fsync backpressure; inspect WAL Slow Fsync Backpressure and Compaction After Slow Fsync panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftPeerPipelineBackpressure".to_string(),
            expr: format!(
                "sum({}) > 0 or sum({}) > 0",
                metrics.peer_append_queue_depth, metrics.peer_reorder_queue_depth
            ),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft peer replication pipeline has sustained append or reorder queue depth; inspect per-peer pipeline panels before trusting QPS and latency parity."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftMembershipTransitionMissing".to_string(),
            expr: format!(
                "{} > 0 or {} > 0",
                membership_readiness_metrics.missing_total,
                membership_readiness_metrics.transition_missing_total
            ),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft membership transition evidence is missing; inspect membership readiness panels for joint consensus, learner catch-up, witness, and scheduler gaps."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftProductionReadinessBlocked".to_string(),
            expr: format!("{} > 0", production_readiness_metrics.blocker_total),
            duration: "1m".to_string(),
            severity: "critical".to_string(),
            summary:
                "RustRaft production readiness is blocked; inspect Production Readiness Blockers and Production Blocker Detail panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftProductionReadinessRuntimePressureBottleneck".to_string(),
            expr: format!(
                "{} > 0",
                production_readiness_metrics.runtime_pressure_bottleneck_score_percent
            ),
            duration: "1m".to_string(),
            severity: "critical".to_string(),
            summary:
                "RustRaft production readiness is blocked by ranked runtime-pressure bottlenecks; inspect Production Runtime Pressure Bottlenecks and Runtime Pressure Action Sources before rollout."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftProductionReadinessMissingEvidence".to_string(),
            expr: format!("{} > 0", production_readiness_metrics.missing_total),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft production readiness has missing evidence; inspect Production Readiness Missing and Production Missing Evidence panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftBaselineRaftBenchmarkFailed".to_string(),
            expr: format!(
                "{} == 0 or {} > 0",
                benchmark_metrics.passed, benchmark_metrics.failed_workload_total
            ),
            duration: "5m".to_string(),
            severity: "critical".to_string(),
            summary:
                "RustRaft release BaselineRaft parity benchmark failed; inspect benchmark failed workloads, blockers, and ratio panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftBaselineRaftBenchmarkRatioRegression".to_string(),
            expr: format!(
                "{} > 1.1 or {} > 1.1 or {} < 0.9",
                benchmark_metrics.worst_p50_ratio,
                benchmark_metrics.worst_p99_ratio,
                benchmark_metrics.worst_throughput_ratio
            ),
            duration: "10m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft release benchmark parity ratios exceeded the default 10 percent tolerance; inspect workload p50/p99/throughput ratio panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftBaselineRaftBenchmarkResourceRegression".to_string(),
            expr: format!(
                "{} > 1.1 or {} > 1.1",
                benchmark_metrics.worst_cpu_ratio,
                benchmark_metrics.worst_peak_resident_memory_ratio
            ),
            duration: "10m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft release benchmark resource ratios exceeded the default 10 percent tolerance; inspect CPU and peak resident-memory parity panels."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftBaselineRaftBenchmarkFreshnessLost".to_string(),
            expr: format!("{} == 0", benchmark_metrics.fresh),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary:
                "RustRaft BaselineRaft parity benchmark evidence is stale, missing, or from the future; rerun release-mode benchmark parity before accepting QPS, latency, CPU, or memory claims."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftFatalEvents".to_string(),
            expr: format!("{} > 0", metrics.fatal_total),
            duration: "1m".to_string(),
            severity: "critical".to_string(),
            summary: "RustRaft fatal blocker events are present.".to_string(),
        },
        AlertRule {
            alert: "RustRaftDiagnosticErrors".to_string(),
            expr: format!("{}{{severity=\"error\"}} > 0", metrics.diagnostic_log_total),
            duration: "1m".to_string(),
            severity: "critical".to_string(),
            summary:
                "RustRaft diagnostic errors are present; follow inspect_error_diagnostics."
                    .to_string(),
        },
        AlertRule {
            alert: "RustRaftBlockersPresent".to_string(),
            expr: format!("{} > 0", metrics.blocker_total),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary: "RustRaft readiness blockers are present.".to_string(),
        },
        AlertRule {
            alert: "RustRaftOperatorTriageWatch".to_string(),
            expr: format!("{}{{status=\"watch\"}} > 0", metrics.operator_triage_status),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary: "RustRaft operator triage is in watch status.".to_string(),
        },
        AlertRule {
            alert: "RustRaftOperatorTriageNeedsAttention".to_string(),
            expr: format!(
                "{}{{status=\"needs_attention\"}} > 0",
                metrics.operator_triage_status
            ),
            duration: "1m".to_string(),
            severity: "critical".to_string(),
            summary: "RustRaft operator triage needs attention.".to_string(),
        },
        AlertRule {
            alert: "RustRaftRunbookCriticalSteps".to_string(),
            expr: format!(
                "{}{{severity=\"critical\"}} > 0",
                metrics.operator_runbook_step_total
            ),
            duration: "1m".to_string(),
            severity: "critical".to_string(),
            summary: "RustRaft critical runbook steps are active; inspect operator_runbook_first_step for the first action."
                .to_string(),
        },
        AlertRule {
            alert: "RustRaftDebugBundleValidationFailed".to_string(),
            expr: format!("{} == 0", metrics.debug_bundle_validation_ready),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary: "RustRaft debug bundle validation is not passing.".to_string(),
        },
        AlertRule {
            alert: "RustRaftSupportEnvelopeValidationFailed".to_string(),
            expr: format!(
                "{}{{artifact=\"support_envelope\"}} == 0",
                metrics.debug_bundle_validation_ready
            ),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary: format!(
                "RustRaft support envelope validation is not passing; inspect {}{{artifact=\"support_envelope\"}} and {}{{artifact=\"support_envelope\"}} for debug_snapshot_stale or debug_snapshot_low_fresh.",
                metrics.debug_bundle_validation_first_issue,
                metrics.debug_bundle_validation_issue
            ),
        },
        AlertRule {
            alert: "RustRaftSupportEnvelopeCritical".to_string(),
            expr: format!(
                "{}{{artifact=\"support_envelope\",support_envelope_severity=\"critical\"}} == 0",
                metrics.debug_bundle_validation_ready
            ),
            duration: "1m".to_string(),
            severity: "critical".to_string(),
            summary: format!(
                "RustRaft support envelope is critical; inspect {}{{artifact=\"support_envelope\",support_envelope_severity=\"critical\"}} and the support_envelope_status label.",
                metrics.debug_bundle_validation_first_issue
            ),
        },
        AlertRule {
            alert: "RustRaftDebugSnapshotStale".to_string(),
            expr: format!(
                "{} > {}",
                metrics.debug_snapshot_age_ms, metrics.debug_snapshot_max_age_ms
            ),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary: "RustRaft debug snapshot metadata is older than the configured freshness window."
                .to_string(),
        },
        AlertRule {
            alert: "RustRaftDebugSnapshotFreshnessLow".to_string(),
            expr: format!("{} == 0", metrics.debug_snapshot_low_fresh),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary: "RustRaft debug snapshot has less than five minutes before the freshness window expires."
                .to_string(),
        },
        AlertRule {
            alert: "RustRaftDebugSnapshotFreshnessLost".to_string(),
            expr: format!("{} == 0", metrics.debug_snapshot_fresh),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary: "RustRaft debug snapshot freshness flag is not passing.".to_string(),
        },
        AlertRule {
            alert: "RustRaftObservabilityProvisioningValidationFailed".to_string(),
            expr: format!(
                "{} == 0",
                metrics.observability_provisioning_validation_ready
            ),
            duration: "5m".to_string(),
            severity: "warning".to_string(),
            summary: "RustRaft observability provisioning validation is not passing.".to_string(),
        },
    ]
}

pub fn matrixraft_scale_grafana_panels() -> Vec<GrafanaPanel> {
    let metrics = matrixraft_scale_metric_names();
    vec![
        GrafanaPanel {
            id: 1001,
            title: "Proposal QPS".to_string(),
            panel_type: "timeseries".to_string(),
            expr: qps_expr(&metrics.proposal_qps_total),
            unit: "ops/s".to_string(),
            description: "Leader proposal throughput for release parity and saturation testing."
                .to_string(),
        },
        GrafanaPanel {
            id: 1002,
            title: "AppendEntries QPS".to_string(),
            panel_type: "timeseries".to_string(),
            expr: qps_expr(&metrics.append_entries_qps_total),
            unit: "ops/s".to_string(),
            description: "Follower append RPC throughput grouped by peer and workload.".to_string(),
        },
        GrafanaPanel {
            id: 1003,
            title: "ReadIndex QPS".to_string(),
            panel_type: "timeseries".to_string(),
            expr: qps_expr(&metrics.read_index_qps_total),
            unit: "ops/s".to_string(),
            description: "Linearizable read-index and lease-read throughput for read scale tests."
                .to_string(),
        },
        GrafanaPanel {
            id: 1004,
            title: "Apply QPS".to_string(),
            panel_type: "timeseries".to_string(),
            expr: qps_expr(&metrics.apply_entries_qps_total),
            unit: "ops/s".to_string(),
            description: "Committed entry apply throughput for state-machine bottleneck diagnosis."
                .to_string(),
        },
        GrafanaPanel {
            id: 1005,
            title: "Replication MB/s".to_string(),
            panel_type: "timeseries".to_string(),
            expr: throughput_mib_expr(&metrics.replication_bytes_total),
            unit: "MiB/s".to_string(),
            description: "Replication byte throughput for packet-loss and large-entry scale runs."
                .to_string(),
        },
        GrafanaPanel {
            id: 1006,
            title: "Apply MB/s".to_string(),
            panel_type: "timeseries".to_string(),
            expr: throughput_mib_expr(&metrics.apply_bytes_total),
            unit: "MiB/s".to_string(),
            description: "State-machine apply byte throughput for memory and latency tuning."
                .to_string(),
        },
    ]
}

pub fn matrixraft_scale_target_grafana_panels() -> Vec<GrafanaPanel> {
    let metrics = matrixraft_scale_target_metric_names();
    vec![
        GrafanaPanel {
            id: 1101,
            title: "Proposal Target".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.min_proposal_qps.clone(),
            unit: "ops/s".to_string(),
            description: "Release-scale minimum proposal QPS target derived from parity evidence."
                .to_string(),
        },
        GrafanaPanel {
            id: 1102,
            title: "AppendEntries Target".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.min_append_entries_qps.clone(),
            unit: "ops/s".to_string(),
            description:
                "Release-scale minimum AppendEntries QPS target derived from parity evidence."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1103,
            title: "ReadIndex Target".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.min_read_index_qps.clone(),
            unit: "ops/s".to_string(),
            description: "Release-scale minimum ReadIndex QPS target derived from parity evidence."
                .to_string(),
        },
        GrafanaPanel {
            id: 1104,
            title: "Apply Target".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.min_apply_entries_qps.clone(),
            unit: "ops/s".to_string(),
            description: "Release-scale minimum apply QPS target derived from parity evidence."
                .to_string(),
        },
        GrafanaPanel {
            id: 1105,
            title: "Replication Throughput Target".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.min_replication_mib_per_sec.clone(),
            unit: "MiB/s".to_string(),
            description:
                "Release-scale minimum replication throughput target derived from parity evidence."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1106,
            title: "Apply Throughput Target".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.min_apply_mib_per_sec.clone(),
            unit: "MiB/s".to_string(),
            description:
                "Release-scale minimum apply throughput target derived from parity evidence."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1107,
            title: "Proposal Target Attainment".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.proposal_target_percent,
            unit: "percent".to_string(),
            description: "Observed proposal QPS as a percentage of the release-scale target."
                .to_string(),
        },
        GrafanaPanel {
            id: 1108,
            title: "AppendEntries Target Attainment".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.append_entries_target_percent,
            unit: "percent".to_string(),
            description:
                "Observed follower append throughput as a percentage of the release-scale target."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1109,
            title: "ReadIndex Target Attainment".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.read_index_target_percent,
            unit: "percent".to_string(),
            description: "Observed read-index QPS as a percentage of the release-scale target."
                .to_string(),
        },
        GrafanaPanel {
            id: 1110,
            title: "Apply Target Attainment".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.apply_entries_target_percent,
            unit: "percent".to_string(),
            description: "Observed apply QPS as a percentage of the release-scale target."
                .to_string(),
        },
        GrafanaPanel {
            id: 1111,
            title: "Replication Throughput Target".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.replication_target_percent,
            unit: "percent".to_string(),
            description: "Observed replication MiB/s as a percentage of the release-scale target."
                .to_string(),
        },
        GrafanaPanel {
            id: 1112,
            title: "Apply Throughput Target".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.apply_target_percent,
            unit: "percent".to_string(),
            description: "Observed apply MiB/s as a percentage of the release-scale target."
                .to_string(),
        },
    ]
}

pub fn matrixraft_memory_grafana_panels() -> Vec<GrafanaPanel> {
    let metrics = matrixraft_memory_metric_names();
    vec![
        GrafanaPanel {
            id: 1007,
            title: "Resident Memory".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.process_resident_memory_bytes,
            unit: "bytes".to_string(),
            description: "Process resident memory for release scale and leak triage.".to_string(),
        },
        GrafanaPanel {
            id: 1008,
            title: "Heap Allocated".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.heap_allocated_bytes,
            unit: "bytes".to_string(),
            description: "Allocator-owned heap bytes observed by the RustRaft runtime.".to_string(),
        },
        GrafanaPanel {
            id: 1009,
            title: "Log Cache Memory".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.log_cache_bytes,
            unit: "bytes".to_string(),
            description: "In-memory Raft log cache footprint for compaction and retention tuning."
                .to_string(),
        },
        GrafanaPanel {
            id: 1010,
            title: "Snapshot Buffer Memory".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.snapshot_buffer_bytes,
            unit: "bytes".to_string(),
            description:
                "Snapshot sender/downloader buffer footprint under sustained snapshot load."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1011,
            title: "Replication Buffer Memory".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.replication_buffer_bytes,
            unit: "bytes".to_string(),
            description:
                "Per-peer replication buffer footprint for packet-loss and reorder tuning."
                    .to_string(),
        },
    ]
}

pub fn matrixraft_runtime_pressure_grafana_panels() -> Vec<GrafanaPanel> {
    let metrics = matrixraft_runtime_pressure_metric_names();
    vec![
        GrafanaPanel {
            id: 1012,
            title: "Runtime Admission Accepted".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.admission_accepted,
            unit: "bool".to_string(),
            description:
                "Latest runtime pressure admission decision; 1 means the request was accepted."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1013,
            title: "Runtime Admission Rejected".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, reason, rejected_component) ({})",
                metrics.admission_rejected
            ),
            unit: "short".to_string(),
            description:
                "Rejected admission decisions grouped by reason and component for fail-closed rollout."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1014,
            title: "Runtime Memory Pressure".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.memory_pressure,
            unit: "bool".to_string(),
            description: "Runtime admission memory-pressure signal derived from memory gauges."
                .to_string(),
        },
        GrafanaPanel {
            id: 1015,
            title: "Runtime Latency Pressure".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.latency_pressure,
            unit: "bool".to_string(),
            description: "Runtime admission p99 latency-pressure signal derived from histograms."
                .to_string(),
        },
        GrafanaPanel {
            id: 1114,
            title: "Runtime Scale Pressure".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.scale_pressure,
            unit: "bool".to_string(),
            description: "Runtime admission scale-pressure signal derived from QPS and throughput targets."
                .to_string(),
        },
        GrafanaPanel {
            id: 1016,
            title: "Runtime Pressure Actions".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, action) ({})",
                metrics.action_total
            ),
            unit: "short".to_string(),
            description:
                "Suggested runtime pressure actions, such as releasing memory or reducing append inflight bytes."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1168,
            title: "Runtime Pressure Action Sources".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component, action) ({})",
                metrics.action_source_total
            ),
            unit: "short".to_string(),
            description:
                "Runtime pressure action provenance grouped by source component and action."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1174,
            title: "Runtime Pressure Bottlenecks".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "max by (service, group, workload, rank, category, component) ({})",
                metrics.bottleneck_score_percent
            ),
            unit: "percent".to_string(),
            description:
                "Ranked runtime-pressure bottlenecks by excess or deficit percentage for QPS, latency, memory, read, timer, and peer-pipeline triage."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1180,
            title: "Runtime Pressure Freshness".to_string(),
            panel_type: "stat".to_string(),
            expr: format!(
                "max by (service, group, workload, freshness_status) ({})",
                metrics.freshness_status
            ),
            unit: "short".to_string(),
            description:
                "Runtime-pressure evidence freshness status for release-scale QPS, latency, and memory claims."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1181,
            title: "Runtime Pressure Fresh".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "max by (service, group, workload, freshness_status) ({})",
                metrics.freshness_fresh
            ),
            unit: "bool".to_string(),
            description:
                "1 means the runtime-pressure evidence is inside the configured release freshness window."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1182,
            title: "Runtime Pressure Freshness Age".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "max by (service, group, workload, freshness_status) ({})",
                metrics.freshness_age_ms
            ),
            unit: "ms".to_string(),
            description:
                "Age of the runtime-pressure evidence used for production-readiness QPS, latency, and memory gates."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1183,
            title: "Runtime Pressure Freshness Remaining".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "max by (service, group, workload, freshness_status) ({})",
                metrics.freshness_remaining_fresh_ms
            ),
            unit: "ms".to_string(),
            description:
                "Milliseconds remaining before the runtime-pressure evidence reaches the configured stale boundary."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1184,
            title: "Runtime Pressure Freshness Issues".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, issue) ({})",
                metrics.freshness_issue
            ),
            unit: "short".to_string(),
            description:
                "Runtime-pressure freshness validation issues, including stale or malformed evidence timestamps."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1185,
            title: "Runtime Pressure Generated At".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.freshness_generated_at_unix_ms.clone(),
            unit: "ms".to_string(),
            description:
                "Runtime-pressure evidence generation time in Unix milliseconds for release evidence audits."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1186,
            title: "Runtime Pressure Stale After".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.freshness_stale_after_unix_ms.clone(),
            unit: "ms".to_string(),
            description:
                "Unix millisecond boundary after which runtime-pressure evidence is considered stale."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1187,
            title: "Runtime Pressure Max Age".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.freshness_max_age_ms.clone(),
            unit: "ms".to_string(),
            description:
                "Configured maximum runtime-pressure evidence age for release dashboards.".to_string(),
        },
        GrafanaPanel {
            id: 1188,
            title: "Runtime Pressure Low Fresh Threshold".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.freshness_low_fresh_ms.clone(),
            unit: "ms".to_string(),
            description:
                "Configured low-fresh warning threshold for runtime-pressure evidence.".to_string(),
        },
        GrafanaPanel {
            id: 1189,
            title: "Runtime Pressure Freshness Issue Total".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.freshness_issue_total.clone(),
            unit: "short".to_string(),
            description:
                "Total runtime-pressure freshness validation issues for a release evidence sample."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1190,
            title: "Runtime Pressure Low Fresh".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "max by (service, group, workload, freshness_status) ({})",
                metrics.freshness_low_fresh
            ),
            unit: "bool".to_string(),
            description:
                "1 means runtime-pressure evidence has enough remaining freshness for release dashboards."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1115,
            title: "Runtime Memory Pressure Observed".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.memory_pressure_observed_value
            ),
            unit: "bytes".to_string(),
            description:
                "Per-component observed memory value used by runtime pressure admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1116,
            title: "Runtime Memory Pressure Excess".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.memory_pressure_excess
            ),
            unit: "bytes".to_string(),
            description:
                "Per-component memory pressure amount above the configured admission threshold."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1117,
            title: "Runtime Memory Pressure Thresholds".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.memory_pressure_threshold_value
            ),
            unit: "bytes".to_string(),
            description:
                "Configured per-component memory thresholds used by runtime pressure admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1118,
            title: "Runtime Latency Pressure P99".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.latency_pressure_observed_p99_ms
            ),
            unit: "ms".to_string(),
            description:
                "Per-component observed p99 latency used by runtime pressure admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1119,
            title: "Runtime Latency Pressure P95".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.latency_pressure_observed_p95_ms
            ),
            unit: "ms".to_string(),
            description:
                "Per-component observed p95 latency beside p99 so operators can distinguish broad latency pressure from tail-only spikes."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1120,
            title: "Runtime Latency Pressure Samples".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.latency_pressure_sample_count
            ),
            unit: "short".to_string(),
            description:
                "Per-component latency sample count behind the admission decision, useful for judging whether p99 pressure has enough evidence."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1121,
            title: "Runtime Latency Pressure Excess".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.latency_pressure_excess_ms
            ),
            unit: "ms".to_string(),
            description:
                "Per-component p99 latency pressure amount above the configured admission threshold."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1122,
            title: "Runtime Latency Pressure Thresholds".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.latency_pressure_threshold_p99_ms
            ),
            unit: "ms".to_string(),
            description:
                "Configured per-component p99 latency thresholds used by runtime pressure admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1123,
            title: "Runtime Scale Pressure Observed".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.scale_pressure_observed_value
            ),
            unit: "short".to_string(),
            description:
                "Per-component observed scale value used by runtime pressure admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1124,
            title: "Runtime Scale Pressure Target".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.scale_pressure_target_value
            ),
            unit: "short".to_string(),
            description:
                "Per-component production scale target used by runtime pressure admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1158,
            title: "Runtime Scale Pressure Deficit".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.scale_pressure_deficit
            ),
            unit: "short".to_string(),
            description:
                "Per-component scale pressure deficit below the configured production target."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1159,
            title: "Runtime Scale Target Percent".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.scale_pressure_target_percent
            ),
            unit: "percent".to_string(),
            description:
                "Per-component target attainment percent used by runtime scale-pressure admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1160,
            title: "Runtime Pipeline Pressure".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.pipeline_pressure,
            unit: "bool".to_string(),
            description:
                "Runtime admission pipeline-pressure signal derived from per-peer append/apply/reorder queues and backpressure rejections."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1164,
            title: "Runtime Read Backlog Pressure".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.read_backlog_pressure,
            unit: "bool".to_string(),
            description:
                "Runtime admission read-backlog signal derived from pending ReadIndex and bounded-stale read queues."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1165,
            title: "Runtime Read Backlog Detail".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.read_backlog_pressure_observed_value
            ),
            unit: "short".to_string(),
            description:
                "Observed pending read backlog values used by runtime pressure admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1166,
            title: "Runtime Read Backlog Excess".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.read_backlog_pressure_excess
            ),
            unit: "short".to_string(),
            description:
                "Pending read backlog amount above the configured admission threshold."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1167,
            title: "Runtime Read Backlog Threshold".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.read_backlog_pressure_threshold_value
            ),
            unit: "short".to_string(),
            description:
                "Configured pending read backlog thresholds used by runtime admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1170,
            title: "Runtime Node Timer Pressure".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.node_runtime_timer_pressure,
            unit: "bool".to_string(),
            description:
                "Runtime admission timer-pressure signal derived from node-runtime pending tick queue utilization."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1171,
            title: "Runtime Node Timer Observed".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.node_runtime_timer_pressure_observed_percent
            ),
            unit: "percent".to_string(),
            description:
                "Observed node-runtime timer queue utilization used by runtime pressure admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1172,
            title: "Runtime Node Timer Excess".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.node_runtime_timer_pressure_excess_percent
            ),
            unit: "percent".to_string(),
            description:
                "Node-runtime timer queue utilization above the configured admission threshold."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1173,
            title: "Runtime Node Timer Threshold".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, component) ({})",
                metrics.node_runtime_timer_pressure_threshold_percent
            ),
            unit: "percent".to_string(),
            description:
                "Configured node-runtime timer utilization threshold used by runtime admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1161,
            title: "Runtime Pipeline Pressure Detail".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, peer_id, component) ({})",
                metrics.pipeline_pressure_observed_value
            ),
            unit: "short".to_string(),
            description:
                "Per-peer pipeline pressure observed value used by runtime admission."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1162,
            title: "Runtime Pipeline Pressure Excess".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, peer_id, component) ({})",
                metrics.pipeline_pressure_excess
            ),
            unit: "short".to_string(),
            description:
                "Per-peer pipeline pressure excess over the configured saturation threshold."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1163,
            title: "Runtime Pipeline Pressure Threshold".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, group, workload, peer_id, component) ({})",
                metrics.pipeline_pressure_threshold_value
            ),
            unit: "short".to_string(),
            description:
                "Per-peer pipeline pressure threshold used by runtime admission."
                    .to_string(),
        },
    ]
}

pub fn matrixraft_node_runtime_grafana_panels() -> Vec<GrafanaPanel> {
    vec![
        GrafanaPanel {
            id: 1152,
            title: "Node Runtime Pending Ticks".to_string(),
            panel_type: "timeseries".to_string(),
            expr: "sum by (service, group, node, state) (rustraft_node_runtime_timer_pending_ticks)"
                .to_string(),
            unit: "short".to_string(),
            description: "Live node-runtime timer ticks waiting to be drained.".to_string(),
        },
        GrafanaPanel {
            id: 1153,
            title: "Node Runtime Max Pending Ticks".to_string(),
            panel_type: "timeseries".to_string(),
            expr: "sum by (service, group, node, state) (rustraft_node_runtime_timer_max_pending_ticks)"
                .to_string(),
            unit: "short".to_string(),
            description: "Configured node-runtime timer tick admission limit.".to_string(),
        },
        GrafanaPanel {
            id: 1154,
            title: "Node Runtime Accepted Ticks".to_string(),
            panel_type: "timeseries".to_string(),
            expr: "sum by (service, group, node, state) (rate(rustraft_node_runtime_timer_accepted_ticks_total[1m]))"
                .to_string(),
            unit: "ops".to_string(),
            description: "Accepted node-runtime timer ticks per second.".to_string(),
        },
        GrafanaPanel {
            id: 1155,
            title: "Node Runtime Completed Ticks".to_string(),
            panel_type: "timeseries".to_string(),
            expr: "sum by (service, group, node, state) (rate(rustraft_node_runtime_timer_completed_ticks_total[1m]))"
                .to_string(),
            unit: "ops".to_string(),
            description: "Completed node-runtime timer ticks per second.".to_string(),
        },
        GrafanaPanel {
            id: 1156,
            title: "Node Runtime Tick Backpressure".to_string(),
            panel_type: "timeseries".to_string(),
            expr: "sum by (service, group, node, state) (rustraft_node_runtime_timer_backpressure)"
                .to_string(),
            unit: "bool".to_string(),
            description:
                "Live node-runtime timer-loop backpressure derived from pending or rejected ticks."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1157,
            title: "Node Runtime Rejected Ticks".to_string(),
            panel_type: "timeseries".to_string(),
            expr: "sum by (service, group, node, state) (rate(rustraft_node_runtime_timer_rejected_ticks_total[1m]))"
                .to_string(),
            unit: "ops".to_string(),
            description:
                "Rejected node-runtime timer ticks per second, used to detect scheduler starvation."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1169,
            title: "Node Runtime Timer Utilization".to_string(),
            panel_type: "timeseries".to_string(),
            expr: "max by (service, group, node, state) (rustraft_node_runtime_timer_utilization_percent)"
                .to_string(),
            unit: "percent".to_string(),
            description:
                "Node-runtime timer queue utilization, used to tune tick capacity before scheduler starvation."
                    .to_string(),
        },
    ]
}

pub fn matrixraft_snapshot_lifecycle_grafana_panels() -> Vec<GrafanaPanel> {
    let metrics = matrixraft_snapshot_lifecycle_metric_names();
    let panel_specs = [
        (
            1125,
            "Snapshot Sender Lifecycle",
            metrics.sender_lifecycle_present,
            "Sender lifecycle evidence captured snapshot send attempts or active sending.",
        ),
        (
            1126,
            "Snapshot Downloader Lifecycle",
            metrics.downloader_lifecycle_present,
            "Downloader lifecycle evidence captured installation chunks or active installation.",
        ),
        (
            1127,
            "Snapshot Retry Backpressure",
            metrics.retry_backpressure_present,
            "Snapshot transfer observed retry or max-inflight backpressure.",
        ),
        (
            1128,
            "Snapshot Chunk Retry",
            metrics.chunk_retry_present,
            "Snapshot transfer observed chunk retry activity.",
        ),
        (
            1129,
            "Snapshot Send Timeout",
            metrics.send_timeout_present,
            "Snapshot transfer observed send timeout handling.",
        ),
        (
            1130,
            "Snapshot Rate Limit",
            metrics.rate_limit_present,
            "Snapshot transfer observed rate-limit rejection handling.",
        ),
        (
            1131,
            "Snapshot Sustained Sender Load",
            metrics.sustained_sender_load_present,
            "Sender lifecycle evidence covered sustained transfer pressure.",
        ),
        (
            1132,
            "Snapshot Sustained Downloader Load",
            metrics.sustained_downloader_load_present,
            "Downloader lifecycle evidence covered sustained installation pressure.",
        ),
        (
            1133,
            "Snapshot Sender Completion",
            metrics.sustained_sender_completion_present,
            "Sender lifecycle evidence reached required snapshot acknowledgement.",
        ),
        (
            1134,
            "Snapshot Downloader Completion",
            metrics.sustained_downloader_completion_present,
            "Downloader lifecycle evidence reached full installation completion.",
        ),
        (
            1399,
            "Snapshot Sustained Transfer Completion",
            metrics.sustained_transfer_completion_present,
            "Snapshot lifecycle evidence tied sustained sender acknowledgement and downloader installation to one peer.",
        ),
        (
            1400,
            "Snapshot Lifecycle Peers",
            metrics.snapshot_peer_count,
            "Peers included in the snapshot lifecycle evidence artifact.",
        ),
        (
            1401,
            "Snapshot Sustained Transfer Completed Peers",
            metrics.sustained_transfer_completed_peer_count,
            "Peers that completed sustained same-peer snapshot send acknowledgement and downloader installation.",
        ),
        (
            1135,
            "Snapshot Install Progress",
            metrics.install_progress_present,
            "Snapshot installation progress was observed.",
        ),
        (
            1136,
            "Snapshot Install Rollback",
            metrics.install_rollback_present,
            "Snapshot installation rollback handling was observed.",
        ),
        (
            1137,
            "Snapshot Membership Change",
            metrics.membership_change_present,
            "Snapshot transfer during membership change was observed.",
        ),
        (
            1138,
            "Snapshot Rejoin After Compacted Log",
            metrics.rejoin_after_compacted_log_present,
            "Follower rejoin after compacted log required snapshot transfer evidence.",
        ),
    ];
    panel_specs
        .into_iter()
        .map(|(id, title, metric, description)| GrafanaPanel {
            id,
            title: title.to_string(),
            panel_type: "stat".to_string(),
            expr: metric,
            unit: "bool".to_string(),
            description: description.to_string(),
        })
        .collect()
}

pub fn matrixraft_wal_lifecycle_grafana_panels() -> Vec<GrafanaPanel> {
    let metrics = matrixraft_wal_lifecycle_metric_names();
    let panel_specs = [
        (
            1139,
            "WAL Segment Lifecycle",
            metrics.segment_lifecycle_present,
            "WAL segment lifecycle evidence has at least one active retained segment.",
            "bool",
        ),
        (
            1140,
            "WAL Retained Range",
            metrics.retained_range_present,
            "WAL retained segment range is internally ordered.",
            "bool",
        ),
        (
            1141,
            "WAL Sequence Range",
            metrics.sequence_range_present,
            "WAL sequence range is present for recovered records.",
            "bool",
        ),
        (
            1142,
            "WAL Log Index Range",
            metrics.log_index_range_present,
            "WAL log index range is present and ordered.",
            "bool",
        ),
        (
            1143,
            "WAL Compaction Observed",
            metrics.compaction_observed,
            "WAL compaction released at least one segment.",
            "bool",
        ),
        (
            1144,
            "WAL Slow Fsync Backpressure",
            metrics.slow_fsync_backpressure_observed,
            "WAL slow fsync backpressure was observed at or above threshold.",
            "bool",
        ),
        (
            1145,
            "WAL Compaction After Slow Fsync",
            metrics.compaction_after_slow_fsync_observed,
            "WAL compaction after slow fsync was observed for segment lifecycle parity.",
            "bool",
        ),
        (
            1208,
            "WAL Released Segments",
            metrics.released_segment_count,
            "Total WAL segments released by compaction.",
            "short",
        ),
        (
            1209,
            "WAL Compactions After Slow Fsync",
            metrics.compacted_after_slow_fsync_count,
            "Number of WAL compactions that released a segment after slow-fsync pressure.",
            "short",
        ),
        (
            1206,
            "WAL Slow Fsync Segments",
            metrics.slow_fsync_segment_count,
            "Number of retained or compacted WAL segments that observed slow fsync.",
            "short",
        ),
        (
            1207,
            "WAL Compacted Slow Fsync Segments",
            metrics.compacted_slow_fsync_segment_count,
            "Number of slow-fsync WAL segments that were compacted and released.",
            "short",
        ),
    ];
    panel_specs
        .into_iter()
        .map(|(id, title, metric, description, unit)| GrafanaPanel {
            id,
            title: title.to_string(),
            panel_type: "stat".to_string(),
            expr: metric,
            unit: unit.to_string(),
            description: description.to_string(),
        })
        .collect()
}

pub fn matrixraft_membership_readiness_grafana_panels() -> Vec<GrafanaPanel> {
    let metrics = matrixraft_membership_readiness_metric_names();
    vec![
        GrafanaPanel {
            id: 1146,
            title: "Membership Readiness Ready".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.ready,
            unit: "bool".to_string(),
            description:
                "Overall membership transition readiness across meta and data-node scopes."
                    .to_string(),
        },
        GrafanaPanel {
            id: 1147,
            title: "Membership Readiness Satisfied".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.satisfied_total,
            unit: "short".to_string(),
            description: "Total satisfied membership transition evidence fields.".to_string(),
        },
        GrafanaPanel {
            id: 1148,
            title: "Membership Readiness Missing".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.missing_total,
            unit: "short".to_string(),
            description: "Total missing membership transition evidence fields.".to_string(),
        },
        GrafanaPanel {
            id: 1149,
            title: "Membership Transition Ready".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, scope, transition) ({})",
                metrics.transition_ready
            ),
            unit: "bool".to_string(),
            description:
                "Per-scope failover, scale-up, and scale-down transition readiness."
                .to_string(),
        },
        GrafanaPanel {
            id: 1150,
            title: "Membership Transition Missing".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, scope, transition) ({})",
                metrics.transition_missing_total
            ),
            unit: "short".to_string(),
            description: "Missing membership evidence count by scope and transition.".to_string(),
        },
        GrafanaPanel {
            id: 1151,
            title: "Membership Missing Evidence".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, scope, transition, missing) ({})",
                metrics.transition_missing
            ),
            unit: "short".to_string(),
            description:
                "Named missing membership evidence, including joint consensus and learner catch-up gaps."
                    .to_string(),
        },
    ]
}

pub fn matrixraft_production_readiness_grafana_panels() -> Vec<GrafanaPanel> {
    let metrics = matrixraft_production_readiness_metric_names();
    vec![
        GrafanaPanel {
            id: 1017,
            title: "Production Readiness Ready".to_string(),
            panel_type: "stat".to_string(),
            expr: metrics.ready,
            unit: "bool".to_string(),
            description: "Fail-closed production readiness gate; 1 means RustRaft has all required production evidence."
                .to_string(),
        },
        GrafanaPanel {
            id: 1018,
            title: "Production Readiness Satisfied".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.satisfied_total,
            unit: "short".to_string(),
            description: "Count of satisfied production readiness evidence checks.".to_string(),
        },
        GrafanaPanel {
            id: 1019,
            title: "Production Readiness Missing".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.missing_total,
            unit: "short".to_string(),
            description: "Count of missing production readiness evidence checks.".to_string(),
        },
        GrafanaPanel {
            id: 1020,
            title: "Production Readiness Blockers".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.blocker_total,
            unit: "short".to_string(),
            description: "Count of production blockers that must clear before rollout.".to_string(),
        },
        GrafanaPanel {
            id: 1021,
            title: "Production Readiness Next Actions".to_string(),
            panel_type: "timeseries".to_string(),
            expr: metrics.next_action_total,
            unit: "short".to_string(),
            description: "Count of recommended next actions emitted by the readiness gate.".to_string(),
        },
        GrafanaPanel {
            id: 1022,
            title: "Production Missing Evidence".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, cluster, missing) ({})",
                metrics.missing_present
            ),
            unit: "short".to_string(),
            description: "Missing production evidence grouped by concrete readiness key.".to_string(),
        },
        GrafanaPanel {
            id: 1023,
            title: "Production Blocker Detail".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, cluster, blocker) ({})",
                metrics.blocker_present
            ),
            unit: "short".to_string(),
            description: "Production blockers grouped by concrete readiness key.".to_string(),
        },
        GrafanaPanel {
            id: 1024,
            title: "Production Next Action Detail".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, cluster, action) ({})",
                metrics.next_action_present
            ),
            unit: "short".to_string(),
            description: "Recommended production-readiness next actions grouped by action label."
                .to_string(),
        },
        GrafanaPanel {
            id: 1040,
            title: "Production Runtime Pressure Bottlenecks".to_string(),
            panel_type: "timeseries".to_string(),
            expr: format!(
                "sum by (service, cluster, rank, category, component) ({})",
                metrics.runtime_pressure_bottleneck_score_percent
            ),
            unit: "percent".to_string(),
            description: "Ranked runtime-pressure bottlenecks that are currently blocking production readiness."
                .to_string(),
        },
    ]
}

pub fn matrixraft_alert_rules_json() -> String {
    serde_json::to_string_pretty(&matrixraft_alert_rules())
        .expect("RustRaft alert rules must serialize")
}

pub fn matrixraft_observability_required_metric_names() -> Vec<String> {
    matrixraft_observability_provisioning().required_metric_names
}

pub fn matrixraft_validate_required_metric_scrape_texts(
    prometheus_texts: &[&str],
) -> DebugBundleValidationReport {
    let provisioning = matrixraft_observability_provisioning();
    let provided_metrics: BTreeSet<&str> = prometheus_texts
        .iter()
        .flat_map(|text| text.lines())
        .filter_map(matrixraft_prometheus_metric_sample_name)
        .collect();
    let mut issues: Vec<String> = provisioning
        .required_metric_names
        .iter()
        .filter(|metric| !provided_metrics.contains(metric.as_str()))
        .map(|metric| format!("required_metric_missing:{metric}"))
        .collect();
    issues.extend(
        provisioning
            .validation_metric_names
            .iter()
            .filter(|metric| !provided_metrics.contains(metric.as_str()))
            .map(|metric| format!("validation_metric_missing:{metric}")),
    );

    if prometheus_texts
        .iter()
        .any(|text| !matrixraft_prometheus_sample_lines_are_well_formed(text))
    {
        issues.push("required_metric_scrape_malformed".to_string());
    }

    matrixraft_debug_bundle_validation_report(issues)
}

pub fn matrixraft_observability_provisioning() -> ObservabilityProvisioning {
    let metrics = matrixraft_metric_names();
    let scale_metrics = matrixraft_scale_metric_names();
    let scale_target_metrics = matrixraft_scale_target_metric_names();
    let memory_metrics = matrixraft_memory_metric_names();
    let runtime_pressure_metrics = matrixraft_runtime_pressure_metric_names();
    let snapshot_lifecycle_metrics = matrixraft_snapshot_lifecycle_metric_names();
    let wal_lifecycle_metrics = matrixraft_wal_lifecycle_metric_names();
    let membership_readiness_metrics = matrixraft_membership_readiness_metric_names();
    let production_readiness_metrics = matrixraft_production_readiness_metric_names();
    let benchmark_metrics = matrixraft_baseline_raft_benchmark_metric_names();
    ObservabilityProvisioning {
        service: "rustraft".to_string(),
        prometheus_format: "prometheus_text_v0.0.4".to_string(),
        required_metric_names: vec![
            metrics.ready,
            metrics.append_latency_ms,
            metrics.vote_latency_ms,
            metrics.pre_vote_latency_ms,
            metrics.read_index_latency_ms,
            metrics.snapshot_install_latency_ms,
            metrics.peer_append_queue_depth,
            metrics.peer_reorder_queue_depth,
            metrics.peer_reorder_entries_converged_total,
            metrics.peer_snapshot_installed_index,
            metrics.wal_segment_count,
            metrics.blocker_total,
            metrics.fatal_total,
            metrics.diagnostic_log_total,
            metrics.diagnostic_log_entry_total,
            metrics.optimization_ready,
            metrics.optimization_critical_total,
            metrics.optimization_warning_total,
            metrics.optimization_hint_total,
            metrics.optimization_component_hint_total,
            metrics.operator_triage_status,
            metrics.operator_triage_diagnostic_error_total,
            metrics.operator_triage_diagnostic_warning_total,
            metrics.operator_triage_optimization_critical_total,
            metrics.operator_triage_optimization_warning_total,
            metrics.operator_triage_alert_rule_total,
            metrics.operator_triage_first_action,
            metrics.operator_triage_top_diagnostic,
            metrics.operator_triage_top_alert,
            metrics.operator_triage_top_optimization_hint,
            metrics.operator_runbook_step_total,
            metrics.operator_runbook_step_present,
            metrics.operator_runbook_first_step,
            scale_metrics.proposal_qps_total,
            scale_metrics.append_entries_qps_total,
            scale_metrics.read_index_qps_total,
            scale_metrics.apply_entries_qps_total,
            scale_metrics.replication_bytes_total,
            scale_metrics.apply_bytes_total,
            scale_target_metrics.min_proposal_qps,
            scale_target_metrics.min_append_entries_qps,
            scale_target_metrics.min_read_index_qps,
            scale_target_metrics.min_apply_entries_qps,
            scale_target_metrics.min_replication_mib_per_sec,
            scale_target_metrics.min_apply_mib_per_sec,
            scale_target_metrics.proposal_target_percent,
            scale_target_metrics.append_entries_target_percent,
            scale_target_metrics.read_index_target_percent,
            scale_target_metrics.apply_entries_target_percent,
            scale_target_metrics.replication_target_percent,
            scale_target_metrics.apply_target_percent,
            memory_metrics.process_resident_memory_bytes,
            memory_metrics.heap_allocated_bytes,
            memory_metrics.log_cache_bytes,
            memory_metrics.snapshot_buffer_bytes,
            memory_metrics.replication_buffer_bytes,
            runtime_pressure_metrics.admission_accepted,
            runtime_pressure_metrics.admission_rejected,
            runtime_pressure_metrics.memory_pressure,
            runtime_pressure_metrics.memory_pressure_observed_value,
            runtime_pressure_metrics.memory_pressure_threshold_value,
            runtime_pressure_metrics.memory_pressure_excess,
            runtime_pressure_metrics.latency_pressure,
            runtime_pressure_metrics.latency_pressure_sample_count,
            runtime_pressure_metrics.latency_pressure_observed_p95_ms,
            runtime_pressure_metrics.latency_pressure_observed_p99_ms,
            runtime_pressure_metrics.latency_pressure_threshold_p99_ms,
            runtime_pressure_metrics.latency_pressure_excess_ms,
            runtime_pressure_metrics.scale_pressure,
            runtime_pressure_metrics.scale_pressure_observed_value,
            runtime_pressure_metrics.scale_pressure_target_value,
            runtime_pressure_metrics.scale_pressure_deficit,
            runtime_pressure_metrics.scale_pressure_target_percent,
            runtime_pressure_metrics.pipeline_pressure,
            runtime_pressure_metrics.pipeline_pressure_observed_value,
            runtime_pressure_metrics.pipeline_pressure_threshold_value,
            runtime_pressure_metrics.pipeline_pressure_excess,
            runtime_pressure_metrics.read_backlog_pressure,
            runtime_pressure_metrics.read_backlog_pressure_observed_value,
            runtime_pressure_metrics.read_backlog_pressure_threshold_value,
            runtime_pressure_metrics.read_backlog_pressure_excess,
            runtime_pressure_metrics.node_runtime_timer_pressure,
            runtime_pressure_metrics.node_runtime_timer_pressure_observed_percent,
            runtime_pressure_metrics.node_runtime_timer_pressure_threshold_percent,
            runtime_pressure_metrics.node_runtime_timer_pressure_excess_percent,
            runtime_pressure_metrics.action_total,
            runtime_pressure_metrics.action_source_total,
            runtime_pressure_metrics.bottleneck_score_percent,
            runtime_pressure_metrics.freshness_generated_at_unix_ms,
            runtime_pressure_metrics.freshness_age_ms,
            runtime_pressure_metrics.freshness_max_age_ms,
            runtime_pressure_metrics.freshness_stale_after_unix_ms,
            runtime_pressure_metrics.freshness_remaining_fresh_ms,
            runtime_pressure_metrics.freshness_low_fresh_ms,
            runtime_pressure_metrics.freshness_low_fresh,
            runtime_pressure_metrics.freshness_fresh,
            runtime_pressure_metrics.freshness_status,
            runtime_pressure_metrics.freshness_issue_total,
            runtime_pressure_metrics.freshness_issue,
            "rustraft_node_runtime_timer_pending_ticks".to_string(),
            "rustraft_node_runtime_timer_max_pending_ticks".to_string(),
            "rustraft_node_runtime_timer_accepted_ticks_total".to_string(),
            "rustraft_node_runtime_timer_rejected_ticks_total".to_string(),
            "rustraft_node_runtime_timer_completed_ticks_total".to_string(),
            "rustraft_node_runtime_timer_backpressure".to_string(),
            "rustraft_node_runtime_timer_utilization_percent".to_string(),
            snapshot_lifecycle_metrics.sender_lifecycle_present,
            snapshot_lifecycle_metrics.downloader_lifecycle_present,
            snapshot_lifecycle_metrics.retry_backpressure_present,
            snapshot_lifecycle_metrics.chunk_retry_present,
            snapshot_lifecycle_metrics.send_timeout_present,
            snapshot_lifecycle_metrics.rate_limit_present,
            snapshot_lifecycle_metrics.sustained_sender_load_present,
            snapshot_lifecycle_metrics.sustained_downloader_load_present,
            snapshot_lifecycle_metrics.sustained_sender_completion_present,
            snapshot_lifecycle_metrics.sustained_downloader_completion_present,
            snapshot_lifecycle_metrics.sustained_transfer_completion_present,
            snapshot_lifecycle_metrics.snapshot_peer_count,
            snapshot_lifecycle_metrics.sustained_transfer_completed_peer_count,
            snapshot_lifecycle_metrics.install_progress_present,
            snapshot_lifecycle_metrics.install_rollback_present,
            snapshot_lifecycle_metrics.membership_change_present,
            snapshot_lifecycle_metrics.rejoin_after_compacted_log_present,
            wal_lifecycle_metrics.segment_lifecycle_present,
            wal_lifecycle_metrics.retained_range_present,
            wal_lifecycle_metrics.sequence_range_present,
            wal_lifecycle_metrics.log_index_range_present,
            wal_lifecycle_metrics.compaction_observed,
            wal_lifecycle_metrics.slow_fsync_backpressure_observed,
            wal_lifecycle_metrics.compaction_after_slow_fsync_observed,
            wal_lifecycle_metrics.released_segment_count,
            wal_lifecycle_metrics.compacted_after_slow_fsync_count,
            wal_lifecycle_metrics.slow_fsync_segment_count,
            wal_lifecycle_metrics.compacted_slow_fsync_segment_count,
            membership_readiness_metrics.ready,
            membership_readiness_metrics.satisfied_total,
            membership_readiness_metrics.missing_total,
            membership_readiness_metrics.transition_ready,
            membership_readiness_metrics.transition_missing_total,
            membership_readiness_metrics.transition_missing,
            production_readiness_metrics.ready,
            production_readiness_metrics.satisfied_total,
            production_readiness_metrics.missing_total,
            production_readiness_metrics.blocker_total,
            production_readiness_metrics.next_action_total,
            production_readiness_metrics.missing_present,
            production_readiness_metrics.blocker_present,
            production_readiness_metrics.runtime_pressure_bottleneck_score_percent,
            production_readiness_metrics.next_action_present,
            benchmark_metrics.passed,
            benchmark_metrics.production_evidence_ready,
            benchmark_metrics.generated_at_unix_ms,
            benchmark_metrics.age_ms,
            benchmark_metrics.max_age_ms,
            benchmark_metrics.stale_after_unix_ms,
            benchmark_metrics.remaining_fresh_ms,
            benchmark_metrics.fresh,
            benchmark_metrics.freshness_status,
            benchmark_metrics.failed_workload_total,
            benchmark_metrics.blocker_total,
            benchmark_metrics.worst_p50_ratio,
            benchmark_metrics.worst_p99_ratio,
            benchmark_metrics.worst_throughput_ratio,
            benchmark_metrics.worst_cpu_ratio,
            benchmark_metrics.worst_peak_resident_memory_ratio,
            benchmark_metrics.workload_passed,
            benchmark_metrics.workload_p50_ratio,
            benchmark_metrics.workload_p99_ratio,
            benchmark_metrics.workload_throughput_ratio,
            benchmark_metrics.workload_cpu_ratio,
            benchmark_metrics.workload_peak_resident_memory_ratio,
        ],
        validation_metric_names: vec![
            metrics.debug_snapshot_generated_at_unix_ms,
            metrics.debug_snapshot_age_ms,
            metrics.debug_snapshot_max_age_ms,
            metrics.debug_snapshot_stale_after_unix_ms,
            metrics.debug_snapshot_remaining_fresh_ms,
            metrics.debug_snapshot_low_fresh_ms,
            metrics.debug_snapshot_low_fresh,
            metrics.debug_snapshot_fresh,
            metrics.debug_bundle_validation_ready,
            metrics.debug_bundle_validation_issue_total,
            metrics.debug_bundle_validation_issue,
            metrics.debug_bundle_validation_first_issue,
            metrics.observability_provisioning_validation_ready,
            metrics.observability_provisioning_validation_issue_total,
            metrics.observability_provisioning_validation_issue,
            metrics.observability_provisioning_validation_first_issue,
        ],
        debug_artifact_names: vec![
            "debug_snapshot".to_string(),
            "debug_snapshot_json".to_string(),
            "debug_snapshot_metadata_prometheus".to_string(),
            "diagnostic_json_lines".to_string(),
            "local_status_diagnostic_json_lines".to_string(),
            "diagnostic_prometheus".to_string(),
            "peer_pipeline_prometheus".to_string(),
            "latency_prometheus".to_string(),
            "memory_prometheus".to_string(),
            "scale_prometheus".to_string(),
            "scale_target_prometheus".to_string(),
            "runtime_pressure_freshness_prometheus".to_string(),
            "snapshot_lifecycle_prometheus".to_string(),
            "wal_lifecycle_prometheus".to_string(),
            "membership_readiness_prometheus".to_string(),
            "benchmark_prometheus".to_string(),
            "optimization_prometheus".to_string(),
            "triage_prometheus".to_string(),
            "runbook_prometheus".to_string(),
            "grafana_dashboard_json".to_string(),
            "alert_rules_json".to_string(),
            "observability_provisioning_json".to_string(),
            "observability_provisioning".to_string(),
            "validation".to_string(),
            "validation_prometheus".to_string(),
            "provisioning_validation".to_string(),
            "provisioning_validation_prometheus".to_string(),
            "provisioning_runbook_prometheus".to_string(),
            "support_envelope_validation".to_string(),
            "support_envelope_validation_prometheus".to_string(),
        ],
        prometheus_artifact_names: vec![
            "diagnostic_prometheus".to_string(),
            "peer_pipeline_prometheus".to_string(),
            "latency_prometheus".to_string(),
            "memory_prometheus".to_string(),
            "scale_prometheus".to_string(),
            "scale_target_prometheus".to_string(),
            "runtime_pressure_freshness_prometheus".to_string(),
            "snapshot_lifecycle_prometheus".to_string(),
            "wal_lifecycle_prometheus".to_string(),
            "membership_readiness_prometheus".to_string(),
            "benchmark_prometheus".to_string(),
            "optimization_prometheus".to_string(),
            "triage_prometheus".to_string(),
            "runbook_prometheus".to_string(),
            "debug_snapshot_metadata_prometheus".to_string(),
            "validation_prometheus".to_string(),
            "provisioning_validation_prometheus".to_string(),
            "provisioning_runbook_prometheus".to_string(),
            "support_envelope_validation_prometheus".to_string(),
        ],
        dashboard: matrixraft_grafana_dashboard(),
        alert_rules: matrixraft_alert_rules(),
        runbook_steps: matrixraft_observability_provisioning_runbook_steps(),
        debug_bundle_contract: matrixraft_debug_bundle_contract(),
        sample_artifact_command: "cargo run --example debug_artifacts".to_string(),
    }
}

pub fn matrixraft_observability_provisioning_runbook_steps() -> Vec<OperatorRunbookStep> {
    let triage = OperatorTriageSummary {
        status: "needs_attention".to_string(),
        severity: "critical".to_string(),
        first_action: "Inspect error diagnostics and critical optimization hints first."
            .to_string(),
        diagnostic_error_count: 1,
        diagnostic_warning_count: 1,
        critical_optimization_count: 1,
        warning_optimization_count: 1,
        alert_rule_count: matrixraft_alert_rules().len(),
        top_diagnostic_target: Some("rustraft.observability".to_string()),
        top_diagnostic_message: Some("observability_contract_stale".to_string()),
        top_alert: Some("RustRaftOperatorTriageNeedsAttention".to_string()),
        top_optimization_hint: Some("critical_observability_contract".to_string()),
    };
    let optimization = OptimizationReport {
        ready: false,
        critical_count: 1,
        warning_count: 1,
        hint_count: 2,
        hints: vec![],
    };
    let mut steps =
        matrixraft_operator_runbook_steps(&triage, &optimization, &matrixraft_alert_rules());
    steps.push(matrixraft_runbook_step(
        "refresh_debug_snapshot",
        "warning",
        "debug_bundle",
        "Regenerate the RustRaft debug artifact when validation fails, snapshot age is stale, RustRaftDebugSnapshotFreshnessLow warns, or RustRaftDebugSnapshotFreshnessLost fires.",
        "rustraft_debug_bundle_validation_ready is 1, rustraft_debug_snapshot_age_ms is below rustraft_debug_snapshot_max_age_ms, rustraft_debug_snapshot_stale_after_unix_ms is in the future, rustraft_debug_snapshot_remaining_fresh_ms is above rustraft_debug_snapshot_low_fresh_ms, rustraft_debug_snapshot_low_fresh is 1, and rustraft_debug_snapshot_fresh is 1.",
    ));
    steps.push(matrixraft_runbook_step(
        "refresh_runtime_pressure_evidence",
        "warning",
        "runtime_pressure",
        "Regenerate release-scale runtime-pressure evidence when RustRaftRuntimePressureFreshnessLow, RustRaftRuntimePressureFreshnessLost, or RustRaftRuntimePressureFreshnessInvalid fires.",
        "rustraft_runtime_pressure_freshness_fresh is 1, rustraft_runtime_pressure_freshness_low_fresh is 1, rustraft_runtime_pressure_freshness_issue_total is 0, rustraft_runtime_pressure_freshness_status is fresh, and QPS, latency, and memory parity dashboards use the refreshed sample timestamp.",
    ));
    steps.push(matrixraft_runbook_step(
        "validate_support_envelope",
        "warning",
        "support_envelope",
        "Confirm the RustRaft support envelope validation artifact and Prometheus scrape payload are both present.",
        "rustraft_debug_bundle_validation_ready{artifact=\"support_envelope\"} is 1, rustraft_debug_bundle_validation_first_issue{artifact=\"support_envelope\"} is absent, support envelope missing artifact lists are empty, debug_snapshot_low_fresh is true, debug_snapshot_fresh is true, debug_snapshot_freshness_status is fresh, support_envelope_status is ready, and support_envelope_severity is ok.",
    ));
    steps
}

pub fn matrixraft_observability_provisioning_json() -> String {
    serde_json::to_string_pretty(&matrixraft_observability_provisioning())
        .expect("RustRaft observability provisioning must serialize")
}

pub fn matrixraft_validate_observability_provisioning(
    provisioning: &ObservabilityProvisioning,
) -> DebugBundleValidationReport {
    let expected = matrixraft_observability_provisioning();
    let mut issues = Vec::new();

    if provisioning != &expected {
        issues.push("observability_provisioning_contract_mismatch".to_string());
    }
    if provisioning.service != expected.service {
        issues.push("observability_service_mismatch".to_string());
    }
    if provisioning.prometheus_format != expected.prometheus_format {
        issues.push("observability_prometheus_format_mismatch".to_string());
    }
    if provisioning.required_metric_names != expected.required_metric_names {
        issues.push("observability_required_metrics_mismatch".to_string());
    }
    if provisioning.validation_metric_names != expected.validation_metric_names {
        issues.push("observability_validation_metrics_mismatch".to_string());
    }
    if provisioning.debug_artifact_names != expected.debug_artifact_names {
        issues.push("observability_debug_artifacts_mismatch".to_string());
    }
    if provisioning.prometheus_artifact_names != expected.prometheus_artifact_names {
        issues.push("observability_prometheus_artifacts_mismatch".to_string());
    }
    if provisioning.dashboard != expected.dashboard {
        issues.push("observability_dashboard_mismatch".to_string());
    }
    if matrixraft_dashboard_has_unadvertised_metrics(provisioning) {
        issues.push("observability_dashboard_metric_not_advertised".to_string());
    }
    issues.extend(matrixraft_dashboard_missing_runtime_pressure_metric_issues(
        provisioning,
    ));
    if provisioning.alert_rules != expected.alert_rules {
        issues.push("observability_alert_rules_mismatch".to_string());
    }
    if matrixraft_alert_rules_have_unadvertised_metrics(provisioning) {
        issues.push("observability_alert_metric_not_advertised".to_string());
    }
    if provisioning.runbook_steps != expected.runbook_steps {
        issues.push("observability_runbook_steps_mismatch".to_string());
        issues.extend(matrixraft_observability_runbook_step_drift_issues(
            &provisioning.runbook_steps,
            &expected.runbook_steps,
        ));
    }
    if provisioning.debug_bundle_contract != expected.debug_bundle_contract {
        issues.push("observability_debug_bundle_contract_mismatch".to_string());
    }
    if provisioning.sample_artifact_command != expected.sample_artifact_command {
        issues.push("observability_sample_artifact_command_mismatch".to_string());
    }

    matrixraft_debug_bundle_validation_report(issues)
}

fn matrixraft_observability_runbook_step_drift_issues(
    actual: &[OperatorRunbookStep],
    expected: &[OperatorRunbookStep],
) -> Vec<String> {
    let actual_ids: BTreeSet<&str> = actual.iter().map(|step| step.id.as_str()).collect();
    let expected_ids: BTreeSet<&str> = expected.iter().map(|step| step.id.as_str()).collect();
    let mut issues = Vec::new();

    issues.extend(
        expected_ids
            .difference(&actual_ids)
            .map(|id| format!("observability_runbook_step_missing:{id}")),
    );
    issues.extend(
        actual_ids
            .difference(&expected_ids)
            .map(|id| format!("observability_runbook_step_unexpected:{id}")),
    );

    issues
}

fn matrixraft_alert_rules_have_unadvertised_metrics(
    provisioning: &ObservabilityProvisioning,
) -> bool {
    let advertised_metrics: BTreeSet<&str> = provisioning
        .required_metric_names
        .iter()
        .chain(provisioning.validation_metric_names.iter())
        .map(String::as_str)
        .collect();

    provisioning.alert_rules.iter().any(|rule| {
        !advertised_metrics
            .iter()
            .any(|metric| matrixraft_expr_references_metric(&rule.expr, metric))
    })
}

fn matrixraft_dashboard_has_unadvertised_metrics(provisioning: &ObservabilityProvisioning) -> bool {
    let advertised_metrics: BTreeSet<&str> = provisioning
        .required_metric_names
        .iter()
        .chain(provisioning.validation_metric_names.iter())
        .map(String::as_str)
        .collect();

    provisioning.dashboard.panels.iter().any(|panel| {
        !advertised_metrics
            .iter()
            .any(|metric| matrixraft_expr_references_metric(&panel.expr, metric))
    })
}

fn matrixraft_dashboard_missing_runtime_pressure_metric_issues(
    provisioning: &ObservabilityProvisioning,
) -> Vec<String> {
    provisioning
        .required_metric_names
        .iter()
        .filter(|metric| metric.starts_with("rustraft_runtime_pressure_"))
        .filter(|metric| {
            !provisioning
                .dashboard
                .panels
                .iter()
                .any(|panel| matrixraft_expr_references_metric(&panel.expr, metric))
        })
        .map(|metric| {
            format!("observability_runtime_pressure_metric_without_dashboard_panel:{metric}")
        })
        .collect()
}

fn matrixraft_expr_references_metric(expr: &str, metric: &str) -> bool {
    expr.match_indices(metric).any(|(start, _)| {
        let end = start + metric.len();
        let before_ok = start == 0
            || !expr[..start]
                .chars()
                .next_back()
                .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_');
        let after_ok = expr[end..].starts_with("_bucket")
            || end == expr.len()
            || !expr[end..]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_');
        before_ok && after_ok
    })
}

pub fn matrixraft_validate_observability_provisioning_json(
    json: &str,
) -> DebugBundleValidationReport {
    match serde_json::from_str::<ObservabilityProvisioning>(json) {
        Ok(provisioning) => matrixraft_validate_observability_provisioning(&provisioning),
        Err(_) => matrixraft_debug_bundle_validation_report(vec![
            "observability_provisioning_json_parse_error".to_string(),
        ]),
    }
}

pub fn matrixraft_observability_provisioning_validation_prometheus(
    report: &DebugBundleValidationReport,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let metrics = matrixraft_metric_names();
    let mut text = String::new();
    let mut metric_count = 0_u64;

    push_metric(
        &mut text,
        &metrics.observability_provisioning_validation_ready,
        labels,
        u64::from(report.ready),
    );
    metric_count += 1;
    push_metric(
        &mut text,
        &metrics.observability_provisioning_validation_issue_total,
        labels,
        report.issue_count as u64,
    );
    metric_count += 1;
    if let Some(first_issue) = report.issues.first() {
        let mut first_issue_labels = labels.to_vec();
        first_issue_labels.push(("issue", first_issue.as_str()));
        push_metric(
            &mut text,
            &metrics.observability_provisioning_validation_first_issue,
            &first_issue_labels,
            1,
        );
        metric_count += 1;
    }

    for issue in &report.issues {
        let mut issue_labels = labels.to_vec();
        issue_labels.push(("issue", issue.as_str()));
        push_metric(
            &mut text,
            &metrics.observability_provisioning_validation_issue,
            &issue_labels,
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

pub fn matrixraft_debug_bundle_contract() -> DebugBundleContract {
    DebugBundleContract {
        name: "matrixraft_debug_snapshot".to_string(),
        version: 1,
        producer: "matrixraft".to_string(),
        schema: "rustraft.debug_snapshot.v1".to_string(),
    }
}

pub fn matrixraft_validate_debug_snapshot(snapshot: &DebugSnapshot) -> DebugBundleValidationReport {
    let expected = matrixraft_debug_bundle_contract();
    let mut issues = Vec::new();

    if snapshot.contract != expected {
        issues.push("contract_mismatch".to_string());
    }
    if snapshot.generated_at_unix_ms == 0 {
        issues.push("generated_at_missing".to_string());
    } else if snapshot.generated_at_unix_ms
        > matrixraft_debug_snapshot_now_unix_ms().saturating_add(60_000)
    {
        issues.push("generated_at_in_future".to_string());
    } else if matrixraft_debug_snapshot_now_unix_ms().saturating_sub(snapshot.generated_at_unix_ms)
        > 3_600_000
    {
        issues.push("generated_at_stale".to_string());
    }
    if snapshot.optimization_prometheus.format != "prometheus_text_v0.0.4" {
        issues.push("prometheus_format_mismatch".to_string());
    }
    if snapshot.optimization_prometheus.text.is_empty() {
        issues.push("prometheus_metrics_missing".to_string());
    }
    let expected_prometheus_metric_count = 3
        + snapshot.optimization.hints.len() as u64
        + matrixraft_optimization_component_hint_counts(&snapshot.optimization).len() as u64;
    if snapshot.optimization_prometheus.metric_count != expected_prometheus_metric_count {
        issues.push("prometheus_metric_count_mismatch".to_string());
    }
    let metric_names = matrixraft_metric_names();
    for required_metric in [
        metric_names.optimization_ready.as_str(),
        metric_names.optimization_critical_total.as_str(),
        metric_names.optimization_warning_total.as_str(),
    ] {
        if !matrixraft_prometheus_text_has_metric_sample(
            &snapshot.optimization_prometheus.text,
            required_metric,
        ) {
            issues.push("prometheus_metric_contract_missing".to_string());
        }
    }
    for hint in &snapshot.optimization.hints {
        if !snapshot.optimization_prometheus.text.contains(&format!(
            "hint=\"{}\"",
            escape_prometheus_label_value(hint.id.as_str())
        )) {
            issues.push("prometheus_hint_metric_missing".to_string());
        }
    }
    for ((component, severity), _) in
        matrixraft_optimization_component_hint_counts(&snapshot.optimization)
    {
        let component_label = format!(
            "component=\"{}\"",
            escape_prometheus_label_value(component.as_str())
        );
        let severity_label = format!("severity=\"{}\"", severity);
        if !snapshot.optimization_prometheus.text.lines().any(|line| {
            line.contains(metric_names.optimization_component_hint_total.as_str())
                && line.contains(&component_label)
                && line.contains(&severity_label)
        }) {
            issues.push("prometheus_component_hint_metric_missing".to_string());
            break;
        }
    }
    if snapshot.grafana.panels.is_empty() {
        issues.push("grafana_panels_missing".to_string());
    }
    let expected_grafana = matrixraft_grafana_dashboard();
    if snapshot.grafana.uid != expected_grafana.uid
        || snapshot.grafana.title != expected_grafana.title
        || snapshot.grafana.schema_version != expected_grafana.schema_version
    {
        issues.push("grafana_contract_mismatch".to_string());
    }
    for expected_panel in expected_grafana.panels {
        match snapshot
            .grafana
            .panels
            .iter()
            .find(|panel| panel.id == expected_panel.id)
        {
            Some(actual_panel) if actual_panel != &expected_panel => {
                issues.push("grafana_panel_contract_mismatch".to_string());
            }
            Some(_) => {}
            None => issues.push("grafana_panel_contract_missing".to_string()),
        }
    }
    if snapshot.alerts.is_empty() {
        issues.push("alert_rules_missing".to_string());
    }
    if snapshot.triage.status.is_empty() {
        issues.push("triage_status_missing".to_string());
    }
    let expected_triage = matrixraft_operator_triage_summary(
        &snapshot.diagnostics,
        &snapshot.optimization,
        &snapshot.alerts,
    );
    if snapshot.triage != expected_triage {
        issues.push("triage_contract_mismatch".to_string());
    }
    let mut expected_diagnostics = matrixraft_admin_diagnostic_log_entries(&snapshot.admin_report);
    if let Some(admission) = &snapshot.runtime_pressure_admission {
        if let Err(evidence_issues) =
            matrixraft_validate_runtime_pressure_admission_evidence(admission)
        {
            issues.extend(evidence_issues);
        }
        let expected_runtime_pressure_diagnostics =
            matrixraft_runtime_pressure_diagnostic_log_entries(admission);
        if snapshot.runtime_pressure_diagnostics != expected_runtime_pressure_diagnostics {
            issues.push("runtime_pressure_diagnostic_log_contract_mismatch".to_string());
        }
        expected_diagnostics.extend(expected_runtime_pressure_diagnostics);
        if snapshot.runtime_pressure_prometheus.format != "prometheus_text_v0.0.4" {
            issues.push("runtime_pressure_prometheus_format_mismatch".to_string());
        }
        if snapshot.runtime_pressure_prometheus.text.is_empty() {
            issues.push("runtime_pressure_prometheus_metrics_missing".to_string());
        }
        if snapshot.runtime_pressure_prometheus.metric_count
            != snapshot.runtime_pressure_prometheus.text.lines().count() as u64
        {
            issues.push("runtime_pressure_prometheus_metric_count_mismatch".to_string());
        }
        if !matrixraft_prometheus_sample_lines_are_well_formed(
            &snapshot.runtime_pressure_prometheus.text,
        ) {
            issues.push("runtime_pressure_prometheus_malformed_sample".to_string());
        }
        let runtime_pressure_metrics = matrixraft_runtime_pressure_metric_names();
        for required_metric in [
            runtime_pressure_metrics.admission_accepted.as_str(),
            runtime_pressure_metrics.admission_rejected.as_str(),
            runtime_pressure_metrics.memory_pressure.as_str(),
            runtime_pressure_metrics.latency_pressure.as_str(),
            runtime_pressure_metrics.scale_pressure.as_str(),
            runtime_pressure_metrics.pipeline_pressure.as_str(),
            runtime_pressure_metrics.read_backlog_pressure.as_str(),
            runtime_pressure_metrics
                .node_runtime_timer_pressure
                .as_str(),
            runtime_pressure_metrics.action_total.as_str(),
            runtime_pressure_metrics.action_source_total.as_str(),
        ] {
            if !matrixraft_prometheus_text_has_metric_sample(
                &snapshot.runtime_pressure_prometheus.text,
                required_metric,
            ) {
                issues.push("runtime_pressure_prometheus_metric_contract_missing".to_string());
                break;
            }
        }
    } else if !snapshot.runtime_pressure_diagnostics.is_empty()
        || !snapshot.runtime_pressure_prometheus.text.is_empty()
        || snapshot.runtime_pressure_prometheus.metric_count != 0
    {
        issues.push("runtime_pressure_evidence_without_admission".to_string());
    }
    if snapshot.diagnostics != expected_diagnostics {
        issues.push("diagnostic_log_contract_mismatch".to_string());
    }
    if snapshot.diagnostic_prometheus.format != "prometheus_text_v0.0.4" {
        issues.push("diagnostic_prometheus_format_mismatch".to_string());
    }
    if snapshot.diagnostic_prometheus.text.is_empty() {
        issues.push("diagnostic_prometheus_metrics_missing".to_string());
    }
    let expected_diagnostic_metric_count = 3 + snapshot.diagnostics.len() as u64;
    if snapshot.diagnostic_prometheus.metric_count != expected_diagnostic_metric_count {
        issues.push("diagnostic_prometheus_metric_count_mismatch".to_string());
    }
    for required_metric in [
        metric_names.diagnostic_log_total.as_str(),
        metric_names.diagnostic_log_entry_total.as_str(),
    ] {
        if !matrixraft_prometheus_text_has_metric_sample(
            &snapshot.diagnostic_prometheus.text,
            required_metric,
        ) {
            issues.push("diagnostic_prometheus_metric_contract_missing".to_string());
        }
    }
    for severity in ["info", "warn", "error"] {
        let severity_label = format!("severity=\"{}\"", severity);
        if !snapshot.diagnostic_prometheus.text.lines().any(|line| {
            line.contains(metric_names.diagnostic_log_total.as_str())
                && line.contains(&severity_label)
        }) {
            issues.push("diagnostic_prometheus_severity_total_missing".to_string());
            break;
        }
    }
    for entry in &snapshot.diagnostics {
        let target_label = format!(
            "target=\"{}\"",
            escape_prometheus_label_value(entry.target.as_str())
        );
        let severity_label = format!(
            "severity=\"{}\"",
            matrixraft_diagnostic_severity_label(entry.severity)
        );
        let message_label = format!(
            "message=\"{}\"",
            escape_prometheus_label_value(entry.message.as_str())
        );
        if !snapshot.diagnostic_prometheus.text.lines().any(|line| {
            line.contains(metric_names.diagnostic_log_entry_total.as_str())
                && line.contains(&target_label)
                && line.contains(&severity_label)
                && line.contains(&message_label)
        }) {
            issues.push("diagnostic_prometheus_entry_missing".to_string());
            break;
        }
    }
    if snapshot.latency_prometheus.format != "prometheus_text_v0.0.4" {
        issues.push("latency_prometheus_format_mismatch".to_string());
    }
    if snapshot.latency_prometheus.text.is_empty() {
        issues.push("latency_prometheus_metrics_missing".to_string());
    }
    if snapshot.latency_prometheus.metric_count
        != snapshot.latency_prometheus.text.lines().count() as u64
    {
        issues.push("latency_prometheus_metric_count_mismatch".to_string());
    }
    for required_metric in [
        metric_names.append_latency_ms.as_str(),
        metric_names.vote_latency_ms.as_str(),
        metric_names.pre_vote_latency_ms.as_str(),
        metric_names.read_index_latency_ms.as_str(),
        metric_names.snapshot_install_latency_ms.as_str(),
    ] {
        for suffix in ["_bucket", "_sum", "_count"] {
            if !matrixraft_prometheus_text_has_metric_sample(
                &snapshot.latency_prometheus.text,
                &format!("{required_metric}{suffix}"),
            ) {
                issues.push("latency_prometheus_metric_contract_missing".to_string());
                break;
            }
        }
    }
    if snapshot.memory_prometheus.format != "prometheus_text_v0.0.4" {
        issues.push("memory_prometheus_format_mismatch".to_string());
    }
    if snapshot.memory_prometheus.text.is_empty() {
        issues.push("memory_prometheus_metrics_missing".to_string());
    }
    if snapshot.memory_prometheus.metric_count != 5 {
        issues.push("memory_prometheus_metric_count_mismatch".to_string());
    }
    let memory_metric_names = matrixraft_memory_metric_names();
    for required_metric in [
        memory_metric_names.process_resident_memory_bytes.as_str(),
        memory_metric_names.heap_allocated_bytes.as_str(),
        memory_metric_names.log_cache_bytes.as_str(),
        memory_metric_names.snapshot_buffer_bytes.as_str(),
        memory_metric_names.replication_buffer_bytes.as_str(),
    ] {
        if !matrixraft_prometheus_text_has_metric_sample(
            &snapshot.memory_prometheus.text,
            required_metric,
        ) {
            issues.push("memory_prometheus_metric_contract_missing".to_string());
        }
    }
    if snapshot.scale_prometheus.format != "prometheus_text_v0.0.4" {
        issues.push("scale_prometheus_format_mismatch".to_string());
    }
    if snapshot.scale_prometheus.text.is_empty() {
        issues.push("scale_prometheus_metrics_missing".to_string());
    }
    if snapshot.scale_prometheus.metric_count != 6 {
        issues.push("scale_prometheus_metric_count_mismatch".to_string());
    }
    let scale_metric_names = matrixraft_scale_metric_names();
    for required_metric in [
        scale_metric_names.proposal_qps_total.as_str(),
        scale_metric_names.append_entries_qps_total.as_str(),
        scale_metric_names.read_index_qps_total.as_str(),
        scale_metric_names.apply_entries_qps_total.as_str(),
        scale_metric_names.replication_bytes_total.as_str(),
        scale_metric_names.apply_bytes_total.as_str(),
    ] {
        if !matrixraft_prometheus_text_has_metric_sample(
            &snapshot.scale_prometheus.text,
            required_metric,
        ) {
            issues.push("scale_prometheus_metric_contract_missing".to_string());
        }
    }
    if snapshot.scale_target_prometheus.format != "prometheus_text_v0.0.4" {
        issues.push("scale_target_prometheus_format_mismatch".to_string());
    }
    if snapshot.scale_target_prometheus.text.is_empty() {
        issues.push("scale_target_prometheus_metrics_missing".to_string());
    }
    if snapshot.scale_target_prometheus.metric_count != 12 {
        issues.push("scale_target_prometheus_metric_count_mismatch".to_string());
    }
    let scale_target_metric_names = matrixraft_scale_target_metric_names();
    for required_metric in [
        scale_target_metric_names.min_proposal_qps.as_str(),
        scale_target_metric_names.min_append_entries_qps.as_str(),
        scale_target_metric_names.min_read_index_qps.as_str(),
        scale_target_metric_names.min_apply_entries_qps.as_str(),
        scale_target_metric_names
            .min_replication_mib_per_sec
            .as_str(),
        scale_target_metric_names.min_apply_mib_per_sec.as_str(),
        scale_target_metric_names.proposal_target_percent.as_str(),
        scale_target_metric_names
            .append_entries_target_percent
            .as_str(),
        scale_target_metric_names.read_index_target_percent.as_str(),
        scale_target_metric_names
            .apply_entries_target_percent
            .as_str(),
        scale_target_metric_names
            .replication_target_percent
            .as_str(),
        scale_target_metric_names.apply_target_percent.as_str(),
    ] {
        if !matrixraft_prometheus_text_has_metric_sample(
            &snapshot.scale_target_prometheus.text,
            required_metric,
        ) {
            issues.push("scale_target_prometheus_metric_contract_missing".to_string());
        }
    }
    let optimization_hint_count = snapshot.optimization.hints.len() as u64;
    if !snapshot.benchmark_prometheus.text.is_empty()
        || snapshot.benchmark_prometheus.metric_count > 0
    {
        if snapshot.benchmark_prometheus.format != "prometheus_text_v0.0.4" {
            issues.push("benchmark_prometheus_format_mismatch".to_string());
        }
        if snapshot.benchmark_prometheus.text.is_empty() {
            issues.push("benchmark_prometheus_metrics_missing".to_string());
        }
        let benchmark_metric_names = matrixraft_baseline_raft_benchmark_metric_names();
        for required_metric in [
            benchmark_metric_names.passed.as_str(),
            benchmark_metric_names.production_evidence_ready.as_str(),
            benchmark_metric_names.failed_workload_total.as_str(),
            benchmark_metric_names.blocker_total.as_str(),
            benchmark_metric_names.worst_p50_ratio.as_str(),
            benchmark_metric_names.worst_p99_ratio.as_str(),
            benchmark_metric_names.worst_throughput_ratio.as_str(),
            benchmark_metric_names.worst_cpu_ratio.as_str(),
            benchmark_metric_names
                .worst_peak_resident_memory_ratio
                .as_str(),
            benchmark_metric_names.workload_passed.as_str(),
            benchmark_metric_names.workload_p50_ratio.as_str(),
            benchmark_metric_names.workload_p99_ratio.as_str(),
            benchmark_metric_names.workload_throughput_ratio.as_str(),
            benchmark_metric_names.workload_cpu_ratio.as_str(),
            benchmark_metric_names
                .workload_peak_resident_memory_ratio
                .as_str(),
        ] {
            if !matrixraft_prometheus_text_has_metric_sample(
                &snapshot.benchmark_prometheus.text,
                required_metric,
            ) {
                issues.push("benchmark_prometheus_metric_contract_missing".to_string());
                break;
            }
        }
        let actual_metric_count = snapshot
            .benchmark_prometheus
            .text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count() as u64;
        if snapshot.benchmark_prometheus.metric_count != actual_metric_count {
            issues.push("benchmark_prometheus_metric_count_mismatch".to_string());
        }
    }
    let optimization_critical_count = snapshot
        .optimization
        .hints
        .iter()
        .filter(|hint| hint.severity == OptimizationHintSeverity::Critical)
        .count() as u64;
    let optimization_warning_count = snapshot
        .optimization
        .hints
        .iter()
        .filter(|hint| hint.severity == OptimizationHintSeverity::Warning)
        .count() as u64;
    if snapshot.optimization.hint_count != optimization_hint_count {
        issues.push("optimization_hint_count_mismatch".to_string());
    }
    if snapshot.optimization.critical_count != optimization_critical_count {
        issues.push("optimization_critical_count_mismatch".to_string());
    }
    if snapshot.optimization.warning_count != optimization_warning_count {
        issues.push("optimization_warning_count_mismatch".to_string());
    }
    if snapshot.optimization.ready != (optimization_critical_count == 0) {
        issues.push("optimization_ready_mismatch".to_string());
    }
    let diagnostic_error_count = snapshot
        .diagnostics
        .iter()
        .filter(|entry| entry.severity == DiagnosticSeverity::Error)
        .count();
    let diagnostic_warning_count = snapshot
        .diagnostics
        .iter()
        .filter(|entry| entry.severity == DiagnosticSeverity::Warn)
        .count();
    if snapshot.triage.diagnostic_error_count != diagnostic_error_count {
        issues.push("triage_diagnostic_error_count_mismatch".to_string());
    }
    if snapshot.triage.diagnostic_warning_count != diagnostic_warning_count {
        issues.push("triage_diagnostic_warning_count_mismatch".to_string());
    }
    if snapshot.triage.critical_optimization_count != snapshot.optimization.critical_count {
        issues.push("triage_critical_count_mismatch".to_string());
    }
    if snapshot.triage.warning_optimization_count != snapshot.optimization.warning_count {
        issues.push("triage_warning_count_mismatch".to_string());
    }
    if snapshot.triage.alert_rule_count != snapshot.alerts.len() {
        issues.push("triage_alert_count_mismatch".to_string());
    }
    for expected_alert in matrixraft_alert_rules() {
        match snapshot
            .alerts
            .iter()
            .find(|rule| rule.alert == expected_alert.alert)
        {
            Some(actual_alert) if actual_alert != &expected_alert => {
                issues.push("alert_rule_contract_mismatch".to_string());
            }
            Some(_) => {}
            None => issues.push("alert_rule_contract_missing".to_string()),
        }
    }
    if let Some(top_alert) = &snapshot.triage.top_alert {
        if !snapshot.alerts.iter().any(|rule| rule.alert == *top_alert) {
            issues.push("triage_top_alert_missing".to_string());
        }
    }
    match (
        &snapshot.triage.top_diagnostic_target,
        &snapshot.triage.top_diagnostic_message,
    ) {
        (Some(top_target), Some(top_message)) => {
            if !snapshot
                .diagnostics
                .iter()
                .any(|entry| entry.target == *top_target && entry.message == *top_message)
            {
                issues.push("triage_top_diagnostic_missing".to_string());
            }
        }
        (Some(_), None) | (None, Some(_)) => {
            issues.push("triage_top_diagnostic_incomplete".to_string());
        }
        (None, None) => {}
    }
    if let Some(top_optimization_hint) = &snapshot.triage.top_optimization_hint {
        if !snapshot
            .optimization
            .hints
            .iter()
            .any(|hint| hint.id == *top_optimization_hint)
        {
            issues.push("triage_top_optimization_hint_missing".to_string());
        }
    }
    if snapshot.runbook_steps.is_empty() {
        issues.push("runbook_steps_missing".to_string());
    }
    if snapshot.runbook_prometheus.format != "prometheus_text_v0.0.4" {
        issues.push("runbook_prometheus_format_mismatch".to_string());
    }
    if snapshot.runbook_prometheus.text.is_empty() {
        issues.push("runbook_prometheus_metrics_missing".to_string());
    }
    let expected_runbook_metric_count = snapshot.runbook_steps.len() as u64
        + matrixraft_runbook_step_counts(&snapshot.runbook_steps).len() as u64
        + u64::from(!snapshot.runbook_steps.is_empty());
    if snapshot.runbook_prometheus.metric_count != expected_runbook_metric_count {
        issues.push("runbook_prometheus_metric_count_mismatch".to_string());
    }
    for required_metric in [
        metric_names.operator_runbook_step_total.as_str(),
        metric_names.operator_runbook_step_present.as_str(),
        metric_names.operator_runbook_first_step.as_str(),
    ] {
        if !matrixraft_prometheus_text_has_metric_sample(
            &snapshot.runbook_prometheus.text,
            required_metric,
        ) {
            issues.push("runbook_prometheus_metric_contract_missing".to_string());
        }
    }
    if let Some(first_step) = snapshot.runbook_steps.first() {
        let step_label = format!(
            "step=\"{}\"",
            escape_prometheus_label_value(first_step.id.as_str())
        );
        let severity_label = format!(
            "severity=\"{}\"",
            escape_prometheus_label_value(first_step.severity.as_str())
        );
        let target_label = format!(
            "target=\"{}\"",
            escape_prometheus_label_value(first_step.target.as_str())
        );
        if !snapshot.runbook_prometheus.text.lines().any(|line| {
            line.contains(metric_names.operator_runbook_first_step.as_str())
                && line.contains(&step_label)
                && line.contains(&severity_label)
                && line.contains(&target_label)
        }) {
            issues.push("runbook_prometheus_first_step_missing".to_string());
        }
    }
    for step in &snapshot.runbook_steps {
        let step_label = format!(
            "step=\"{}\"",
            escape_prometheus_label_value(step.id.as_str())
        );
        let severity_label = format!(
            "severity=\"{}\"",
            escape_prometheus_label_value(step.severity.as_str())
        );
        let target_label = format!(
            "target=\"{}\"",
            escape_prometheus_label_value(step.target.as_str())
        );
        if !snapshot.runbook_prometheus.text.lines().any(|line| {
            line.contains(metric_names.operator_runbook_step_present.as_str())
                && line.contains(&step_label)
                && line.contains(&severity_label)
                && line.contains(&target_label)
        }) {
            issues.push("runbook_prometheus_step_missing".to_string());
            break;
        }
    }
    let mut expected_runbook_steps = matrixraft_operator_runbook_steps(
        &snapshot.triage,
        &snapshot.optimization,
        &snapshot.alerts,
    );
    let benchmark_runbook_steps =
        matrixraft_operator_benchmark_runbook_steps_from_prometheus(&snapshot.benchmark_prometheus);
    if !benchmark_runbook_steps.is_empty() {
        expected_runbook_steps.retain(|step| step.id != "continue_normal_observation");
        for step in benchmark_runbook_steps {
            if !expected_runbook_steps
                .iter()
                .any(|existing| existing.id == step.id)
            {
                expected_runbook_steps.push(step);
            }
        }
    }
    if snapshot.runbook_steps.len() != expected_runbook_steps.len() {
        issues.push("runbook_step_count_mismatch".to_string());
    }
    for expected_step in expected_runbook_steps {
        match snapshot
            .runbook_steps
            .iter()
            .find(|step| step.id == expected_step.id)
        {
            Some(actual_step) if actual_step != &expected_step => {
                issues.push("runbook_step_contract_mismatch".to_string());
            }
            Some(_) => {}
            None => issues.push("runbook_step_contract_missing".to_string()),
        }
    }
    let has_benchmark_runbook_steps = snapshot
        .runbook_steps
        .iter()
        .any(|step| step.target == "benchmark_parity");
    match snapshot.triage.status.as_str() {
        "ready" => {
            if snapshot.triage.severity != "info" {
                issues.push("triage_ready_severity_mismatch".to_string());
            }
            if !has_benchmark_runbook_steps
                && !snapshot
                    .runbook_steps
                    .iter()
                    .any(|step| step.id == "continue_normal_observation")
            {
                issues.push("runbook_ready_step_missing".to_string());
            }
        }
        "watch" => {
            if snapshot.triage.severity != "warning" {
                issues.push("triage_watch_severity_mismatch".to_string());
            }
            if !snapshot
                .runbook_steps
                .iter()
                .any(|step| step.severity == "warning")
            {
                issues.push("runbook_warning_step_missing".to_string());
            }
        }
        "needs_attention" => {
            if snapshot.triage.severity != "critical" {
                issues.push("triage_attention_severity_mismatch".to_string());
            }
            if !snapshot
                .runbook_steps
                .iter()
                .any(|step| step.severity == "critical")
            {
                issues.push("runbook_critical_step_missing".to_string());
            }
        }
        "" => {}
        _ => issues.push("triage_status_unknown".to_string()),
    }

    matrixraft_debug_bundle_validation_report(issues)
}

pub fn matrixraft_validate_debug_snapshot_json(json: &str) -> DebugBundleValidationReport {
    match serde_json::from_str::<DebugSnapshot>(json) {
        Ok(snapshot) => matrixraft_validate_debug_snapshot(&snapshot),
        Err(_) => matrixraft_debug_bundle_validation_report(vec!["json_parse_failed".to_string()]),
    }
}

pub fn matrixraft_debug_bundle_validation_prometheus(
    report: &DebugBundleValidationReport,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let metrics = matrixraft_metric_names();
    let mut text = String::new();
    let mut metric_count = 0_u64;

    push_metric(
        &mut text,
        &metrics.debug_bundle_validation_ready,
        labels,
        u64::from(report.ready),
    );
    metric_count += 1;
    push_metric(
        &mut text,
        &metrics.debug_bundle_validation_issue_total,
        labels,
        report.issue_count as u64,
    );
    metric_count += 1;
    if let Some(first_issue) = report.issues.first() {
        let mut first_issue_labels = labels.to_vec();
        first_issue_labels.push(("issue", first_issue.as_str()));
        push_metric(
            &mut text,
            &metrics.debug_bundle_validation_first_issue,
            &first_issue_labels,
            1,
        );
        metric_count += 1;
    }

    for issue in &report.issues {
        let mut issue_labels = labels.to_vec();
        issue_labels.push(("issue", issue.as_str()));
        push_metric(
            &mut text,
            &metrics.debug_bundle_validation_issue,
            &issue_labels,
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

fn matrixraft_debug_bundle_validation_report(issues: Vec<String>) -> DebugBundleValidationReport {
    DebugBundleValidationReport {
        ready: issues.is_empty(),
        issue_count: issues.len(),
        issues,
    }
}

fn matrixraft_debug_snapshot_now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

pub fn matrixraft_operator_triage_summary(
    diagnostics: &[DiagnosticLogEntry],
    optimization: &OptimizationReport,
    alerts: &[AlertRule],
) -> OperatorTriageSummary {
    let diagnostic_error_count = diagnostics
        .iter()
        .filter(|entry| entry.severity == DiagnosticSeverity::Error)
        .count();
    let diagnostic_warning_count = diagnostics
        .iter()
        .filter(|entry| entry.severity == DiagnosticSeverity::Warn)
        .count();
    let top_alert = alerts
        .iter()
        .find(|rule| rule.severity == "critical")
        .or_else(|| alerts.iter().find(|rule| rule.severity == "warning"))
        .map(|rule| rule.alert.clone());
    let top_diagnostic = diagnostics
        .iter()
        .find(|entry| entry.severity == DiagnosticSeverity::Error)
        .or_else(|| {
            diagnostics
                .iter()
                .find(|entry| entry.severity == DiagnosticSeverity::Warn)
        })
        .or_else(|| diagnostics.first());
    let top_optimization_hint = optimization
        .hints
        .iter()
        .find(|hint| hint.severity == OptimizationHintSeverity::Critical)
        .or_else(|| {
            optimization
                .hints
                .iter()
                .find(|hint| hint.severity == OptimizationHintSeverity::Warning)
        })
        .map(|hint| hint.id.clone());

    let (status, severity, first_action) =
        if diagnostic_error_count > 0 || optimization.critical_count > 0 {
            (
                "needs_attention",
                "critical",
                "Inspect error diagnostics and critical optimization hints first.",
            )
        } else if diagnostic_warning_count > 0 || optimization.warning_count > 0 {
            (
                "watch",
                "warning",
                "Review warning diagnostics and optimization hints before rollout.",
            )
        } else {
            ("ready", "info", "No immediate operator action is required.")
        };

    OperatorTriageSummary {
        status: status.to_string(),
        severity: severity.to_string(),
        first_action: first_action.to_string(),
        diagnostic_error_count,
        diagnostic_warning_count,
        critical_optimization_count: optimization.critical_count,
        warning_optimization_count: optimization.warning_count,
        alert_rule_count: alerts.len(),
        top_diagnostic_target: top_diagnostic.map(|entry| entry.target.clone()),
        top_diagnostic_message: top_diagnostic.map(|entry| entry.message.clone()),
        top_alert,
        top_optimization_hint,
    }
}

pub fn matrixraft_operator_triage_prometheus(
    triage: &OperatorTriageSummary,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let metrics = matrixraft_metric_names();
    let mut text = String::new();
    let mut status_labels = labels.to_vec();
    status_labels.push(("status", triage.status.as_str()));
    status_labels.push(("severity", triage.severity.as_str()));

    push_metric(
        &mut text,
        &metrics.operator_triage_status,
        &status_labels,
        1,
    );
    push_metric(
        &mut text,
        &metrics.operator_triage_diagnostic_error_total,
        labels,
        triage.diagnostic_error_count as u64,
    );
    push_metric(
        &mut text,
        &metrics.operator_triage_diagnostic_warning_total,
        labels,
        triage.diagnostic_warning_count as u64,
    );
    push_metric(
        &mut text,
        &metrics.operator_triage_optimization_critical_total,
        labels,
        triage.critical_optimization_count,
    );
    push_metric(
        &mut text,
        &metrics.operator_triage_optimization_warning_total,
        labels,
        triage.warning_optimization_count,
    );
    push_metric(
        &mut text,
        &metrics.operator_triage_alert_rule_total,
        labels,
        triage.alert_rule_count as u64,
    );
    let mut first_action_labels = labels.to_vec();
    first_action_labels.push(("action", triage.first_action.as_str()));
    first_action_labels.push(("status", triage.status.as_str()));
    first_action_labels.push(("severity", triage.severity.as_str()));
    push_metric(
        &mut text,
        &metrics.operator_triage_first_action,
        &first_action_labels,
        1,
    );

    let mut metric_count = 7;
    if let (Some(top_target), Some(top_message)) = (
        &triage.top_diagnostic_target,
        &triage.top_diagnostic_message,
    ) {
        let mut top_diagnostic_labels = labels.to_vec();
        top_diagnostic_labels.push(("target", top_target.as_str()));
        top_diagnostic_labels.push(("message", top_message.as_str()));
        top_diagnostic_labels.push(("severity", triage.severity.as_str()));
        push_metric(
            &mut text,
            &metrics.operator_triage_top_diagnostic,
            &top_diagnostic_labels,
            1,
        );
        metric_count += 1;
    }
    if let Some(top_alert) = &triage.top_alert {
        let mut top_alert_labels = labels.to_vec();
        top_alert_labels.push(("alert", top_alert.as_str()));
        top_alert_labels.push(("severity", triage.severity.as_str()));
        push_metric(
            &mut text,
            &metrics.operator_triage_top_alert,
            &top_alert_labels,
            1,
        );
        metric_count += 1;
    }
    if let Some(top_hint) = &triage.top_optimization_hint {
        let mut top_hint_labels = labels.to_vec();
        top_hint_labels.push(("hint", top_hint.as_str()));
        top_hint_labels.push(("severity", triage.severity.as_str()));
        push_metric(
            &mut text,
            &metrics.operator_triage_top_optimization_hint,
            &top_hint_labels,
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

pub fn matrixraft_diagnostic_log_prometheus(
    diagnostics: &[DiagnosticLogEntry],
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let metrics = matrixraft_metric_names();
    let mut text = String::new();
    let mut metric_count = 0_u64;

    for severity in ["info", "warn", "error"] {
        let count = diagnostics
            .iter()
            .filter(|entry| matrixraft_diagnostic_severity_label(entry.severity) == severity)
            .count() as u64;
        let mut severity_labels = labels.to_vec();
        severity_labels.push(("severity", severity));
        push_metric(
            &mut text,
            &metrics.diagnostic_log_total,
            &severity_labels,
            count,
        );
        metric_count += 1;
    }

    for entry in diagnostics {
        let mut entry_labels = labels.to_vec();
        entry_labels.push(("target", entry.target.as_str()));
        entry_labels.push((
            "severity",
            matrixraft_diagnostic_severity_label(entry.severity),
        ));
        entry_labels.push(("message", entry.message.as_str()));
        push_metric(
            &mut text,
            &metrics.diagnostic_log_entry_total,
            &entry_labels,
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

fn matrixraft_diagnostic_severity_label(severity: DiagnosticSeverity) -> &'static str {
    match severity {
        DiagnosticSeverity::Info => "info",
        DiagnosticSeverity::Warn => "warn",
        DiagnosticSeverity::Error => "error",
    }
}

pub fn matrixraft_operator_runbook_steps(
    triage: &OperatorTriageSummary,
    optimization: &OptimizationReport,
    alerts: &[AlertRule],
) -> Vec<OperatorRunbookStep> {
    let mut steps = Vec::new();

    if triage.diagnostic_error_count > 0 {
        steps.push(matrixraft_runbook_step(
            "inspect_error_diagnostics",
            "critical",
            "diagnostics",
            "Review RustRaft diagnostic log entries with error severity.",
            "Error diagnostic count returns to 0 in the debug snapshot.",
        ));
    }
    if optimization.critical_count > 0 {
        steps.push(matrixraft_runbook_step(
            "resolve_critical_optimization_hints",
            "critical",
            "optimization",
            "Resolve critical RustRaft optimization hints before rollout.",
            "rustraft_optimization_critical_total is 0 and triage severity is not critical.",
        ));
    }
    if triage.severity == "critical" && alerts.iter().any(|rule| rule.severity == "critical") {
        steps.push(matrixraft_runbook_step(
            "wire_critical_alerts",
            "critical",
            "alerts",
            "Install or verify critical RustRaft alert rules in monitoring.",
            "Critical alert expressions are active in the monitoring backend.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftRuntimeAdmissionRejected")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_runtime_pressure_bottleneck",
            "critical",
            "runtime_pressure",
            "Inspect Runtime Pressure Bottlenecks first, then apply the matching action-source remediation before trusting release-scale QPS, latency, or memory parity.",
            "rustraft_runtime_pressure_admission_rejected is 0, rustraft_runtime_pressure_bottleneck_score_percent has no active samples above 0, and runtime pressure action-source metrics no longer attribute pressure to the top bottleneck.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftRuntimePressureBottleneckActive")
    {
        steps.push(matrixraft_runbook_step(
            "inspect_runtime_pressure_bottleneck_warning",
            "warning",
            "runtime_pressure",
            "Inspect Runtime Pressure Bottlenecks and Runtime Pressure Action Sources while admission is still observe-only or below hard rejection.",
            "rustraft_runtime_pressure_bottleneck_score_percent has no active samples above 0, and runtime pressure action-source metrics no longer attribute warning pressure to the top bottleneck before release-scale QPS, latency, or memory parity is accepted.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftProductionReadinessRuntimePressureBottleneck")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_production_readiness_runtime_pressure_bottleneck",
            "critical",
            "production_readiness",
            "Inspect Production Runtime Pressure Bottlenecks, then clear each ranked runtime-pressure blocker before rollout.",
            "rustraft_production_readiness_runtime_pressure_bottleneck_score_percent has no active samples above 0 and rustraft_production_readiness_blocker_total no longer attributes production blocking to runtime pressure.",
        ));
    }
    if triage.diagnostic_warning_count > 0 || optimization.warning_count > 0 {
        steps.push(matrixraft_runbook_step(
            "review_warning_signals",
            "warning",
            "diagnostics",
            "Review warning diagnostics and optimization hints for trend risk.",
            "rustraft_operator_triage_diagnostic_warning_total and rustraft_operator_triage_optimization_warning_total are 0 or acknowledged before production claims.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftMemoryPressure")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_memory_pressure",
            "warning",
            "memory",
            "Inspect resident, heap, log-cache, snapshot-buffer, and replication-buffer memory panels before trusting release-scale QPS or latency evidence.",
            "rustraft_process_resident_memory_bytes, rustraft_heap_allocated_bytes, rustraft_log_cache_bytes, rustraft_snapshot_buffer_bytes, and rustraft_replication_buffer_bytes stay below configured warning thresholds or have accepted capacity changes.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftLatencyPressure")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_latency_pressure",
            "warning",
            "latency",
            "Inspect append, vote, pre-vote, read-index, and snapshot-install p99 latency panels before accepting release-scale latency parity.",
            "append, vote, pre-vote, read-index, and snapshot-install p99 latency stay below configured warning thresholds while QPS and throughput targets remain satisfied.",
        ));
    }
    if triage.severity == "warning"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftRuntimeScalePressure")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_scale_target_pressure",
            "warning",
            "scale",
            "Inspect Runtime Scale Pressure, release-scale target attainment, runtime pressure actions, and runtime pressure action sources before raising QPS parity claims.",
            "rustraft_runtime_pressure_scale is 0, scale target-attainment panels are at or above 100 percent, rustraft_runtime_pressure_action_total no longer includes scale tuning actions, and rustraft_runtime_pressure_action_source_total no longer attributes pressure to scale components.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftNodeRuntimeTimerBackpressure")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_node_runtime_timer_backpressure",
            "warning",
            "node_runtime",
            "Inspect node-runtime timer utilization, pending, rejected, accepted, and completed tick panels before trusting release-scale latency or QPS evidence.",
            "rustraft_node_runtime_timer_utilization_percent stays below 80, rustraft_node_runtime_timer_backpressure is 0, rustraft_node_runtime_timer_pending_ticks drains below rustraft_node_runtime_timer_max_pending_ticks, and rate(rustraft_node_runtime_timer_rejected_ticks_total[1m]) is 0.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftSnapshotRetryBackpressure")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_snapshot_retry_backpressure",
            "warning",
            "snapshot_lifecycle",
            "Inspect snapshot retry, send-timeout, rate-limit, sender-load, downloader-load, and coherent transfer-completion panels before trusting snapshot catch-up evidence.",
            "rustraft_snapshot_lifecycle_retry_backpressure_present is 0, retry/timeout/rate-limit panels are acknowledged, and sustained sender, downloader, and transfer-completion signals remain 1.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftWalSlowFsyncBackpressure")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_wal_slow_fsync_backpressure",
            "warning",
            "wal_lifecycle",
            "Inspect WAL slow-fsync, segment lifecycle, retained range, and compaction-after-slow-fsync panels before trusting write-latency or durability parity evidence.",
            "rustraft_wal_lifecycle_slow_fsync_backpressure_observed is 0 or acknowledged, rustraft_wal_lifecycle_segment_present remains 1, and rustraft_wal_lifecycle_compaction_after_slow_fsync_observed is 1 after slow-fsync pressure tests.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftPeerPipelineBackpressure")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_peer_pipeline_backpressure",
            "warning",
            "peer_pipeline",
            "Inspect per-peer append queue depth, reorder queue depth, reorder convergence, and snapshot installed index before trusting release-scale QPS or p99 latency evidence.",
            "rustraft_peer_append_queue_depth and rustraft_peer_reorder_queue_depth remain 0 under steady state, rustraft_peer_reorder_entries_converged_total advances during reorder tests, and lagging peers catch up or move to snapshot install.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftRuntimeReadBacklogPressure")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_read_backlog_pressure",
            "warning",
            "read_backlog",
            "Inspect Runtime Read Backlog Pressure, pending ReadIndex, bounded-stale read backlog, and read latency panels before trusting release-scale read QPS or random-replica reads.",
            "rustraft_runtime_pressure_read_backlog is 0, rustraft_runtime_pressure_read_backlog_excess is 0, pending ReadIndex requests drain below threshold, bounded-stale replica reads remain deadline-bound, and read_index p99 latency stays below the configured warning threshold.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftMembershipTransitionMissing")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_membership_transition_evidence",
            "warning",
            "membership",
            "Inspect membership readiness missing-transition panels before trusting learner promotion, witness quorum, joint consensus, or scale-up/down readiness.",
            "rustraft_membership_readiness_missing_total is 0, rustraft_membership_transition_missing_total is 0 across metaserver and data-node scopes, and rustraft_membership_transition_missing has no active joint consensus, learner catch-up, witness, or scheduler-generation gaps.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftBaselineRaftBenchmarkResourceRegression")
    {
        steps.push(matrixraft_runbook_step(
            "resolve_benchmark_resource_regression",
            "warning",
            "benchmark_parity",
            "Inspect BaselineRaft benchmark CPU and peak resident-memory ratio panels before accepting release-scale parity.",
            "rustraft_baseline_raft_benchmark_worst_cpu_ratio is at or below 1.1, rustraft_baseline_raft_benchmark_worst_peak_resident_memory_ratio is at or below 1.1, and workload-level CPU/memory ratios are explained or fixed.",
        ));
    }
    if triage.status != "ready"
        && alerts
            .iter()
            .any(|rule| rule.alert == "RustRaftBaselineRaftBenchmarkFreshnessLost")
    {
        steps.push(matrixraft_runbook_step(
            "refresh_baseline_raft_benchmark_evidence",
            "warning",
            "benchmark_parity",
            "Rerun release-mode BaselineRaft-vs-RustRaft benchmark parity when benchmark evidence is stale, missing, or future-dated before accepting QPS, latency, CPU, or memory claims.",
            "rustraft_baseline_raft_benchmark_fresh is 1, rustraft_baseline_raft_benchmark_freshness_status is fresh, rustraft_baseline_raft_benchmark_age_ms is below rustraft_baseline_raft_benchmark_max_age_ms, and benchmark generated_at/stale_after timestamps match the refreshed release artifact.",
        ));
    }
    if steps.is_empty() {
        steps.push(matrixraft_runbook_step(
            "continue_normal_observation",
            "info",
            "observability",
            "Continue normal RustRaft dashboard and alert observation.",
            "Triage status remains ready and optimization readiness remains 1.",
        ));
    }

    steps
}

pub fn matrixraft_operator_runbook_steps_with_diagnostics(
    diagnostics: &[DiagnosticLogEntry],
    triage: &OperatorTriageSummary,
    optimization: &OptimizationReport,
    alerts: &[AlertRule],
) -> Vec<OperatorRunbookStep> {
    let mut steps = matrixraft_operator_runbook_steps(triage, optimization, alerts);
    if diagnostics
        .iter()
        .any(|entry| entry.target == "rustraft.production_readiness.runtime_pressure_evidence")
        && !steps
            .iter()
            .any(|step| step.id == "fix_runtime_pressure_evidence")
    {
        steps.push(matrixraft_runbook_step(
            "fix_runtime_pressure_evidence",
            "critical",
            "runtime_pressure",
            "Regenerate RustRaft runtime-pressure admission evidence from the release-scale run before trusting QPS, latency, or backlog readiness claims.",
            "No rustraft.production_readiness.runtime_pressure_evidence diagnostic entries remain, runtime_pressure:evidence_valid is satisfied, and runtime-pressure detail records pass semantic validation.",
        ));
    }
    let production_readiness_severity = diagnostics
        .iter()
        .filter(|entry| entry.target.starts_with("rustraft.production_readiness"))
        .map(|entry| entry.severity)
        .max_by_key(|severity| match severity {
            DiagnosticSeverity::Error => 2,
            DiagnosticSeverity::Warn => 1,
            DiagnosticSeverity::Info => 0,
        });
    if let Some(severity) = production_readiness_severity {
        if severity != DiagnosticSeverity::Info
            && !steps
                .iter()
                .any(|step| step.id == "resolve_production_readiness_blockers")
        {
            let (step_severity, validation) = match severity {
                DiagnosticSeverity::Error => (
                    "critical",
                    "rustraft_production_readiness_blocker_total is 0, rustraft_production_readiness_missing_total is 0, and production readiness diagnostic entries no longer have error severity.",
                ),
                DiagnosticSeverity::Warn | DiagnosticSeverity::Info => (
                    "warning",
                    "rustraft_production_readiness_missing_total is 0 or acknowledged, and production readiness diagnostic entries no longer have warning severity.",
                ),
            };
            steps.push(matrixraft_runbook_step(
                "resolve_production_readiness_blockers",
                step_severity,
                "production_readiness",
                "Resolve RustRaft production readiness missing evidence and blockers before rollout.",
                validation,
            ));
        }
    }
    steps
}

fn matrixraft_operator_benchmark_runbook_steps_from_prometheus(
    benchmark_prometheus: &PrometheusMetricSet,
) -> Vec<OperatorRunbookStep> {
    if benchmark_prometheus.text.is_empty() {
        return Vec::new();
    }
    let metrics = matrixraft_baseline_raft_benchmark_metric_names();
    let benchmark_failed =
        matrixraft_prometheus_metric_min_value(&benchmark_prometheus.text, metrics.passed.as_str())
            .is_some_and(|passed| passed == 0.0)
            || matrixraft_prometheus_metric_max_value(
                &benchmark_prometheus.text,
                metrics.failed_workload_total.as_str(),
            )
            .is_some_and(|failed| failed > 0.0)
            || matrixraft_prometheus_metric_max_value(
                &benchmark_prometheus.text,
                metrics.blocker_total.as_str(),
            )
            .is_some_and(|blockers| blockers > 0.0);
    let benchmark_regressed = matrixraft_prometheus_metric_max_value(
        &benchmark_prometheus.text,
        metrics.worst_p50_ratio.as_str(),
    )
    .is_some_and(|ratio| ratio > 1.1)
        || matrixraft_prometheus_metric_max_value(
            &benchmark_prometheus.text,
            metrics.worst_p99_ratio.as_str(),
        )
        .is_some_and(|ratio| ratio > 1.1)
        || matrixraft_prometheus_metric_min_value(
            &benchmark_prometheus.text,
            metrics.worst_throughput_ratio.as_str(),
        )
        .is_some_and(|ratio| ratio < 0.9);
    let benchmark_stale =
        matrixraft_prometheus_metric_min_value(&benchmark_prometheus.text, metrics.fresh.as_str())
            .is_some_and(|fresh| fresh == 0.0);

    matrixraft_benchmark_runbook_steps_for_state(
        benchmark_failed,
        benchmark_regressed,
        benchmark_stale,
    )
}

pub(crate) fn matrixraft_benchmark_runbook_steps_for_state(
    benchmark_failed: bool,
    benchmark_regressed: bool,
    benchmark_stale: bool,
) -> Vec<OperatorRunbookStep> {
    let mut steps = Vec::new();
    if benchmark_failed {
        steps.push(matrixraft_runbook_step(
            "resolve_baseline_raft_benchmark_failure",
            "critical",
            "benchmark_parity",
            "Inspect benchmark_prometheus, failed workloads, benchmark blockers, and BaselineRaft ratio panels before claiming C++ parity.",
            "rustraft_baseline_raft_benchmark_passed is 1, rustraft_baseline_raft_benchmark_failed_workload_total is 0, rustraft_baseline_raft_benchmark_blocker_total is 0, and every required workload passed under matrixraft_debug_snapshot_with_benchmark_artifacts.",
        ));
    }
    if benchmark_regressed {
        steps.push(matrixraft_runbook_step(
            "resolve_baseline_raft_benchmark_ratio_regression",
            "warning",
            "benchmark_parity",
            "Compare RustRaft and BaselineRaft workload p50, p99, and throughput ratios from benchmark_prometheus and the release-scale Grafana panels.",
            "rustraft_baseline_raft_benchmark_worst_p50_ratio is at or below 1.1, rustraft_baseline_raft_benchmark_worst_p99_ratio is at or below 1.1, and rustraft_baseline_raft_benchmark_worst_throughput_ratio is at or above 0.9.",
        ));
    }
    if benchmark_stale {
        steps.push(matrixraft_runbook_step(
            "refresh_baseline_raft_benchmark_evidence",
            "warning",
            "benchmark_parity",
            "Rerun release-mode BaselineRaft-vs-RustRaft benchmark parity when benchmark evidence is stale, missing, or future-dated before accepting QPS, latency, CPU, or memory claims.",
            "rustraft_baseline_raft_benchmark_fresh is 1, rustraft_baseline_raft_benchmark_freshness_status is fresh, rustraft_baseline_raft_benchmark_age_ms is below rustraft_baseline_raft_benchmark_max_age_ms, and benchmark generated_at/stale_after timestamps match the refreshed release artifact.",
        ));
    }
    steps
}

fn matrixraft_prometheus_metric_max_value(text: &str, metric_name: &str) -> Option<f64> {
    matrixraft_prometheus_metric_values(text, metric_name).reduce(f64::max)
}

fn matrixraft_prometheus_metric_min_value(text: &str, metric_name: &str) -> Option<f64> {
    matrixraft_prometheus_metric_values(text, metric_name).reduce(f64::min)
}

fn matrixraft_prometheus_metric_sample_name(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let name_end = trimmed
        .find(|c: char| c == '{' || c.is_ascii_whitespace())
        .unwrap_or(trimmed.len());
    let name = &trimmed[..name_end];
    if name.is_empty() {
        return None;
    }
    let mut chars = name.chars();
    let first = chars.next()?;
    if !(first.is_ascii_alphabetic() || first == '_' || first == ':') {
        return None;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':') {
        return None;
    }
    Some(name)
}

fn matrixraft_prometheus_label_name_is_valid(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn matrixraft_prometheus_label_set_is_well_formed(labels: &str) -> bool {
    if labels.is_empty() {
        return true;
    }
    let bytes = labels.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let name_start = cursor;
        while cursor < bytes.len()
            && ((bytes[cursor] as char).is_ascii_alphanumeric() || bytes[cursor] == b'_')
        {
            cursor += 1;
        }
        if !matrixraft_prometheus_label_name_is_valid(&labels[name_start..cursor]) {
            return false;
        }
        if labels[cursor..].starts_with("=\"") {
            cursor += 2;
        } else {
            return false;
        }
        let mut closed_value = false;
        while cursor < bytes.len() {
            match bytes[cursor] {
                b'\\' => {
                    cursor += 2;
                }
                b'"' => {
                    cursor += 1;
                    closed_value = true;
                    break;
                }
                b'\n' | b'\r' => return false,
                _ => cursor += 1,
            }
        }
        if !closed_value {
            return false;
        }
        if cursor == bytes.len() {
            return true;
        }
        if bytes[cursor] != b',' {
            return false;
        }
        cursor += 1;
        if cursor == bytes.len() {
            return false;
        }
    }
    true
}

fn matrixraft_prometheus_sample_line_is_well_formed(line: &str) -> bool {
    let trimmed = line.trim_start();
    let Some(name) = matrixraft_prometheus_metric_sample_name(trimmed) else {
        return false;
    };
    let mut rest = &trimmed[name.len()..];
    if rest.starts_with('{') {
        let Some(labels_end) = rest.find('}') else {
            return false;
        };
        if !matrixraft_prometheus_label_set_is_well_formed(&rest[1..labels_end]) {
            return false;
        }
        rest = &rest[labels_end + 1..];
    }
    rest.trim_start()
        .split_whitespace()
        .next()
        .is_some_and(|value| !value.starts_with('#'))
}

pub(crate) fn matrixraft_prometheus_sample_lines_are_well_formed(text: &str) -> bool {
    text.lines()
        .all(matrixraft_prometheus_sample_line_is_well_formed)
}

pub(crate) fn matrixraft_prometheus_text_has_metric_sample(text: &str, metric_name: &str) -> bool {
    text.lines()
        .any(|line| matrixraft_prometheus_metric_sample_name(line) == Some(metric_name))
}

fn matrixraft_prometheus_metric_values<'a>(
    text: &'a str,
    metric_name: &'a str,
) -> impl Iterator<Item = f64> + 'a {
    text.lines().filter_map(move |line| {
        if matrixraft_prometheus_metric_sample_name(line) != Some(metric_name) {
            return None;
        }
        line.rsplit_once(' ')
            .and_then(|(_, value)| value.parse::<f64>().ok())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_metric_values_ignore_shadow_metric_names() {
        let text = concat!(
            "rustraft_baseline_raft_benchmark_worst_p99_ratio_shadow 99\n",
            "  rustraft_baseline_raft_benchmark_worst_p99_ratio{workload=\"steady\"} 1.05\n",
            "rustraft_baseline_raft_benchmark_worst_p99_ratio_extra 42\n",
        );

        assert_eq!(
            matrixraft_prometheus_metric_max_value(
                text,
                "rustraft_baseline_raft_benchmark_worst_p99_ratio",
            ),
            Some(1.05),
        );
        assert_eq!(
            matrixraft_prometheus_metric_min_value(
                text,
                "rustraft_baseline_raft_benchmark_worst_p99_ratio",
            ),
            Some(1.05),
        );
    }

    #[test]
    fn grafana_metric_reference_checks_all_occurrences_without_shadow_suffixes() {
        assert!(matrixraft_expr_references_metric(
            "sum(rate(rustraft_append_latency_ms_shadow[5m])) + histogram_quantile(0.99, sum(rate(rustraft_append_latency_ms_bucket[5m])))",
            "rustraft_append_latency_ms",
        ));
        assert!(!matrixraft_expr_references_metric(
            "sum(rate(rustraft_append_latency_ms_shadow[5m]))",
            "rustraft_append_latency_ms",
        ));
        assert!(matrixraft_expr_references_metric(
            "sum(rate(rustraft_append_latency_ms_bucket[5m]))",
            "rustraft_append_latency_ms",
        ));
    }
}

pub fn matrixraft_operator_runbook_prometheus(
    steps: &[OperatorRunbookStep],
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let metrics = matrixraft_metric_names();
    let mut text = String::new();
    let mut metric_count = 0_u64;

    for ((severity, target), count) in matrixraft_runbook_step_counts(steps) {
        let mut step_labels = labels.to_vec();
        step_labels.push(("severity", severity.as_str()));
        step_labels.push(("target", target.as_str()));
        push_metric(
            &mut text,
            &metrics.operator_runbook_step_total,
            &step_labels,
            count,
        );
        metric_count += 1;
    }
    for step in steps {
        let mut step_labels = labels.to_vec();
        step_labels.push(("step", step.id.as_str()));
        step_labels.push(("severity", step.severity.as_str()));
        step_labels.push(("target", step.target.as_str()));
        push_metric(
            &mut text,
            &metrics.operator_runbook_step_present,
            &step_labels,
            1,
        );
        metric_count += 1;
    }
    if let Some(first_step) = steps.first() {
        let mut first_step_labels = labels.to_vec();
        first_step_labels.push(("step", first_step.id.as_str()));
        first_step_labels.push(("severity", first_step.severity.as_str()));
        first_step_labels.push(("target", first_step.target.as_str()));
        push_metric(
            &mut text,
            &metrics.operator_runbook_first_step,
            &first_step_labels,
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

fn matrixraft_runbook_step_counts(
    steps: &[OperatorRunbookStep],
) -> BTreeMap<(String, String), u64> {
    let mut counts = BTreeMap::new();
    for step in steps {
        *counts
            .entry((step.severity.clone(), step.target.clone()))
            .or_insert(0) += 1;
    }
    counts
}

fn matrixraft_runbook_step(
    id: &str,
    severity: &str,
    target: &str,
    action: &str,
    validation: &str,
) -> OperatorRunbookStep {
    OperatorRunbookStep {
        id: id.to_string(),
        severity: severity.to_string(),
        target: target.to_string(),
        action: action.to_string(),
        validation: validation.to_string(),
    }
}

pub fn matrixraft_optimization_report_prometheus(
    report: &OptimizationReport,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let metrics = matrixraft_metric_names();
    let mut text = String::new();
    let mut metric_count = 0_u64;

    push_metric(
        &mut text,
        &metrics.optimization_ready,
        labels,
        u64::from(report.ready),
    );
    metric_count += 1;
    push_metric(
        &mut text,
        &metrics.optimization_critical_total,
        labels,
        report.critical_count,
    );
    metric_count += 1;
    push_metric(
        &mut text,
        &metrics.optimization_warning_total,
        labels,
        report.warning_count,
    );
    metric_count += 1;

    for hint in &report.hints {
        let severity = match hint.severity {
            OptimizationHintSeverity::Info => "info",
            OptimizationHintSeverity::Warning => "warning",
            OptimizationHintSeverity::Critical => "critical",
        };
        let mut hint_labels = labels.to_vec();
        hint_labels.push(("hint", hint.id.as_str()));
        hint_labels.push(("component", hint.component.as_str()));
        hint_labels.push(("severity", severity));
        push_metric(&mut text, &metrics.optimization_hint_total, &hint_labels, 1);
        metric_count += 1;
    }
    for ((component, severity), count) in matrixraft_optimization_component_hint_counts(report) {
        let mut component_labels = labels.to_vec();
        component_labels.push(("component", component.as_str()));
        component_labels.push(("severity", severity.as_str()));
        push_metric(
            &mut text,
            &metrics.optimization_component_hint_total,
            &component_labels,
            count,
        );
        metric_count += 1;
    }

    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count,
        text,
    }
}

fn matrixraft_optimization_component_hint_counts(
    report: &OptimizationReport,
) -> BTreeMap<(String, String), u64> {
    let mut counts = BTreeMap::new();
    for hint in &report.hints {
        let severity = match hint.severity {
            OptimizationHintSeverity::Info => "info",
            OptimizationHintSeverity::Warning => "warning",
            OptimizationHintSeverity::Critical => "critical",
        };
        *counts
            .entry((hint.component.clone(), severity.to_string()))
            .or_insert(0) += 1;
    }
    counts
}

pub fn matrixraft_debug_snapshot(
    admin_report: &RuntimeAdminReport,
    status_surface: &AdminStatusSurfaceInput,
    labels: &[(&str, &str)],
) -> DebugSnapshot {
    matrixraft_debug_snapshot_with_scale_metrics(
        admin_report,
        status_surface,
        &ScaleMetrics::zero(),
        labels,
    )
}

pub fn matrixraft_debug_snapshot_with_scale_metrics(
    admin_report: &RuntimeAdminReport,
    status_surface: &AdminStatusSurfaceInput,
    scale_metrics: &ScaleMetrics,
    labels: &[(&str, &str)],
) -> DebugSnapshot {
    matrixraft_debug_snapshot_with_runtime_metrics(
        admin_report,
        status_surface,
        &LatencyMetrics::zero(),
        scale_metrics,
        labels,
    )
}

pub fn matrixraft_debug_snapshot_with_runtime_metrics(
    admin_report: &RuntimeAdminReport,
    status_surface: &AdminStatusSurfaceInput,
    latency_metrics: &LatencyMetrics,
    scale_metrics: &ScaleMetrics,
    labels: &[(&str, &str)],
) -> DebugSnapshot {
    matrixraft_debug_snapshot_with_observability_metrics(
        admin_report,
        status_surface,
        latency_metrics,
        &MemoryMetrics::zero(),
        scale_metrics,
        labels,
    )
}

pub fn matrixraft_debug_snapshot_with_observability_metrics(
    admin_report: &RuntimeAdminReport,
    status_surface: &AdminStatusSurfaceInput,
    latency_metrics: &LatencyMetrics,
    memory_metrics: &MemoryMetrics,
    scale_metrics: &ScaleMetrics,
    labels: &[(&str, &str)],
) -> DebugSnapshot {
    matrixraft_debug_snapshot_with_performance_targets(
        admin_report,
        status_surface,
        latency_metrics,
        memory_metrics,
        scale_metrics,
        &ScaleRateMetrics::zero(),
        &ScaleOptimizationTargets::default(),
        labels,
    )
}

pub fn matrixraft_debug_snapshot_with_performance_targets(
    admin_report: &RuntimeAdminReport,
    status_surface: &AdminStatusSurfaceInput,
    latency_metrics: &LatencyMetrics,
    memory_metrics: &MemoryMetrics,
    scale_metrics: &ScaleMetrics,
    scale_rates: &ScaleRateMetrics,
    scale_targets: &ScaleOptimizationTargets,
    labels: &[(&str, &str)],
) -> DebugSnapshot {
    let optimization = matrixraft_merge_scale_optimization_hints(
        matrixraft_merge_memory_optimization_hints(
            matrixraft_optimization_report(status_surface),
            memory_metrics,
        ),
        scale_rates,
        scale_targets,
    );
    let diagnostics = matrixraft_admin_diagnostic_log_entries(admin_report);
    let diagnostic_prometheus = matrixraft_diagnostic_log_prometheus(&diagnostics, labels);
    let latency_prometheus = matrixraft_latency_metrics_prometheus(latency_metrics, labels);
    let memory_prometheus = matrixraft_memory_metrics_prometheus(memory_metrics, labels);
    let scale_prometheus = matrixraft_scale_metrics_prometheus(scale_metrics, labels);
    let scale_target_prometheus =
        matrixraft_scale_target_metrics_prometheus(scale_rates, scale_targets, labels);
    let alerts = matrixraft_alert_rules();
    let triage = matrixraft_operator_triage_summary(&diagnostics, &optimization, &alerts);
    let runbook_steps = matrixraft_operator_runbook_steps(&triage, &optimization, &alerts);
    let runbook_prometheus = matrixraft_operator_runbook_prometheus(&runbook_steps, labels);
    DebugSnapshot {
        contract: matrixraft_debug_bundle_contract(),
        generated_at_unix_ms: matrixraft_debug_snapshot_now_unix_ms(),
        admin_report: admin_report.clone(),
        diagnostics,
        diagnostic_prometheus,
        latency_prometheus,
        memory_prometheus,
        scale_prometheus,
        scale_target_prometheus,
        benchmark_prometheus: PrometheusMetricSet::default(),
        runtime_pressure_admission: None,
        runtime_pressure_diagnostics: Vec::new(),
        runtime_pressure_prometheus: PrometheusMetricSet::default(),
        optimization_prometheus: matrixraft_optimization_report_prometheus(&optimization, labels),
        optimization,
        grafana: matrixraft_grafana_dashboard(),
        alerts,
        triage,
        runbook_prometheus,
        runbook_steps,
    }
}

pub fn matrixraft_debug_snapshot_with_runtime_pressure_evidence(
    admin_report: &RuntimeAdminReport,
    status_surface: &AdminStatusSurfaceInput,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    scale_metrics: &ScaleMetrics,
    scale_rates: &ScaleRateMetrics,
    scale_targets: &ScaleOptimizationTargets,
    peer_pipeline: &[PeerProgress],
    policy: &RuntimePressureAdmissionPolicy,
    labels: &[(&str, &str)],
) -> DebugSnapshot {
    matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence(
        admin_report,
        status_surface,
        latency_metrics,
        latency_thresholds,
        memory_metrics,
        memory_thresholds,
        scale_metrics,
        scale_rates,
        scale_targets,
        peer_pipeline,
        &ReadBacklogMetrics::zero(),
        &ReadBacklogThresholds::default(),
        policy,
        labels,
    )
}

pub fn matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence(
    admin_report: &RuntimeAdminReport,
    status_surface: &AdminStatusSurfaceInput,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    scale_metrics: &ScaleMetrics,
    scale_rates: &ScaleRateMetrics,
    scale_targets: &ScaleOptimizationTargets,
    peer_pipeline: &[PeerProgress],
    read_backlog_metrics: &ReadBacklogMetrics,
    read_backlog_thresholds: &ReadBacklogThresholds,
    policy: &RuntimePressureAdmissionPolicy,
    labels: &[(&str, &str)],
) -> DebugSnapshot {
    let mut snapshot = matrixraft_debug_snapshot_with_performance_targets(
        admin_report,
        status_surface,
        latency_metrics,
        memory_metrics,
        scale_metrics,
        scale_rates,
        scale_targets,
        labels,
    );
    let admission =
        matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure(
            memory_metrics,
            memory_thresholds,
            latency_metrics,
            latency_thresholds,
            scale_rates,
            scale_targets,
            peer_pipeline,
            read_backlog_metrics,
            read_backlog_thresholds,
            policy,
        );
    let runtime_pressure_diagnostics =
        matrixraft_runtime_pressure_diagnostic_log_entries(&admission);
    snapshot
        .diagnostics
        .extend(runtime_pressure_diagnostics.clone());
    snapshot.diagnostic_prometheus =
        matrixraft_diagnostic_log_prometheus(&snapshot.diagnostics, labels);
    snapshot.runtime_pressure_prometheus =
        matrixraft_runtime_pressure_admission_prometheus(&admission, labels);
    snapshot.runtime_pressure_admission = Some(admission);
    snapshot.runtime_pressure_diagnostics = runtime_pressure_diagnostics;
    snapshot.triage = matrixraft_operator_triage_summary(
        &snapshot.diagnostics,
        &snapshot.optimization,
        &snapshot.alerts,
    );
    snapshot.runbook_steps = matrixraft_operator_runbook_steps(
        &snapshot.triage,
        &snapshot.optimization,
        &snapshot.alerts,
    );
    snapshot.runbook_prometheus =
        matrixraft_operator_runbook_prometheus(&snapshot.runbook_steps, labels);
    snapshot
}

pub fn matrixraft_debug_snapshot_with_runtime_pressure_read_backlog_and_node_runtime_timer_evidence(
    admin_report: &RuntimeAdminReport,
    status_surface: &AdminStatusSurfaceInput,
    latency_metrics: &LatencyMetrics,
    latency_thresholds: &LatencyOptimizationThresholds,
    memory_metrics: &MemoryMetrics,
    memory_thresholds: &MemoryOptimizationThresholds,
    scale_metrics: &ScaleMetrics,
    scale_rates: &ScaleRateMetrics,
    scale_targets: &ScaleOptimizationTargets,
    peer_pipeline: &[PeerProgress],
    read_backlog_metrics: &ReadBacklogMetrics,
    read_backlog_thresholds: &ReadBacklogThresholds,
    timer_status: &RuntimeTimerStatus,
    timer_thresholds: &NodeRuntimeTimerThresholds,
    policy: &RuntimePressureAdmissionPolicy,
    labels: &[(&str, &str)],
) -> DebugSnapshot {
    let mut snapshot = matrixraft_debug_snapshot_with_performance_targets(
        admin_report,
        status_surface,
        latency_metrics,
        memory_metrics,
        scale_metrics,
        scale_rates,
        scale_targets,
        labels,
    );
    let admission = matrixraft_runtime_pressure_admission_with_scale_pipeline_read_backlog_and_node_runtime_timer_pressure(
        memory_metrics,
        memory_thresholds,
        latency_metrics,
        latency_thresholds,
        scale_rates,
        scale_targets,
        peer_pipeline,
        read_backlog_metrics,
        read_backlog_thresholds,
        timer_status,
        timer_thresholds,
        policy,
    );
    let runtime_pressure_diagnostics =
        matrixraft_runtime_pressure_diagnostic_log_entries(&admission);
    snapshot
        .diagnostics
        .extend(runtime_pressure_diagnostics.clone());
    snapshot.diagnostic_prometheus =
        matrixraft_diagnostic_log_prometheus(&snapshot.diagnostics, labels);
    snapshot.runtime_pressure_prometheus =
        matrixraft_runtime_pressure_admission_prometheus(&admission, labels);
    snapshot.runtime_pressure_admission = Some(admission);
    snapshot.runtime_pressure_diagnostics = runtime_pressure_diagnostics;
    snapshot.triage = matrixraft_operator_triage_summary(
        &snapshot.diagnostics,
        &snapshot.optimization,
        &snapshot.alerts,
    );
    snapshot.runbook_steps = matrixraft_operator_runbook_steps(
        &snapshot.triage,
        &snapshot.optimization,
        &snapshot.alerts,
    );
    snapshot.runbook_prometheus =
        matrixraft_operator_runbook_prometheus(&snapshot.runbook_steps, labels);
    snapshot
}

pub fn matrixraft_debug_snapshot_json(
    admin_report: &RuntimeAdminReport,
    status_surface: &AdminStatusSurfaceInput,
    labels: &[(&str, &str)],
) -> String {
    serde_json::to_string_pretty(&matrixraft_debug_snapshot(
        admin_report,
        status_surface,
        labels,
    ))
    .expect("RustRaft debug snapshot must serialize")
}

pub fn matrixraft_debug_snapshot_metadata_prometheus(
    snapshot: &DebugSnapshot,
    labels: &[(&str, &str)],
) -> PrometheusMetricSet {
    let metrics = matrixraft_metric_names();
    let mut text = String::new();
    let generated_at_unix_ms = snapshot.generated_at_unix_ms;
    let age_ms = matrixraft_debug_snapshot_now_unix_ms().saturating_sub(generated_at_unix_ms);
    let max_age_ms = 3_600_000u64;
    let low_fresh_ms = 300_000u64;
    let stale_after_unix_ms = generated_at_unix_ms.saturating_add(3_600_000);
    let remaining_fresh_ms = max_age_ms.saturating_sub(age_ms);
    let low_fresh = u64::from(remaining_fresh_ms >= low_fresh_ms);
    let fresh = u64::from(age_ms <= max_age_ms);
    push_metric(
        &mut text,
        &metrics.debug_snapshot_generated_at_unix_ms,
        labels,
        generated_at_unix_ms,
    );
    push_metric(&mut text, &metrics.debug_snapshot_age_ms, labels, age_ms);
    push_metric(
        &mut text,
        &metrics.debug_snapshot_max_age_ms,
        labels,
        max_age_ms,
    );
    push_metric(
        &mut text,
        &metrics.debug_snapshot_stale_after_unix_ms,
        labels,
        stale_after_unix_ms,
    );
    push_metric(
        &mut text,
        &metrics.debug_snapshot_remaining_fresh_ms,
        labels,
        remaining_fresh_ms,
    );
    push_metric(
        &mut text,
        &metrics.debug_snapshot_low_fresh_ms,
        labels,
        low_fresh_ms,
    );
    push_metric(
        &mut text,
        &metrics.debug_snapshot_low_fresh,
        labels,
        low_fresh,
    );
    push_metric(&mut text, &metrics.debug_snapshot_fresh, labels, fresh);
    PrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count: 8,
        text,
    }
}

pub fn matrixraft_grafana_dashboard() -> GrafanaDashboard {
    let metrics = matrixraft_metric_names();
    GrafanaDashboard {
        title: "RustRaft Runtime Overview".to_string(),
        uid: "rustraft-runtime-overview".to_string(),
        timezone: "browser".to_string(),
        schema_version: 39,
        refresh: "10s".to_string(),
        tags: vec![
            "rustraft".to_string(),
            "raft".to_string(),
            "grafana".to_string(),
        ],
        panels: {
            let mut panels = vec![
            GrafanaPanel {
                id: 1,
                title: "Raft Ready".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.ready.clone(),
                unit: "bool".to_string(),
                description: "Cluster-level RustRaft readiness gauge.".to_string(),
            },
            GrafanaPanel {
                id: 2,
                title: "Append Latency".to_string(),
                panel_type: "timeseries".to_string(),
                expr: percentile_expr(&metrics.append_latency_ms, "0.99"),
                unit: "ms".to_string(),
                description: "p99 append RPC latency from RustRaft metrics.".to_string(),
            },
            GrafanaPanel {
                id: 3,
                title: "Vote Latency".to_string(),
                panel_type: "timeseries".to_string(),
                expr: percentile_expr(&metrics.vote_latency_ms, "0.99"),
                unit: "ms".to_string(),
                description: "p99 vote RPC latency from RustRaft metrics.".to_string(),
            },
            GrafanaPanel {
                id: 4,
                title: "Pre-Vote Latency".to_string(),
                panel_type: "timeseries".to_string(),
                expr: percentile_expr(&metrics.pre_vote_latency_ms, "0.99"),
                unit: "ms".to_string(),
                description: "p99 pre-vote RPC latency from RustRaft metrics.".to_string(),
            },
            GrafanaPanel {
                id: 5,
                title: "Read Index Latency".to_string(),
                panel_type: "timeseries".to_string(),
                expr: percentile_expr(&metrics.read_index_latency_ms, "0.99"),
                unit: "ms".to_string(),
                description: "p99 read-index and lease-read latency.".to_string(),
            },
            GrafanaPanel {
                id: 6,
                title: "Snapshot Install Latency".to_string(),
                panel_type: "timeseries".to_string(),
                expr: percentile_expr(&metrics.snapshot_install_latency_ms, "0.99"),
                unit: "ms".to_string(),
                description: "p99 snapshot install latency.".to_string(),
            },
            GrafanaPanel {
                id: 7,
                title: "Peer Append Queue Depth".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.peer_append_queue_depth.clone(),
                unit: "short".to_string(),
                description: "Per-peer append pipeline queue depth.".to_string(),
            },
            GrafanaPanel {
                id: 8,
                title: "Peer Reorder Queue Depth".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.peer_reorder_queue_depth.clone(),
                unit: "short".to_string(),
                description: "Per-peer message reorder queue depth.".to_string(),
            },
            GrafanaPanel {
                id: 104,
                title: "Peer Reorder Converged Entries".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (service, group, peer) (rate({}[1m]))",
                    metrics.peer_reorder_entries_converged_total
                ),
                unit: "ops".to_string(),
                description:
                    "Per-peer out-of-order append entries that later converged through the reorder queue."
                        .to_string(),
            },
            GrafanaPanel {
                id: 9,
                title: "Peer Snapshot Installed Index".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.peer_snapshot_installed_index.clone(),
                unit: "short".to_string(),
                description: "Installed snapshot index by peer.".to_string(),
            },
            GrafanaPanel {
                id: 10,
                title: "WAL Segment Count".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.wal_segment_count.clone(),
                unit: "short".to_string(),
                description: "WAL segment count for retention and compaction tracking.".to_string(),
            },
            GrafanaPanel {
                id: 11,
                title: "Blockers".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!("sum by (blocker) ({})", metrics.blocker_total),
                unit: "short".to_string(),
                description: "Active blocker totals grouped by blocker label.".to_string(),
            },
            GrafanaPanel {
                id: 12,
                title: "Fatal Events".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!("sum by (blocker) ({})", metrics.fatal_total),
                unit: "short".to_string(),
                description: "Fatal blocker totals grouped by blocker label.".to_string(),
            },
            GrafanaPanel {
                id: 13,
                title: "Diagnostic Log Entries".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!("sum by (severity) ({})", metrics.diagnostic_log_total),
                unit: "short".to_string(),
                description:
                    "Structured RustRaft diagnostic log entries grouped by severity; follow inspect_error_diagnostics when errors appear."
                        .to_string(),
            },
            GrafanaPanel {
                id: 14,
                title: "Diagnostic Log Entry Detail".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (target, severity, message) ({})",
                    metrics.diagnostic_log_entry_total
                ),
                unit: "short".to_string(),
                description:
                    "Structured RustRaft diagnostic log entries grouped by target, severity, and message for inspect_error_diagnostics."
                        .to_string(),
            },
            GrafanaPanel {
                id: 15,
                title: "Optimization Ready".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.optimization_ready.clone(),
                unit: "bool".to_string(),
                description:
                    "Optimization readiness gauge; 1 means no critical hints. When it drops to 0, follow resolve_critical_optimization_hints."
                        .to_string(),
            },
            GrafanaPanel {
                id: 16,
                title: "Optimization Critical Hints".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.optimization_critical_total.clone(),
                unit: "short".to_string(),
                description:
                    "Critical optimization hints exported by RustRaft status evidence; drive resolve_critical_optimization_hints before rollout."
                        .to_string(),
            },
            GrafanaPanel {
                id: 17,
                title: "Optimization Warning Hints".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.optimization_warning_total.clone(),
                unit: "short".to_string(),
                description: "Warning optimization hints exported by RustRaft status evidence."
                    .to_string(),
            },
            GrafanaPanel {
                id: 18,
                title: "Optimization Hint Detail".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (hint, component, severity) ({})",
                    metrics.optimization_hint_total
                ),
                unit: "short".to_string(),
                description: "Optimization hints grouped by hint, component, and severity."
                    .to_string(),
            },
            GrafanaPanel {
                id: 19,
                title: "Optimization Component Hints".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (component, severity) ({})",
                    metrics.optimization_component_hint_total
                ),
                unit: "short".to_string(),
                description: "Optimization hints grouped by component and severity.".to_string(),
            },
            GrafanaPanel {
                id: 20,
                title: "Operator Triage Status".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.operator_triage_status.clone(),
                unit: "short".to_string(),
                description: "Current operator triage status labeled by status and severity."
                    .to_string(),
            },
            GrafanaPanel {
                id: 21,
                title: "Triage Diagnostic Errors".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.operator_triage_diagnostic_error_total.clone(),
                unit: "short".to_string(),
                description: "Diagnostic error count from the operator triage summary."
                    .to_string(),
            },
            GrafanaPanel {
                id: 22,
                title: "Triage Diagnostic Warnings".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.operator_triage_diagnostic_warning_total.clone(),
                unit: "short".to_string(),
                description: "Diagnostic warning count from the operator triage summary."
                    .to_string(),
            },
            GrafanaPanel {
                id: 23,
                title: "Triage Critical Optimizations".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.operator_triage_optimization_critical_total.clone(),
                unit: "short".to_string(),
                description: "Critical optimization count from the operator triage summary."
                    .to_string(),
            },
            GrafanaPanel {
                id: 24,
                title: "Triage Warning Optimizations".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.operator_triage_optimization_warning_total.clone(),
                unit: "short".to_string(),
                description: "Warning optimization count from the operator triage summary."
                    .to_string(),
            },
            GrafanaPanel {
                id: 25,
                title: "Triage First Action".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.operator_triage_first_action.clone(),
                unit: "short".to_string(),
                description: "First operator action selected by the triage summary.".to_string(),
            },
            GrafanaPanel {
                id: 26,
                title: "Triage Alert Rules".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.operator_triage_alert_rule_total.clone(),
                unit: "short".to_string(),
                description: "Alert rule count from the operator triage summary.".to_string(),
            },
            GrafanaPanel {
                id: 27,
                title: "Triage Top Diagnostic".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.operator_triage_top_diagnostic.clone(),
                unit: "short".to_string(),
                description:
                    "Top diagnostic entry selected by the operator triage summary for inspect_error_diagnostics."
                        .to_string(),
            },
            GrafanaPanel {
                id: 28,
                title: "Triage Top Alert".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.operator_triage_top_alert.clone(),
                unit: "short".to_string(),
                description: "Top alert selected by the operator triage summary.".to_string(),
            },
            GrafanaPanel {
                id: 29,
                title: "Triage Top Optimization Hint".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.operator_triage_top_optimization_hint.clone(),
                unit: "short".to_string(),
                description:
                    "Top optimization hint selected by the operator triage summary for resolve_critical_optimization_hints."
                        .to_string(),
            },
            GrafanaPanel {
                id: 30,
                title: "Runbook Steps".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (severity, target) ({})",
                    metrics.operator_runbook_step_total
                ),
                unit: "short".to_string(),
                description: "Active operator runbook steps grouped by severity and target."
                    .to_string(),
            },
            GrafanaPanel {
                id: 31,
                title: "Runbook Step Presence".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (step, severity, target) ({})",
                    metrics.operator_runbook_step_present
                ),
                unit: "short".to_string(),
                description: "Active operator runbook steps grouped by step, severity, and target."
                    .to_string(),
            },
            GrafanaPanel {
                id: 32,
                title: "Runbook First Step".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.operator_runbook_first_step.clone(),
                unit: "short".to_string(),
                description: "First active operator runbook step selected for remediation."
                    .to_string(),
            },
            GrafanaPanel {
                id: 33,
                title: "Debug Snapshot Generated At".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.debug_snapshot_generated_at_unix_ms.clone(),
                unit: "ms".to_string(),
                description: "Debug snapshot generation timestamp in Unix milliseconds."
                    .to_string(),
            },
            GrafanaPanel {
                id: 34,
                title: "Debug Snapshot Age".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.debug_snapshot_age_ms.clone(),
                unit: "ms".to_string(),
                description: "Debug snapshot age in milliseconds when metadata metrics are exported."
                    .to_string(),
            },
            GrafanaPanel {
                id: 35,
                title: "Debug Snapshot Stale After".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.debug_snapshot_stale_after_unix_ms.clone(),
                unit: "ms".to_string(),
                description: "Unix millisecond timestamp when the debug snapshot crosses the stale threshold; refresh the debug artifact before this deadline."
                    .to_string(),
            },
            GrafanaPanel {
                id: 36,
                title: "Debug Snapshot Remaining Fresh".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.debug_snapshot_remaining_fresh_ms.clone(),
                unit: "ms".to_string(),
                description:
                    "Milliseconds remaining before RustRaftDebugSnapshotFreshnessLost can fire; RustRaftDebugSnapshotFreshnessLow warns below the low-fresh threshold and refresh_debug_snapshot expects this above rustraft_debug_snapshot_low_fresh_ms."
                        .to_string(),
            },
            GrafanaPanel {
                id: 37,
                title: "Debug Snapshot Fresh".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.debug_snapshot_fresh.clone(),
                unit: "bool".to_string(),
                description:
                    "Debug snapshot freshness; RustRaftDebugSnapshotFreshnessLost fires when this drops to 0."
                        .to_string(),
            },
            GrafanaPanel {
                id: 38,
                title: "Debug Bundle Validation Ready".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.debug_bundle_validation_ready.clone(),
                unit: "bool".to_string(),
                description:
                    "Support bundle validator readiness; 1 means bundle contract checks pass."
                        .to_string(),
            },
            GrafanaPanel {
                id: 39,
                title: "Debug Bundle Validation Issues".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics.debug_bundle_validation_issue_total.clone(),
                unit: "short".to_string(),
                description: "Total support bundle validation issues emitted by RustRaft tooling."
                    .to_string(),
            },
            GrafanaPanel {
                id: 40,
                title: "Debug Bundle Issue Breakdown".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!("sum by (issue) ({})", metrics.debug_bundle_validation_issue),
                unit: "short".to_string(),
                description: "Support bundle validation issues grouped by issue label."
                    .to_string(),
            },
            GrafanaPanel {
                id: 41,
                title: "Debug Bundle First Issue".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.debug_bundle_validation_first_issue.clone(),
                unit: "short".to_string(),
                description: "First support bundle validation issue selected for operator triage."
                    .to_string(),
            },
            GrafanaPanel {
                id: 42,
                title: "Support Envelope Validation Ready".to_string(),
                panel_type: "stat".to_string(),
                expr: format!(
                    "{}{{artifact=\"support_envelope\"}}",
                    metrics.debug_bundle_validation_ready
                ),
                unit: "bool".to_string(),
                description: "Support envelope self-validation readiness; 1 means advertised artifacts and scrape payloads match the emitted bundle."
                    .to_string(),
            },
            GrafanaPanel {
                id: 43,
                title: "Support Envelope Validation Issues".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "{}{{artifact=\"support_envelope\"}}",
                    metrics.debug_bundle_validation_issue_total
                ),
                unit: "short".to_string(),
                description: "Total support envelope validation issues emitted by RustRaft tooling."
                    .to_string(),
            },
            GrafanaPanel {
                id: 44,
                title: "Support Envelope Issue Breakdown".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (issue) ({}{{artifact=\"support_envelope\"}})",
                    metrics.debug_bundle_validation_issue
                ),
                unit: "short".to_string(),
                description: "Support envelope validation issues grouped by issue label."
                    .to_string(),
            },
            GrafanaPanel {
                id: 45,
                title: "Support Envelope First Issue".to_string(),
                panel_type: "stat".to_string(),
                expr: format!(
                    "{}{{artifact=\"support_envelope\"}}",
                    metrics.debug_bundle_validation_first_issue
                ),
                unit: "short".to_string(),
                description: "First support envelope validation issue selected for operator triage."
                    .to_string(),
            },
            GrafanaPanel {
                id: 46,
                title: "Provisioning Validation Ready".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics
                    .observability_provisioning_validation_ready
                    .clone(),
                unit: "bool".to_string(),
                description: "Observability provisioning validation readiness; 1 means dashboard and alert contracts match."
                    .to_string(),
            },
            GrafanaPanel {
                id: 47,
                title: "Provisioning First Issue".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics
                    .observability_provisioning_validation_first_issue
                    .clone(),
                unit: "short".to_string(),
                description: "First observability provisioning validation issue selected for operator triage."
                    .to_string(),
            },
            GrafanaPanel {
                id: 48,
                title: "Provisioning Validation Issues".to_string(),
                panel_type: "timeseries".to_string(),
                expr: metrics
                    .observability_provisioning_validation_issue_total
                    .clone(),
                unit: "short".to_string(),
                description: "Total observability provisioning validation issues emitted by RustRaft tooling."
                    .to_string(),
            },
            GrafanaPanel {
                id: 49,
                title: "Provisioning Issue Breakdown".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (issue) ({})",
                    metrics.observability_provisioning_validation_issue
                ),
                unit: "short".to_string(),
                description: "Observability provisioning validation issues grouped by issue label."
                    .to_string(),
            },
            GrafanaPanel {
                id: 50,
                title: "Debug Snapshot Max Age".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.debug_snapshot_max_age_ms.clone(),
                unit: "ms".to_string(),
                description:
                    "Configured freshness window for debug snapshots; stale and freshness alerts compare age against this threshold."
                        .to_string(),
            },
            GrafanaPanel {
                id: 51,
                title: "Debug Snapshot Low Fresh Threshold".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.debug_snapshot_low_fresh_ms.clone(),
                unit: "ms".to_string(),
                description:
                    "Configured runway threshold for RustRaftDebugSnapshotFreshnessLow before the freshness window expires."
                        .to_string(),
            },
            GrafanaPanel {
                id: 52,
                title: "Debug Snapshot Low Fresh".to_string(),
                panel_type: "stat".to_string(),
                expr: metrics.debug_snapshot_low_fresh.clone(),
                unit: "bool".to_string(),
                description:
                    "Debug snapshot early-warning freshness; 1 means remaining freshness is above rustraft_debug_snapshot_low_fresh_ms."
                        .to_string(),
            },
            GrafanaPanel {
                id: 53,
                title: "Support Envelope Freshness Status".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (freshness_status) ({}{{artifact=\"support_envelope\"}})",
                    metrics.debug_bundle_validation_ready
                ),
                unit: "bool".to_string(),
                description: "Support envelope readiness grouped by debug snapshot freshness_status label."
                    .to_string(),
            },
            GrafanaPanel {
                id: 54,
                title: "Support Envelope Status".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (support_envelope_status) ({}{{artifact=\"support_envelope\"}})",
                    metrics.debug_bundle_validation_ready
                ),
                unit: "bool".to_string(),
                description: "Support envelope readiness grouped by derived support_envelope_status label."
                    .to_string(),
            },
            GrafanaPanel {
                id: 55,
                title: "Support Envelope Severity".to_string(),
                panel_type: "timeseries".to_string(),
                expr: format!(
                    "sum by (support_envelope_severity) ({}{{artifact=\"support_envelope\"}})",
                    metrics.debug_bundle_validation_ready
                ),
                unit: "bool".to_string(),
                description: "Support envelope readiness grouped by derived support_envelope_severity label."
                    .to_string(),
            },
            ];
            panels.extend(matrixraft_scale_grafana_panels());
            panels.extend(matrixraft_scale_target_grafana_panels());
            panels.extend(matrixraft_memory_grafana_panels());
            panels.extend(matrixraft_runtime_pressure_grafana_panels());
            panels.extend(matrixraft_node_runtime_grafana_panels());
            panels.extend(matrixraft_snapshot_lifecycle_grafana_panels());
            panels.extend(matrixraft_wal_lifecycle_grafana_panels());
            panels.extend(matrixraft_membership_readiness_grafana_panels());
            panels.extend(matrixraft_production_readiness_grafana_panels());
            panels.extend(matrixraft_baseline_raft_benchmark_grafana_panels());
            panels
        },
    }
}

pub fn matrixraft_grafana_dashboard_json() -> String {
    serde_json::to_string_pretty(&matrixraft_grafana_dashboard())
        .expect("RustRaft Grafana dashboard must serialize")
}

fn percentile_expr(metric: &str, quantile: &str) -> String {
    format!("histogram_quantile({quantile}, sum by (le) (rate({metric}_bucket[5m])))")
}

fn qps_expr(metric: &str) -> String {
    format!("sum by (service, group, workload) (rate({metric}[1m]))")
}

fn throughput_mib_expr(metric: &str) -> String {
    format!("sum by (service, group, workload) (rate({metric}[1m])) / 1048576")
}

fn target_percent(observed: u64, target: u64) -> u64 {
    if target == 0 {
        0
    } else {
        observed.saturating_mul(100) / target
    }
}

fn bool_metric(value: bool) -> u64 {
    u64::from(value)
}

fn push_latency_histogram(
    out: &mut String,
    name: &str,
    labels: &[(&str, &str)],
    histogram: &LatencyHistogram,
) -> u64 {
    let mut metric_count = 0_u64;
    for bucket in &histogram.buckets {
        let mut bucket_labels = labels.to_vec();
        bucket_labels.push(("le", bucket.le_ms.as_str()));
        push_metric(out, &format!("{name}_bucket"), &bucket_labels, bucket.count);
        metric_count += 1;
    }
    push_metric(out, &format!("{name}_sum"), labels, histogram.sum_ms);
    metric_count += 1;
    push_metric(out, &format!("{name}_count"), labels, histogram.count);
    metric_count + 1
}

fn push_metric(out: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (idx, (label_name, label_value)) in labels.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            out.push_str(label_name);
            out.push_str("=\"");
            out.push_str(&escape_prometheus_label_value(label_value));
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn escape_prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}
