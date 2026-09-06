// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;

use matrixraft::{
    matrixraft_baseline_raft_benchmark_grafana_panels,
    matrixraft_baseline_raft_benchmark_metric_names,
    membership::{
        MembershipReadinessReport, MembershipScope, MembershipTransitionDecision,
        MembershipTransitionKind,
    },
    metrics::{
        matrixraft_alert_rules, matrixraft_latency_metrics_prometheus,
        matrixraft_membership_readiness_diagnostic_json_lines,
        matrixraft_membership_readiness_diagnostic_log_entries,
        matrixraft_membership_readiness_grafana_panels,
        matrixraft_membership_readiness_metric_names, matrixraft_membership_readiness_prometheus,
        matrixraft_memory_grafana_panels, matrixraft_memory_metric_names,
        matrixraft_memory_metrics_prometheus, matrixraft_memory_optimization_hints,
        matrixraft_observability_provisioning, matrixraft_production_readiness_grafana_panels,
        matrixraft_production_readiness_metric_names, matrixraft_queue_pressure_grafana_panels,
        matrixraft_queue_pressure_metric_names, matrixraft_queue_pressure_prometheus,
        matrixraft_runtime_pressure_admission, matrixraft_runtime_pressure_admission_prometheus,
        matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure,
        matrixraft_runtime_pressure_admission_with_pipeline_pressure,
        matrixraft_runtime_pressure_admission_with_read_backlog_pressure,
        matrixraft_runtime_pressure_admission_with_scale_and_pipeline_pressure,
        matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure,
        matrixraft_runtime_pressure_admission_with_scale_pipeline_read_backlog_and_node_runtime_timer_pressure,
        matrixraft_runtime_pressure_admission_with_scale_targets,
        matrixraft_runtime_pressure_bottleneck_summary,
        matrixraft_runtime_pressure_diagnostic_log_entries,
        matrixraft_runtime_pressure_freshness_prometheus,
        matrixraft_runtime_pressure_freshness_report, matrixraft_runtime_pressure_grafana_panels,
        matrixraft_runtime_pressure_metric_names, matrixraft_scale_grafana_panels,
        matrixraft_scale_metric_names, matrixraft_scale_metrics_prometheus,
        matrixraft_scale_optimization_hints, matrixraft_scale_target_grafana_panels,
        matrixraft_scale_target_metric_names, matrixraft_scale_target_metrics_prometheus,
        matrixraft_snapshot_lifecycle_evidence_prometheus,
        matrixraft_snapshot_lifecycle_grafana_panels, matrixraft_snapshot_lifecycle_metric_names,
        matrixraft_wal_lifecycle_evidence_prometheus, matrixraft_wal_lifecycle_grafana_panels,
        matrixraft_wal_lifecycle_metric_names, LatencyBucket, LatencyHistogram, LatencyMetrics,
        LatencyOptimizationThresholds, MemoryMetrics, MemoryOptimizationThresholds,
        NodeRuntimeTimerThresholds, ReadBacklogMetrics, ReadBacklogThresholds,
        RuntimePressureAdmissionPolicy, ScaleMetrics, ScaleOptimizationTargets, ScaleRateMetrics,
    },
    MailBoxPressureStats, MailChannelPressureStats, PeerProgress, PipelineLimits,
    RuntimeTimerStatus, SnapshotLifecycleEvidence, WalLifecycleEvidence,
};

#[test]
fn scale_observability_exports_canonical_qps_and_throughput_panels() {
    let metrics = matrixraft_scale_metric_names();
    assert_eq!(metrics.proposal_qps_total, "rustraft_proposal_total");
    assert_eq!(
        metrics.append_entries_qps_total,
        "rustraft_append_entries_total"
    );
    assert_eq!(metrics.read_index_qps_total, "rustraft_read_index_total");
    assert_eq!(
        metrics.apply_entries_qps_total,
        "rustraft_apply_entries_total"
    );
    assert_eq!(
        metrics.replication_bytes_total,
        "rustraft_replication_bytes_total"
    );
    assert_eq!(metrics.apply_bytes_total, "rustraft_apply_bytes_total");

    let panels = matrixraft_scale_grafana_panels();
    assert_eq!(panels.len(), 6);
    assert_eq!(
        panels
            .iter()
            .map(|panel| panel.id)
            .collect::<BTreeSet<_>>()
            .len(),
        panels.len(),
        "scale panel IDs must stay unique so they can be merged into dashboards"
    );

    for metric in [
        metrics.proposal_qps_total,
        metrics.append_entries_qps_total,
        metrics.read_index_qps_total,
        metrics.apply_entries_qps_total,
    ] {
        assert!(
            panels.iter().any(|panel| panel.expr
                == format!("sum by (service, group, workload) (rate({metric}[1m]))")
                && panel.unit == "ops/s"),
            "missing QPS panel for {metric}"
        );
    }

    for metric in [metrics.replication_bytes_total, metrics.apply_bytes_total] {
        assert!(
            panels.iter().any(|panel| panel.expr
                == format!("sum by (service, group, workload) (rate({metric}[1m])) / 1048576")
                && panel.unit == "MiB/s"),
            "missing MiB/s panel for {metric}"
        );
    }
}

#[test]
fn scale_target_observability_exports_canonical_target_metrics_and_panels() {
    let metrics = matrixraft_scale_target_metric_names();
    assert_eq!(
        metrics.min_proposal_qps,
        "rustraft_scale_target_min_proposal_qps"
    );
    assert_eq!(
        metrics.min_append_entries_qps,
        "rustraft_scale_target_min_append_entries_qps"
    );
    assert_eq!(
        metrics.min_read_index_qps,
        "rustraft_scale_target_min_read_index_qps"
    );
    assert_eq!(
        metrics.min_apply_entries_qps,
        "rustraft_scale_target_min_apply_entries_qps"
    );
    assert_eq!(
        metrics.min_replication_mib_per_sec,
        "rustraft_scale_target_min_replication_mib_per_sec"
    );
    assert_eq!(
        metrics.min_apply_mib_per_sec,
        "rustraft_scale_target_min_apply_mib_per_sec"
    );
    assert_eq!(
        metrics.proposal_target_percent,
        "rustraft_scale_observed_proposal_target_percent"
    );
    assert_eq!(
        metrics.append_entries_target_percent,
        "rustraft_scale_observed_append_entries_target_percent"
    );
    assert_eq!(
        metrics.read_index_target_percent,
        "rustraft_scale_observed_read_index_target_percent"
    );
    assert_eq!(
        metrics.apply_entries_target_percent,
        "rustraft_scale_observed_apply_entries_target_percent"
    );
    assert_eq!(
        metrics.replication_target_percent,
        "rustraft_scale_observed_replication_target_percent"
    );
    assert_eq!(
        metrics.apply_target_percent,
        "rustraft_scale_observed_apply_target_percent"
    );

    let panels = matrixraft_scale_target_grafana_panels();
    assert_eq!(panels.len(), 12);
    assert_eq!(
        panels
            .iter()
            .map(|panel| panel.id)
            .collect::<BTreeSet<_>>()
            .len(),
        panels.len(),
        "scale target panel IDs must stay unique so they can be merged into dashboards"
    );
    for (metric, unit) in [
        (metrics.min_proposal_qps, "ops/s"),
        (metrics.min_append_entries_qps, "ops/s"),
        (metrics.min_read_index_qps, "ops/s"),
        (metrics.min_apply_entries_qps, "ops/s"),
        (metrics.min_replication_mib_per_sec, "MiB/s"),
        (metrics.min_apply_mib_per_sec, "MiB/s"),
        (metrics.proposal_target_percent, "percent"),
        (metrics.append_entries_target_percent, "percent"),
        (metrics.read_index_target_percent, "percent"),
        (metrics.apply_entries_target_percent, "percent"),
        (metrics.replication_target_percent, "percent"),
        (metrics.apply_target_percent, "percent"),
    ] {
        assert!(
            panels
                .iter()
                .any(|panel| panel.expr == metric && panel.unit == unit),
            "missing scale target panel for {metric}"
        );
    }
}

#[test]
fn scale_target_grafana_panels_keep_unique_global_ids() {
    let mut ids = matrixraft_scale_grafana_panels()
        .into_iter()
        .map(|panel| panel.id)
        .collect::<BTreeSet<_>>();
    ids.extend(
        matrixraft_memory_grafana_panels()
            .into_iter()
            .map(|panel| panel.id),
    );
    ids.extend(
        matrixraft_runtime_pressure_grafana_panels()
            .into_iter()
            .map(|panel| panel.id),
    );
    ids.extend(
        matrixraft_snapshot_lifecycle_grafana_panels()
            .into_iter()
            .map(|panel| panel.id),
    );
    ids.extend(
        matrixraft_wal_lifecycle_grafana_panels()
            .into_iter()
            .map(|panel| panel.id),
    );
    ids.extend(
        matrixraft_membership_readiness_grafana_panels()
            .into_iter()
            .map(|panel| panel.id),
    );
    ids.extend(
        matrixraft_production_readiness_grafana_panels()
            .into_iter()
            .map(|panel| panel.id),
    );
    ids.extend(
        matrixraft_baseline_raft_benchmark_grafana_panels()
            .into_iter()
            .map(|panel| panel.id),
    );
    for panel in matrixraft_scale_target_grafana_panels() {
        assert!(
            ids.insert(panel.id),
            "scale, memory, runtime-pressure, snapshot-lifecycle, wal-lifecycle, membership-readiness, production-readiness, benchmark-parity, and scale-target panel IDs must not collide: {}",
            panel.id
        );
    }
}

#[test]
fn snapshot_lifecycle_observability_exports_metrics_panels_and_artifact() {
    let metrics = matrixraft_snapshot_lifecycle_metric_names();
    assert_eq!(
        metrics.sender_lifecycle_present,
        "rustraft_snapshot_lifecycle_sender_present"
    );
    assert_eq!(
        metrics.sustained_sender_completion_present,
        "rustraft_snapshot_lifecycle_sustained_sender_completion_present"
    );
    assert_eq!(
        metrics.sustained_transfer_completion_present,
        "rustraft_snapshot_lifecycle_sustained_transfer_completion_present"
    );
    assert_eq!(
        metrics.sustained_transfer_completed_peer_count,
        "rustraft_snapshot_lifecycle_sustained_transfer_completed_peer_count"
    );
    assert_eq!(
        metrics.rejoin_after_compacted_log_present,
        "rustraft_snapshot_lifecycle_rejoin_after_compacted_log_present"
    );

    let panels = matrixraft_snapshot_lifecycle_grafana_panels();
    assert_eq!(panels.len(), 17);
    assert_eq!(
        panels
            .iter()
            .map(|panel| panel.id)
            .collect::<BTreeSet<_>>()
            .len(),
        panels.len(),
        "snapshot lifecycle panel IDs must stay unique"
    );
    for metric in [
        metrics.sender_lifecycle_present.clone(),
        metrics.downloader_lifecycle_present.clone(),
        metrics.retry_backpressure_present.clone(),
        metrics.chunk_retry_present.clone(),
        metrics.send_timeout_present.clone(),
        metrics.rate_limit_present.clone(),
        metrics.sustained_sender_load_present.clone(),
        metrics.sustained_downloader_load_present.clone(),
        metrics.sustained_sender_completion_present.clone(),
        metrics.sustained_downloader_completion_present.clone(),
        metrics.sustained_transfer_completion_present.clone(),
        metrics.snapshot_peer_count.clone(),
        metrics.sustained_transfer_completed_peer_count.clone(),
        metrics.install_progress_present.clone(),
        metrics.install_rollback_present.clone(),
        metrics.membership_change_present.clone(),
        metrics.rejoin_after_compacted_log_present.clone(),
    ] {
        assert!(
            panels.iter().any(|panel| panel.expr.contains(&metric)),
            "missing snapshot lifecycle panel for {metric}"
        );
    }

    let evidence = SnapshotLifecycleEvidence {
        sender_lifecycle_present: true,
        downloader_lifecycle_present: true,
        retry_backpressure_present: true,
        chunk_retry_present: true,
        send_timeout_present: false,
        rate_limit_present: true,
        sustained_sender_load_present: true,
        sustained_downloader_load_present: true,
        sustained_sender_completion_present: true,
        sustained_downloader_completion_present: true,
        sustained_transfer_completion_present: true,
        snapshot_peer_count: 3,
        sustained_transfer_completed_peer_count: 2,
        install_progress_present: true,
        install_rollback_present: false,
        membership_change_present: true,
        rejoin_after_compacted_log_present: true,
    };
    let prometheus = matrixraft_snapshot_lifecycle_evidence_prometheus(
        &evidence,
        &[("service", "raft\"a"), ("group", "g1")],
    );
    assert_eq!(prometheus.format, "prometheus_text_v0.0.4");
    assert_eq!(prometheus.metric_count, 17);
    assert_eq!(prometheus.text.lines().count(), 17);
    assert!(prometheus.text.contains(
        "rustraft_snapshot_lifecycle_sender_present{service=\"raft\\\"a\",group=\"g1\",signal=\"sender_lifecycle\"} 1"
    ));
    assert!(prometheus.text.contains(
        "rustraft_snapshot_lifecycle_send_timeout_present{service=\"raft\\\"a\",group=\"g1\",signal=\"send_timeout\"} 0"
    ));
    assert!(prometheus.text.contains(
        "rustraft_snapshot_lifecycle_rejoin_after_compacted_log_present{service=\"raft\\\"a\",group=\"g1\",signal=\"rejoin_after_compacted_log\"} 1"
    ));
    assert!(prometheus
        .text
        .contains("rustraft_snapshot_lifecycle_peer_count{service=\"raft\\\"a\",group=\"g1\"} 3"));
    assert!(prometheus.text.contains(
        "rustraft_snapshot_lifecycle_sustained_transfer_completed_peer_count{service=\"raft\\\"a\",group=\"g1\"} 2"
    ));

    let provisioning = matrixraft_observability_provisioning();
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_snapshot_lifecycle_sender_present".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_snapshot_lifecycle_peer_count".to_string()));
    assert!(provisioning.required_metric_names.contains(
        &"rustraft_snapshot_lifecycle_sustained_transfer_completed_peer_count".to_string()
    ));
    assert!(provisioning
        .debug_artifact_names
        .contains(&"snapshot_lifecycle_prometheus".to_string()));
    assert!(provisioning
        .prometheus_artifact_names
        .contains(&"snapshot_lifecycle_prometheus".to_string()));
}

#[test]
fn wal_lifecycle_observability_exports_metrics_panels_and_artifact() {
    let metrics = matrixraft_wal_lifecycle_metric_names();
    assert_eq!(
        metrics.segment_lifecycle_present,
        "rustraft_wal_lifecycle_segment_present"
    );
    assert_eq!(
        metrics.slow_fsync_backpressure_observed,
        "rustraft_wal_lifecycle_slow_fsync_backpressure_observed"
    );
    assert_eq!(
        metrics.compaction_after_slow_fsync_observed,
        "rustraft_wal_lifecycle_compaction_after_slow_fsync_observed"
    );
    assert_eq!(
        metrics.released_segment_count,
        "rustraft_wal_lifecycle_released_segment_count"
    );
    assert_eq!(
        metrics.compacted_after_slow_fsync_count,
        "rustraft_wal_lifecycle_compacted_after_slow_fsync_count"
    );
    assert_eq!(
        metrics.slow_fsync_segment_count,
        "rustraft_wal_lifecycle_slow_fsync_segment_count"
    );
    assert_eq!(
        metrics.compacted_slow_fsync_segment_count,
        "rustraft_wal_lifecycle_compacted_slow_fsync_segment_count"
    );

    let panels = matrixraft_wal_lifecycle_grafana_panels();
    assert_eq!(panels.len(), 11);
    assert_eq!(
        panels
            .iter()
            .map(|panel| panel.id)
            .collect::<BTreeSet<_>>()
            .len(),
        panels.len(),
        "WAL lifecycle panel IDs must stay unique"
    );
    for metric in [
        metrics.segment_lifecycle_present.clone(),
        metrics.retained_range_present.clone(),
        metrics.sequence_range_present.clone(),
        metrics.log_index_range_present.clone(),
        metrics.compaction_observed.clone(),
        metrics.slow_fsync_backpressure_observed.clone(),
        metrics.compaction_after_slow_fsync_observed.clone(),
        metrics.released_segment_count.clone(),
        metrics.compacted_after_slow_fsync_count.clone(),
        metrics.slow_fsync_segment_count.clone(),
        metrics.compacted_slow_fsync_segment_count.clone(),
    ] {
        assert!(
            panels.iter().any(|panel| panel.expr.contains(&metric)),
            "missing WAL lifecycle panel for {metric}"
        );
    }

    let evidence = WalLifecycleEvidence {
        segment_lifecycle_present: true,
        retained_range_present: true,
        sequence_range_present: true,
        log_index_range_present: true,
        compaction_observed: true,
        slow_fsync_backpressure_observed: true,
        compaction_after_slow_fsync_observed: true,
        released_segment_count: 3,
        compacted_after_slow_fsync_count: 1,
        slow_fsync_segment_count: 2,
        compacted_slow_fsync_segment_count: 1,
    };
    let prometheus = matrixraft_wal_lifecycle_evidence_prometheus(
        &evidence,
        &[("service", "raft\"a"), ("group", "g1")],
    );
    assert_eq!(prometheus.format, "prometheus_text_v0.0.4");
    assert_eq!(prometheus.metric_count, 11);
    assert_eq!(prometheus.text.lines().count(), 11);
    assert!(prometheus.text.contains(
        "rustraft_wal_lifecycle_segment_present{service=\"raft\\\"a\",group=\"g1\",signal=\"segment_lifecycle\"} 1"
    ));
    assert!(prometheus.text.contains(
        "rustraft_wal_lifecycle_slow_fsync_backpressure_observed{service=\"raft\\\"a\",group=\"g1\",signal=\"slow_fsync_backpressure\"} 1"
    ));
    assert!(prometheus.text.contains(
        "rustraft_wal_lifecycle_compaction_after_slow_fsync_observed{service=\"raft\\\"a\",group=\"g1\",signal=\"compaction_after_slow_fsync\"} 1"
    ));
    assert!(prometheus.text.contains(
        "rustraft_wal_lifecycle_released_segment_count{service=\"raft\\\"a\",group=\"g1\",signal=\"released_segment_count\"} 3"
    ));
    assert!(prometheus.text.contains(
        "rustraft_wal_lifecycle_compacted_after_slow_fsync_count{service=\"raft\\\"a\",group=\"g1\",signal=\"compacted_after_slow_fsync_count\"} 1"
    ));
    assert!(prometheus.text.contains(
        "rustraft_wal_lifecycle_slow_fsync_segment_count{service=\"raft\\\"a\",group=\"g1\",signal=\"slow_fsync_segment_count\"} 2"
    ));
    assert!(prometheus.text.contains(
        "rustraft_wal_lifecycle_compacted_slow_fsync_segment_count{service=\"raft\\\"a\",group=\"g1\",signal=\"compacted_slow_fsync_segment_count\"} 1"
    ));

    let provisioning = matrixraft_observability_provisioning();
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_wal_lifecycle_slow_fsync_backpressure_observed".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_wal_lifecycle_compacted_slow_fsync_segment_count".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_wal_lifecycle_released_segment_count".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_wal_lifecycle_compacted_after_slow_fsync_count".to_string()));
    assert!(provisioning
        .debug_artifact_names
        .contains(&"wal_lifecycle_prometheus".to_string()));
    assert!(provisioning
        .prometheus_artifact_names
        .contains(&"wal_lifecycle_prometheus".to_string()));
}

#[test]
fn membership_readiness_observability_exports_metrics_panels_and_artifact() {
    let metrics = matrixraft_membership_readiness_metric_names();
    assert_eq!(metrics.ready, "rustraft_membership_readiness_ready");
    assert_eq!(
        metrics.transition_missing,
        "rustraft_membership_transition_missing"
    );

    let panels = matrixraft_membership_readiness_grafana_panels();
    assert_eq!(panels.len(), 6);
    assert_eq!(
        panels
            .iter()
            .map(|panel| panel.id)
            .collect::<BTreeSet<_>>()
            .len(),
        panels.len(),
        "membership readiness panel IDs must stay unique"
    );
    for metric in [
        metrics.ready.clone(),
        metrics.satisfied_total.clone(),
        metrics.missing_total.clone(),
        metrics.transition_ready.clone(),
        metrics.transition_missing_total.clone(),
        metrics.transition_missing.clone(),
    ] {
        assert!(
            panels.iter().any(|panel| panel.expr.contains(&metric)),
            "missing membership readiness panel for {metric}"
        );
    }

    let report = MembershipReadinessReport {
        ready: false,
        satisfied: vec!["metaserver_failover".to_string()],
        missing: vec!["data_node_scale_up:joint_quorum_commit_proven".to_string()],
        decisions: vec![
            MembershipTransitionDecision {
                scope: MembershipScope::Metaserver,
                transition: MembershipTransitionKind::Failover,
                ready: true,
                missing: Vec::new(),
            },
            MembershipTransitionDecision {
                scope: MembershipScope::DataNode,
                transition: MembershipTransitionKind::ScaleUp,
                ready: false,
                missing: vec!["joint_quorum_commit_proven".to_string()],
            },
        ],
    };
    let prometheus = matrixraft_membership_readiness_prometheus(&report, &[("service", "raft\"a")]);
    assert_eq!(prometheus.format, "prometheus_text_v0.0.4");
    assert_eq!(prometheus.metric_count, 8);
    assert_eq!(prometheus.text.lines().count(), 8);
    assert!(prometheus
        .text
        .contains("rustraft_membership_readiness_ready{service=\"raft\\\"a\"} 0"));
    assert!(prometheus.text.contains(
        "rustraft_membership_transition_ready{service=\"raft\\\"a\",scope=\"metaserver\",transition=\"failover\"} 1"
    ));
    assert!(prometheus.text.contains(
        "rustraft_membership_transition_missing{service=\"raft\\\"a\",scope=\"data_node\",transition=\"scale_up\",missing=\"joint_quorum_commit_proven\"} 1"
    ));

    let provisioning = matrixraft_observability_provisioning();
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_membership_transition_missing".to_string()));
    assert!(provisioning
        .debug_artifact_names
        .contains(&"membership_readiness_prometheus".to_string()));
    assert!(provisioning
        .prometheus_artifact_names
        .contains(&"membership_readiness_prometheus".to_string()));

    let diagnostics = matrixraft_membership_readiness_diagnostic_log_entries(&report);
    assert_eq!(diagnostics.len(), 3);
    assert_eq!(diagnostics[0].target, "rustraft.membership_readiness");
    assert_eq!(
        diagnostics[0].severity,
        matrixraft::DiagnosticSeverity::Warn
    );
    assert_eq!(diagnostics[0].message, "membership readiness blocked");
    let blocked_transition = diagnostics
        .iter()
        .find(|entry| entry.target == "rustraft.membership_readiness.data_node.scale_up")
        .expect("blocked scale-up diagnostic");
    assert_eq!(
        blocked_transition.severity,
        matrixraft::DiagnosticSeverity::Warn
    );
    assert!(blocked_transition.fields.contains(&(
        "missing".to_string(),
        "joint_quorum_commit_proven".to_string()
    )));

    let json_lines = matrixraft_membership_readiness_diagnostic_json_lines(&report);
    assert_eq!(json_lines.lines().count(), 3);
    assert!(json_lines.contains("\"target\":\"rustraft.membership_readiness\""));
    assert!(json_lines.contains("\"target\":\"rustraft.membership_readiness.data_node.scale_up\""));
    assert!(json_lines.contains("joint_quorum_commit_proven"));
}

#[test]
fn benchmark_parity_observability_exports_canonical_metrics_panels_and_alerts() {
    let metrics = matrixraft_baseline_raft_benchmark_metric_names();
    assert_eq!(metrics.passed, "rustraft_baseline_raft_benchmark_passed");
    assert_eq!(
        metrics.production_evidence_ready,
        "rustraft_baseline_raft_benchmark_production_evidence_ready"
    );
    assert_eq!(
        metrics.generated_at_unix_ms,
        "rustraft_baseline_raft_benchmark_generated_at_unix_ms"
    );
    assert_eq!(metrics.age_ms, "rustraft_baseline_raft_benchmark_age_ms");
    assert_eq!(
        metrics.max_age_ms,
        "rustraft_baseline_raft_benchmark_max_age_ms"
    );
    assert_eq!(
        metrics.stale_after_unix_ms,
        "rustraft_baseline_raft_benchmark_stale_after_unix_ms"
    );
    assert_eq!(
        metrics.remaining_fresh_ms,
        "rustraft_baseline_raft_benchmark_remaining_fresh_ms"
    );
    assert_eq!(metrics.fresh, "rustraft_baseline_raft_benchmark_fresh");
    assert_eq!(
        metrics.freshness_status,
        "rustraft_baseline_raft_benchmark_freshness_status"
    );
    assert_eq!(
        metrics.failed_workload_total,
        "rustraft_baseline_raft_benchmark_failed_workload_total"
    );
    assert_eq!(
        metrics.blocker_total,
        "rustraft_baseline_raft_benchmark_blocker_total"
    );
    assert_eq!(
        metrics.worst_p50_ratio,
        "rustraft_baseline_raft_benchmark_worst_p50_ratio"
    );
    assert_eq!(
        metrics.worst_p99_ratio,
        "rustraft_baseline_raft_benchmark_worst_p99_ratio"
    );
    assert_eq!(
        metrics.worst_throughput_ratio,
        "rustraft_baseline_raft_benchmark_worst_throughput_ratio"
    );
    assert_eq!(
        metrics.workload_passed,
        "rustraft_baseline_raft_benchmark_workload_passed"
    );
    assert_eq!(
        metrics.workload_p50_ratio,
        "rustraft_baseline_raft_benchmark_workload_p50_ratio"
    );
    assert_eq!(
        metrics.workload_p99_ratio,
        "rustraft_baseline_raft_benchmark_workload_p99_ratio"
    );
    assert_eq!(
        metrics.workload_throughput_ratio,
        "rustraft_baseline_raft_benchmark_workload_throughput_ratio"
    );
    assert_eq!(
        metrics.worst_cpu_ratio,
        "rustraft_baseline_raft_benchmark_worst_cpu_ratio"
    );
    assert_eq!(
        metrics.worst_peak_resident_memory_ratio,
        "rustraft_baseline_raft_benchmark_worst_peak_resident_memory_ratio"
    );
    assert_eq!(
        metrics.workload_cpu_ratio,
        "rustraft_baseline_raft_benchmark_workload_cpu_ratio"
    );
    assert_eq!(
        metrics.workload_peak_resident_memory_ratio,
        "rustraft_baseline_raft_benchmark_workload_peak_resident_memory_ratio"
    );

    let panels = matrixraft_baseline_raft_benchmark_grafana_panels();
    assert_eq!(panels.len(), 22);
    assert_eq!(
        panels
            .iter()
            .map(|panel| panel.id)
            .collect::<BTreeSet<_>>()
            .len(),
        panels.len(),
        "benchmark parity panel IDs must stay unique"
    );
    for metric in [
        metrics.passed.clone(),
        metrics.production_evidence_ready.clone(),
        metrics.generated_at_unix_ms.clone(),
        metrics.age_ms.clone(),
        metrics.max_age_ms.clone(),
        metrics.stale_after_unix_ms.clone(),
        metrics.remaining_fresh_ms.clone(),
        metrics.fresh.clone(),
        metrics.freshness_status.clone(),
        metrics.failed_workload_total.clone(),
        metrics.blocker_total.clone(),
        metrics.worst_p50_ratio.clone(),
        metrics.worst_p99_ratio.clone(),
        metrics.worst_throughput_ratio.clone(),
        metrics.worst_cpu_ratio.clone(),
        metrics.worst_peak_resident_memory_ratio.clone(),
        metrics.workload_passed.clone(),
        metrics.workload_p50_ratio.clone(),
        metrics.workload_p99_ratio.clone(),
        metrics.workload_throughput_ratio.clone(),
        metrics.workload_cpu_ratio.clone(),
        metrics.workload_peak_resident_memory_ratio.clone(),
    ] {
        assert!(
            panels.iter().any(|panel| panel.expr.contains(&metric)),
            "missing benchmark parity panel for {metric}"
        );
    }

    let provisioning = matrixraft_observability_provisioning();
    for metric in [
        metrics.passed.clone(),
        metrics.production_evidence_ready.clone(),
        metrics.generated_at_unix_ms.clone(),
        metrics.age_ms.clone(),
        metrics.max_age_ms.clone(),
        metrics.stale_after_unix_ms.clone(),
        metrics.remaining_fresh_ms.clone(),
        metrics.fresh.clone(),
        metrics.freshness_status.clone(),
        metrics.failed_workload_total.clone(),
        metrics.blocker_total.clone(),
        metrics.worst_p50_ratio.clone(),
        metrics.worst_p99_ratio.clone(),
        metrics.worst_throughput_ratio.clone(),
        metrics.worst_cpu_ratio.clone(),
        metrics.worst_peak_resident_memory_ratio.clone(),
        metrics.workload_passed.clone(),
        metrics.workload_p50_ratio.clone(),
        metrics.workload_p99_ratio.clone(),
        metrics.workload_throughput_ratio.clone(),
        metrics.workload_cpu_ratio.clone(),
        metrics.workload_peak_resident_memory_ratio.clone(),
    ] {
        assert!(
            provisioning.required_metric_names.contains(&metric),
            "provisioning missing benchmark parity metric {metric}"
        );
    }
    assert!(provisioning
        .dashboard
        .panels
        .iter()
        .any(|panel| panel.title == "Benchmark Worst P99 Ratio"));
    assert!(provisioning
        .dashboard
        .panels
        .iter()
        .any(|panel| panel.title == "Benchmark Freshness Remaining"));

    let alerts = matrixraft_alert_rules();
    assert!(alerts.iter().any(|alert| {
        alert.alert == "RustRaftBaselineRaftBenchmarkFailed"
            && alert
                .expr
                .contains("rustraft_baseline_raft_benchmark_passed")
            && alert
                .expr
                .contains("rustraft_baseline_raft_benchmark_failed_workload_total")
            && alert.severity == "critical"
    }));
    assert!(alerts.iter().any(|alert| {
        alert.alert == "RustRaftBaselineRaftBenchmarkRatioRegression"
            && alert
                .expr
                .contains("rustraft_baseline_raft_benchmark_worst_p99_ratio")
            && alert
                .expr
                .contains("rustraft_baseline_raft_benchmark_worst_throughput_ratio")
            && alert.severity == "warning"
    }));
    assert!(alerts.iter().any(|alert| {
        alert.alert == "RustRaftBaselineRaftBenchmarkResourceRegression"
            && alert
                .expr
                .contains("rustraft_baseline_raft_benchmark_worst_cpu_ratio")
            && alert
                .expr
                .contains("rustraft_baseline_raft_benchmark_worst_peak_resident_memory_ratio")
            && alert.severity == "warning"
    }));
    assert!(alerts.iter().any(|alert| {
        alert.alert == "RustRaftBaselineRaftBenchmarkFreshnessLost"
            && alert
                .expr
                .contains("rustraft_baseline_raft_benchmark_fresh == 0")
            && alert.severity == "warning"
    }));
}

#[test]
fn production_readiness_observability_exports_canonical_metrics_panels_and_alerts() {
    let metrics = matrixraft_production_readiness_metric_names();
    assert_eq!(metrics.ready, "rustraft_production_readiness_ready");
    assert_eq!(
        metrics.satisfied_total,
        "rustraft_production_readiness_satisfied_total"
    );
    assert_eq!(
        metrics.missing_total,
        "rustraft_production_readiness_missing_total"
    );
    assert_eq!(
        metrics.blocker_total,
        "rustraft_production_readiness_blocker_total"
    );
    assert_eq!(
        metrics.next_action_total,
        "rustraft_production_readiness_next_action_total"
    );
    assert_eq!(
        metrics.missing_present,
        "rustraft_production_readiness_missing_present"
    );
    assert_eq!(
        metrics.blocker_present,
        "rustraft_production_readiness_blocker_present"
    );
    assert_eq!(
        metrics.runtime_pressure_bottleneck_score_percent,
        "rustraft_production_readiness_runtime_pressure_bottleneck_score_percent"
    );
    assert_eq!(
        metrics.next_action_present,
        "rustraft_production_readiness_next_action_present"
    );

    let panels = matrixraft_production_readiness_grafana_panels();
    assert_eq!(panels.len(), 9);
    assert_eq!(
        panels
            .iter()
            .map(|panel| panel.id)
            .collect::<BTreeSet<_>>()
            .len(),
        panels.len(),
        "production readiness panel IDs must stay unique"
    );
    for metric in [
        metrics.ready.clone(),
        metrics.satisfied_total.clone(),
        metrics.missing_total.clone(),
        metrics.blocker_total.clone(),
        metrics.next_action_total.clone(),
        metrics.missing_present.clone(),
        metrics.blocker_present.clone(),
        metrics.runtime_pressure_bottleneck_score_percent.clone(),
        metrics.next_action_present.clone(),
    ] {
        assert!(
            panels.iter().any(|panel| panel.expr.contains(&metric)),
            "missing production readiness panel for {metric}"
        );
    }

    let provisioning = matrixraft_observability_provisioning();
    for metric in [
        metrics.ready,
        metrics.satisfied_total,
        metrics.missing_total,
        metrics.blocker_total,
        metrics.next_action_total,
        metrics.missing_present,
        metrics.blocker_present,
        metrics.runtime_pressure_bottleneck_score_percent,
        metrics.next_action_present,
    ] {
        assert!(
            provisioning.required_metric_names.contains(&metric),
            "provisioning missing production readiness metric {metric}"
        );
    }
    assert!(provisioning
        .dashboard
        .panels
        .iter()
        .any(|panel| panel.title == "Production Readiness Blockers"));
    assert!(provisioning
        .dashboard
        .panels
        .iter()
        .any(|panel| panel.title == "Production Runtime Pressure Bottlenecks"));
    assert!(matrixraft_alert_rules().iter().any(|alert| {
        alert.alert == "RustRaftProductionReadinessBlocked"
            && alert
                .expr
                .contains("rustraft_production_readiness_blocker_total")
            && alert.severity == "critical"
    }));
    assert!(matrixraft_alert_rules().iter().any(|alert| {
        alert.alert == "RustRaftProductionReadinessRuntimePressureBottleneck"
            && alert
                .expr
                .contains("rustraft_production_readiness_runtime_pressure_bottleneck_score_percent")
            && alert.severity == "critical"
    }));
    assert!(matrixraft_alert_rules().iter().any(|alert| {
        alert.alert == "RustRaftProductionReadinessMissingEvidence"
            && alert
                .expr
                .contains("rustraft_production_readiness_missing_total")
            && alert.severity == "warning"
    }));
}

#[test]
fn runtime_pressure_observability_exports_canonical_metrics_panels_and_alert() {
    let metrics = matrixraft_runtime_pressure_metric_names();
    assert_eq!(
        metrics.admission_accepted,
        "rustraft_runtime_pressure_admission_accepted"
    );
    assert_eq!(
        metrics.admission_rejected,
        "rustraft_runtime_pressure_admission_rejected"
    );
    assert_eq!(
        metrics.bottleneck_score_percent,
        "rustraft_runtime_pressure_bottleneck_score_percent"
    );
    assert_eq!(metrics.memory_pressure, "rustraft_runtime_pressure_memory");
    assert_eq!(
        metrics.memory_pressure_observed_value,
        "rustraft_runtime_pressure_memory_observed_value"
    );
    assert_eq!(
        metrics.memory_pressure_threshold_value,
        "rustraft_runtime_pressure_memory_threshold_value"
    );
    assert_eq!(
        metrics.memory_pressure_excess,
        "rustraft_runtime_pressure_memory_excess"
    );
    assert_eq!(
        metrics.latency_pressure,
        "rustraft_runtime_pressure_latency"
    );
    assert_eq!(
        metrics.latency_pressure_sample_count,
        "rustraft_runtime_pressure_latency_sample_count"
    );
    assert_eq!(
        metrics.latency_pressure_observed_p95_ms,
        "rustraft_runtime_pressure_latency_observed_p95_ms"
    );
    assert_eq!(
        metrics.latency_pressure_observed_p99_ms,
        "rustraft_runtime_pressure_latency_observed_p99_ms"
    );
    assert_eq!(
        metrics.latency_pressure_threshold_p99_ms,
        "rustraft_runtime_pressure_latency_threshold_p99_ms"
    );
    assert_eq!(
        metrics.latency_pressure_excess_ms,
        "rustraft_runtime_pressure_latency_excess_ms"
    );
    assert_eq!(metrics.scale_pressure, "rustraft_runtime_pressure_scale");
    assert_eq!(
        metrics.scale_pressure_observed_value,
        "rustraft_runtime_pressure_scale_observed_value"
    );
    assert_eq!(
        metrics.scale_pressure_target_value,
        "rustraft_runtime_pressure_scale_target_value"
    );
    assert_eq!(
        metrics.scale_pressure_deficit,
        "rustraft_runtime_pressure_scale_deficit"
    );
    assert_eq!(
        metrics.scale_pressure_target_percent,
        "rustraft_runtime_pressure_scale_target_percent"
    );
    assert_eq!(
        metrics.pipeline_pressure,
        "rustraft_runtime_pressure_pipeline"
    );
    assert_eq!(
        metrics.pipeline_pressure_observed_value,
        "rustraft_runtime_pressure_pipeline_observed_value"
    );
    assert_eq!(
        metrics.pipeline_pressure_threshold_value,
        "rustraft_runtime_pressure_pipeline_threshold_value"
    );
    assert_eq!(
        metrics.pipeline_pressure_excess,
        "rustraft_runtime_pressure_pipeline_excess"
    );
    assert_eq!(
        metrics.read_backlog_pressure,
        "rustraft_runtime_pressure_read_backlog"
    );
    assert_eq!(
        metrics.read_backlog_pressure_observed_value,
        "rustraft_runtime_pressure_read_backlog_observed_value"
    );
    assert_eq!(
        metrics.read_backlog_pressure_threshold_value,
        "rustraft_runtime_pressure_read_backlog_threshold_value"
    );
    assert_eq!(
        metrics.read_backlog_pressure_excess,
        "rustraft_runtime_pressure_read_backlog_excess"
    );
    assert_eq!(
        metrics.action_total,
        "rustraft_runtime_pressure_action_total"
    );
    assert_eq!(
        metrics.action_source_total,
        "rustraft_runtime_pressure_action_source_total"
    );
    assert_eq!(
        metrics.freshness_generated_at_unix_ms,
        "rustraft_runtime_pressure_freshness_generated_at_unix_ms"
    );
    assert_eq!(
        metrics.freshness_age_ms,
        "rustraft_runtime_pressure_freshness_age_ms"
    );
    assert_eq!(
        metrics.freshness_max_age_ms,
        "rustraft_runtime_pressure_freshness_max_age_ms"
    );
    assert_eq!(
        metrics.freshness_stale_after_unix_ms,
        "rustraft_runtime_pressure_freshness_stale_after_unix_ms"
    );
    assert_eq!(
        metrics.freshness_remaining_fresh_ms,
        "rustraft_runtime_pressure_freshness_remaining_fresh_ms"
    );
    assert_eq!(
        metrics.freshness_low_fresh_ms,
        "rustraft_runtime_pressure_freshness_low_fresh_ms"
    );
    assert_eq!(
        metrics.freshness_low_fresh,
        "rustraft_runtime_pressure_freshness_low_fresh"
    );
    assert_eq!(
        metrics.freshness_fresh,
        "rustraft_runtime_pressure_freshness_fresh"
    );
    assert_eq!(
        metrics.freshness_status,
        "rustraft_runtime_pressure_freshness_status"
    );
    assert_eq!(
        metrics.freshness_issue_total,
        "rustraft_runtime_pressure_freshness_issue_total"
    );
    assert_eq!(
        metrics.freshness_issue,
        "rustraft_runtime_pressure_freshness_issue"
    );

    let panels = matrixraft_runtime_pressure_grafana_panels();
    assert_eq!(panels.len(), 43);
    assert_eq!(
        panels
            .iter()
            .map(|panel| panel.id)
            .collect::<BTreeSet<_>>()
            .len(),
        panels.len(),
        "runtime pressure panel IDs must stay unique"
    );
    for metric in [
        metrics.admission_accepted,
        metrics.admission_rejected,
        metrics.memory_pressure,
        metrics.memory_pressure_observed_value,
        metrics.memory_pressure_threshold_value,
        metrics.memory_pressure_excess,
        metrics.latency_pressure,
        metrics.latency_pressure_sample_count,
        metrics.latency_pressure_observed_p95_ms,
        metrics.latency_pressure_observed_p99_ms,
        metrics.latency_pressure_threshold_p99_ms,
        metrics.latency_pressure_excess_ms,
        metrics.scale_pressure,
        metrics.scale_pressure_observed_value,
        metrics.scale_pressure_target_value,
        metrics.scale_pressure_deficit,
        metrics.scale_pressure_target_percent,
        metrics.pipeline_pressure,
        metrics.pipeline_pressure_observed_value,
        metrics.pipeline_pressure_threshold_value,
        metrics.pipeline_pressure_excess,
        metrics.read_backlog_pressure,
        metrics.read_backlog_pressure_observed_value,
        metrics.read_backlog_pressure_threshold_value,
        metrics.read_backlog_pressure_excess,
        metrics.node_runtime_timer_pressure,
        metrics.node_runtime_timer_pressure_observed_percent,
        metrics.node_runtime_timer_pressure_threshold_percent,
        metrics.node_runtime_timer_pressure_excess_percent,
        metrics.action_total,
        metrics.action_source_total,
        metrics.bottleneck_score_percent,
        metrics.freshness_generated_at_unix_ms,
        metrics.freshness_age_ms,
        metrics.freshness_max_age_ms,
        metrics.freshness_stale_after_unix_ms,
        metrics.freshness_remaining_fresh_ms,
        metrics.freshness_low_fresh_ms,
        metrics.freshness_low_fresh,
        metrics.freshness_fresh,
        metrics.freshness_status,
        metrics.freshness_issue_total,
        metrics.freshness_issue,
    ] {
        assert!(
            panels.iter().any(|panel| panel.expr.contains(&metric)),
            "missing runtime pressure panel for {metric}"
        );
    }

    let provisioning = matrixraft_observability_provisioning();
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_admission_rejected".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_scale".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_scale_deficit".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_pipeline".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_action_source_total".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_bottleneck_score_percent".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_freshness_age_ms".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_freshness_status".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_freshness_issue".to_string()));
    assert!(provisioning
        .prometheus_artifact_names
        .contains(&"runtime_pressure_freshness_prometheus".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_pipeline_excess".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_read_backlog".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_read_backlog_excess".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_runtime_pressure_node_runtime_timer".to_string()));
    assert!(provisioning
        .dashboard
        .panels
        .iter()
        .any(|panel| panel.title == "Runtime Admission Rejected"));
    assert!(provisioning
        .dashboard
        .panels
        .iter()
        .any(|panel| panel.title == "Runtime Read Backlog Pressure"));
    assert!(provisioning
        .dashboard
        .panels
        .iter()
        .any(|panel| panel.title == "Runtime Node Timer Pressure"));
    assert!(provisioning
        .dashboard
        .panels
        .iter()
        .any(|panel| panel.title == "Runtime Pressure Bottlenecks"));

    assert!(matrixraft_alert_rules().iter().any(|alert| {
        alert.alert == "RustRaftRuntimeAdmissionRejected"
            && alert
                .expr
                .contains("rustraft_runtime_pressure_admission_rejected")
            && alert.severity == "critical"
    }));
    assert!(matrixraft_alert_rules().iter().any(|alert| {
        alert.alert == "RustRaftRuntimePressureBottleneckActive"
            && alert
                .expr
                .contains("rustraft_runtime_pressure_bottleneck_score_percent")
            && alert.severity == "warning"
            && alert.summary.contains("Runtime Pressure Bottlenecks")
            && alert.summary.contains("QPS, latency, or memory parity")
    }));
    assert!(matrixraft_alert_rules().iter().any(|alert| {
        alert.alert == "RustRaftRuntimeScalePressure"
            && alert.expr.contains("rustraft_runtime_pressure_scale")
            && alert.severity == "warning"
    }));
    assert!(matrixraft_alert_rules().iter().any(|alert| {
        alert.alert == "RustRaftRuntimePipelinePressure"
            && alert.expr.contains("rustraft_runtime_pressure_pipeline")
            && alert.severity == "warning"
    }));
    assert!(matrixraft_alert_rules().iter().any(|alert| {
        alert.alert == "RustRaftRuntimeReadBacklogPressure"
            && alert
                .expr
                .contains("rustraft_runtime_pressure_read_backlog")
            && alert.severity == "warning"
    }));
}

#[test]
fn scale_target_observability_exports_attainment_panels() {
    let metrics = matrixraft_scale_target_metric_names();
    let panels = matrixraft_scale_target_grafana_panels();
    for metric in [
        metrics.proposal_target_percent,
        metrics.append_entries_target_percent,
        metrics.read_index_target_percent,
        metrics.apply_entries_target_percent,
        metrics.replication_target_percent,
        metrics.apply_target_percent,
    ] {
        assert!(
            panels
                .iter()
                .any(|panel| panel.expr == metric && panel.unit == "percent"),
            "missing target-attainment panel for {metric}"
        );
    }
}

#[test]
fn memory_observability_exports_canonical_memory_panels() {
    let metrics = matrixraft_memory_metric_names();
    assert_eq!(
        metrics.process_resident_memory_bytes,
        "rustraft_process_resident_memory_bytes"
    );
    assert_eq!(
        metrics.heap_allocated_bytes,
        "rustraft_heap_allocated_bytes"
    );
    assert_eq!(metrics.log_cache_bytes, "rustraft_log_cache_bytes");
    assert_eq!(
        metrics.snapshot_buffer_bytes,
        "rustraft_snapshot_buffer_bytes"
    );
    assert_eq!(
        metrics.replication_buffer_bytes,
        "rustraft_replication_buffer_bytes"
    );

    let panels = matrixraft_memory_grafana_panels();
    assert_eq!(panels.len(), 5);
    assert_eq!(
        panels
            .iter()
            .map(|panel| panel.id)
            .collect::<BTreeSet<_>>()
            .len(),
        panels.len(),
        "memory panel IDs must stay unique so they can be merged into dashboards"
    );

    for metric in [
        metrics.process_resident_memory_bytes,
        metrics.heap_allocated_bytes,
        metrics.log_cache_bytes,
        metrics.snapshot_buffer_bytes,
        metrics.replication_buffer_bytes,
    ] {
        assert!(
            panels
                .iter()
                .any(|panel| panel.expr == metric && panel.unit == "bytes"),
            "missing byte panel for {metric}"
        );
    }
}

#[test]
fn scale_observability_exports_prometheus_counter_payload() {
    let metrics = matrixraft_scale_metrics_prometheus(
        &ScaleMetrics {
            proposal_total: 101,
            append_entries_total: 202,
            read_index_total: 303,
            apply_entries_total: 404,
            replication_bytes_total: 505,
            apply_bytes_total: 606,
        },
        &[
            ("service", "raft\"a"),
            ("group", "g1"),
            ("workload", "scale"),
        ],
    );

    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(metrics.metric_count, 6);
    assert!(metrics.text.contains(
        "rustraft_proposal_total{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 101"
    ));
    assert!(metrics.text.contains(
        "rustraft_append_entries_total{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 202"
    ));
    assert!(metrics.text.contains(
        "rustraft_read_index_total{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 303"
    ));
    assert!(metrics.text.contains(
        "rustraft_apply_entries_total{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 404"
    ));
    assert!(metrics.text.contains(
        "rustraft_replication_bytes_total{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 505"
    ));
    assert!(metrics.text.contains(
        "rustraft_apply_bytes_total{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 606"
    ));
}

#[test]
fn scale_target_observability_exports_prometheus_gauge_payload() {
    let metrics = matrixraft_scale_target_metrics_prometheus(
        &ScaleRateMetrics {
            proposal_qps: 90,
            append_entries_qps: 200,
            read_index_qps: 60,
            apply_entries_qps: 550,
            replication_mib_per_sec: 75,
            apply_mib_per_sec: 0,
        },
        &ScaleOptimizationTargets {
            min_proposal_qps: 100,
            min_append_entries_qps: 200,
            min_read_index_qps: 50,
            min_apply_entries_qps: 500,
            min_replication_mib_per_sec: 150,
            min_apply_mib_per_sec: 0,
        },
        &[
            ("service", "raft\"a"),
            ("group", "g1"),
            ("workload", "scale"),
        ],
    );

    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(metrics.metric_count, 12);
    assert!(metrics.text.contains(
        "rustraft_scale_target_min_proposal_qps{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 100"
    ));
    assert!(metrics.text.contains(
        "rustraft_scale_target_min_replication_mib_per_sec{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 150"
    ));
    assert!(metrics.text.contains(
        "rustraft_scale_observed_proposal_target_percent{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 90"
    ));
    assert!(metrics.text.contains(
        "rustraft_scale_observed_read_index_target_percent{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 120"
    ));
    assert!(metrics.text.contains(
        "rustraft_scale_observed_apply_entries_target_percent{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 110"
    ));
    assert!(metrics.text.contains(
        "rustraft_scale_observed_replication_target_percent{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 50"
    ));
    assert!(metrics.text.contains(
        "rustraft_scale_observed_apply_target_percent{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 0"
    ));
}

#[test]
fn scale_observability_keeps_default_targets_quiet() {
    let hints = matrixraft_scale_optimization_hints(
        &ScaleRateMetrics::zero(),
        &ScaleOptimizationTargets::default(),
    );

    assert!(
        hints.is_empty(),
        "release-scale target hints must be opt-in because QPS targets are workload-specific"
    );
}

#[test]
fn scale_observability_exports_target_optimization_hints() {
    let hints = matrixraft_scale_optimization_hints(
        &ScaleRateMetrics {
            proposal_qps: 90,
            append_entries_qps: 210,
            read_index_qps: 40,
            apply_entries_qps: 450,
            replication_mib_per_sec: 120,
            apply_mib_per_sec: 180,
        },
        &ScaleOptimizationTargets {
            min_proposal_qps: 100,
            min_append_entries_qps: 200,
            min_read_index_qps: 50,
            min_apply_entries_qps: 500,
            min_replication_mib_per_sec: 150,
            min_apply_mib_per_sec: 150,
        },
    );

    assert_eq!(hints.len(), 4);
    assert!(hints.iter().any(|hint| {
        hint.id == "proposal_qps_below_target"
            && hint.component == "proposal_pipeline"
            && hint.observed_value == 90
            && hint.threshold == 100
    }));
    assert!(hints.iter().any(|hint| {
        hint.id == "read_index_qps_below_target"
            && hint.component == "read_path"
            && hint.observed_value == 40
            && hint.threshold == 50
    }));
    assert!(hints.iter().any(|hint| {
        hint.id == "apply_entries_qps_below_target"
            && hint.component == "apply_pipeline"
            && hint.observed_value == 450
            && hint.threshold == 500
    }));
    assert!(hints.iter().any(|hint| {
        hint.id == "replication_throughput_below_target"
            && hint.component == "replication_pipeline"
            && hint.observed_value == 120
            && hint.threshold == 150
    }));
}

#[test]
fn memory_observability_exports_prometheus_gauge_payload() {
    let metrics = matrixraft_memory_metrics_prometheus(
        &MemoryMetrics {
            process_resident_memory_bytes: 1024,
            heap_allocated_bytes: 2048,
            log_cache_bytes: 4096,
            snapshot_buffer_bytes: 8192,
            replication_buffer_bytes: 16_384,
        },
        &[
            ("service", "raft\"a"),
            ("group", "g1"),
            ("workload", "scale"),
        ],
    );

    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(metrics.metric_count, 5);
    assert!(metrics.text.contains(
        "rustraft_process_resident_memory_bytes{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 1024"
    ));
    assert!(metrics.text.contains(
        "rustraft_heap_allocated_bytes{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 2048"
    ));
    assert!(metrics.text.contains(
        "rustraft_log_cache_bytes{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 4096"
    ));
    assert!(metrics.text.contains(
        "rustraft_snapshot_buffer_bytes{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 8192"
    ));
    assert!(metrics.text.contains(
        "rustraft_replication_buffer_bytes{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\"} 16384"
    ));
}

#[test]
fn queue_pressure_observability_exports_prometheus_and_grafana_payload() {
    let names = matrixraft_queue_pressure_metric_names();
    assert_eq!(names.mailbox_total_len, "rustraft_mailbox_total_len");
    assert_eq!(
        names.mailbox_rejected_send_total,
        "rustraft_mailbox_rejected_send_total"
    );
    assert_eq!(
        names.mail_channel_queued_len,
        "rustraft_mail_channel_queued_len"
    );
    assert_eq!(
        names.mail_channel_rejected_send_total,
        "rustraft_mail_channel_rejected_send_total"
    );

    let metrics = matrixraft_queue_pressure_prometheus(
        &[(
            "scheduler\"urgent",
            MailBoxPressureStats {
                high_watermark: 128,
                total_len: 64,
                max_channel_depth: 96,
                rejected_send_count: 7,
            },
        )],
        &[MailChannelPressureStats {
            replica_id: 42,
            num_mail_limit: 256,
            queued_len: 144,
            selector_total_mail_count: 512,
            max_depth: 192,
            rejected_send_count: 9,
        }],
        &[
            ("service", "raft\"a"),
            ("group", "g1"),
            ("workload", "scale"),
        ],
    );

    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(metrics.metric_count, 8);
    assert!(metrics.text.contains(
        "rustraft_mailbox_total_len{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\",mailbox=\"scheduler\\\"urgent\"} 64"
    ));
    assert!(metrics.text.contains(
        "rustraft_mailbox_high_watermark{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\",mailbox=\"scheduler\\\"urgent\"} 128"
    ));
    assert!(metrics.text.contains(
        "rustraft_mailbox_max_channel_depth{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\",mailbox=\"scheduler\\\"urgent\"} 96"
    ));
    assert!(metrics.text.contains(
        "rustraft_mailbox_rejected_send_total{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\",mailbox=\"scheduler\\\"urgent\"} 7"
    ));
    assert!(metrics.text.contains(
        "rustraft_mail_channel_queued_len{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\",replica_id=\"42\"} 144"
    ));
    assert!(metrics.text.contains(
        "rustraft_mail_channel_limit{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\",replica_id=\"42\"} 256"
    ));
    assert!(metrics.text.contains(
        "rustraft_mail_channel_max_depth{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\",replica_id=\"42\"} 192"
    ));
    assert!(metrics.text.contains(
        "rustraft_mail_channel_rejected_send_total{service=\"raft\\\"a\",group=\"g1\",workload=\"scale\",replica_id=\"42\"} 9"
    ));

    let panels = matrixraft_queue_pressure_grafana_panels();
    assert_eq!(panels.len(), 4);
    assert!(panels
        .iter()
        .any(|panel| panel.title == "Queue Pressure Depth"
            && panel.expr.contains("rustraft_mailbox_total_len")
            && panel.expr.contains("rustraft_mail_channel_queued_len")));
    assert!(panels
        .iter()
        .any(|panel| panel.title == "Queue Pressure Rejections"
            && panel.expr.contains("rustraft_mailbox_rejected_send_total")
            && panel
                .expr
                .contains("rustraft_mail_channel_rejected_send_total")));
    assert!(panels
        .iter()
        .any(|panel| panel.title == "Queue Pressure Limits"
            && panel.expr.contains("rustraft_mailbox_high_watermark")
            && panel.expr.contains("rustraft_mail_channel_limit")));
    assert!(panels
        .iter()
        .any(|panel| panel.title == "Queue Pressure Max Depth"
            && panel.expr.contains("rustraft_mailbox_max_channel_depth")
            && panel.expr.contains("rustraft_mail_channel_max_depth")));
}

#[test]
fn memory_observability_exports_threshold_optimization_hints() {
    let hints = matrixraft_memory_optimization_hints(
        &MemoryMetrics {
            process_resident_memory_bytes: 9,
            heap_allocated_bytes: 7,
            log_cache_bytes: 5,
            snapshot_buffer_bytes: 3,
            replication_buffer_bytes: 1,
        },
        &MemoryOptimizationThresholds {
            process_resident_warning_bytes: 8,
            heap_allocated_warning_bytes: 8,
            log_cache_warning_bytes: 4,
            snapshot_buffer_warning_bytes: 4,
            replication_buffer_warning_bytes: 1,
        },
    );

    assert_eq!(hints.len(), 3);
    assert!(hints.iter().any(|hint| {
        hint.id == "process_resident_memory_high"
            && hint.component == "memory"
            && hint.observed_value == 9
            && hint.threshold == 8
    }));
    assert!(hints.iter().any(|hint| {
        hint.id == "log_cache_memory_high"
            && hint.component == "log_cache"
            && hint.observed_value == 5
            && hint.threshold == 4
    }));
    assert!(hints.iter().any(|hint| {
        hint.id == "replication_buffer_memory_high"
            && hint.component == "replication_pipeline"
            && hint.observed_value == 1
            && hint.threshold == 1
    }));
}

#[test]
fn latency_observability_exports_canonical_histogram_payload() {
    let latency = LatencyMetrics {
        append_latency_ms: LatencyHistogram {
            buckets: vec![
                LatencyBucket {
                    le_ms: "1".to_string(),
                    count: 7,
                },
                LatencyBucket {
                    le_ms: "10".to_string(),
                    count: 11,
                },
                LatencyBucket {
                    le_ms: "+Inf".to_string(),
                    count: 13,
                },
            ],
            sum_ms: 44,
            count: 13,
        },
        vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        pre_vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        read_index_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        snapshot_install_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
    };

    let metrics = matrixraft_latency_metrics_prometheus(
        &latency,
        &[("service", "raft\"a"), ("workload", "release-scale")],
    );

    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(metrics.metric_count, 21);
    assert!(metrics.text.contains(
        "rustraft_append_latency_ms_bucket{service=\"raft\\\"a\",workload=\"release-scale\",le=\"1\"} 7"
    ));
    assert!(metrics.text.contains(
        "rustraft_append_latency_ms_bucket{service=\"raft\\\"a\",workload=\"release-scale\",le=\"10\"} 11"
    ));
    assert!(metrics.text.contains(
        "rustraft_append_latency_ms_bucket{service=\"raft\\\"a\",workload=\"release-scale\",le=\"+Inf\"} 13"
    ));
    assert!(metrics.text.contains(
        "rustraft_append_latency_ms_sum{service=\"raft\\\"a\",workload=\"release-scale\"} 44"
    ));
    assert!(metrics.text.contains(
        "rustraft_append_latency_ms_count{service=\"raft\\\"a\",workload=\"release-scale\"} 13"
    ));
    assert!(metrics.text.contains(
        "rustraft_read_index_latency_ms_bucket{service=\"raft\\\"a\",workload=\"release-scale\",le=\"+Inf\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_snapshot_install_latency_ms_count{service=\"raft\\\"a\",workload=\"release-scale\"} 0"
    ));
}

#[test]
fn runtime_pressure_admission_defaults_to_observe_only_under_memory_and_latency_pressure() {
    let decision = matrixraft_runtime_pressure_admission(
        &MemoryMetrics {
            process_resident_memory_bytes: 10,
            heap_allocated_bytes: 2,
            log_cache_bytes: 9,
            snapshot_buffer_bytes: 0,
            replication_buffer_bytes: 0,
        },
        &MemoryOptimizationThresholds {
            process_resident_warning_bytes: 8,
            heap_allocated_warning_bytes: 8,
            log_cache_warning_bytes: 4,
            snapshot_buffer_warning_bytes: 4,
            replication_buffer_warning_bytes: 4,
        },
        &LatencyMetrics {
            append_latency_ms: p99_latency_histogram(120),
            vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            pre_vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            read_index_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            snapshot_install_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        },
        &LatencyOptimizationThresholds {
            append_p99_warning_ms: 100,
            vote_p99_warning_ms: 100,
            pre_vote_p99_warning_ms: 100,
            read_index_p99_warning_ms: 50,
            snapshot_install_p99_warning_ms: 5_000,
        },
        &RuntimePressureAdmissionPolicy::observe_only(),
    );

    assert!(decision.accepted);
    assert!(decision.memory_pressure);
    assert!(decision.latency_pressure);
    assert!(!decision.scale_pressure);
    assert_eq!(decision.reason, "accepted_observe_only_pressure");
    assert_eq!(decision.rejected_component, None);
    assert!(decision.actions.contains(&"release_memory".to_string()));
    assert!(decision
        .actions
        .contains(&"compact_applied_log_cache".to_string()));
    assert!(decision
        .actions
        .contains(&"reduce_append_batch_or_raise_replication_parallelism".to_string()));
}

#[test]
fn runtime_pressure_admission_exports_prometheus_payload() {
    let decision = matrixraft_runtime_pressure_admission(
        &MemoryMetrics {
            process_resident_memory_bytes: 10,
            heap_allocated_bytes: 0,
            log_cache_bytes: 0,
            snapshot_buffer_bytes: 0,
            replication_buffer_bytes: 0,
        },
        &MemoryOptimizationThresholds {
            process_resident_warning_bytes: 8,
            heap_allocated_warning_bytes: 8,
            log_cache_warning_bytes: 8,
            snapshot_buffer_warning_bytes: 8,
            replication_buffer_warning_bytes: 8,
        },
        &LatencyMetrics {
            append_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            pre_vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            read_index_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            snapshot_install_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        },
        &LatencyOptimizationThresholds::default(),
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );

    let metrics = matrixraft_runtime_pressure_admission_prometheus(
        &decision,
        &[
            ("service", "raft\"a"),
            ("group", "g1"),
            ("workload", "release-scale"),
        ],
    );

    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(metrics.metric_count, metrics.text.lines().count() as u64);
    assert_eq!(metrics.metric_count, 32);
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_admission_accepted{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",reason=\"rejected_runtime_pressure\",rejected_component=\"memory.process_resident\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_admission_rejected{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",reason=\"rejected_runtime_pressure\",rejected_component=\"memory.process_resident\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_memory{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_memory_observed_value{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",component=\"memory.process_resident\"} 10"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_memory_threshold_value{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",component=\"memory.process_resident\"} 8"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_memory_excess{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",component=\"memory.process_resident\"} 2"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_latency{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_scale{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_pipeline{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_latency_observed_p99_ms{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",component=\"none\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_scale_target_percent{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",component=\"none\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_pipeline_excess{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",peer_id=\"none\",component=\"none\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_read_backlog_excess{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",component=\"none\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_node_runtime_timer_excess_percent{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",component=\"none\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_action_total{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",action=\"release_memory\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_action_source_total{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",component=\"memory.process_resident\",action=\"release_memory\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_bottleneck_score_percent{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",rank=\"1\",category=\"memory\",component=\"memory.process_resident\"} 25"
    ));
}

#[test]
fn runtime_pressure_freshness_prometheus_exports_release_evidence_age() {
    let report = matrixraft_runtime_pressure_freshness_report(1_000, 2_050, 1_000, 300);
    let metrics = matrixraft_runtime_pressure_freshness_prometheus(
        &report,
        &[
            ("service", "raft\"a"),
            ("group", "g1"),
            ("workload", "release-scale"),
        ],
    );

    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(metrics.metric_count, metrics.text.lines().count() as u64);
    assert_eq!(metrics.metric_count, 11);
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_generated_at_unix_ms{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\"} 1000"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_age_ms{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",freshness_status=\"stale\"} 1050"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_max_age_ms{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\"} 1000"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_stale_after_unix_ms{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\"} 2000"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_remaining_fresh_ms{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",freshness_status=\"stale\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_low_fresh_ms{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\"} 300"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_low_fresh{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",freshness_status=\"stale\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_fresh{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",freshness_status=\"stale\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_status{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",freshness_status=\"stale\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_issue_total{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_freshness_issue{service=\"raft\\\"a\",group=\"g1\",workload=\"release-scale\",issue=\"runtime_pressure_generated_at_stale\"} 1"
    ));
}

#[test]
fn runtime_pressure_admission_fail_closed_rejects_on_tail_latency_before_memory_pressure() {
    let decision = matrixraft_runtime_pressure_admission(
        &MemoryMetrics {
            process_resident_memory_bytes: 10,
            heap_allocated_bytes: 0,
            log_cache_bytes: 0,
            snapshot_buffer_bytes: 0,
            replication_buffer_bytes: 0,
        },
        &MemoryOptimizationThresholds {
            process_resident_warning_bytes: 8,
            heap_allocated_warning_bytes: 8,
            log_cache_warning_bytes: 8,
            snapshot_buffer_warning_bytes: 8,
            replication_buffer_warning_bytes: 8,
        },
        &LatencyMetrics {
            append_latency_ms: p99_latency_histogram(120),
            vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            pre_vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            read_index_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            snapshot_install_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        },
        &LatencyOptimizationThresholds {
            append_p99_warning_ms: 100,
            vote_p99_warning_ms: 100,
            pre_vote_p99_warning_ms: 100,
            read_index_p99_warning_ms: 50,
            snapshot_install_p99_warning_ms: 5_000,
        },
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );

    assert!(!decision.accepted);
    assert!(decision.memory_pressure);
    assert!(decision.latency_pressure);
    assert!(!decision.scale_pressure);
    assert_eq!(decision.reason, "rejected_runtime_pressure");
    assert_eq!(
        decision.rejected_component,
        Some("latency.append".to_string())
    );
}

#[test]
fn runtime_pressure_admission_can_fail_closed_on_latency_only_pressure() {
    let decision = matrixraft_runtime_pressure_admission(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds {
            process_resident_warning_bytes: 8,
            heap_allocated_warning_bytes: 8,
            log_cache_warning_bytes: 8,
            snapshot_buffer_warning_bytes: 8,
            replication_buffer_warning_bytes: 8,
        },
        &LatencyMetrics {
            append_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            pre_vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            read_index_latency_ms: p99_latency_histogram(75),
            snapshot_install_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        },
        &LatencyOptimizationThresholds {
            append_p99_warning_ms: 100,
            vote_p99_warning_ms: 100,
            pre_vote_p99_warning_ms: 100,
            read_index_p99_warning_ms: 50,
            snapshot_install_p99_warning_ms: 5_000,
        },
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );

    assert!(!decision.accepted);
    assert!(!decision.memory_pressure);
    assert!(decision.latency_pressure);
    assert!(!decision.scale_pressure);
    assert_eq!(
        decision.rejected_component,
        Some("latency.read_index".to_string())
    );
    assert!(decision.actions.contains(
        &"route_reads_to_healthy_leaders_or_reduce_read_index_quorum_latency".to_string()
    ));
}

#[test]
fn runtime_pressure_admission_treats_malformed_latency_histograms_as_pressure() {
    let latency_metrics = LatencyMetrics {
        append_latency_ms: malformed_latency_histogram(),
        vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        pre_vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        read_index_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        snapshot_install_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
    };
    let latency_thresholds = LatencyOptimizationThresholds {
        append_p99_warning_ms: 100,
        vote_p99_warning_ms: 100,
        pre_vote_p99_warning_ms: 100,
        read_index_p99_warning_ms: 50,
        snapshot_install_p99_warning_ms: 5_000,
    };

    let observe_only_decision = matrixraft_runtime_pressure_admission(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &latency_metrics,
        &latency_thresholds,
        &RuntimePressureAdmissionPolicy::observe_only(),
    );

    assert!(observe_only_decision.accepted);
    assert!(observe_only_decision.latency_pressure);
    assert_eq!(observe_only_decision.latency_pressure_details.len(), 1);
    let detail = observe_only_decision
        .latency_pressure_details
        .first()
        .expect("malformed histogram should produce latency pressure detail");
    assert_eq!(detail.component, "latency.append");
    assert_eq!(detail.sample_count, 10);
    assert_eq!(detail.threshold_p99_ms, 100);
    assert_eq!(detail.observed_p99_ms, 101);
    assert_eq!(detail.excess_ms, 1);
    assert!(observe_only_decision
        .actions
        .contains(&"reduce_append_batch_or_raise_replication_parallelism".to_string()));
    assert!(
        matrixraft::metrics::matrixraft_validate_runtime_pressure_admission_evidence(
            &observe_only_decision
        )
        .is_ok()
    );

    let fail_closed_decision = matrixraft_runtime_pressure_admission(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &latency_metrics,
        &latency_thresholds,
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );

    assert!(!fail_closed_decision.accepted);
    assert!(fail_closed_decision.latency_pressure);
    assert_eq!(
        fail_closed_decision.rejected_component,
        Some("latency.append".to_string())
    );
}

#[test]
fn runtime_pressure_admission_can_fail_closed_on_scale_target_pressure() {
    let decision = matrixraft_runtime_pressure_admission_with_scale_targets(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &ScaleRateMetrics {
            proposal_qps: 700,
            append_entries_qps: 2_100,
            read_index_qps: 950,
            apply_entries_qps: 900,
            replication_mib_per_sec: 60,
            apply_mib_per_sec: 120,
        },
        &ScaleOptimizationTargets {
            min_proposal_qps: 1_000,
            min_append_entries_qps: 2_000,
            min_read_index_qps: 900,
            min_apply_entries_qps: 1_000,
            min_replication_mib_per_sec: 100,
            min_apply_mib_per_sec: 100,
        },
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );

    assert!(!decision.accepted);
    assert!(!decision.memory_pressure);
    assert!(!decision.latency_pressure);
    assert!(decision.scale_pressure);
    assert_eq!(decision.scale_pressure_details.len(), 3);
    assert!(decision.scale_pressure_details.iter().any(|detail| {
        detail.component == "scale.proposal_qps"
            && detail.observed_value == 700
            && detail.target_value == 1_000
            && detail.deficit == 300
            && detail.target_percent == 70
    }));
    assert!(decision.scale_pressure_details.iter().any(|detail| {
        detail.component == "scale.replication_mib_per_sec"
            && detail.observed_value == 60
            && detail.target_value == 100
            && detail.deficit == 40
            && detail.target_percent == 60
    }));
    assert_eq!(decision.reason, "rejected_runtime_pressure");
    assert_eq!(
        decision.rejected_component,
        Some("scale.proposal_qps".to_string())
    );
    assert!(decision
        .actions
        .contains(&"increase_proposal_pipeline_parallelism".to_string()));
    assert!(decision
        .actions
        .contains(&"raise_apply_worker_capacity".to_string()));
    assert!(decision
        .actions
        .contains(&"increase_replication_batching_or_network_capacity".to_string()));
}

#[test]
fn runtime_pressure_admission_can_fail_closed_on_peer_pipeline_pressure() {
    let mut peer = PeerProgress::new(2, 10, PipelineLimits::production_default());
    peer.append_queue_limit = 4;
    peer.append_queue_depth = 4;
    peer.reorder_queue_depth = 2;
    peer.memory_backpressure_rejections = 1;

    let decision = matrixraft_runtime_pressure_admission_with_pipeline_pressure(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &[peer],
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );

    assert!(!decision.accepted);
    assert!(!decision.memory_pressure);
    assert!(!decision.latency_pressure);
    assert!(!decision.scale_pressure);
    assert!(decision.pipeline_pressure);
    assert_eq!(
        decision.rejected_component,
        Some("pipeline.append_queue".to_string())
    );
    assert_eq!(decision.pipeline_pressure_details.len(), 3);
    assert!(decision.pipeline_pressure_details.iter().any(|detail| {
        detail.peer_id == 2
            && detail.component == "pipeline.append_queue"
            && detail.observed_value == 4
            && detail.threshold_value == 4
            && detail.excess == 0
    }));
    assert!(decision.pipeline_pressure_details.iter().any(|detail| {
        detail.peer_id == 2
            && detail.component == "pipeline.memory_backpressure_rejections"
            && detail.observed_value == 1
            && detail.threshold_value == 1
    }));
    assert!(decision
        .actions
        .contains(&"increase_append_queue_capacity_or_reduce_peer_append_burst".to_string()));
    assert!(decision
        .actions
        .contains(&"reduce_append_inflight_bytes".to_string()));
}

#[test]
fn runtime_pressure_admission_can_fail_closed_on_read_backlog_pressure() {
    let decision = matrixraft_runtime_pressure_admission_with_read_backlog_pressure(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &ReadBacklogMetrics {
            pending_read_index_requests: 2048,
            pending_bounded_stale_reads: 12,
        },
        &ReadBacklogThresholds {
            pending_read_index_warning: 1024,
            pending_bounded_stale_read_warning: 8,
        },
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );

    assert!(!decision.accepted);
    assert!(!decision.memory_pressure);
    assert!(!decision.latency_pressure);
    assert!(!decision.scale_pressure);
    assert!(!decision.pipeline_pressure);
    assert!(decision.read_backlog_pressure);
    assert_eq!(
        decision.rejected_component,
        Some("read_backlog.pending_read_index".to_string())
    );
    assert_eq!(decision.read_backlog_pressure_details.len(), 2);
    assert!(decision.read_backlog_pressure_details.iter().any(|detail| {
        detail.component == "read_backlog.pending_read_index"
            && detail.observed_value == 2048
            && detail.threshold_value == 1024
            && detail.excess == 1024
    }));
    assert!(decision.read_backlog_pressure_details.iter().any(|detail| {
        detail.component == "read_backlog.pending_bounded_stale"
            && detail.observed_value == 12
            && detail.threshold_value == 8
            && detail.excess == 4
    }));
    assert!(decision
        .actions
        .contains(&"shed_or_route_read_index_requests_to_healthy_leaders".to_string()));
    assert!(decision.actions.contains(
        &"reduce_bounded_stale_read_fanout_or_tighten_replica_read_deadlines".to_string()
    ));

    let metrics = matrixraft_runtime_pressure_admission_prometheus(
        &decision,
        &[
            ("service", "raft"),
            ("group", "g1"),
            ("workload", "read-scale"),
        ],
    );
    assert_eq!(metrics.metric_count, metrics.text.lines().count() as u64);
    assert_eq!(metrics.metric_count, 38);
    assert!(metrics
        .text
        .contains("rustraft_runtime_pressure_read_backlog{service=\"raft\",group=\"g1\",workload=\"read-scale\"} 1"));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_memory_excess{service=\"raft\",group=\"g1\",workload=\"read-scale\",component=\"none\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_latency_excess_ms{service=\"raft\",group=\"g1\",workload=\"read-scale\",component=\"none\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_scale_deficit{service=\"raft\",group=\"g1\",workload=\"read-scale\",component=\"none\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_pipeline_excess{service=\"raft\",group=\"g1\",workload=\"read-scale\",peer_id=\"none\",component=\"none\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_node_runtime_timer_excess_percent{service=\"raft\",group=\"g1\",workload=\"read-scale\",component=\"none\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_read_backlog_observed_value{service=\"raft\",group=\"g1\",workload=\"read-scale\",component=\"read_backlog.pending_read_index\"} 2048"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_read_backlog_threshold_value{service=\"raft\",group=\"g1\",workload=\"read-scale\",component=\"read_backlog.pending_bounded_stale\"} 8"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_read_backlog_excess{service=\"raft\",group=\"g1\",workload=\"read-scale\",component=\"read_backlog.pending_bounded_stale\"} 4"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_bottleneck_score_percent{service=\"raft\",group=\"g1\",workload=\"read-scale\",rank=\"1\",category=\"read_backlog\",component=\"read_backlog.pending_read_index\"} 100"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_bottleneck_score_percent{service=\"raft\",group=\"g1\",workload=\"read-scale\",rank=\"2\",category=\"read_backlog\",component=\"read_backlog.pending_bounded_stale\"} 50"
    ));
}

#[test]
fn runtime_pressure_diagnostics_warn_on_observe_only_read_backlog_pressure() {
    let decision = matrixraft_runtime_pressure_admission_with_read_backlog_pressure(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &ReadBacklogMetrics {
            pending_read_index_requests: 2048,
            pending_bounded_stale_reads: 0,
        },
        &ReadBacklogThresholds {
            pending_read_index_warning: 1024,
            pending_bounded_stale_read_warning: 8,
        },
        &RuntimePressureAdmissionPolicy::observe_only(),
    );

    assert!(decision.accepted);
    assert!(decision.read_backlog_pressure);
    assert_eq!(decision.reason, "accepted_observe_only_pressure");

    let entries = matrixraft_runtime_pressure_diagnostic_log_entries(&decision);
    assert_eq!(entries[0].target, "rustraft.runtime_pressure.admission");
    assert_eq!(entries[0].severity, matrixraft::DiagnosticSeverity::Warn);
    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.runtime_pressure.read_backlog"
            && entry.severity == matrixraft::DiagnosticSeverity::Warn
            && entry.fields.contains(&(
                "component".to_string(),
                "read_backlog.pending_read_index".to_string(),
            ))
            && entry
                .fields
                .contains(&("observed_value".to_string(), "2048".to_string()))
            && entry
                .fields
                .contains(&("threshold_value".to_string(), "1024".to_string()))
    }));
}

#[test]
fn runtime_pressure_admission_can_fail_closed_on_node_runtime_timer_pressure() {
    let decision = matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &RuntimeTimerStatus {
            heartbeat_interval_ms: 10,
            election_timeout_ms: 50,
            leader_lease_timeout_ms: 20,
            leader_lease_elapsed_ms: 0,
            leader_lease_valid: true,
            heartbeat_ticks: 8,
            election_ticks: 2,
            pending_ticks: 4,
            max_pending_ticks: 5,
            accepted_ticks: 12,
            rejected_ticks: 0,
            completed_ticks: 8,
            pre_vote_executions: 0,
            campaign_executions: 0,
            leader_transfer_executions: 0,
            last_tick_reason: "heartbeat".to_string(),
            last_tick_admission_reason: "tick_admitted".to_string(),
        },
        &NodeRuntimeTimerThresholds::default(),
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );

    assert!(!decision.accepted);
    assert!(!decision.memory_pressure);
    assert!(!decision.latency_pressure);
    assert!(!decision.scale_pressure);
    assert!(!decision.pipeline_pressure);
    assert!(!decision.read_backlog_pressure);
    assert!(decision.node_runtime_timer_pressure);
    assert_eq!(
        decision.rejected_component,
        Some("node_runtime.timer_utilization".to_string())
    );
    assert_eq!(decision.node_runtime_timer_pressure_details.len(), 1);
    assert_eq!(
        decision.node_runtime_timer_pressure_details[0].observed_percent,
        80
    );
    assert_eq!(
        decision.node_runtime_timer_pressure_details[0].threshold_percent,
        80
    );
    assert!(decision
        .actions
        .contains(&"raise_timer_queue_capacity_or_reduce_tick_burst".to_string()));

    matrixraft::metrics::matrixraft_validate_runtime_pressure_admission_evidence(&decision)
        .expect("timer pressure admission evidence validates");

    let metrics = matrixraft_runtime_pressure_admission_prometheus(
        &decision,
        &[("service", "raft"), ("group", "g1")],
    );
    assert!(metrics
        .text
        .contains("rustraft_runtime_pressure_node_runtime_timer{service=\"raft\",group=\"g1\"} 1"));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_node_runtime_timer_observed_percent{service=\"raft\",group=\"g1\",component=\"node_runtime.timer_utilization\"} 80"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_node_runtime_timer_threshold_percent{service=\"raft\",group=\"g1\",component=\"node_runtime.timer_utilization\"} 80"
    ));
    assert!(metrics.text.contains(
        "rustraft_runtime_pressure_action_source_total{service=\"raft\",group=\"g1\",component=\"node_runtime.timer_utilization\",action=\"raise_timer_queue_capacity_or_reduce_tick_burst\"} 1"
    ));

    let diagnostics = matrixraft_runtime_pressure_diagnostic_log_entries(&decision);
    assert!(diagnostics.iter().any(|entry| {
        entry.target == "rustraft.runtime_pressure.node_runtime_timer"
            && entry.fields.contains(&(
                "recommended_actions".to_string(),
                "raise_timer_queue_capacity_or_reduce_tick_burst".to_string(),
            ))
    }));
}

#[test]
fn runtime_pressure_admission_combines_scale_and_peer_pipeline_pressure() {
    let mut peer = PeerProgress::new(3, 10, PipelineLimits::production_default());
    peer.apply_inflight_limit = 2;
    peer.apply_queue_depth = 3;

    let decision = matrixraft_runtime_pressure_admission_with_scale_and_pipeline_pressure(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &ScaleRateMetrics {
            proposal_qps: 700,
            append_entries_qps: 2_100,
            read_index_qps: 950,
            apply_entries_qps: 1_000,
            replication_mib_per_sec: 100,
            apply_mib_per_sec: 100,
        },
        &ScaleOptimizationTargets {
            min_proposal_qps: 1_000,
            min_append_entries_qps: 2_000,
            min_read_index_qps: 900,
            min_apply_entries_qps: 1_000,
            min_replication_mib_per_sec: 100,
            min_apply_mib_per_sec: 100,
        },
        &[peer],
        &RuntimePressureAdmissionPolicy::observe_only(),
    );

    assert!(decision.accepted);
    assert!(decision.scale_pressure);
    assert!(decision.pipeline_pressure);
    assert_eq!(decision.reason, "accepted_observe_only_pressure");
    assert!(decision
        .actions
        .contains(&"increase_proposal_pipeline_parallelism".to_string()));
    assert!(decision
        .actions
        .contains(&"raise_apply_worker_capacity".to_string()));
}

#[test]
fn runtime_pressure_admission_combines_scale_pipeline_and_read_backlog_pressure() {
    let mut peer = PeerProgress::new(4, 10, PipelineLimits::production_default());
    peer.append_queue_limit = 4;
    peer.append_queue_depth = 4;

    let decision =
        matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure(
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &ScaleRateMetrics {
                proposal_qps: 1_000,
                append_entries_qps: 1_500,
                read_index_qps: 600,
                apply_entries_qps: 1_000,
                replication_mib_per_sec: 100,
                apply_mib_per_sec: 100,
            },
            &ScaleOptimizationTargets {
                min_proposal_qps: 1_000,
                min_append_entries_qps: 2_000,
                min_read_index_qps: 900,
                min_apply_entries_qps: 1_000,
                min_replication_mib_per_sec: 100,
                min_apply_mib_per_sec: 100,
            },
            &[peer],
            &ReadBacklogMetrics {
                pending_read_index_requests: 2048,
                pending_bounded_stale_reads: 0,
            },
            &ReadBacklogThresholds {
                pending_read_index_warning: 1024,
                pending_bounded_stale_read_warning: 1024,
            },
            &RuntimePressureAdmissionPolicy::fail_closed(),
        );

    assert!(!decision.accepted);
    assert!(decision.scale_pressure);
    assert!(decision.pipeline_pressure);
    assert!(decision.read_backlog_pressure);
    assert_eq!(
        decision.rejected_component,
        Some("read_backlog.pending_read_index".to_string())
    );
    assert!(decision.scale_pressure_details.iter().any(|detail| {
        detail.component == "scale.read_index_qps"
            && detail.observed_value == 600
            && detail.target_value == 900
    }));
    assert!(decision
        .pipeline_pressure_details
        .iter()
        .any(|detail| { detail.peer_id == 4 && detail.component == "pipeline.append_queue" }));
    assert!(decision.read_backlog_pressure_details.iter().any(|detail| {
        detail.component == "read_backlog.pending_read_index"
            && detail.observed_value == 2048
            && detail.threshold_value == 1024
    }));
    assert!(decision
        .actions
        .contains(&"increase_read_index_fast_path_capacity".to_string()));
    assert!(decision
        .actions
        .contains(&"increase_append_queue_capacity_or_reduce_peer_append_burst".to_string()));
    assert!(decision
        .actions
        .contains(&"shed_or_route_read_index_requests_to_healthy_leaders".to_string()));
}

#[test]
fn runtime_pressure_admission_combines_scale_pipeline_read_backlog_and_timer_pressure() {
    let mut peer = PeerProgress::new(5, 10, PipelineLimits::production_default());
    peer.append_queue_limit = 4;
    peer.append_queue_depth = 4;

    let decision =
        matrixraft_runtime_pressure_admission_with_scale_pipeline_read_backlog_and_node_runtime_timer_pressure(
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &ScaleRateMetrics {
                proposal_qps: 1_000,
                append_entries_qps: 1_500,
                read_index_qps: 600,
                apply_entries_qps: 1_000,
                replication_mib_per_sec: 100,
                apply_mib_per_sec: 100,
            },
            &ScaleOptimizationTargets {
                min_proposal_qps: 1_000,
                min_append_entries_qps: 2_000,
                min_read_index_qps: 900,
                min_apply_entries_qps: 1_000,
                min_replication_mib_per_sec: 100,
                min_apply_mib_per_sec: 100,
            },
            &[peer],
            &ReadBacklogMetrics {
                pending_read_index_requests: 0,
                pending_bounded_stale_reads: 0,
            },
            &ReadBacklogThresholds {
                pending_read_index_warning: 1_024,
                pending_bounded_stale_read_warning: 1_024,
            },
            &RuntimeTimerStatus {
                heartbeat_interval_ms: 10,
                election_timeout_ms: 50,
                leader_lease_timeout_ms: 20,
                leader_lease_elapsed_ms: 0,
                leader_lease_valid: true,
                heartbeat_ticks: 8,
                election_ticks: 2,
                pending_ticks: 4,
                max_pending_ticks: 5,
                accepted_ticks: 12,
                rejected_ticks: 0,
                completed_ticks: 8,
                pre_vote_executions: 0,
                campaign_executions: 0,
                leader_transfer_executions: 0,
                last_tick_reason: "heartbeat".to_string(),
                last_tick_admission_reason: "tick_admitted".to_string(),
            },
            &NodeRuntimeTimerThresholds::default(),
            &RuntimePressureAdmissionPolicy::fail_closed(),
        );

    assert!(!decision.accepted);
    assert!(decision.scale_pressure);
    assert!(decision.pipeline_pressure);
    assert!(!decision.read_backlog_pressure);
    assert!(decision.node_runtime_timer_pressure);
    assert_eq!(
        decision.rejected_component,
        Some("node_runtime.timer_utilization".to_string())
    );
    assert!(decision
        .node_runtime_timer_pressure_details
        .iter()
        .any(|detail| {
            detail.component == "node_runtime.timer_utilization"
                && detail.observed_percent == 80
                && detail.threshold_percent == 80
        }));
    assert!(decision.scale_pressure_details.iter().any(|detail| {
        detail.component == "scale.read_index_qps"
            && detail.observed_value == 600
            && detail.target_value == 900
    }));
    assert!(decision
        .actions
        .contains(&"raise_timer_queue_capacity_or_reduce_tick_burst".to_string()));
}

#[test]
fn runtime_pressure_bottleneck_summary_tie_breaks_by_release_impact() {
    let mut peer = PeerProgress::new(6, 10, PipelineLimits::production_default());
    peer.append_queue_limit = 100;
    peer.append_queue_depth = 150;

    let decision =
        matrixraft_runtime_pressure_admission_with_scale_pipeline_read_backlog_and_node_runtime_timer_pressure(
            &MemoryMetrics {
                process_resident_memory_bytes: 150,
                heap_allocated_bytes: 0,
                log_cache_bytes: 0,
                snapshot_buffer_bytes: 0,
                replication_buffer_bytes: 0,
            },
            &MemoryOptimizationThresholds {
                process_resident_warning_bytes: 100,
                heap_allocated_warning_bytes: 1_000,
                log_cache_warning_bytes: 1_000,
                snapshot_buffer_warning_bytes: 1_000,
                replication_buffer_warning_bytes: 1_000,
            },
            &LatencyMetrics {
                append_latency_ms: p99_latency_histogram(150),
                vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
                pre_vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
                read_index_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
                snapshot_install_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            },
            &LatencyOptimizationThresholds {
                append_p99_warning_ms: 100,
                vote_p99_warning_ms: 1_000,
                pre_vote_p99_warning_ms: 1_000,
                read_index_p99_warning_ms: 1_000,
                snapshot_install_p99_warning_ms: 10_000,
            },
            &ScaleRateMetrics {
                proposal_qps: 50,
                append_entries_qps: 1_000,
                read_index_qps: 1_000,
                apply_entries_qps: 1_000,
                replication_mib_per_sec: 1_000,
                apply_mib_per_sec: 1_000,
            },
            &ScaleOptimizationTargets {
                min_proposal_qps: 100,
                min_append_entries_qps: 1_000,
                min_read_index_qps: 1_000,
                min_apply_entries_qps: 1_000,
                min_replication_mib_per_sec: 1_000,
                min_apply_mib_per_sec: 1_000,
            },
            &[peer],
            &ReadBacklogMetrics {
                pending_read_index_requests: 150,
                pending_bounded_stale_reads: 0,
            },
            &ReadBacklogThresholds {
                pending_read_index_warning: 100,
                pending_bounded_stale_read_warning: 1_000,
            },
            &RuntimeTimerStatus {
                heartbeat_interval_ms: 10,
                election_timeout_ms: 50,
                leader_lease_timeout_ms: 20,
                leader_lease_elapsed_ms: 0,
                leader_lease_valid: true,
                heartbeat_ticks: 8,
                election_ticks: 2,
                pending_ticks: 75,
                max_pending_ticks: 100,
                accepted_ticks: 12,
                rejected_ticks: 0,
                completed_ticks: 8,
                pre_vote_executions: 0,
                campaign_executions: 0,
                leader_transfer_executions: 0,
                last_tick_reason: "heartbeat".to_string(),
                last_tick_admission_reason: "tick_admitted".to_string(),
            },
            &NodeRuntimeTimerThresholds {
                utilization_warning_percent: 50,
            },
            &RuntimePressureAdmissionPolicy::observe_only(),
        );

    let bottlenecks = matrixraft_runtime_pressure_bottleneck_summary(&decision);
    assert_eq!(
        bottlenecks
            .iter()
            .map(|bottleneck| bottleneck.category.as_str())
            .collect::<Vec<_>>(),
        vec![
            "read_backlog",
            "node_runtime_timer",
            "pipeline",
            "latency",
            "scale",
            "memory"
        ]
    );
    assert!(bottlenecks
        .iter()
        .all(|bottleneck| bottleneck.score_percent == 50));
}

fn p99_latency_histogram(p99_ms: u64) -> LatencyHistogram {
    LatencyHistogram {
        buckets: vec![
            LatencyBucket {
                le_ms: "1".to_string(),
                count: 50,
            },
            LatencyBucket {
                le_ms: p99_ms.to_string(),
                count: 99,
            },
            LatencyBucket {
                le_ms: "+Inf".to_string(),
                count: 100,
            },
        ],
        sum_ms: p99_ms * 99,
        count: 100,
    }
}

fn malformed_latency_histogram() -> LatencyHistogram {
    LatencyHistogram {
        buckets: vec![
            LatencyBucket {
                le_ms: "10".to_string(),
                count: 8,
            },
            LatencyBucket {
                le_ms: "5".to_string(),
                count: 7,
            },
            LatencyBucket {
                le_ms: "+Inf".to_string(),
                count: 9,
            },
        ],
        sum_ms: 64,
        count: 10,
    }
}
