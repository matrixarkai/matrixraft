// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    benchmark::{
        matrixraft_baseline_raft_benchmark_failure_summary,
        matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog,
        matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer,
        matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts,
        matrixraft_production_readiness_input_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts,
        matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts,
        matrixraft_production_readiness_report_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts,
        matrixraft_release_benchmark_runtime_timer_status,
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact,
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog,
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer,
        BenchmarkComparison, BenchmarkEngine, BenchmarkEngineSource, BenchmarkFailureSummary,
        BenchmarkHarnessKind, BenchmarkImplementation, BenchmarkOptions, BenchmarkReport,
        BenchmarkSample, BenchmarkWorkload, BenchmarkWorkloadSummary,
    },
    matrixraft_admin_diagnostic_json_lines, matrixraft_admin_diagnostic_log_entries,
    matrixraft_admin_status_surface_evidence, matrixraft_capability_evidence,
    matrixraft_debug_bundle_validation_prometheus, matrixraft_debug_snapshot,
    matrixraft_debug_snapshot_json, matrixraft_debug_snapshot_metadata_prometheus,
    matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts,
    matrixraft_debug_snapshot_with_benchmark_scale_inputs,
    matrixraft_debug_snapshot_with_benchmark_summary,
    matrixraft_debug_snapshot_with_observability_metrics,
    matrixraft_debug_snapshot_with_performance_targets,
    matrixraft_debug_snapshot_with_runtime_metrics,
    matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence,
    matrixraft_debug_snapshot_with_runtime_pressure_evidence,
    matrixraft_debug_snapshot_with_scale_metrics, matrixraft_diagnostic_log_prometheus,
    matrixraft_local_status_diagnostic_json_lines, matrixraft_local_status_diagnostic_log_entries,
    matrixraft_operator_runbook_prometheus, matrixraft_optimization_diagnostic_json_lines,
    matrixraft_optimization_diagnostic_log_entries, matrixraft_optimization_report,
    matrixraft_optimization_report_prometheus, matrixraft_peer_pipeline_metrics_prometheus,
    matrixraft_production_readiness_report_with_runtime_pressure_policy,
    matrixraft_production_readiness_report_with_runtime_pressure_policy_and_freshness,
    matrixraft_runtime_admin_report, matrixraft_runtime_local_status_report,
    matrixraft_runtime_pressure_admission,
    matrixraft_runtime_pressure_admission_with_pipeline_pressure,
    matrixraft_runtime_pressure_admission_with_scale_targets,
    matrixraft_runtime_pressure_bottleneck_summary,
    matrixraft_runtime_pressure_diagnostic_json_lines,
    matrixraft_runtime_pressure_diagnostic_log_entries,
    matrixraft_runtime_pressure_freshness_diagnostic_json_lines,
    matrixraft_runtime_pressure_freshness_diagnostic_log_entries,
    matrixraft_runtime_pressure_freshness_report, matrixraft_validate_debug_snapshot,
    matrixraft_validate_debug_snapshot_json,
    matrixraft_validate_runtime_pressure_admission_evidence,
    matrixraft_validate_runtime_pressure_admission_evidence_with_policy, AdminStatusSurfaceInput,
    BenchmarkScaleOptimizationInputs, DiagnosticLogEntry, DiagnosticSeverity, HealthStatus,
    LatencyBucket, LatencyHistogram, LatencyMetrics, LatencyOptimizationThresholds,
    LatencyPressureDetail, MemoryMetrics, MemoryOptimizationThresholds, MemoryPressureDetail,
    NodeRuntimeTimerThresholds, OperatorRunbookStep, OptimizationHint, OptimizationHintSeverity,
    Peer, PeerProgress, PipelineLimits, PipelinePressureDetail, ProductionReadinessInput,
    ProgressState, RaftCluster, ReadBacklogMetrics, ReadBacklogPressureDetail,
    ReadBacklogThresholds, ReplicaRole, RuntimePressureAdmission, RuntimePressureAdmissionPolicy,
    ScaleMetrics, ScaleOptimizationTargets, ScalePressureDetail, ScaleRateMetrics, StateRole,
    StatusSnapshot,
};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 6_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 7_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn ready_snapshot() -> matrixraft::ReadinessSnapshot {
    matrixraft::ReadinessSnapshot {
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
    }
}

fn pipeline_peer(peer_id: u64, match_index: u64, next_index: u64) -> PeerProgress {
    PeerProgress {
        peer_id,
        progress_state: ProgressState::Probe,
        paused: false,
        old_paused: false,
        match_index,
        next_index,
        append_requests: 1,
        append_batches: 1,
        max_append_batch_entries: 1,
        max_append_batch_bytes: 64,
        append_accepted: 1,
        append_rejected: 0,
        retry_attempts: 0,
        backoff_ms: 0,
        next_retry_after_ms: 0,
        inflight_entries: 0,
        inflight_bytes: 0,
        append_queue_depth: 0,
        append_queue_limit: 16,
        append_queue_max_depth: 1,
        inflight_bytes_limit: 1024,
        apply_inflight_tasks: 0,
        apply_inflight_limit: 8,
        apply_queue_depth: 0,
        apply_queue_max_depth: 1,
        apply_batch_bytes_limit: 1024,
        apply_backpressure_rejections: 0,
        memory_backpressure_rejections: 0,
        oversized_log_rejections: 0,
        reorder_queue_depth: 0,
        out_of_order_append_rejections: 0,
        reorder_entries_rejected: 0,
        reorder_entries_converged: 0,
        reorder_entry_timeouts: 0,
        reorder_dropped_packages: 0,
        stale_term_rejections: 0,
        packet_loss_events: 0,
        network_error_probe_transitions: 0,
        snapshot_sending: false,
        snapshot_installing: false,
        snapshot_installed_index: 4,
        snapshot_send_attempts: 0,
        snapshot_install_total_chunks: 0,
        snapshot_install_progress_per_mille: 0,
        snapshot_backpressure_rejections: 0,
        snapshot_rate_limit_rejections: 0,
        snapshot_install_rolled_back: 0,
        snapshot_chunk_retry_count: 0,
        snapshot_send_timeouts: 0,
        required_snapshot_index: 0,
        acked_snapshot_index: 0,
        snapshot_during_membership_change: false,
        snapshot_rejoin_after_compacted_log: false,
        transfer_leader_target: false,
        transfer_leader_timeouts: 0,
        pre_vote_rejections: 0,
        election_rejections: 0,
        offline_timeout_reached: false,
        offline_timeout_rejections: 0,
        follower_lag: 0,
        learner_catchup_rounds: 0,
        learner_caught_up: false,
        witness_quorum_required: 0,
        witness_quorum_acked: 0,
        witness_quorum_reached: false,
    }
}

#[test]
fn local_status_report_tracks_replication_apply_and_pipeline_health() {
    let status = StatusSnapshot {
        group_id: 5,
        node_id: 1,
        role: StateRole::Leader,
        term: 3,
        leader_id: Some(1),
        commit_index: 10,
        applied_index: 9,
        last_log_index: 10,
        last_snapshot_index: 4,
        peers: Vec::new(),
    };
    let pipeline = vec![PeerProgress {
        peer_id: 2,
        progress_state: ProgressState::Replicate,
        paused: false,
        old_paused: false,
        match_index: 8,
        next_index: 9,
        append_requests: 10,
        append_batches: 4,
        max_append_batch_entries: 3,
        max_append_batch_bytes: 192,
        append_accepted: 8,
        append_rejected: 2,
        retry_attempts: 1,
        backoff_ms: 20,
        next_retry_after_ms: 10,
        inflight_entries: 1,
        inflight_bytes: 64,
        append_queue_depth: 1,
        append_queue_limit: 16,
        append_queue_max_depth: 2,
        inflight_bytes_limit: 1024,
        apply_inflight_tasks: 1,
        apply_inflight_limit: 8,
        apply_queue_depth: 1,
        apply_queue_max_depth: 2,
        apply_batch_bytes_limit: 1024,
        apply_backpressure_rejections: 0,
        memory_backpressure_rejections: 0,
        oversized_log_rejections: 0,
        reorder_queue_depth: 0,
        out_of_order_append_rejections: 0,
        reorder_entries_rejected: 0,
        reorder_entries_converged: 0,
        reorder_entry_timeouts: 0,
        reorder_dropped_packages: 0,
        stale_term_rejections: 0,
        packet_loss_events: 0,
        network_error_probe_transitions: 0,
        snapshot_sending: false,
        snapshot_installing: false,
        snapshot_installed_index: 4,
        snapshot_send_attempts: 0,
        snapshot_install_total_chunks: 0,
        snapshot_install_progress_per_mille: 0,
        snapshot_backpressure_rejections: 0,
        snapshot_rate_limit_rejections: 0,
        snapshot_install_rolled_back: 0,
        snapshot_chunk_retry_count: 0,
        snapshot_send_timeouts: 0,
        required_snapshot_index: 0,
        acked_snapshot_index: 0,
        snapshot_during_membership_change: false,
        snapshot_rejoin_after_compacted_log: false,
        transfer_leader_target: false,
        transfer_leader_timeouts: 0,
        pre_vote_rejections: 0,
        election_rejections: 0,
        offline_timeout_reached: false,
        offline_timeout_rejections: 0,
        follower_lag: 2,
        learner_catchup_rounds: 0,
        learner_caught_up: false,
        witness_quorum_required: 0,
        witness_quorum_acked: 0,
        witness_quorum_reached: false,
    }];

    let report = matrixraft_runtime_local_status_report(status, pipeline, ready_snapshot());
    assert_eq!(report.replication_health.status, HealthStatus::Degraded);
    assert_eq!(report.apply_health.status, HealthStatus::Degraded);
    assert!(report.blockers.contains(&"replication_lagging".to_string()));
    assert!(report.blockers.contains(&"apply_lagging".to_string()));
}

#[test]
fn peer_pipeline_prometheus_exports_reorder_convergence_metrics() {
    let status = StatusSnapshot {
        group_id: 5,
        node_id: 1,
        role: StateRole::Leader,
        term: 3,
        leader_id: Some(1),
        commit_index: 10,
        applied_index: 10,
        last_log_index: 10,
        last_snapshot_index: 4,
        peers: Vec::new(),
    };
    let mut peer = pipeline_peer(2, 10, 11);
    peer.append_queue_depth = 3;
    peer.reorder_queue_depth = 1;
    peer.reorder_entries_converged = 7;
    peer.snapshot_installed_index = 4;

    let report = matrixraft_runtime_local_status_report(status, vec![peer], ready_snapshot());
    let metrics = matrixraft_peer_pipeline_metrics_prometheus(&report, &[("service", "raft-a")]);

    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(metrics.metric_count, 4);
    assert!(metrics.text.contains(
        "rustraft_peer_append_queue_depth{service=\"raft-a\",group=\"5\",node=\"1\",peer=\"2\"} 3"
    ));
    assert!(metrics.text.contains(
        "rustraft_peer_reorder_queue_depth{service=\"raft-a\",group=\"5\",node=\"1\",peer=\"2\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_peer_reorder_entries_converged_total{service=\"raft-a\",group=\"5\",node=\"1\",peer=\"2\"} 7"
    ));
    assert!(metrics.text.contains(
        "rustraft_peer_snapshot_installed_index{service=\"raft-a\",group=\"5\",node=\"1\",peer=\"2\"} 4"
    ));
}

#[test]
fn local_status_diagnostics_export_peer_pipeline_pressure() {
    let status = StatusSnapshot {
        group_id: 5,
        node_id: 1,
        role: StateRole::Leader,
        term: 3,
        leader_id: Some(1),
        commit_index: 12,
        applied_index: 11,
        last_log_index: 12,
        last_snapshot_index: 4,
        peers: Vec::new(),
    };
    let mut peer = pipeline_peer(2, 10, 11);
    peer.append_queue_depth = 3;
    peer.reorder_queue_depth = 1;
    peer.reorder_entries_converged = 7;
    peer.packet_loss_events = 2;
    peer.snapshot_installed_index = 4;

    let report = matrixraft_runtime_local_status_report(status, vec![peer], ready_snapshot());
    let entries = matrixraft_local_status_diagnostic_log_entries(&report);

    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.local_status"
            && entry.message == "rustraft local status degraded"
            && entry.severity == DiagnosticSeverity::Warn
    }));
    let peer_entry = entries
        .iter()
        .find(|entry| entry.target == "rustraft.local_status.peer_pipeline")
        .expect("peer pipeline diagnostic");
    assert_eq!(peer_entry.severity, DiagnosticSeverity::Warn);
    assert_eq!(peer_entry.message, "peer_pipeline_pressure");
    for field in [
        ("group_id", "5"),
        ("node_id", "1"),
        ("peer_id", "2"),
        ("peer_lag", "2"),
        ("append_queue_depth", "3"),
        ("reorder_queue_depth", "1"),
        ("reorder_entries_converged", "7"),
        ("reorder_dropped_packages", "0"),
        ("packet_loss_events", "2"),
        ("network_error_probe_transitions", "0"),
        ("snapshot_installed_index", "4"),
    ] {
        assert!(
            peer_entry
                .fields
                .contains(&(field.0.to_string(), field.1.to_string())),
            "missing field {field:?} from {peer_entry:?}"
        );
    }

    let json_lines = matrixraft_local_status_diagnostic_json_lines(&report);
    let parsed = json_lines
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("local status diagnostic json line"))
        .collect::<Vec<_>>();
    assert_eq!(parsed.len(), entries.len());
    assert!(parsed.iter().any(|entry| {
        entry["target"] == "rustraft.local_status.peer_pipeline"
            && entry["message"] == "peer_pipeline_pressure"
    }));
}

#[test]
fn local_status_diagnostics_mark_recovered_peer_pipeline_history_as_info() {
    let status = StatusSnapshot {
        group_id: 5,
        node_id: 1,
        role: StateRole::Leader,
        term: 3,
        leader_id: Some(1),
        commit_index: 12,
        applied_index: 12,
        last_log_index: 12,
        last_snapshot_index: 4,
        peers: Vec::new(),
    };
    let mut peer = pipeline_peer(2, 12, 13);
    peer.reorder_entries_converged = 7;
    peer.packet_loss_events = 2;
    peer.network_error_probe_transitions = 2;
    peer.snapshot_installed_index = 4;

    let report = matrixraft_runtime_local_status_report(status, vec![peer], ready_snapshot());
    let entries = matrixraft_local_status_diagnostic_log_entries(&report);
    let peer_entry = entries
        .iter()
        .find(|entry| entry.target == "rustraft.local_status.peer_pipeline")
        .expect("peer pipeline diagnostic");

    assert_eq!(peer_entry.severity, DiagnosticSeverity::Info);
    assert_eq!(peer_entry.message, "peer_pipeline_recovered");
    for field in [
        ("peer_lag", "0"),
        ("append_queue_depth", "0"),
        ("reorder_queue_depth", "0"),
        ("reorder_entries_converged", "7"),
        ("reorder_dropped_packages", "0"),
        ("packet_loss_events", "2"),
        ("network_error_probe_transitions", "2"),
    ] {
        assert!(
            peer_entry
                .fields
                .contains(&(field.0.to_string(), field.1.to_string())),
            "missing field {field:?} from {peer_entry:?}"
        );
    }

    let json_lines = matrixraft_local_status_diagnostic_json_lines(&report);
    assert!(json_lines.contains("\"message\":\"peer_pipeline_recovered\""));
    assert!(json_lines.contains("\"severity\":\"info\""));
}

#[test]
fn admin_status_surface_evidence_accepts_quorum_progress_with_lagging_peer() {
    let input = AdminStatusSurfaceInput {
        commit_index: 10,
        max_observed_node_commit_index: 10,
        quorum_size: 2,
        quorum_peer_ids: vec![1, 2, 3],
        peer_pipeline: vec![
            pipeline_peer(1, 10, 11),
            pipeline_peer(2, 10, 11),
            pipeline_peer(3, 8, 9),
        ],
        wal_last_log_index: 10,
        wal_segment_lifecycle_present: true,
    };
    let evidence = matrixraft_admin_status_surface_evidence(&input);
    assert!(evidence.complete, "{evidence:?}");
    assert!(evidence.quorum_peer_progress_observed);
    assert!(evidence.peer_pipeline_runtime_activity_observed);
    assert!(evidence.peer_pipeline_limits_observed);
    assert!(evidence.blockers.is_empty());
}

#[test]
fn admin_status_surface_evidence_fails_closed_on_missing_wal_or_quorum() {
    let input = AdminStatusSurfaceInput {
        commit_index: 10,
        max_observed_node_commit_index: 11,
        quorum_size: 2,
        quorum_peer_ids: vec![1, 2, 3],
        peer_pipeline: vec![pipeline_peer(1, 10, 11), pipeline_peer(2, 8, 9)],
        wal_last_log_index: 9,
        wal_segment_lifecycle_present: false,
    };
    let evidence = matrixraft_admin_status_surface_evidence(&input);
    assert!(!evidence.complete);
    assert!(evidence
        .blockers
        .contains(&"quorum_peer_progress_missing".to_string()));
    assert!(evidence
        .blockers
        .contains(&"wal_segment_lifecycle_missing".to_string()));
    assert!(evidence
        .blockers
        .contains(&"wal_commit_range_missing".to_string()));
    assert!(evidence
        .blockers
        .contains(&"cluster_commit_index_inconsistent".to_string()));
}

#[test]
fn optimization_report_surfaces_pipeline_wal_and_commit_pressure() {
    let mut append_saturated = pipeline_peer(1, 10, 11);
    append_saturated.append_queue_depth = append_saturated.append_queue_limit;
    append_saturated.reorder_queue_depth = 2;
    let mut apply_saturated = pipeline_peer(2, 9, 10);
    apply_saturated.apply_inflight_tasks = apply_saturated.apply_inflight_limit;
    apply_saturated.inflight_bytes = apply_saturated.inflight_bytes_limit;

    let report = matrixraft_optimization_report(&AdminStatusSurfaceInput {
        commit_index: 10,
        max_observed_node_commit_index: 11,
        quorum_size: 2,
        quorum_peer_ids: vec![1, 2, 3],
        peer_pipeline: vec![append_saturated, apply_saturated],
        wal_last_log_index: 9,
        wal_segment_lifecycle_present: false,
    });

    assert!(!report.ready);
    assert_eq!(report.critical_count, 2);
    assert!(report.warning_count >= 4);
    assert!(report.hints.iter().any(|hint| {
        hint.id == "cluster_commit_index_inconsistent"
            && hint.severity == OptimizationHintSeverity::Critical
    }));
    assert!(report
        .hints
        .iter()
        .any(|hint| hint.id == "wal_commit_range_missing"));
    assert!(report
        .hints
        .iter()
        .any(|hint| hint.id == "append_queue_saturated"));
    assert!(report
        .hints
        .iter()
        .any(|hint| hint.id == "apply_inflight_saturated"));
    assert!(report
        .hints
        .iter()
        .any(|hint| hint.id == "inflight_bytes_saturated"));
    assert!(report
        .hints
        .iter()
        .any(|hint| hint.id == "reorder_queue_pressure"));
}

#[test]
fn optimization_report_exports_structured_diagnostic_log_lines() {
    let mut append_saturated = pipeline_peer(1, 10, 11);
    append_saturated.append_queue_depth = append_saturated.append_queue_limit;
    append_saturated.reorder_queue_depth = 2;

    let report = matrixraft_optimization_report(&AdminStatusSurfaceInput {
        commit_index: 10,
        max_observed_node_commit_index: 11,
        quorum_size: 2,
        quorum_peer_ids: vec![1, 2, 3],
        peer_pipeline: vec![append_saturated],
        wal_last_log_index: 9,
        wal_segment_lifecycle_present: false,
    });

    let entries = matrixraft_optimization_diagnostic_log_entries(&report);
    assert_eq!(entries.len() as u64, report.hint_count);
    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.optimization.replication"
            && entry.severity == DiagnosticSeverity::Error
            && entry.message == "cluster_commit_index_inconsistent"
            && entry
                .fields
                .contains(&("observed_value".to_string(), "11".to_string()))
            && entry
                .fields
                .contains(&("threshold".to_string(), "10".to_string()))
    }));
    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.optimization.replication_pipeline"
            && entry.severity == DiagnosticSeverity::Warn
            && entry.message == "append_queue_saturated"
            && entry
                .fields
                .contains(&("optimization_ready".to_string(), "false".to_string()))
    }));
    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.optimization.transport"
            && entry.severity == DiagnosticSeverity::Info
            && entry.message == "reorder_queue_pressure"
    }));

    let json_lines = matrixraft_optimization_diagnostic_json_lines(&report);
    let parsed = json_lines
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("optimization diagnostic json line"))
        .collect::<Vec<_>>();
    assert_eq!(parsed.len(), entries.len());
    assert!(parsed.iter().any(|entry| {
        entry["target"] == "rustraft.optimization.replication"
            && entry["severity"] == "error"
            && entry["message"] == "cluster_commit_index_inconsistent"
    }));
    assert!(parsed.iter().any(|entry| {
        entry["target"] == "rustraft.optimization.replication_pipeline"
            && entry["severity"] == "warn"
            && entry["message"] == "append_queue_saturated"
    }));
}

#[test]
fn runtime_pressure_admission_exports_structured_diagnostic_log_lines() {
    let decision = matrixraft_runtime_pressure_admission(
        &MemoryMetrics {
            process_resident_memory_bytes: 10,
            heap_allocated_bytes: 0,
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
            append_latency_ms: LatencyHistogram {
                buckets: vec![
                    LatencyBucket {
                        le_ms: "1".to_string(),
                        count: 50,
                    },
                    LatencyBucket {
                        le_ms: "120".to_string(),
                        count: 99,
                    },
                    LatencyBucket {
                        le_ms: "+Inf".to_string(),
                        count: 100,
                    },
                ],
                sum_ms: 11_880,
                count: 100,
            },
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

    let entries = matrixraft_runtime_pressure_diagnostic_log_entries(&decision);
    assert_eq!(entries.len(), 4);
    let entry = &entries[0];
    assert_eq!(entry.target, "rustraft.runtime_pressure.admission");
    assert_eq!(entry.severity, DiagnosticSeverity::Error);
    assert_eq!(entry.message, "rejected_runtime_pressure");
    assert!(entry
        .fields
        .contains(&("accepted".to_string(), "false".to_string())));
    assert!(entry
        .fields
        .contains(&("memory_pressure".to_string(), "true".to_string())));
    assert_eq!(decision.memory_pressure_details.len(), 2);
    assert!(decision.memory_pressure_details.iter().any(|detail| {
        detail.component == "memory.process_resident"
            && detail.observed_value == 10
            && detail.threshold_value == 8
            && detail.excess == 2
    }));
    assert!(entry.fields.iter().any(|(key, value)| {
        key == "memory_pressure_details"
            && value.contains("memory.process_resident:10/8/excess=2")
            && value.contains("memory.log_cache:9/4/excess=5")
    }));
    assert!(entry
        .fields
        .contains(&("latency_pressure".to_string(), "true".to_string())));
    assert_eq!(decision.latency_pressure_details.len(), 1);
    assert_eq!(
        decision.latency_pressure_details[0].component,
        "latency.append"
    );
    assert_eq!(decision.latency_pressure_details[0].sample_count, 100);
    assert_eq!(decision.latency_pressure_details[0].observed_p95_ms, 120);
    assert_eq!(decision.latency_pressure_details[0].observed_p99_ms, 120);
    assert_eq!(decision.latency_pressure_details[0].threshold_p99_ms, 100);
    assert_eq!(decision.latency_pressure_details[0].excess_ms, 20);
    assert!(entry.fields.iter().any(|(key, value)| {
        key == "latency_pressure_details"
            && value == "latency.append:samples=100/p95=120ms/p99=120ms/threshold=100ms/excess=20"
    }));
    assert!(entry
        .fields
        .contains(&("scale_pressure".to_string(), "false".to_string())));
    assert!(entry.fields.contains(&(
        "rejected_component".to_string(),
        "latency.append".to_string()
    )));
    assert!(entry
        .fields
        .iter()
        .any(|(key, value)| key == "actions" && value.contains("compact_applied_log_cache")));
    assert!(entry.fields.iter().any(|(key, value)| {
        key == "action_provenance"
            && value.contains("memory.process_resident=>release_memory")
            && value.contains("memory.log_cache=>compact_applied_log_cache")
            && value
                .contains("latency.append=>reduce_append_batch_or_raise_replication_parallelism")
    }));
    assert!(entry
        .fields
        .contains(&("bottleneck_count".to_string(), "3".to_string())));
    assert!(entry
        .fields
        .contains(&("top_bottleneck".to_string(), "memory.log_cache".to_string())));
    assert!(entry
        .fields
        .contains(&("top_bottleneck_category".to_string(), "memory".to_string())));
    assert!(entry.fields.contains(&(
        "top_bottleneck_score_percent".to_string(),
        "125".to_string()
    )));
    assert!(entry.fields.contains(&(
        "top_bottleneck_excess_or_deficit".to_string(),
        "5".to_string()
    )));
    assert!(entry.fields.contains(&(
        "top_bottleneck_threshold_or_target".to_string(),
        "4".to_string()
    )));
    assert!(entry.fields.iter().any(|(key, value)| {
        key == "bottlenecks"
            && value.starts_with("1:memory.log_cache:125pct/5/4")
            && value.contains("2:memory.process_resident:25pct/2/8")
            && value.contains("3:latency.append:20pct/20/100")
    }));
    let bottlenecks = matrixraft_runtime_pressure_bottleneck_summary(&decision);
    assert_eq!(bottlenecks.len(), 3);
    assert_eq!(bottlenecks[0].rank, 1);
    assert_eq!(bottlenecks[0].category, "memory");
    assert_eq!(bottlenecks[0].component, "memory.log_cache");
    assert_eq!(bottlenecks[0].observed_value, 9);
    assert_eq!(bottlenecks[0].threshold_or_target_value, 4);
    assert_eq!(bottlenecks[0].excess_or_deficit, 5);
    assert_eq!(bottlenecks[0].score_percent, 125);
    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.runtime_pressure.memory"
            && entry.severity == DiagnosticSeverity::Error
            && entry.message == "memory pressure component"
            && entry.fields.contains(&(
                "component".to_string(),
                "memory.process_resident".to_string(),
            ))
            && entry
                .fields
                .contains(&("observed_value".to_string(), "10".to_string()))
            && entry
                .fields
                .contains(&("threshold_value".to_string(), "8".to_string()))
            && entry
                .fields
                .contains(&("excess".to_string(), "2".to_string()))
            && entry.fields.contains(&(
                "recommended_actions".to_string(),
                "release_memory".to_string(),
            ))
    }));
    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.runtime_pressure.latency"
            && entry
                .fields
                .contains(&("component".to_string(), "latency.append".to_string()))
            && entry
                .fields
                .contains(&("sample_count".to_string(), "100".to_string()))
            && entry
                .fields
                .contains(&("observed_p95_ms".to_string(), "120".to_string()))
            && entry
                .fields
                .contains(&("observed_p99_ms".to_string(), "120".to_string()))
            && entry
                .fields
                .contains(&("threshold_p99_ms".to_string(), "100".to_string()))
            && entry
                .fields
                .contains(&("excess_ms".to_string(), "20".to_string()))
            && entry.fields.contains(&(
                "recommended_actions".to_string(),
                "reduce_append_batch_or_raise_replication_parallelism".to_string(),
            ))
    }));

    let json_lines = matrixraft_runtime_pressure_diagnostic_json_lines(&decision);
    assert_eq!(json_lines.lines().count(), 4);
    let parsed: Value = serde_json::from_str(json_lines.lines().next().expect("summary line"))
        .expect("runtime pressure diagnostic json line");
    assert_eq!(parsed["target"], "rustraft.runtime_pressure.admission");
    assert_eq!(parsed["severity"], "error");
    assert_eq!(parsed["message"], "rejected_runtime_pressure");
    assert!(parsed["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .any(|field| field[0] == "rejected_component" && field[1] == "latency.append"));
    assert!(parsed["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .any(|field| field[0] == "action_provenance"
            && field[1]
                .as_str()
                .expect("action provenance")
                .contains("memory.log_cache=>compact_applied_log_cache")));
    assert!(parsed["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .any(|field| field[0] == "top_bottleneck_score_percent" && field[1] == "125"));
}

#[test]
fn runtime_pressure_freshness_report_classifies_release_evidence_age() {
    let fresh = matrixraft_runtime_pressure_freshness_report(1_000, 1_200, 1_000, 300);
    assert!(fresh.fresh);
    assert!(fresh.low_fresh);
    assert_eq!(fresh.freshness_status, "fresh");
    assert_eq!(fresh.age_ms, 200);
    assert_eq!(fresh.remaining_fresh_ms, 800);
    assert_eq!(fresh.stale_after_unix_ms, 2_000);
    assert!(fresh.issues.is_empty());

    let low_fresh = matrixraft_runtime_pressure_freshness_report(1_000, 1_800, 1_000, 300);
    assert!(low_fresh.fresh);
    assert!(!low_fresh.low_fresh);
    assert_eq!(low_fresh.freshness_status, "low_fresh");
    assert_eq!(low_fresh.remaining_fresh_ms, 200);

    let stale = matrixraft_runtime_pressure_freshness_report(1_000, 2_050, 1_000, 300);
    assert!(!stale.fresh);
    assert!(!stale.low_fresh);
    assert_eq!(stale.freshness_status, "stale");
    assert!(stale
        .issues
        .contains(&"runtime_pressure_generated_at_stale".to_string()));

    let invalid = matrixraft_runtime_pressure_freshness_report(0, 1_000, 0, 300);
    assert!(!invalid.fresh);
    assert_eq!(invalid.freshness_status, "invalid");
    assert!(invalid
        .issues
        .contains(&"runtime_pressure_generated_at_missing".to_string()));
    assert!(invalid
        .issues
        .contains(&"runtime_pressure_max_age_zero".to_string()));
    assert!(invalid
        .issues
        .contains(&"runtime_pressure_low_fresh_exceeds_max_age".to_string()));

    let fresh_entries = matrixraft_runtime_pressure_freshness_diagnostic_log_entries(&fresh);
    assert_eq!(fresh_entries.len(), 1);
    assert_eq!(
        fresh_entries[0].target,
        "rustraft.runtime_pressure.freshness"
    );
    assert_eq!(fresh_entries[0].severity, DiagnosticSeverity::Info);
    assert_eq!(fresh_entries[0].message, "runtime_pressure_freshness_fresh");
    assert!(fresh_entries[0]
        .fields
        .contains(&("freshness_status".to_string(), "fresh".to_string())));
    assert!(fresh_entries[0]
        .fields
        .contains(&("age_ms".to_string(), "200".to_string())));
    assert!(fresh_entries[0]
        .fields
        .contains(&("issues".to_string(), "none".to_string())));

    let low_fresh_entries =
        matrixraft_runtime_pressure_freshness_diagnostic_log_entries(&low_fresh);
    assert_eq!(low_fresh_entries[0].severity, DiagnosticSeverity::Warn);
    assert_eq!(
        low_fresh_entries[0].message,
        "runtime_pressure_freshness_low"
    );
    assert!(low_fresh_entries[0]
        .fields
        .contains(&("remaining_fresh_ms".to_string(), "200".to_string())));

    let stale_entries = matrixraft_runtime_pressure_freshness_diagnostic_log_entries(&stale);
    assert_eq!(stale_entries.len(), 2);
    assert_eq!(stale_entries[0].severity, DiagnosticSeverity::Error);
    assert_eq!(stale_entries[0].message, "runtime_pressure_freshness_stale");
    assert!(stale_entries[0].fields.contains(&(
        "issues".to_string(),
        "runtime_pressure_generated_at_stale".to_string()
    )));
    assert!(stale_entries.iter().any(|entry| {
        entry.target == "rustraft.runtime_pressure.freshness.issue"
            && entry.severity == DiagnosticSeverity::Error
            && entry.message == "runtime_pressure_generated_at_stale"
    }));

    let invalid_json_lines = matrixraft_runtime_pressure_freshness_diagnostic_json_lines(&invalid);
    assert_eq!(invalid_json_lines.lines().count(), invalid.issues.len() + 1);
    let parsed: Value = serde_json::from_str(
        invalid_json_lines
            .lines()
            .next()
            .expect("freshness summary line"),
    )
    .expect("runtime pressure freshness diagnostic json line");
    assert_eq!(parsed["target"], "rustraft.runtime_pressure.freshness");
    assert_eq!(parsed["severity"], "error");
    assert_eq!(parsed["message"], "runtime_pressure_freshness_invalid");
    assert!(parsed["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .any(|field| field[0] == "freshness_status" && field[1] == "invalid"));
    assert!(parsed["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .any(|field| field[0] == "issue_count" && field[1] == "3"));
}

#[test]
fn runtime_pressure_admission_evidence_validator_rejects_malformed_latency_detail() {
    let mut admission = RuntimePressureAdmission {
        accepted: true,
        memory_pressure: false,
        memory_pressure_details: Vec::new(),
        latency_pressure: true,
        latency_pressure_details: vec![LatencyPressureDetail {
            component: "latency.append".to_string(),
            sample_count: 0,
            observed_p95_ms: 160,
            observed_p99_ms: 150,
            threshold_p99_ms: 100,
            excess_ms: 5,
        }],
        scale_pressure: false,
        scale_pressure_details: Vec::new(),
        pipeline_pressure: false,
        pipeline_pressure_details: Vec::new(),
        read_backlog_pressure: false,
        read_backlog_pressure_details: Vec::new(),
        queue_pressure: false,
        queue_pressure_details: Vec::new(),
        node_runtime_timer_pressure: false,
        node_runtime_timer_pressure_details: Vec::new(),
        reason: "accepted_observe_only_pressure".to_string(),
        rejected_component: None,
        actions: vec!["reduce_append_batch_size".to_string()],
    };

    let issues = matrixraft_validate_runtime_pressure_admission_evidence(&admission).unwrap_err();
    assert!(issues.contains(
        &"runtime_pressure:latency_pressure_detail_sample_count_zero:latency.append".to_string()
    ));
    assert!(issues.iter().any(|issue| issue.starts_with(
        "runtime_pressure:latency_pressure_detail_quantile_order_invalid:latency.append"
    )));
    assert!(issues.iter().any(|issue| issue
        .starts_with("runtime_pressure:latency_pressure_detail_excess_mismatch:latency.append")));

    admission.latency_pressure = false;
    admission.latency_pressure_details[0].sample_count = 100;
    admission.latency_pressure_details[0].observed_p95_ms = 120;
    admission.latency_pressure_details[0].observed_p99_ms = 150;
    admission.latency_pressure_details[0].excess_ms = 50;
    let issues = matrixraft_validate_runtime_pressure_admission_evidence(&admission).unwrap_err();
    assert!(
        issues.contains(&"runtime_pressure:latency_pressure_details_without_signal".to_string())
    );
}

#[test]
fn runtime_pressure_admission_evidence_validator_rejects_malformed_non_latency_details() {
    let admission = RuntimePressureAdmission {
        accepted: true,
        memory_pressure: true,
        memory_pressure_details: vec![MemoryPressureDetail {
            component: "memory.log_cache".to_string(),
            observed_value: 8,
            threshold_value: 10,
            excess: 4,
        }],
        latency_pressure: false,
        latency_pressure_details: Vec::new(),
        scale_pressure: true,
        scale_pressure_details: vec![ScalePressureDetail {
            component: "scale.read_index_qps".to_string(),
            observed_value: 1_200,
            target_value: 1_000,
            deficit: 10,
            target_percent: 20,
        }],
        pipeline_pressure: true,
        pipeline_pressure_details: vec![PipelinePressureDetail {
            peer_id: 0,
            component: "pipeline.append_queue".to_string(),
            observed_value: 3,
            threshold_value: 4,
            excess: 7,
        }],
        read_backlog_pressure: false,
        read_backlog_pressure_details: vec![ReadBacklogPressureDetail {
            component: "read_backlog.pending_read_index".to_string(),
            observed_value: 3,
            threshold_value: 4,
            excess: 9,
        }],
        queue_pressure: false,
        queue_pressure_details: Vec::new(),
        node_runtime_timer_pressure: false,
        node_runtime_timer_pressure_details: Vec::new(),
        reason: "accepted_observe_only_pressure".to_string(),
        rejected_component: None,
        actions: Vec::new(),
    };

    let issues = matrixraft_validate_runtime_pressure_admission_evidence(&admission).unwrap_err();
    assert!(issues.iter().any(|issue| issue.starts_with(
        "runtime_pressure:memory_pressure_detail_not_over_threshold:memory.log_cache"
    )));
    assert!(issues.iter().any(|issue| issue
        .starts_with("runtime_pressure:memory_pressure_detail_excess_mismatch:memory.log_cache")));
    assert!(issues.iter().any(|issue| issue.starts_with(
        "runtime_pressure:scale_pressure_detail_not_below_target:scale.read_index_qps"
    )));
    assert!(issues.iter().any(|issue| issue.starts_with(
        "runtime_pressure:scale_pressure_detail_target_percent_mismatch:scale.read_index_qps"
    )));
    assert!(issues.contains(
        &"runtime_pressure:pipeline_pressure_detail_peer_id_zero:pipeline.append_queue".to_string()
    ));
    assert!(issues.iter().any(|issue| issue.starts_with(
        "runtime_pressure:pipeline_pressure_detail_not_over_threshold:pipeline.append_queue"
    )));
    assert!(issues
        .contains(&"runtime_pressure:read_backlog_pressure_details_without_signal".to_string()));
    assert!(issues.contains(&"runtime_pressure:actions_missing_for_pressure".to_string()));
    assert!(issues.iter().any(|issue| issue.starts_with(
        "runtime_pressure:read_backlog_pressure_detail_excess_mismatch:read_backlog.pending_read_index"
    )));
}

#[test]
fn runtime_pressure_admission_evidence_validator_rejects_inconsistent_decision_shape() {
    let clean = matrixraft_runtime_pressure_admission(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );
    matrixraft_validate_runtime_pressure_admission_evidence(&clean).expect("clean admission");

    let mut rejected_without_pressure = clean.clone();
    rejected_without_pressure.accepted = false;
    rejected_without_pressure.reason = "rejected_runtime_pressure".to_string();
    rejected_without_pressure.rejected_component = Some("memory.log_cache".to_string());
    rejected_without_pressure
        .actions
        .push("release_memory".to_string());
    let issues =
        matrixraft_validate_runtime_pressure_admission_evidence(&rejected_without_pressure)
            .unwrap_err();
    for expected in [
        "runtime_pressure:rejected_without_pressure",
        "runtime_pressure:reason_mismatch:rejected_runtime_pressure:accepted_no_pressure",
        "runtime_pressure:rejected_component_without_pressure",
        "runtime_pressure:actions_without_pressure",
    ] {
        assert!(issues.contains(&expected.to_string()), "{issues:#?}");
    }

    let mut pressure_memory = MemoryMetrics::zero();
    pressure_memory.log_cache_bytes =
        MemoryOptimizationThresholds::default().log_cache_warning_bytes;
    let pressure = matrixraft_runtime_pressure_admission(
        &pressure_memory,
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );
    matrixraft_validate_runtime_pressure_admission_evidence(&pressure)
        .expect("generated pressure admission");

    let mut accepted_with_rejection_shape = pressure.clone();
    accepted_with_rejection_shape.accepted = true;
    accepted_with_rejection_shape.reason = "accepted_no_pressure".to_string();
    accepted_with_rejection_shape.rejected_component = Some("memory.log_cache".to_string());
    accepted_with_rejection_shape.actions.clear();
    let issues =
        matrixraft_validate_runtime_pressure_admission_evidence(&accepted_with_rejection_shape)
            .unwrap_err();
    for expected in [
        "runtime_pressure:actions_missing_for_pressure",
        "runtime_pressure:reason_mismatch:accepted_no_pressure:accepted_observe_only_pressure",
        "runtime_pressure:rejected_component_on_accepted_admission",
    ] {
        assert!(issues.contains(&expected.to_string()), "{issues:#?}");
    }

    let mut rejected_unknown_component = pressure;
    rejected_unknown_component.rejected_component = Some("latency.append".to_string());
    let issues =
        matrixraft_validate_runtime_pressure_admission_evidence(&rejected_unknown_component)
            .unwrap_err();
    assert!(issues.contains(
        &"runtime_pressure:rejected_component_not_in_pressure_details:latency.append".to_string()
    ));
}

#[test]
fn runtime_pressure_admission_policy_validator_rejects_lower_priority_component() {
    let mut admission = matrixraft_runtime_pressure_admission(
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
            append_latency_ms: LatencyHistogram {
                buckets: vec![
                    LatencyBucket {
                        le_ms: "1".to_string(),
                        count: 50,
                    },
                    LatencyBucket {
                        le_ms: "120".to_string(),
                        count: 99,
                    },
                    LatencyBucket {
                        le_ms: "+Inf".to_string(),
                        count: 100,
                    },
                ],
                sum_ms: 11_880,
                count: 100,
            },
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
    assert_eq!(
        admission.rejected_component,
        Some("latency.append".to_string())
    );
    admission.rejected_component = Some("memory.process_resident".to_string());
    matrixraft_validate_runtime_pressure_admission_evidence(&admission)
        .expect("shape-only validator accepts any pressure component");

    let issues = matrixraft_validate_runtime_pressure_admission_evidence_with_policy(
        &admission,
        &RuntimePressureAdmissionPolicy::fail_closed(),
    )
    .unwrap_err();
    assert!(issues.contains(
        &"runtime_pressure:policy_rejected_component_mismatch:memory.process_resident:latency.append"
            .to_string()
    ));

    let observe_only_issues = matrixraft_validate_runtime_pressure_admission_evidence_with_policy(
        &admission,
        &RuntimePressureAdmissionPolicy::observe_only(),
    )
    .unwrap_err();
    assert!(
        observe_only_issues.contains(&"runtime_pressure:policy_rejection_unexpected".to_string())
    );
}

#[test]
fn runtime_pressure_admission_evidence_validator_rejects_malformed_actions_and_component() {
    let admission = RuntimePressureAdmission {
        accepted: false,
        memory_pressure: true,
        memory_pressure_details: vec![MemoryPressureDetail {
            component: "".to_string(),
            observed_value: 12,
            threshold_value: 10,
            excess: 2,
        }],
        latency_pressure: false,
        latency_pressure_details: Vec::new(),
        scale_pressure: false,
        scale_pressure_details: Vec::new(),
        pipeline_pressure: false,
        pipeline_pressure_details: Vec::new(),
        read_backlog_pressure: false,
        read_backlog_pressure_details: Vec::new(),
        queue_pressure: false,
        queue_pressure_details: Vec::new(),
        node_runtime_timer_pressure: false,
        node_runtime_timer_pressure_details: Vec::new(),
        reason: "rejected_runtime_pressure".to_string(),
        rejected_component: Some("".to_string()),
        actions: vec![
            "release_memory".to_string(),
            "release_memory".to_string(),
            " reduce_append_inflight_bytes ".to_string(),
            "".to_string(),
        ],
    };

    let issues = matrixraft_validate_runtime_pressure_admission_evidence(&admission).unwrap_err();

    for expected in [
        "runtime_pressure:action_duplicate:release_memory",
        "runtime_pressure:action_empty",
        "runtime_pressure:action_not_canonical: reduce_append_inflight_bytes ",
        "runtime_pressure:memory_pressure_detail_component_empty",
        "runtime_pressure:rejected_component_empty",
    ] {
        assert!(issues.contains(&expected.to_string()), "{issues:#?}");
    }
}

#[test]
fn runtime_pressure_admission_evidence_validator_rejects_action_provenance_drift() {
    let mut pressure_memory = MemoryMetrics::zero();
    pressure_memory.log_cache_bytes =
        MemoryOptimizationThresholds::default().log_cache_warning_bytes;
    let generated = matrixraft_runtime_pressure_admission(
        &pressure_memory,
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &RuntimePressureAdmissionPolicy::observe_only(),
    );
    matrixraft_validate_runtime_pressure_admission_evidence(&generated)
        .expect("generated log-cache pressure admission should remain canonical");

    let mut fabricated_action = generated.clone();
    fabricated_action
        .actions
        .push("pretend_pressure_is_handled".to_string());
    let issues =
        matrixraft_validate_runtime_pressure_admission_evidence(&fabricated_action).unwrap_err();
    assert!(issues.contains(
        &"runtime_pressure:action_without_pressure_signal:pretend_pressure_is_handled".to_string()
    ));

    let mut missing_action = generated;
    missing_action.actions.clear();
    missing_action.actions.push("release_memory".to_string());
    let issues = matrixraft_validate_runtime_pressure_admission_evidence(&missing_action)
        .expect_err("known log-cache pressure requires its canonical compaction action");
    assert!(issues
        .contains(&"runtime_pressure:action_without_pressure_signal:release_memory".to_string()));
    assert!(issues.contains(
        &"runtime_pressure:action_missing_for_pressure_signal:compact_applied_log_cache"
            .to_string()
    ));
}

#[test]
fn debug_snapshot_validation_rejects_malformed_runtime_pressure_latency_evidence() {
    let mut cluster =
        RaftCluster::new(7, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster
        .propose(b"runtime-pressure".to_vec())
        .expect("write");
    let readiness = ready_snapshot();
    let report = matrixraft_runtime_admin_report(
        cluster.cluster_status_report().expect("cluster status"),
        readiness.clone(),
        matrixraft_capability_evidence(&readiness),
    );
    let status_surface = AdminStatusSurfaceInput {
        commit_index: 10,
        max_observed_node_commit_index: 10,
        quorum_size: 2,
        quorum_peer_ids: vec![1, 2, 3],
        peer_pipeline: vec![
            pipeline_peer(1, 10, 11),
            pipeline_peer(2, 10, 11),
            pipeline_peer(3, 10, 11),
        ],
        wal_last_log_index: 10,
        wal_segment_lifecycle_present: true,
    };
    let mut snapshot = matrixraft_debug_snapshot_with_runtime_pressure_evidence(
        &report,
        &status_surface,
        &LatencyMetrics {
            append_latency_ms: LatencyHistogram {
                buckets: vec![
                    LatencyBucket {
                        le_ms: "1".to_string(),
                        count: 50,
                    },
                    LatencyBucket {
                        le_ms: "120".to_string(),
                        count: 99,
                    },
                    LatencyBucket {
                        le_ms: "+Inf".to_string(),
                        count: 100,
                    },
                ],
                sum_ms: 11_880,
                count: 100,
            },
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
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &ScaleMetrics::zero(),
        &ScaleRateMetrics::zero(),
        &ScaleOptimizationTargets::default(),
        &[],
        &RuntimePressureAdmissionPolicy::fail_closed(),
        &[("service", "raft-a"), ("workload", "release-scale")],
    );
    snapshot
        .runtime_pressure_admission
        .as_mut()
        .expect("runtime pressure admission")
        .latency_pressure_details[0]
        .sample_count = 0;

    let validation = matrixraft_validate_debug_snapshot(&snapshot);
    assert!(!validation.ready);
    assert!(validation.issues.contains(
        &"runtime_pressure:latency_pressure_detail_sample_count_zero:latency.append".to_string()
    ));
}

#[test]
fn runtime_pressure_admission_defaults_scale_fields_for_older_json() {
    let admission: RuntimePressureAdmission = serde_json::from_str(
        r#"{
            "accepted": true,
            "memory_pressure": false,
            "latency_pressure": false,
            "reason": "accepted_no_pressure",
            "rejected_component": null,
            "actions": []
        }"#,
    )
    .expect("old runtime pressure admission json should deserialize");
    assert!(!admission.scale_pressure);
    assert!(!admission.pipeline_pressure);
    assert!(admission.memory_pressure_details.is_empty());
    assert!(admission.latency_pressure_details.is_empty());
    assert!(admission.scale_pressure_details.is_empty());
    assert!(admission.pipeline_pressure_details.is_empty());

    let policy: RuntimePressureAdmissionPolicy = serde_json::from_str(
        r#"{
            "reject_on_memory_pressure": true,
            "reject_on_latency_pressure": true
        }"#,
    )
    .expect("old runtime pressure admission policy json should deserialize");
    assert!(!policy.reject_on_scale_pressure);
    assert!(!policy.reject_on_pipeline_pressure);
}

#[test]
fn runtime_pressure_diagnostics_include_peer_pipeline_pressure_details() {
    let mut peer = PeerProgress::new(4, 12, PipelineLimits::production_default());
    peer.append_queue_limit = 8;
    peer.append_queue_depth = 9;
    peer.reorder_queue_depth = 1;

    let decision = matrixraft_runtime_pressure_admission_with_pipeline_pressure(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &[peer],
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );

    let entries = matrixraft_runtime_pressure_diagnostic_log_entries(&decision);
    assert_eq!(entries.len(), 3);
    let summary = &entries[0];
    assert_eq!(summary.target, "rustraft.runtime_pressure.admission");
    assert_eq!(summary.severity, DiagnosticSeverity::Error);
    assert!(summary
        .fields
        .contains(&("pipeline_pressure".to_string(), "true".to_string())));

    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.runtime_pressure.pipeline"
            && entry.message == "pipeline pressure component"
            && entry
                .fields
                .contains(&("peer_id".to_string(), "4".to_string()))
            && entry
                .fields
                .contains(&("component".to_string(), "pipeline.append_queue".to_string()))
            && entry
                .fields
                .contains(&("observed_value".to_string(), "9".to_string()))
            && entry
                .fields
                .contains(&("threshold_value".to_string(), "8".to_string()))
            && entry
                .fields
                .contains(&("excess".to_string(), "1".to_string()))
    }));

    let json_lines = matrixraft_runtime_pressure_diagnostic_json_lines(&decision);
    assert_eq!(json_lines.lines().count(), 3);
    assert!(json_lines.contains("rustraft.runtime_pressure.pipeline"));
    assert!(json_lines.contains("pipeline.append_queue"));
}

#[test]
fn runtime_pressure_diagnostics_include_scale_target_gap_details() {
    let decision = matrixraft_runtime_pressure_admission_with_scale_targets(
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &ScaleRateMetrics {
            proposal_qps: 750,
            append_entries_qps: 2_500,
            read_index_qps: 500,
            apply_entries_qps: 1_000,
            replication_mib_per_sec: 80,
            apply_mib_per_sec: 64,
        },
        &ScaleOptimizationTargets {
            min_proposal_qps: 1_000,
            min_append_entries_qps: 2_000,
            min_read_index_qps: 800,
            min_apply_entries_qps: 1_000,
            min_replication_mib_per_sec: 100,
            min_apply_mib_per_sec: 128,
        },
        &RuntimePressureAdmissionPolicy::fail_closed(),
    );

    assert!(!decision.accepted);
    assert!(decision.scale_pressure);
    assert_eq!(decision.scale_pressure_details.len(), 4);

    let entries = matrixraft_runtime_pressure_diagnostic_log_entries(&decision);
    assert_eq!(entries.len(), 5);
    let entry = &entries[0];
    assert!(entry
        .fields
        .contains(&("scale_pressure_detail_count".to_string(), "4".to_string())));
    assert!(entry.fields.iter().any(|(key, value)| {
        key == "scale_pressure_details"
            && value.contains("scale.proposal_qps:750/1000/75pct/deficit=250")
            && value.contains("scale.apply_mib_per_sec:64/128/50pct/deficit=64")
    }));
    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.runtime_pressure.scale"
            && entry.severity == DiagnosticSeverity::Error
            && entry.message == "scale pressure component"
            && entry
                .fields
                .contains(&("component".to_string(), "scale.read_index_qps".to_string()))
            && entry
                .fields
                .contains(&("observed_value".to_string(), "500".to_string()))
            && entry
                .fields
                .contains(&("target_value".to_string(), "800".to_string()))
            && entry
                .fields
                .contains(&("target_percent".to_string(), "62".to_string()))
            && entry
                .fields
                .contains(&("deficit".to_string(), "300".to_string()))
    }));

    let json_lines = matrixraft_runtime_pressure_diagnostic_json_lines(&decision);
    assert_eq!(json_lines.lines().count(), 5);
    let parsed: Value = serde_json::from_str(json_lines.lines().next().expect("summary line"))
        .expect("runtime pressure diagnostic json line");
    assert!(parsed["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .any(|field| {
            field[0] == "scale_pressure_details"
                && field[1]
                    .as_str()
                    .expect("scale details")
                    .contains("scale.read_index_qps:500/800/62pct/deficit=300")
        }));
}

#[test]
fn optimization_report_is_ready_for_clean_admin_status_surface() {
    let report = matrixraft_optimization_report(&AdminStatusSurfaceInput {
        commit_index: 10,
        max_observed_node_commit_index: 10,
        quorum_size: 2,
        quorum_peer_ids: vec![1, 2, 3],
        peer_pipeline: vec![pipeline_peer(1, 10, 11), pipeline_peer(2, 10, 11)],
        wal_last_log_index: 10,
        wal_segment_lifecycle_present: true,
    });

    assert!(report.ready);
    assert_eq!(report.hint_count, 0);
    assert!(report.hints.is_empty());
}

#[test]
fn cluster_status_report_is_derived_from_runtime_cluster() {
    let mut cluster =
        RaftCluster::new(5, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"x".to_vec()).expect("write");

    let report = cluster.cluster_status_report().expect("cluster status");
    assert_eq!(report.group_id, 5);
    assert_eq!(report.health, HealthStatus::Healthy);
    assert!(report.ready);
    assert_eq!(report.nodes.len(), 3);
}

#[test]
fn benchmark_readiness_artifact_validator_accepts_matching_read_backlog_evidence() {
    let now = now_unix_ms();
    let mut benchmark_report = ready_benchmark_report();
    benchmark_report.generated_at_unix_ms = now;
    benchmark_report.passed = true;
    benchmark_report.comparisons[0].passed = true;
    benchmark_report.comparisons[0].blockers.clear();
    benchmark_report.comparisons[0].p50_ratio = 1.0;
    benchmark_report.comparisons[0].p99_ratio = 1.0;
    benchmark_report.comparisons[0].throughput_ratio = 1.0;
    benchmark_report.comparisons[0].cpu_ratio = 1.0;
    benchmark_report.comparisons[0].peak_resident_memory_ratio = 1.0;
    benchmark_report.comparisons[0].rustraft.p50_latency_micros = 100;
    benchmark_report.comparisons[0].rustraft.p99_latency_micros = 200;
    benchmark_report.comparisons[0]
        .rustraft
        .throughput_ops_per_sec = 1_000.0;
    benchmark_report.comparisons[0]
        .rustraft
        .cpu_utilization_percent = 50.0;
    benchmark_report.comparisons[0]
        .rustraft
        .peak_resident_memory_bytes = 512 * 1024 * 1024;

    let mut benchmark_summary =
        matrixraft_baseline_raft_benchmark_failure_summary(&benchmark_report);
    benchmark_summary.generated_at_unix_ms = now;
    let input = ProductionReadinessInput {
        readiness: ready_snapshot(),
        peer_pipeline: None,
        runtime_pressure_admission: None,
        snapshot_lifecycle: None,
        wal_lifecycle: None,
        admin_status_surface: None,
        fault_harness: None,
        data_node_rollout: None,
        metaserver_rollout: None,
        membership_transitions: Vec::new(),
        baseline_raft_benchmark: None,
    };
    let peer = pipeline_peer(2, 10, 11);
    let read_backlog_metrics = ReadBacklogMetrics {
        pending_read_index_requests: 2_048,
        pending_bounded_stale_reads: 64,
    };
    let read_backlog_thresholds = ReadBacklogThresholds {
        pending_read_index_warning: 1_024,
        pending_bounded_stale_read_warning: 32,
    };
    let labels = [("service", "raft-a"), ("workload", "read-scale")];

    let readiness_input =
        matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts(
            input.clone(),
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
        )
        .expect("read backlog readiness input");
    let admission = readiness_input
        .runtime_pressure_admission
        .as_ref()
        .expect("readiness input should include runtime pressure admission");
    assert!(admission.read_backlog_pressure);
    assert_eq!(
        admission.rejected_component.as_deref(),
        Some("read_backlog.pending_read_index")
    );
    assert!(admission
        .read_backlog_pressure_details
        .iter()
        .any(
            |detail| detail.component == "read_backlog.pending_read_index"
                && detail.observed_value == 2_048
                && detail.threshold_value == 1_024
        ));

    let artifact = matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog(
        &input,
        &benchmark_report,
        &benchmark_summary,
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &[peer.clone()],
        &read_backlog_metrics,
        &read_backlog_thresholds,
        &RuntimePressureAdmissionPolicy::fail_closed(),
        &labels,
    )
    .expect("read backlog artifact");
    let artifact_freshness = matrixraft_runtime_pressure_freshness_report(
        artifact.generated_at_unix_ms,
        artifact.generated_at_unix_ms,
        24 * 60 * 60 * 1_000,
        24 * 60 * 60 * 100,
    );
    let expected_artifact_report =
        matrixraft_production_readiness_report_with_runtime_pressure_policy_and_freshness(
            &readiness_input,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &artifact_freshness,
        );
    assert_eq!(artifact.report, expected_artifact_report);
    assert_eq!(
        artifact.diagnostic_json_lines,
        matrixraft::matrixraft_production_readiness_diagnostic_json_lines(
            &expected_artifact_report
        )
    );
    assert!(artifact
        .report
        .satisfied
        .contains(&"runtime_pressure:freshness_evidence_fresh".to_string()));
    assert!(artifact
        .runtime_pressure_freshness_prometheus
        .text
        .contains("rustraft_runtime_pressure_freshness_fresh"));
    assert!(artifact
        .runtime_pressure_freshness_prometheus
        .text
        .contains("freshness_status=\"fresh\""));
    let expected_policy_report =
        matrixraft_production_readiness_report_with_runtime_pressure_policy(
            &readiness_input,
            &RuntimePressureAdmissionPolicy::fail_closed(),
        );
    let one_call_report =
        matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts(
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
        )
        .expect("one-call read backlog report");
    assert_eq!(one_call_report, expected_policy_report);
    let old_validator_error = matrixraft_validate_benchmark_runtime_pressure_readiness_artifact(
        &artifact,
        &input,
        &benchmark_report,
        &benchmark_summary,
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &[peer.clone()],
        &RuntimePressureAdmissionPolicy::fail_closed(),
        &labels,
    )
    .expect_err("zero-backlog compatibility validator must reject full-pressure artifact");
    assert!(old_validator_error.contains("benchmark:runtime_pressure_readiness_report_mismatch"));

    let mut missing_read_backlog_detail_artifact = artifact.clone();
    missing_read_backlog_detail_artifact
        .runtime_pressure_prometheus
        .text = missing_read_backlog_detail_artifact
        .runtime_pressure_prometheus
        .text
        .lines()
        .filter(|line| !line.starts_with("rustraft_runtime_pressure_read_backlog_observed_value{"))
        .collect::<Vec<_>>()
        .join("\n");
    missing_read_backlog_detail_artifact
        .runtime_pressure_prometheus
        .metric_count = missing_read_backlog_detail_artifact
        .runtime_pressure_prometheus
        .text
        .lines()
        .count() as u64;
    let missing_detail_error =
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog(
            &missing_read_backlog_detail_artifact,
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect_err("matching validator rejects artifacts missing pressure detail metrics");
    assert!(missing_detail_error.contains(
        "benchmark:runtime_pressure_readiness_runtime_prometheus_metric_missing:rustraft_runtime_pressure_read_backlog_observed_value"
    ));

    let mut missing_latency_detail_artifact = artifact.clone();
    missing_latency_detail_artifact
        .runtime_pressure_prometheus
        .text = missing_latency_detail_artifact
        .runtime_pressure_prometheus
        .text
        .lines()
        .filter(|line| !line.starts_with("rustraft_runtime_pressure_latency_observed_p99_ms{"))
        .collect::<Vec<_>>()
        .join("\n");
    missing_latency_detail_artifact
        .runtime_pressure_prometheus
        .metric_count = missing_latency_detail_artifact
        .runtime_pressure_prometheus
        .text
        .lines()
        .count() as u64;
    let missing_latency_detail_error =
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog(
            &missing_latency_detail_artifact,
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect_err("matching validator rejects artifacts missing latency detail metrics");
    assert!(missing_latency_detail_error.contains(
        "benchmark:runtime_pressure_readiness_runtime_prometheus_metric_missing:rustraft_runtime_pressure_latency_observed_p99_ms"
    ));

    let mut missing_freshness_metric_artifact = artifact.clone();
    missing_freshness_metric_artifact
        .runtime_pressure_freshness_prometheus
        .text = missing_freshness_metric_artifact
        .runtime_pressure_freshness_prometheus
        .text
        .lines()
        .filter(|line| !line.starts_with("rustraft_runtime_pressure_freshness_fresh{"))
        .collect::<Vec<_>>()
        .join("\n");
    missing_freshness_metric_artifact
        .runtime_pressure_freshness_prometheus
        .metric_count = missing_freshness_metric_artifact
        .runtime_pressure_freshness_prometheus
        .text
        .lines()
        .count() as u64;
    let missing_freshness_metric_error =
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog(
            &missing_freshness_metric_artifact,
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect_err("matching validator rejects artifacts missing freshness metrics");
    assert!(missing_freshness_metric_error.contains(
        "benchmark:runtime_pressure_readiness_freshness_prometheus_metric_missing:rustraft_runtime_pressure_freshness_fresh"
    ));

    matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog(
        &artifact,
        &input,
        &benchmark_report,
        &benchmark_summary,
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &[peer],
        &read_backlog_metrics,
        &read_backlog_thresholds,
        &RuntimePressureAdmissionPolicy::fail_closed(),
        &labels,
    )
    .expect("matching read backlog validator accepts full-pressure artifact");
}

#[test]
fn benchmark_readiness_artifact_validator_preserves_timer_aware_release_baseline() {
    let now = now_unix_ms();
    let mut benchmark_report = ready_benchmark_report();
    benchmark_report.generated_at_unix_ms = now;
    benchmark_report.passed = true;
    benchmark_report.comparisons[0].passed = true;
    benchmark_report.comparisons[0].blockers.clear();
    benchmark_report.comparisons[0].p50_ratio = 1.0;
    benchmark_report.comparisons[0].p99_ratio = 1.0;
    benchmark_report.comparisons[0].throughput_ratio = 1.0;
    benchmark_report.comparisons[0].cpu_ratio = 1.0;
    benchmark_report.comparisons[0].peak_resident_memory_ratio = 1.0;
    benchmark_report.comparisons[0].rustraft.p50_latency_micros = 100;
    benchmark_report.comparisons[0].rustraft.p99_latency_micros = 200;
    benchmark_report.comparisons[0]
        .rustraft
        .throughput_ops_per_sec = 1_000.0;
    benchmark_report.comparisons[0]
        .rustraft
        .cpu_utilization_percent = 50.0;
    benchmark_report.comparisons[0]
        .rustraft
        .peak_resident_memory_bytes = 512 * 1024 * 1024;

    let mut benchmark_summary =
        matrixraft_baseline_raft_benchmark_failure_summary(&benchmark_report);
    benchmark_summary.generated_at_unix_ms = now;
    let input = ProductionReadinessInput {
        readiness: ready_snapshot(),
        peer_pipeline: None,
        runtime_pressure_admission: None,
        snapshot_lifecycle: None,
        wal_lifecycle: None,
        admin_status_surface: None,
        fault_harness: None,
        data_node_rollout: None,
        metaserver_rollout: None,
        membership_transitions: Vec::new(),
        baseline_raft_benchmark: None,
    };
    let peer = pipeline_peer(2, 10, 11);
    let read_backlog_metrics = ReadBacklogMetrics::zero();
    let read_backlog_thresholds = ReadBacklogThresholds::default();
    let timer_status = matrixraft_release_benchmark_runtime_timer_status();
    let timer_thresholds = NodeRuntimeTimerThresholds::default();
    let labels = [("service", "raft-a"), ("workload", "release-scale")];

    assert_eq!(timer_status.pending_ticks, 0);
    assert_eq!(timer_status.rejected_ticks, 0);
    assert_eq!(timer_status.completed_ticks, 1);
    assert_eq!(timer_status.last_tick_admission_reason, "tick_admitted");

    let baseline_artifact =
        matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect("timer-aware artifact");

    assert!(baseline_artifact
        .runtime_pressure_prometheus
        .text
        .contains("rustraft_runtime_pressure_node_runtime_timer"));
    assert!(!baseline_artifact
        .report
        .production_blockers
        .contains(&"runtime_pressure:no_node_runtime_timer_pressure".to_string()));
    let baseline_readiness_input =
        matrixraft_production_readiness_input_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts(
            input.clone(),
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
        )
        .expect("timer-aware readiness input");
    let expected_timer_artifact_report =
        matrixraft_production_readiness_report_with_runtime_pressure_policy_and_freshness(
            &baseline_readiness_input,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &matrixraft_runtime_pressure_freshness_report(
                baseline_artifact.generated_at_unix_ms,
                baseline_artifact.generated_at_unix_ms,
                24 * 60 * 60 * 1_000,
                24 * 60 * 60 * 100,
            ),
        );
    assert_eq!(baseline_artifact.report, expected_timer_artifact_report);
    assert!(baseline_artifact
        .report
        .satisfied
        .contains(&"runtime_pressure:freshness_evidence_fresh".to_string()));
    assert!(baseline_artifact
        .runtime_pressure_freshness_prometheus
        .text
        .contains("rustraft_runtime_pressure_freshness_status"));
    assert!(baseline_artifact
        .runtime_pressure_freshness_prometheus
        .text
        .contains("freshness_status=\"fresh\""));
    let expected_timer_policy_report =
        matrixraft_production_readiness_report_with_runtime_pressure_policy(
            &baseline_readiness_input,
            &RuntimePressureAdmissionPolicy::fail_closed(),
        );
    let one_call_timer_report =
        matrixraft_production_readiness_report_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts(
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
        )
        .expect("one-call timer-aware report");
    assert_eq!(one_call_timer_report, expected_timer_policy_report);

    let mut missing_scale_metric_artifact = baseline_artifact.clone();
    missing_scale_metric_artifact
        .runtime_pressure_prometheus
        .text = missing_scale_metric_artifact
        .runtime_pressure_prometheus
        .text
        .lines()
        .filter(|line| !line.starts_with("rustraft_runtime_pressure_scale{"))
        .collect::<Vec<_>>()
        .join("\n");
    missing_scale_metric_artifact
        .runtime_pressure_prometheus
        .metric_count = missing_scale_metric_artifact
        .runtime_pressure_prometheus
        .text
        .lines()
        .count() as u64;
    let missing_scale_metric_error =
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog(
            &missing_scale_metric_artifact,
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect_err("validator rejects artifacts missing release-scale pressure metrics");
    assert!(missing_scale_metric_error.contains(
        "benchmark:runtime_pressure_readiness_runtime_prometheus_metric_missing:rustraft_runtime_pressure_scale"
    ));

    let mut missing_queue_metric_artifact = baseline_artifact.clone();
    missing_queue_metric_artifact
        .runtime_pressure_prometheus
        .text = missing_queue_metric_artifact
        .runtime_pressure_prometheus
        .text
        .lines()
        .filter(|line| !line.starts_with("rustraft_runtime_pressure_queue{"))
        .collect::<Vec<_>>()
        .join("\n");
    missing_queue_metric_artifact
        .runtime_pressure_prometheus
        .metric_count = missing_queue_metric_artifact
        .runtime_pressure_prometheus
        .text
        .lines()
        .count() as u64;
    let missing_queue_metric_error =
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &missing_queue_metric_artifact,
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect_err("timer-aware validator rejects artifacts missing queue pressure metrics");
    assert!(missing_queue_metric_error.contains(
        "benchmark:runtime_pressure_readiness_runtime_prometheus_metric_missing:rustraft_runtime_pressure_queue"
    ));

    let mut missing_timer_metric_artifact = baseline_artifact.clone();
    missing_timer_metric_artifact
        .runtime_pressure_prometheus
        .text = missing_timer_metric_artifact
        .runtime_pressure_prometheus
        .text
        .replace(
            "rustraft_runtime_pressure_node_runtime_timer",
            "rustraft_runtime_pressure_node_timer_removed",
        );
    let missing_timer_metric_error =
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &missing_timer_metric_artifact,
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect_err("timer-aware validator rejects artifacts missing node timer metrics");
    assert!(missing_timer_metric_error.contains(
        "benchmark:runtime_pressure_readiness_runtime_prometheus_metric_missing:rustraft_runtime_pressure_node_runtime_timer"
    ));

    let mut shadow_admission_metric_artifact = baseline_artifact.clone();
    shadow_admission_metric_artifact
        .runtime_pressure_prometheus
        .text = shadow_admission_metric_artifact
        .runtime_pressure_prometheus
        .text
        .replace(
            "rustraft_runtime_pressure_admission_accepted",
            "rustraft_runtime_pressure_admission_accepted_shadow",
        );
    let shadow_admission_metric_error =
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &shadow_admission_metric_artifact,
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect_err("timer-aware validator rejects shadow admission metrics");
    assert!(shadow_admission_metric_error.contains(
        "benchmark:runtime_pressure_readiness_runtime_prometheus_metric_contract_missing"
    ));

    let mut malformed_sample_artifact = baseline_artifact.clone();
    malformed_sample_artifact.runtime_pressure_prometheus.text = malformed_sample_artifact
        .runtime_pressure_prometheus
        .text
        .lines()
        .map(|line| {
            if line.starts_with("rustraft_runtime_pressure_admission_accepted") {
                "rustraft_runtime_pressure_admission_accepted"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        malformed_sample_artifact
            .runtime_pressure_prometheus
            .metric_count,
        malformed_sample_artifact
            .runtime_pressure_prometheus
            .text
            .lines()
            .count() as u64
    );
    let malformed_sample_error =
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &malformed_sample_artifact,
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect_err("timer-aware validator rejects malformed Prometheus samples");
    assert!(malformed_sample_error
        .contains("benchmark:runtime_pressure_readiness_runtime_prometheus_malformed_sample"));

    let mut malformed_label_artifact = baseline_artifact.clone();
    malformed_label_artifact.runtime_pressure_prometheus.text = malformed_label_artifact
        .runtime_pressure_prometheus
        .text
        .lines()
        .map(|line| {
            if line.starts_with("rustraft_runtime_pressure_admission_accepted{") {
                line.replace("service=\"raft-a\"", "service=\"raft-a")
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        malformed_label_artifact
            .runtime_pressure_prometheus
            .metric_count,
        malformed_label_artifact
            .runtime_pressure_prometheus
            .text
            .lines()
            .count() as u64
    );
    let malformed_label_error =
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &malformed_label_artifact,
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect_err("timer-aware validator rejects malformed Prometheus labels");
    assert!(malformed_label_error
        .contains("benchmark:runtime_pressure_readiness_runtime_prometheus_malformed_sample"));

    matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog(
        &baseline_artifact,
        &input,
        &benchmark_report,
        &benchmark_summary,
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &[peer.clone()],
        &read_backlog_metrics,
        &read_backlog_thresholds,
        &RuntimePressureAdmissionPolicy::fail_closed(),
        &labels,
    )
    .expect("no-pressure timer baseline remains compatible with the read-backlog validator");

    let mut saturated_timer_status = timer_status.clone();
    saturated_timer_status.pending_ticks = 900;
    saturated_timer_status.max_pending_ticks = 1_000;

    let saturated_artifact =
        matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &saturated_timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect("saturated timer-aware artifact");
    assert!(saturated_artifact
        .report
        .production_blockers
        .contains(&"runtime_pressure:no_node_runtime_timer_pressure".to_string()));

    let read_backlog_validator_error =
        matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog(
            &saturated_artifact,
            &input,
            &benchmark_report,
            &benchmark_summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[peer.clone()],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &labels,
        )
        .expect_err("read-backlog validator must reject saturated timer-aware artifact");
    assert!(read_backlog_validator_error
        .contains("benchmark:runtime_pressure_readiness_runtime_prometheus_mismatch"));

    matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
        &saturated_artifact,
        &input,
        &benchmark_report,
        &benchmark_summary,
        &MemoryMetrics::zero(),
        &MemoryOptimizationThresholds::default(),
        &LatencyMetrics::zero(),
        &LatencyOptimizationThresholds::default(),
        &[peer],
        &read_backlog_metrics,
        &read_backlog_thresholds,
        &saturated_timer_status,
        &timer_thresholds,
        &RuntimePressureAdmissionPolicy::fail_closed(),
        &labels,
    )
    .expect("matching timer-aware validator accepts saturated timer artifact");
}

fn ready_benchmark_summary() -> BenchmarkFailureSummary {
    BenchmarkFailureSummary {
        schema: matrixraft::benchmark::MATRIXRAFT_BENCHMARK_SUMMARY_SCHEMA.to_string(),
        generated_at_unix_ms: now_unix_ms(),
        benchmark_run_id: "debug-snapshot-benchmark".to_string(),
        environment_fingerprint:
            "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false"
                .to_string(),
        passed: true,
        production_evidence_ready: true,
        options: BenchmarkOptions::default(),
        required_workloads: vec!["single_key_writes".to_string()],
        workload_count: 1,
        failed_workload_count: 0,
        workloads: vec![BenchmarkWorkloadSummary {
            workload: BenchmarkWorkload::SingleKeyWrites,
            passed: true,
            baseline_raft_correctness_passed: true,
            matrixraft_correctness_passed: true,
            baseline_raft_engine_source: BenchmarkEngineSource::RealBaselineRaft,
            matrixraft_engine_source: BenchmarkEngineSource::RustRaftRuntime,
            baseline_raft_benchmark_run_id: "debug-snapshot-benchmark".to_string(),
            matrixraft_benchmark_run_id: "debug-snapshot-benchmark".to_string(),
            baseline_raft_implementation: BenchmarkImplementation::BaselineRaft,
            matrixraft_implementation: BenchmarkImplementation::RustRaftRust,
            baseline_raft_binary_path: Some("/opt/baseline-raft/bin/kvbench".to_string()),
            matrixraft_binary_path: Some("/opt/rustraft/bin/rustraft-kvbench".to_string()),
            baseline_raft_git_revision: Some(
                "1111111111111111111111111111111111111111".to_string(),
            ),
            matrixraft_git_revision: Some("2222222222222222222222222222222222222222".to_string()),
            baseline_raft_build_profile: "release".to_string(),
            matrixraft_build_profile: "release".to_string(),
            baseline_raft_harness_kind: BenchmarkHarnessKind::FullBaselineRaftHarness,
            matrixraft_harness_kind: BenchmarkHarnessKind::RustRaftRuntime,
            node_count: 5,
            baseline_raft_node_count: 5,
            matrixraft_node_count: 5,
            baseline_raft_iterations_per_workload: 128,
            matrixraft_iterations_per_workload: 128,
            baseline_raft_batch_size: 16,
            matrixraft_batch_size: 16,
            baseline_raft_payload_size_bytes: 4096,
            matrixraft_payload_size_bytes: 4096,
            baseline_raft_timed_iteration_count: 128,
            matrixraft_timed_iteration_count: 128,
            baseline_raft_operations_per_timed_iteration: 1,
            matrixraft_operations_per_timed_iteration: 1,
            baseline_raft_total_duration_micros: 128_000,
            matrixraft_total_duration_micros: 140_000,
            baseline_raft_operation_count: 128,
            matrixraft_operation_count: 128,
            baseline_raft_p50_latency_micros: 100,
            matrixraft_p50_latency_micros: 110,
            baseline_raft_p99_latency_micros: 200,
            matrixraft_p99_latency_micros: 220,
            baseline_raft_throughput_ops_per_sec: 1_000.0,
            matrixraft_throughput_ops_per_sec: 900.0,
            baseline_raft_cpu_utilization_percent: 50.0,
            matrixraft_cpu_utilization_percent: 52.0,
            baseline_raft_peak_resident_memory_bytes: 512 * 1024 * 1024,
            matrixraft_peak_resident_memory_bytes: 544 * 1024 * 1024,
            p50_ratio: 1.1,
            p99_ratio: 1.1,
            throughput_ratio: 0.9,
            cpu_ratio: 1.04,
            peak_resident_memory_ratio: 1.0625,
            blockers: Vec::new(),
        }],
        missing_baseline_raft_binary_count: 0,
        unsupported_workload_count: 0,
        correctness_blocker_count: 0,
        performance_blocker_count: 0,
        uncategorized_blocker_count: 0,
        worst_p50_ratio: 1.1,
        worst_p99_ratio: 1.1,
        worst_throughput_ratio: 0.9,
        worst_cpu_ratio: 1.04,
        worst_peak_resident_memory_ratio: 1.0625,
        blockers: Vec::new(),
    }
}

fn ready_benchmark_report() -> BenchmarkReport {
    let workload = BenchmarkWorkload::SingleKeyWrites;
    let baseline_raft = benchmark_sample(
        workload,
        BenchmarkEngine::BaselineRaft,
        BenchmarkEngineSource::RealBaselineRaft,
        BenchmarkImplementation::BaselineRaft,
        1_000.0,
        100,
        200,
        "/opt/baseline-raft/bin/kvbench",
        "1111111111111111111111111111111111111111",
    );
    let rustraft = benchmark_sample(
        workload,
        BenchmarkEngine::RustRaft,
        BenchmarkEngineSource::RustRaftRuntime,
        BenchmarkImplementation::RustRaftRust,
        700.0,
        140,
        260,
        "/opt/rustraft/bin/rustraft-kvbench",
        "2222222222222222222222222222222222222222",
    );

    BenchmarkReport {
        schema: matrixraft::benchmark::MATRIXRAFT_BENCHMARK_REPORT_SCHEMA.to_string(),
        generated_at_unix_ms: 1,
        benchmark_run_id: "debug-snapshot-benchmark".to_string(),
        environment_fingerprint:
            "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false"
                .to_string(),
        node_count: 5,
        options: BenchmarkOptions::default(),
        pass_tolerance_percent:
            matrixraft::benchmark::MATRIXRAFT_BENCHMARK_MAX_PRODUCTION_PASS_TOLERANCE_PERCENT,
        correctness_required: true,
        required_workloads: vec![workload.id().to_string()],
        passed: false,
        comparisons: vec![BenchmarkComparison {
            workload,
            baseline_raft,
            rustraft,
            p50_ratio: 1.4,
            p99_ratio: 1.3,
            throughput_ratio: 0.7,
            cpu_ratio: 1.04,
            peak_resident_memory_ratio: 1.0625,
            passed: false,
            blockers: vec!["single_key_writes:throughput_below_baseline".to_string()],
        }],
    }
}

fn benchmark_sample(
    workload: BenchmarkWorkload,
    engine: BenchmarkEngine,
    engine_source: BenchmarkEngineSource,
    implementation: BenchmarkImplementation,
    throughput_ops_per_sec: f64,
    p50_latency_micros: u64,
    p99_latency_micros: u64,
    binary_path: &str,
    git_revision: &str,
) -> BenchmarkSample {
    BenchmarkSample {
        workload,
        engine,
        engine_source,
        benchmark_run_id: "debug-snapshot-benchmark".to_string(),
        implementation,
        binary_path: Some(binary_path.to_string()),
        git_revision: Some(git_revision.to_string()),
        build_profile: "release".to_string(),
        harness_kind: BenchmarkHarnessKind::FullBaselineRaftHarness,
        node_count: 5,
        iterations_per_workload: 128,
        batch_size: 16,
        payload_size_bytes: 4096,
        timed_iteration_count: 128,
        operations_per_timed_iteration: 1,
        total_duration_micros: 128_000,
        operation_count: 128,
        p50_latency_micros,
        p99_latency_micros,
        throughput_ops_per_sec,
        cpu_utilization_percent: 50.0,
        peak_resident_memory_bytes: 512 * 1024 * 1024,
        correctness_passed: true,
        blockers: Vec::new(),
    }
}

#[test]
fn admin_report_genericizes_baseline_raft_parity_evidence_for_rustraft() {
    let mut cluster =
        RaftCluster::new(5, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"x".to_vec()).expect("write");
    let readiness = ready_snapshot();
    let capability_evidence = matrixraft_capability_evidence(&readiness);
    let report = matrixraft_runtime_admin_report(
        cluster.cluster_status_report().expect("cluster status"),
        readiness,
        capability_evidence,
    );

    assert!(report.ready);
    assert_eq!(report.health, HealthStatus::Healthy);
    assert_eq!(report.public_api.transport_trait, "Transport");
    assert!(report
        .capability_evidence
        .iter()
        .any(|item| item.capability == "leader_write_authority"));
    assert!(report
        .parity
        .baseline_raft_reference_policy
        .feature_reference
        .contains("BaselineRaft"));

    let entries = matrixraft_admin_diagnostic_log_entries(&report);
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].target, "rustraft.admin");
    assert_eq!(entries[0].severity, DiagnosticSeverity::Info);
    assert!(entries[0]
        .fields
        .contains(&("health".to_string(), "Healthy".to_string())));
    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.replication" && entry.message == "replication_healthy"
    }));
    assert!(entries
        .iter()
        .any(|entry| entry.target == "rustraft.apply" && entry.message == "apply_healthy"));

    let json_lines = matrixraft_admin_diagnostic_json_lines(&report);
    let parsed = json_lines
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("diagnostic json line"))
        .collect::<Vec<_>>();
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[0]["target"], "rustraft.admin");
    assert_eq!(parsed[0]["severity"], "info");

    let status_surface = AdminStatusSurfaceInput {
        commit_index: 10,
        max_observed_node_commit_index: 10,
        quorum_size: 2,
        quorum_peer_ids: vec![1, 2, 3],
        peer_pipeline: vec![
            pipeline_peer(1, 10, 11),
            pipeline_peer(2, 10, 11),
            pipeline_peer(3, 8, 9),
        ],
        wal_last_log_index: 10,
        wal_segment_lifecycle_present: true,
    };
    let snapshot = matrixraft_debug_snapshot(&report, &status_surface, &[("service", "raft-a")]);
    assert_eq!(snapshot.contract.name, "matrixraft_debug_snapshot");
    assert_eq!(snapshot.contract.version, 1);
    assert_eq!(snapshot.contract.schema, "rustraft.debug_snapshot.v1");
    assert!(snapshot.generated_at_unix_ms > 0);
    assert_eq!(snapshot.admin_report.cluster_status.group_id, 5);
    assert_eq!(snapshot.diagnostics.len(), entries.len());
    assert!(snapshot.optimization.ready);
    assert_eq!(
        snapshot.optimization_prometheus.format,
        "prometheus_text_v0.0.4"
    );
    assert!(snapshot
        .latency_prometheus
        .text
        .contains("rustraft_append_latency_ms_bucket{service=\"raft-a\",le=\"+Inf\"} 0"));
    assert!(snapshot
        .latency_prometheus
        .text
        .contains("rustraft_read_index_latency_ms_count{service=\"raft-a\"} 0"));
    assert_eq!(snapshot.memory_prometheus.metric_count, 5);
    assert!(snapshot
        .memory_prometheus
        .text
        .contains("rustraft_process_resident_memory_bytes{service=\"raft-a\"} 0"));
    assert!(snapshot
        .memory_prometheus
        .text
        .contains("rustraft_replication_buffer_bytes{service=\"raft-a\"} 0"));
    assert_eq!(snapshot.scale_prometheus.metric_count, 6);
    assert!(snapshot
        .scale_prometheus
        .text
        .contains("rustraft_proposal_total{service=\"raft-a\"} 0"));
    assert!(snapshot
        .scale_prometheus
        .text
        .contains("rustraft_apply_bytes_total{service=\"raft-a\"} 0"));
    assert_eq!(snapshot.benchmark_prometheus.metric_count, 0);
    assert!(snapshot.benchmark_prometheus.text.is_empty());
    let scaled_snapshot = matrixraft_debug_snapshot_with_scale_metrics(
        &report,
        &status_surface,
        &ScaleMetrics {
            proposal_total: 11,
            append_entries_total: 22,
            read_index_total: 33,
            apply_entries_total: 44,
            replication_bytes_total: 55,
            apply_bytes_total: 66,
        },
        &[("service", "raft-a"), ("workload", "release-scale")],
    );
    assert!(scaled_snapshot
        .scale_prometheus
        .text
        .contains("rustraft_proposal_total{service=\"raft-a\",workload=\"release-scale\"} 11"));
    assert!(scaled_snapshot.scale_prometheus.text.contains(
        "rustraft_append_entries_total{service=\"raft-a\",workload=\"release-scale\"} 22"
    ));
    assert!(scaled_snapshot
        .scale_prometheus
        .text
        .contains("rustraft_read_index_total{service=\"raft-a\",workload=\"release-scale\"} 33"));
    assert!(scaled_snapshot.scale_prometheus.text.contains(
        "rustraft_apply_entries_total{service=\"raft-a\",workload=\"release-scale\"} 44"
    ));
    assert!(scaled_snapshot.scale_prometheus.text.contains(
        "rustraft_replication_bytes_total{service=\"raft-a\",workload=\"release-scale\"} 55"
    ));
    assert!(scaled_snapshot
        .scale_prometheus
        .text
        .contains("rustraft_apply_bytes_total{service=\"raft-a\",workload=\"release-scale\"} 66"));
    let scaled_snapshot_validation = matrixraft_validate_debug_snapshot(&scaled_snapshot);
    assert!(
        scaled_snapshot_validation.ready,
        "{scaled_snapshot_validation:?}"
    );
    let runtime_metrics_snapshot = matrixraft_debug_snapshot_with_runtime_metrics(
        &report,
        &status_surface,
        &LatencyMetrics {
            append_latency_ms: LatencyHistogram {
                buckets: vec![
                    LatencyBucket {
                        le_ms: "1".to_string(),
                        count: 5,
                    },
                    LatencyBucket {
                        le_ms: "+Inf".to_string(),
                        count: 8,
                    },
                ],
                sum_ms: 17,
                count: 8,
            },
            vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            pre_vote_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            read_index_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
            snapshot_install_latency_ms: LatencyHistogram::zero(&["1", "+Inf"]),
        },
        &ScaleMetrics {
            proposal_total: 111,
            append_entries_total: 222,
            read_index_total: 333,
            apply_entries_total: 444,
            replication_bytes_total: 555,
            apply_bytes_total: 666,
        },
        &[("service", "raft-a"), ("workload", "release-scale")],
    );
    assert!(runtime_metrics_snapshot.latency_prometheus.text.contains(
        "rustraft_append_latency_ms_bucket{service=\"raft-a\",workload=\"release-scale\",le=\"1\"} 5"
    ));
    assert!(runtime_metrics_snapshot.latency_prometheus.text.contains(
        "rustraft_append_latency_ms_sum{service=\"raft-a\",workload=\"release-scale\"} 17"
    ));
    assert!(runtime_metrics_snapshot
        .scale_prometheus
        .text
        .contains("rustraft_apply_bytes_total{service=\"raft-a\",workload=\"release-scale\"} 666"));
    assert!(matrixraft_validate_debug_snapshot(&runtime_metrics_snapshot).ready);
    let observability_metrics_snapshot = matrixraft_debug_snapshot_with_observability_metrics(
        &report,
        &status_surface,
        &LatencyMetrics::zero(),
        &MemoryMetrics {
            process_resident_memory_bytes: 65_536,
            heap_allocated_bytes: 32_768,
            log_cache_bytes: 16_384,
            snapshot_buffer_bytes: 8_192,
            replication_buffer_bytes: 4_096,
        },
        &ScaleMetrics::zero(),
        &[("service", "raft-a"), ("workload", "release-scale")],
    );
    assert!(observability_metrics_snapshot.memory_prometheus.text.contains(
        "rustraft_process_resident_memory_bytes{service=\"raft-a\",workload=\"release-scale\"} 65536"
    ));
    assert!(observability_metrics_snapshot
        .memory_prometheus
        .text
        .contains(
            "rustraft_replication_buffer_bytes{service=\"raft-a\",workload=\"release-scale\"} 4096"
        ));
    assert!(matrixraft_validate_debug_snapshot(&observability_metrics_snapshot).ready);
    let memory_watch_snapshot = matrixraft_debug_snapshot_with_observability_metrics(
        &report,
        &status_surface,
        &LatencyMetrics::zero(),
        &MemoryMetrics {
            process_resident_memory_bytes: 9 * 1024 * 1024 * 1024,
            heap_allocated_bytes: 5 * 1024 * 1024 * 1024,
            log_cache_bytes: 2 * 1024 * 1024 * 1024,
            snapshot_buffer_bytes: 2 * 1024 * 1024 * 1024,
            replication_buffer_bytes: 2 * 1024 * 1024 * 1024,
        },
        &ScaleMetrics::zero(),
        &[("service", "raft-a")],
    );
    assert!(memory_watch_snapshot.optimization.ready);
    assert_eq!(memory_watch_snapshot.optimization.critical_count, 0);
    assert_eq!(memory_watch_snapshot.optimization.warning_count, 5);
    assert!(memory_watch_snapshot
        .optimization
        .hints
        .iter()
        .any(|hint| hint.id == "process_resident_memory_high"));
    assert_eq!(memory_watch_snapshot.triage.status, "watch");
    assert_eq!(
        memory_watch_snapshot.triage.top_optimization_hint,
        Some("heap_allocated_memory_high".to_string())
    );
    assert!(memory_watch_snapshot.runbook_steps.iter().any(|step| {
        step.id == "review_warning_signals" && step.action.contains("optimization hints")
    }));
    assert!(matrixraft_validate_debug_snapshot(&memory_watch_snapshot).ready);
    let scale_target_snapshot = matrixraft_debug_snapshot_with_performance_targets(
        &report,
        &status_surface,
        &LatencyMetrics::zero(),
        &MemoryMetrics::zero(),
        &ScaleMetrics {
            proposal_total: 10_000,
            append_entries_total: 20_000,
            read_index_total: 30_000,
            apply_entries_total: 40_000,
            replication_bytes_total: 50_000,
            apply_bytes_total: 60_000,
        },
        &ScaleRateMetrics {
            proposal_qps: 950,
            append_entries_qps: 2_500,
            read_index_qps: 700,
            apply_entries_qps: 1_400,
            replication_mib_per_sec: 90,
            apply_mib_per_sec: 150,
        },
        &ScaleOptimizationTargets {
            min_proposal_qps: 1_000,
            min_append_entries_qps: 2_000,
            min_read_index_qps: 800,
            min_apply_entries_qps: 1_500,
            min_replication_mib_per_sec: 100,
            min_apply_mib_per_sec: 100,
        },
        &[("service", "raft-a"), ("workload", "release-scale")],
    );
    assert!(scale_target_snapshot
        .optimization
        .hints
        .iter()
        .any(|hint| hint.id == "proposal_qps_below_target"
            && hint.component == "proposal_pipeline"
            && hint.observed_value == 950
            && hint.threshold == 1_000));
    assert!(scale_target_snapshot
        .optimization
        .hints
        .iter()
        .any(|hint| hint.id == "read_index_qps_below_target"
            && hint.component == "read_path"
            && hint.observed_value == 700
            && hint.threshold == 800));
    assert!(scale_target_snapshot
        .optimization
        .hints
        .iter()
        .any(|hint| hint.id == "apply_entries_qps_below_target"
            && hint.component == "apply_pipeline"
            && hint.observed_value == 1_400
            && hint.threshold == 1_500));
    assert!(scale_target_snapshot
        .optimization
        .hints
        .iter()
        .any(|hint| hint.id == "replication_throughput_below_target"
            && hint.component == "replication_pipeline"
            && hint.observed_value == 90
            && hint.threshold == 100));
    assert_eq!(scale_target_snapshot.optimization.critical_count, 0);
    assert_eq!(scale_target_snapshot.optimization.warning_count, 4);
    assert_eq!(scale_target_snapshot.triage.status, "watch");
    assert!(scale_target_snapshot.runbook_steps.iter().any(|step| {
        step.id == "review_warning_signals" && step.action.contains("optimization hints")
    }));
    assert!(scale_target_snapshot.runbook_steps.iter().any(|step| {
        step.id == "resolve_scale_target_pressure"
            && step.target == "scale"
            && step
                .validation
                .contains("rustraft_runtime_pressure_scale is 0")
            && step
                .validation
                .contains("rustraft_runtime_pressure_action_source_total")
    }));
    assert!(scale_target_snapshot.optimization_prometheus.text.contains(
        "rustraft_optimization_hint_total{service=\"raft-a\",workload=\"release-scale\",hint=\"proposal_qps_below_target\",component=\"proposal_pipeline\",severity=\"warning\"} 1"
    ));
    assert!(scale_target_snapshot.optimization_prometheus.text.contains(
        "rustraft_optimization_hint_total{service=\"raft-a\",workload=\"release-scale\",hint=\"replication_throughput_below_target\",component=\"replication_pipeline\",severity=\"warning\"} 1"
    ));
    assert!(scale_target_snapshot.optimization_prometheus.text.contains(
        "rustraft_optimization_component_hint_total{service=\"raft-a\",workload=\"release-scale\",component=\"replication_pipeline\",severity=\"warning\"} 1"
    ));
    assert!(matrixraft_validate_debug_snapshot(&scale_target_snapshot).ready);
    let mut pressured_peer = pipeline_peer(2, 10, 11);
    pressured_peer.append_queue_limit = 4;
    pressured_peer.append_queue_depth = 4;
    let runtime_pressure_snapshot =
        matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence(
            &report,
            &status_surface,
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &ScaleMetrics::zero(),
            &ScaleRateMetrics {
                proposal_qps: 950,
                append_entries_qps: 2_500,
                read_index_qps: 700,
                apply_entries_qps: 1_400,
                replication_mib_per_sec: 90,
                apply_mib_per_sec: 150,
            },
            &ScaleOptimizationTargets {
                min_proposal_qps: 1_000,
                min_append_entries_qps: 2_000,
                min_read_index_qps: 800,
                min_apply_entries_qps: 1_500,
                min_replication_mib_per_sec: 100,
                min_apply_mib_per_sec: 100,
            },
            &[pressured_peer],
            &ReadBacklogMetrics {
                pending_read_index_requests: 2048,
                pending_bounded_stale_reads: 32,
            },
            &ReadBacklogThresholds {
                pending_read_index_warning: 1024,
                pending_bounded_stale_read_warning: 16,
            },
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "raft-a"), ("workload", "release-scale")],
        );
    let admission = runtime_pressure_snapshot
        .runtime_pressure_admission
        .as_ref()
        .expect("runtime pressure snapshot should carry admission evidence");
    assert!(!admission.accepted);
    assert!(admission.scale_pressure);
    assert!(admission.pipeline_pressure);
    assert!(admission.read_backlog_pressure);
    assert!(runtime_pressure_snapshot.runtime_pressure_prometheus.text.contains(
        "rustraft_runtime_pressure_admission_rejected{service=\"raft-a\",workload=\"release-scale\""
    ));
    assert!(runtime_pressure_snapshot
        .runtime_pressure_prometheus
        .text
        .contains("rustraft_runtime_pressure_scale"));
    assert!(runtime_pressure_snapshot
        .runtime_pressure_prometheus
        .text
        .contains("rustraft_runtime_pressure_pipeline"));
    assert!(runtime_pressure_snapshot
        .runtime_pressure_prometheus
        .text
        .contains("rustraft_runtime_pressure_read_backlog"));
    assert!(runtime_pressure_snapshot
        .runtime_pressure_diagnostics
        .iter()
        .any(
            |entry| entry.target == "rustraft.runtime_pressure.admission"
                && entry.severity == DiagnosticSeverity::Error
        ));
    let runtime_pressure_freshness = runtime_pressure_snapshot
        .runtime_pressure_freshness
        .as_ref()
        .expect("runtime pressure snapshot should carry freshness evidence");
    assert!(runtime_pressure_freshness.fresh);
    assert_eq!(runtime_pressure_freshness.freshness_status, "fresh");
    assert!(runtime_pressure_snapshot
        .runtime_pressure_freshness_diagnostics
        .iter()
        .any(|entry| {
            entry.target == "rustraft.runtime_pressure.freshness"
                && entry.severity == DiagnosticSeverity::Info
                && entry.message == "runtime_pressure_freshness_fresh"
        }));
    assert!(runtime_pressure_snapshot
        .diagnostics
        .iter()
        .any(|entry| entry.target == "rustraft.runtime_pressure.freshness"));
    assert!(runtime_pressure_snapshot
        .runtime_pressure_freshness_prometheus
        .text
        .contains("rustraft_runtime_pressure_freshness_fresh"));
    assert!(runtime_pressure_snapshot
        .runtime_pressure_freshness_prometheus
        .text
        .contains("freshness_status=\"fresh\""));
    assert!(runtime_pressure_snapshot
        .diagnostics
        .iter()
        .any(|entry| entry.target == "rustraft.runtime_pressure.scale"));
    assert!(runtime_pressure_snapshot
        .diagnostics
        .iter()
        .any(|entry| entry.target == "rustraft.runtime_pressure.read_backlog"));
    assert_eq!(runtime_pressure_snapshot.triage.status, "needs_attention");
    assert!(matrixraft_validate_debug_snapshot(&runtime_pressure_snapshot).ready);
    let mut missing_freshness_diagnostic_snapshot = runtime_pressure_snapshot.clone();
    missing_freshness_diagnostic_snapshot
        .runtime_pressure_freshness_diagnostics
        .clear();
    let missing_freshness_diagnostic_validation =
        matrixraft_validate_debug_snapshot(&missing_freshness_diagnostic_snapshot);
    assert!(!missing_freshness_diagnostic_validation.ready);
    assert!(missing_freshness_diagnostic_validation
        .issues
        .contains(&"runtime_pressure_freshness_diagnostic_log_contract_mismatch".to_string()));
    let mut missing_freshness_prometheus_snapshot = runtime_pressure_snapshot.clone();
    missing_freshness_prometheus_snapshot
        .runtime_pressure_freshness_prometheus
        .text = missing_freshness_prometheus_snapshot
        .runtime_pressure_freshness_prometheus
        .text
        .lines()
        .filter(|line| !line.starts_with("rustraft_runtime_pressure_freshness_fresh{"))
        .collect::<Vec<_>>()
        .join("\n");
    missing_freshness_prometheus_snapshot
        .runtime_pressure_freshness_prometheus
        .metric_count = missing_freshness_prometheus_snapshot
        .runtime_pressure_freshness_prometheus
        .text
        .lines()
        .count() as u64;
    let missing_freshness_prometheus_validation =
        matrixraft_validate_debug_snapshot(&missing_freshness_prometheus_snapshot);
    assert!(!missing_freshness_prometheus_validation.ready);
    assert!(missing_freshness_prometheus_validation
        .issues
        .contains(&"runtime_pressure_freshness_prometheus_metric_contract_missing".to_string()));
    let mut missing_action_source_snapshot = runtime_pressure_snapshot.clone();
    missing_action_source_snapshot
        .runtime_pressure_prometheus
        .text = missing_action_source_snapshot
        .runtime_pressure_prometheus
        .text
        .lines()
        .filter(|line| !line.contains("rustraft_runtime_pressure_action_source_total"))
        .collect::<Vec<_>>()
        .join("\n");
    missing_action_source_snapshot
        .runtime_pressure_prometheus
        .metric_count = missing_action_source_snapshot
        .runtime_pressure_prometheus
        .text
        .lines()
        .count() as u64;
    let missing_action_source_validation =
        matrixraft_validate_debug_snapshot(&missing_action_source_snapshot);
    assert!(!missing_action_source_validation.ready);
    assert!(missing_action_source_validation
        .issues
        .contains(&"runtime_pressure_prometheus_metric_contract_missing".to_string()));
    let mut shadow_runtime_pressure_metric_snapshot = runtime_pressure_snapshot.clone();
    shadow_runtime_pressure_metric_snapshot
        .runtime_pressure_prometheus
        .text = shadow_runtime_pressure_metric_snapshot
        .runtime_pressure_prometheus
        .text
        .replace(
            "rustraft_runtime_pressure_admission_accepted",
            "rustraft_runtime_pressure_admission_accepted_shadow",
        );
    let shadow_runtime_pressure_metric_validation =
        matrixraft_validate_debug_snapshot(&shadow_runtime_pressure_metric_snapshot);
    assert!(!shadow_runtime_pressure_metric_validation.ready);
    assert!(shadow_runtime_pressure_metric_validation
        .issues
        .contains(&"runtime_pressure_prometheus_metric_contract_missing".to_string()));
    let mut malformed_runtime_pressure_label_snapshot = runtime_pressure_snapshot.clone();
    malformed_runtime_pressure_label_snapshot
        .runtime_pressure_prometheus
        .text = malformed_runtime_pressure_label_snapshot
        .runtime_pressure_prometheus
        .text
        .lines()
        .map(|line| {
            if line.starts_with("rustraft_runtime_pressure_admission_rejected{") {
                line.replace("service=\"raft-a\"", "service=\"raft-a")
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let malformed_runtime_pressure_label_validation =
        matrixraft_validate_debug_snapshot(&malformed_runtime_pressure_label_snapshot);
    assert!(!malformed_runtime_pressure_label_validation.ready);
    assert!(malformed_runtime_pressure_label_validation
        .issues
        .contains(&"runtime_pressure_prometheus_malformed_sample".to_string()));
    let benchmark_scale_snapshot = matrixraft_debug_snapshot_with_benchmark_scale_inputs(
        &report,
        &status_surface,
        &LatencyMetrics::zero(),
        &MemoryMetrics::zero(),
        &ScaleMetrics::zero(),
        &BenchmarkScaleOptimizationInputs {
            scale_rates: ScaleRateMetrics {
                proposal_qps: 950,
                append_entries_qps: 2_500,
                read_index_qps: 700,
                apply_entries_qps: 1_400,
                replication_mib_per_sec: 90,
                apply_mib_per_sec: 150,
            },
            scale_targets: ScaleOptimizationTargets {
                min_proposal_qps: 1_000,
                min_append_entries_qps: 2_000,
                min_read_index_qps: 800,
                min_apply_entries_qps: 1_500,
                min_replication_mib_per_sec: 100,
                min_apply_mib_per_sec: 100,
            },
            hints: Vec::new(),
        },
        &[("service", "raft-a"), ("workload", "release-scale")],
    );
    assert!(benchmark_scale_snapshot.optimization_prometheus.text.contains(
        "rustraft_optimization_hint_total{service=\"raft-a\",workload=\"release-scale\",hint=\"proposal_qps_below_target\",component=\"proposal_pipeline\",severity=\"warning\"} 1"
    ));
    assert_eq!(benchmark_scale_snapshot.triage.status, "watch");
    assert!(matrixraft_validate_debug_snapshot(&benchmark_scale_snapshot).ready);
    let benchmark_summary = ready_benchmark_summary();
    let benchmark_snapshot = matrixraft_debug_snapshot_with_benchmark_summary(
        &report,
        &status_surface,
        &LatencyMetrics::zero(),
        &MemoryMetrics::zero(),
        &ScaleMetrics::zero(),
        &benchmark_summary,
        &[("service", "raft-a")],
    );
    assert_eq!(benchmark_snapshot.benchmark_prometheus.metric_count, 22);
    assert!(benchmark_snapshot.benchmark_prometheus.text.contains(
        "rustraft_baseline_raft_benchmark_fresh{service=\"raft-a\",freshness_status=\"fresh\"} 1"
    ));
    assert!(benchmark_snapshot
        .benchmark_prometheus
        .text
        .contains("rustraft_baseline_raft_benchmark_worst_p99_ratio{service=\"raft-a\"} 1.1"));
    assert!(benchmark_snapshot.benchmark_prometheus.text.contains(
        "rustraft_baseline_raft_benchmark_workload_throughput_ratio{service=\"raft-a\",workload=\"single_key_writes\"} 0.9"
    ));
    assert!(benchmark_snapshot
        .benchmark_prometheus
        .text
        .contains("rustraft_baseline_raft_benchmark_worst_cpu_ratio{service=\"raft-a\"} 1.04"));
    assert!(benchmark_snapshot.benchmark_prometheus.text.contains(
        "rustraft_baseline_raft_benchmark_workload_peak_resident_memory_ratio{service=\"raft-a\",workload=\"single_key_writes\"} 1.0625"
    ));
    assert_eq!(benchmark_snapshot.runbook_steps.len(), 1);
    assert_eq!(
        benchmark_snapshot.runbook_steps[0].id,
        "continue_normal_observation"
    );
    assert!(matrixraft_validate_debug_snapshot(&benchmark_snapshot).ready);

    let mut stale_benchmark_summary = ready_benchmark_summary();
    stale_benchmark_summary.generated_at_unix_ms = 0;
    let stale_benchmark_snapshot = matrixraft_debug_snapshot_with_benchmark_summary(
        &report,
        &status_surface,
        &LatencyMetrics::zero(),
        &MemoryMetrics::zero(),
        &ScaleMetrics::zero(),
        &stale_benchmark_summary,
        &[("service", "raft-a")],
    );
    assert!(stale_benchmark_snapshot.benchmark_prometheus.text.contains(
        "rustraft_baseline_raft_benchmark_fresh{service=\"raft-a\",freshness_status=\"missing\"} 0"
    ));
    assert!(stale_benchmark_snapshot
        .runbook_steps
        .iter()
        .any(|step| step.id == "refresh_baseline_raft_benchmark_evidence"
            && step.severity == "warning"
            && step.target == "benchmark_parity"));
    assert!(!stale_benchmark_snapshot
        .runbook_steps
        .iter()
        .any(|step| step.id == "continue_normal_observation"));
    assert!(stale_benchmark_snapshot.runbook_prometheus.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"refresh_baseline_raft_benchmark_evidence\",severity=\"warning\",target=\"benchmark_parity\"} 1"
    ));
    assert!(matrixraft_validate_debug_snapshot(&stale_benchmark_snapshot).ready);

    let benchmark_report = ready_benchmark_report();
    let mut benchmark_runtime_peer = pipeline_peer(2, 10, 11);
    benchmark_runtime_peer.append_queue_limit = 4;
    benchmark_runtime_peer.append_queue_depth = 4;
    let benchmark_runtime_snapshot =
        matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts(
            &report,
            &status_surface,
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &ScaleMetrics::zero(),
            &benchmark_report,
            &benchmark_summary,
            &[benchmark_runtime_peer],
            &ReadBacklogMetrics {
                pending_read_index_requests: 3072,
                pending_bounded_stale_reads: 0,
            },
            &ReadBacklogThresholds {
                pending_read_index_warning: 1024,
                pending_bounded_stale_read_warning: 1024,
            },
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "raft-a"), ("workload", "release-scale")],
        );
    assert_eq!(
        benchmark_runtime_snapshot.benchmark_prometheus.metric_count,
        22
    );
    let benchmark_runtime_admission = benchmark_runtime_snapshot
        .runtime_pressure_admission
        .as_ref()
        .expect("benchmark runtime bundle should carry admission evidence");
    assert!(!benchmark_runtime_admission.accepted);
    assert!(benchmark_runtime_admission.scale_pressure);
    assert!(benchmark_runtime_admission.pipeline_pressure);
    assert!(benchmark_runtime_admission.read_backlog_pressure);
    assert!(benchmark_runtime_snapshot
        .runtime_pressure_prometheus
        .text
        .contains("rustraft_runtime_pressure_admission_rejected"));
    assert!(benchmark_runtime_snapshot
        .runtime_pressure_prometheus
        .text
        .contains("rustraft_runtime_pressure_read_backlog"));
    assert!(benchmark_runtime_snapshot.benchmark_prometheus.text.contains(
        "rustraft_baseline_raft_benchmark_worst_p99_ratio{service=\"raft-a\",workload=\"release-scale\"} 1.1"
    ));
    assert!(benchmark_runtime_snapshot
        .diagnostics
        .iter()
        .any(|entry| entry.target == "rustraft.runtime_pressure.pipeline"));
    assert!(matrixraft_validate_debug_snapshot(&benchmark_runtime_snapshot).ready);

    let mut failing_benchmark_summary = ready_benchmark_summary();
    failing_benchmark_summary.passed = false;
    failing_benchmark_summary.failed_workload_count = 1;
    failing_benchmark_summary.workloads[0].passed = false;
    failing_benchmark_summary.workloads[0].p50_ratio = 1.2;
    failing_benchmark_summary.workloads[0].p99_ratio = 1.35;
    failing_benchmark_summary.workloads[0].throughput_ratio = 0.75;
    failing_benchmark_summary.worst_p50_ratio = 1.2;
    failing_benchmark_summary.worst_p99_ratio = 1.35;
    failing_benchmark_summary.worst_throughput_ratio = 0.75;
    failing_benchmark_summary.blockers = vec!["single_key_writes:p99_ratio:1.35".to_string()];
    let failing_benchmark_snapshot = matrixraft_debug_snapshot_with_benchmark_summary(
        &report,
        &status_surface,
        &LatencyMetrics::zero(),
        &MemoryMetrics::zero(),
        &ScaleMetrics::zero(),
        &failing_benchmark_summary,
        &[("service", "raft-a")],
    );
    assert!(failing_benchmark_snapshot
        .runbook_steps
        .iter()
        .any(|step| step.id == "resolve_baseline_raft_benchmark_failure"
            && step.severity == "critical"
            && step.target == "benchmark_parity"));
    assert!(failing_benchmark_snapshot
        .runbook_steps
        .iter()
        .any(
            |step| step.id == "resolve_baseline_raft_benchmark_ratio_regression"
                && step.severity == "warning"
                && step.target == "benchmark_parity"
        ));
    assert!(!failing_benchmark_snapshot
        .runbook_steps
        .iter()
        .any(|step| step.id == "continue_normal_observation"));
    assert!(failing_benchmark_snapshot.runbook_prometheus.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_baseline_raft_benchmark_failure\",severity=\"critical\",target=\"benchmark_parity\"} 1"
    ));
    assert!(failing_benchmark_snapshot.runbook_prometheus.text.contains(
        "rustraft_operator_runbook_first_step{service=\"raft-a\",step=\"resolve_baseline_raft_benchmark_failure\",severity=\"critical\",target=\"benchmark_parity\"} 1"
    ));
    let failing_benchmark_validation =
        matrixraft_validate_debug_snapshot(&failing_benchmark_snapshot);
    assert!(failing_benchmark_validation.ready);
    assert!(snapshot
        .optimization_prometheus
        .text
        .contains("rustraft_optimization_critical_total{service=\"raft-a\"} 0"));
    assert_eq!(snapshot.grafana.uid, "rustraft-runtime-overview");
    assert!(snapshot
        .alerts
        .iter()
        .any(|rule| rule.alert == "RustRaftOptimizationNotReady"));
    assert_eq!(snapshot.triage.status, "ready");
    assert_eq!(snapshot.triage.severity, "info");
    assert_eq!(snapshot.triage.critical_optimization_count, 0);
    assert_eq!(snapshot.triage.alert_rule_count, snapshot.alerts.len());
    assert_eq!(snapshot.runbook_steps.len(), 1);
    assert_eq!(snapshot.runbook_steps[0].id, "continue_normal_observation");
    assert!(snapshot.runbook_prometheus.text.contains(
        "rustraft_operator_runbook_step_total{service=\"raft-a\",severity=\"info\",target=\"observability\"} 1"
    ));
    assert!(snapshot.runbook_prometheus.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"continue_normal_observation\",severity=\"info\",target=\"observability\"} 1"
    ));
    let validation = matrixraft_validate_debug_snapshot(&snapshot);
    assert!(validation.ready);
    assert_eq!(validation.issue_count, 0);
    let metadata_prometheus =
        matrixraft_debug_snapshot_metadata_prometheus(&snapshot, &[("service", "raft\"a")]);
    assert_eq!(metadata_prometheus.metric_count, 8);
    assert!(metadata_prometheus
        .text
        .contains("rustraft_debug_snapshot_generated_at_unix_ms{service=\"raft\\\"a\"}"));
    assert!(metadata_prometheus
        .text
        .contains(&snapshot.generated_at_unix_ms.to_string()));
    assert!(metadata_prometheus
        .text
        .contains("rustraft_debug_snapshot_age_ms{service=\"raft\\\"a\"}"));
    assert!(metadata_prometheus
        .text
        .contains("rustraft_debug_snapshot_max_age_ms{service=\"raft\\\"a\"} 3600000"));
    assert!(metadata_prometheus
        .text
        .contains("rustraft_debug_snapshot_stale_after_unix_ms{service=\"raft\\\"a\"}"));
    assert!(metadata_prometheus
        .text
        .contains("rustraft_debug_snapshot_remaining_fresh_ms{service=\"raft\\\"a\"}"));
    assert!(metadata_prometheus
        .text
        .contains("rustraft_debug_snapshot_low_fresh_ms{service=\"raft\\\"a\"} 300000"));
    assert!(metadata_prometheus
        .text
        .contains("rustraft_debug_snapshot_low_fresh{service=\"raft\\\"a\"} 1"));
    assert!(metadata_prometheus
        .text
        .contains("rustraft_debug_snapshot_fresh{service=\"raft\\\"a\"} 1"));

    let mut invalid_snapshot = snapshot.clone();
    invalid_snapshot.contract.schema = "rustraft.debug_snapshot.v0".to_string();
    let invalid_validation = matrixraft_validate_debug_snapshot(&invalid_snapshot);
    assert!(!invalid_validation.ready);
    assert_eq!(
        invalid_validation.issues,
        vec!["contract_mismatch".to_string()]
    );
    let invalid_validation_metrics = matrixraft_debug_bundle_validation_prometheus(
        &invalid_validation,
        &[("service", "raft\"a")],
    );
    assert_eq!(invalid_validation_metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(invalid_validation_metrics.metric_count, 4);
    assert!(invalid_validation_metrics
        .text
        .contains("rustraft_debug_bundle_validation_ready{service=\"raft\\\"a\"} 0"));
    assert!(invalid_validation_metrics
        .text
        .contains("rustraft_debug_bundle_validation_issue_total{service=\"raft\\\"a\"} 1"));
    assert!(invalid_validation_metrics.text.contains(
        "rustraft_debug_bundle_validation_issue{service=\"raft\\\"a\",issue=\"contract_mismatch\"} 1"
    ));
    assert!(invalid_validation_metrics.text.contains(
        "rustraft_debug_bundle_validation_first_issue{service=\"raft\\\"a\",issue=\"contract_mismatch\"} 1"
    ));

    let mut missing_timestamp_snapshot = snapshot.clone();
    missing_timestamp_snapshot.generated_at_unix_ms = 0;
    let missing_timestamp_validation =
        matrixraft_validate_debug_snapshot(&missing_timestamp_snapshot);
    assert!(!missing_timestamp_validation.ready);
    assert!(missing_timestamp_validation
        .issues
        .contains(&"generated_at_missing".to_string()));

    let mut future_timestamp_snapshot = snapshot.clone();
    future_timestamp_snapshot.generated_at_unix_ms = u64::MAX;
    let future_timestamp_validation =
        matrixraft_validate_debug_snapshot(&future_timestamp_snapshot);
    assert!(!future_timestamp_validation.ready);
    assert!(future_timestamp_validation
        .issues
        .contains(&"generated_at_in_future".to_string()));

    let mut stale_timestamp_snapshot = snapshot.clone();
    stale_timestamp_snapshot.generated_at_unix_ms = 1;
    let stale_timestamp_validation = matrixraft_validate_debug_snapshot(&stale_timestamp_snapshot);
    assert!(!stale_timestamp_validation.ready);
    assert!(stale_timestamp_validation
        .issues
        .contains(&"generated_at_stale".to_string()));
    let stale_timestamp_validation_metrics = matrixraft_debug_bundle_validation_prometheus(
        &stale_timestamp_validation,
        &[("service", "raft-a")],
    );
    assert!(stale_timestamp_validation_metrics.text.contains(
        "rustraft_debug_bundle_validation_issue{service=\"raft-a\",issue=\"generated_at_stale\"} 1"
    ));
    assert!(stale_timestamp_validation_metrics.text.contains(
        "rustraft_debug_bundle_validation_first_issue{service=\"raft-a\",issue=\"generated_at_stale\"} 1"
    ));

    let mut stale_snapshot = snapshot.clone();
    stale_snapshot.triage.critical_optimization_count = 7;
    let stale_validation = matrixraft_validate_debug_snapshot(&stale_snapshot);
    assert!(!stale_validation.ready);
    assert!(stale_validation
        .issues
        .contains(&"triage_critical_count_mismatch".to_string()));

    let mut stale_diagnostic_snapshot = snapshot.clone();
    stale_diagnostic_snapshot.triage.diagnostic_warning_count = 13;
    let stale_diagnostic_validation =
        matrixraft_validate_debug_snapshot(&stale_diagnostic_snapshot);
    assert!(!stale_diagnostic_validation.ready);
    assert!(stale_diagnostic_validation
        .issues
        .contains(&"triage_diagnostic_warning_count_mismatch".to_string()));

    let mut stale_triage_snapshot = snapshot.clone();
    stale_triage_snapshot.triage.first_action = "stale action".to_string();
    let stale_triage_validation = matrixraft_validate_debug_snapshot(&stale_triage_snapshot);
    assert!(!stale_triage_validation.ready);
    assert!(stale_triage_validation
        .issues
        .contains(&"triage_contract_mismatch".to_string()));

    let mut stale_diagnostic_log_snapshot = snapshot.clone();
    stale_diagnostic_log_snapshot.diagnostics[0].message = "stale diagnostic".to_string();
    let stale_diagnostic_log_validation =
        matrixraft_validate_debug_snapshot(&stale_diagnostic_log_snapshot);
    assert!(!stale_diagnostic_log_validation.ready);
    assert!(stale_diagnostic_log_validation
        .issues
        .contains(&"diagnostic_log_contract_mismatch".to_string()));

    let mut missing_diagnostic_severity_snapshot = snapshot.clone();
    missing_diagnostic_severity_snapshot
        .diagnostic_prometheus
        .text = missing_diagnostic_severity_snapshot
        .diagnostic_prometheus
        .text
        .lines()
        .filter(|line| {
            !line.contains("rustraft_diagnostic_log_total") || !line.contains("severity=\"error\"")
        })
        .map(|line| format!("{}\n", line))
        .collect();
    let missing_diagnostic_severity_validation =
        matrixraft_validate_debug_snapshot(&missing_diagnostic_severity_snapshot);
    assert!(!missing_diagnostic_severity_validation.ready);
    assert!(missing_diagnostic_severity_validation
        .issues
        .contains(&"diagnostic_prometheus_severity_total_missing".to_string()));

    let mut missing_diagnostic_entry_snapshot = snapshot.clone();
    missing_diagnostic_entry_snapshot.diagnostic_prometheus.text =
        missing_diagnostic_entry_snapshot
            .diagnostic_prometheus
            .text
            .lines()
            .filter(|line| !line.contains("rustraft_diagnostic_log_entry_total"))
            .map(|line| format!("{}\n", line))
            .collect();
    let missing_diagnostic_entry_validation =
        matrixraft_validate_debug_snapshot(&missing_diagnostic_entry_snapshot);
    assert!(!missing_diagnostic_entry_validation.ready);
    assert!(missing_diagnostic_entry_validation
        .issues
        .contains(&"diagnostic_prometheus_entry_missing".to_string()));

    let mut escaped_diagnostic_snapshot = snapshot.clone();
    escaped_diagnostic_snapshot
        .diagnostics
        .push(DiagnosticLogEntry {
            target: "rustraft.target\"with\\escape".to_string(),
            severity: DiagnosticSeverity::Warn,
            message: "diagnostic\"message\\escaped".to_string(),
            fields: vec![(
                "detail".to_string(),
                "escaped labels stay valid".to_string(),
            )],
        });
    escaped_diagnostic_snapshot.diagnostic_prometheus =
        matrixraft_diagnostic_log_prometheus(&escaped_diagnostic_snapshot.diagnostics, &[]);
    let escaped_diagnostic_validation =
        matrixraft_validate_debug_snapshot(&escaped_diagnostic_snapshot);
    assert!(
        !escaped_diagnostic_validation
            .issues
            .contains(&"diagnostic_prometheus_entry_missing".to_string()),
        "{escaped_diagnostic_validation:?}"
    );
    assert!(escaped_diagnostic_snapshot
        .diagnostic_prometheus
        .text
        .contains("target=\"rustraft.target\\\"with\\\\escape\""));
    assert!(escaped_diagnostic_snapshot
        .diagnostic_prometheus
        .text
        .contains("message=\"diagnostic\\\"message\\\\escaped\""));

    let mut stale_optimization_hint_count_snapshot = snapshot.clone();
    stale_optimization_hint_count_snapshot
        .optimization
        .hint_count = 99;
    let stale_optimization_hint_count_validation =
        matrixraft_validate_debug_snapshot(&stale_optimization_hint_count_snapshot);
    assert!(!stale_optimization_hint_count_validation.ready);
    assert!(stale_optimization_hint_count_validation
        .issues
        .contains(&"optimization_hint_count_mismatch".to_string()));

    let mut stale_optimization_warning_count_snapshot = snapshot.clone();
    stale_optimization_warning_count_snapshot
        .optimization
        .warning_count = 99;
    let stale_optimization_warning_count_validation =
        matrixraft_validate_debug_snapshot(&stale_optimization_warning_count_snapshot);
    assert!(!stale_optimization_warning_count_validation.ready);
    assert!(stale_optimization_warning_count_validation
        .issues
        .contains(&"optimization_warning_count_mismatch".to_string()));

    let mut stale_optimization_ready_snapshot = snapshot.clone();
    stale_optimization_ready_snapshot
        .optimization
        .hints
        .push(OptimizationHint {
            id: "critical_ready_mismatch".to_string(),
            severity: OptimizationHintSeverity::Critical,
            component: "optimization".to_string(),
            recommendation: "clear critical hints before ready".to_string(),
            observed_value: 1,
            threshold: 0,
        });
    stale_optimization_ready_snapshot.optimization.hint_count += 1;
    stale_optimization_ready_snapshot
        .optimization
        .critical_count += 1;
    let stale_optimization_ready_validation =
        matrixraft_validate_debug_snapshot(&stale_optimization_ready_snapshot);
    assert!(!stale_optimization_ready_validation.ready);
    assert!(stale_optimization_ready_validation
        .issues
        .contains(&"optimization_ready_mismatch".to_string()));

    let mut stale_prometheus_count_snapshot = snapshot.clone();
    stale_prometheus_count_snapshot
        .optimization_prometheus
        .metric_count = 99;
    let stale_prometheus_count_validation =
        matrixraft_validate_debug_snapshot(&stale_prometheus_count_snapshot);
    assert!(!stale_prometheus_count_validation.ready);
    assert!(stale_prometheus_count_validation
        .issues
        .contains(&"prometheus_metric_count_mismatch".to_string()));

    let mut missing_prometheus_metric_snapshot = snapshot.clone();
    missing_prometheus_metric_snapshot
        .optimization_prometheus
        .text = "rustraft_optimization_ready 1\n".to_string();
    let missing_prometheus_metric_validation =
        matrixraft_validate_debug_snapshot(&missing_prometheus_metric_snapshot);
    assert!(!missing_prometheus_metric_validation.ready);
    assert!(missing_prometheus_metric_validation
        .issues
        .contains(&"prometheus_metric_contract_missing".to_string()));

    let mut missing_scale_metric_snapshot = snapshot.clone();
    missing_scale_metric_snapshot.scale_prometheus.text = missing_scale_metric_snapshot
        .scale_prometheus
        .text
        .lines()
        .filter(|line| !line.contains("rustraft_read_index_total"))
        .map(|line| format!("{}\n", line))
        .collect();
    let missing_scale_metric_validation =
        matrixraft_validate_debug_snapshot(&missing_scale_metric_snapshot);
    assert!(!missing_scale_metric_validation.ready);
    assert!(missing_scale_metric_validation
        .issues
        .contains(&"scale_prometheus_metric_contract_missing".to_string()));

    let mut shadow_scale_metric_snapshot = snapshot.clone();
    shadow_scale_metric_snapshot.scale_prometheus.text =
        shadow_scale_metric_snapshot.scale_prometheus.text.replace(
            "rustraft_read_index_total",
            "rustraft_read_index_total_shadow",
        );
    let shadow_scale_metric_validation =
        matrixraft_validate_debug_snapshot(&shadow_scale_metric_snapshot);
    assert!(!shadow_scale_metric_validation.ready);
    assert!(shadow_scale_metric_validation
        .issues
        .contains(&"scale_prometheus_metric_contract_missing".to_string()));

    let mut missing_latency_metric_snapshot = snapshot.clone();
    missing_latency_metric_snapshot.latency_prometheus.text = missing_latency_metric_snapshot
        .latency_prometheus
        .text
        .lines()
        .filter(|line| !line.contains("rustraft_read_index_latency_ms_count"))
        .map(|line| format!("{}\n", line))
        .collect();
    let missing_latency_metric_validation =
        matrixraft_validate_debug_snapshot(&missing_latency_metric_snapshot);
    assert!(!missing_latency_metric_validation.ready);
    assert!(missing_latency_metric_validation
        .issues
        .contains(&"latency_prometheus_metric_contract_missing".to_string()));

    let mut missing_memory_metric_snapshot = snapshot.clone();
    missing_memory_metric_snapshot.memory_prometheus.text = missing_memory_metric_snapshot
        .memory_prometheus
        .text
        .lines()
        .filter(|line| !line.contains("rustraft_replication_buffer_bytes"))
        .map(|line| format!("{}\n", line))
        .collect();
    let missing_memory_metric_validation =
        matrixraft_validate_debug_snapshot(&missing_memory_metric_snapshot);
    assert!(!missing_memory_metric_validation.ready);
    assert!(missing_memory_metric_validation
        .issues
        .contains(&"memory_prometheus_metric_contract_missing".to_string()));

    let mut missing_benchmark_metric_snapshot = benchmark_snapshot.clone();
    missing_benchmark_metric_snapshot.benchmark_prometheus.text =
        "rustraft_baseline_raft_benchmark_passed 1\n".to_string();
    missing_benchmark_metric_snapshot
        .benchmark_prometheus
        .metric_count = 1;
    let missing_benchmark_metric_validation =
        matrixraft_validate_debug_snapshot(&missing_benchmark_metric_snapshot);
    assert!(!missing_benchmark_metric_validation.ready);
    assert!(missing_benchmark_metric_validation
        .issues
        .contains(&"benchmark_prometheus_metric_contract_missing".to_string()));

    let mut shadow_benchmark_metric_snapshot = benchmark_snapshot.clone();
    shadow_benchmark_metric_snapshot.benchmark_prometheus.text = shadow_benchmark_metric_snapshot
        .benchmark_prometheus
        .text
        .replace(
            "rustraft_baseline_raft_benchmark_workload_p99_ratio",
            "rustraft_baseline_raft_benchmark_workload_p99_ratio_shadow",
        );
    let shadow_benchmark_metric_validation =
        matrixraft_validate_debug_snapshot(&shadow_benchmark_metric_snapshot);
    assert!(!shadow_benchmark_metric_validation.ready);
    assert!(shadow_benchmark_metric_validation
        .issues
        .contains(&"benchmark_prometheus_metric_contract_missing".to_string()));

    let mut stale_benchmark_count_snapshot = benchmark_snapshot.clone();
    stale_benchmark_count_snapshot
        .benchmark_prometheus
        .metric_count += 1;
    let stale_benchmark_count_validation =
        matrixraft_validate_debug_snapshot(&stale_benchmark_count_snapshot);
    assert!(!stale_benchmark_count_validation.ready);
    assert!(stale_benchmark_count_validation
        .issues
        .contains(&"benchmark_prometheus_metric_count_mismatch".to_string()));

    let mut missing_prometheus_hint_snapshot = snapshot.clone();
    missing_prometheus_hint_snapshot
        .optimization
        .hints
        .push(OptimizationHint {
            id: "stale_missing_hint_metric".to_string(),
            severity: OptimizationHintSeverity::Warning,
            component: "observability".to_string(),
            recommendation: "refresh Prometheus hint metrics".to_string(),
            observed_value: 1,
            threshold: 0,
        });
    missing_prometheus_hint_snapshot.optimization.hint_count += 1;
    let missing_prometheus_hint_validation =
        matrixraft_validate_debug_snapshot(&missing_prometheus_hint_snapshot);
    assert!(!missing_prometheus_hint_validation.ready);
    assert!(missing_prometheus_hint_validation
        .issues
        .contains(&"prometheus_hint_metric_missing".to_string()));
    assert!(missing_prometheus_hint_validation
        .issues
        .contains(&"prometheus_component_hint_metric_missing".to_string()));

    let mut escaped_hint_snapshot = snapshot.clone();
    escaped_hint_snapshot
        .optimization
        .hints
        .push(OptimizationHint {
            id: "hint\"with\\escape".to_string(),
            severity: OptimizationHintSeverity::Warning,
            component: "observability".to_string(),
            recommendation: "keep escaped labels valid".to_string(),
            observed_value: 1,
            threshold: 0,
        });
    escaped_hint_snapshot.optimization.hint_count += 1;
    escaped_hint_snapshot.optimization.warning_count += 1;
    escaped_hint_snapshot.optimization_prometheus =
        matrixraft_optimization_report_prometheus(&escaped_hint_snapshot.optimization, &[]);
    let escaped_hint_validation = matrixraft_validate_debug_snapshot(&escaped_hint_snapshot);
    assert!(
        !escaped_hint_validation
            .issues
            .contains(&"prometheus_hint_metric_missing".to_string()),
        "{escaped_hint_validation:?}"
    );
    assert!(escaped_hint_snapshot
        .optimization_prometheus
        .text
        .contains("hint=\"hint\\\"with\\\\escape\""));

    let mut stale_grafana_contract_snapshot = snapshot.clone();
    stale_grafana_contract_snapshot.grafana.uid = "old-dashboard".to_string();
    let stale_grafana_contract_validation =
        matrixraft_validate_debug_snapshot(&stale_grafana_contract_snapshot);
    assert!(!stale_grafana_contract_validation.ready);
    assert!(stale_grafana_contract_validation
        .issues
        .contains(&"grafana_contract_mismatch".to_string()));

    let mut stale_grafana_panel_snapshot = snapshot.clone();
    stale_grafana_panel_snapshot.grafana.panels[0].expr = "rustraft_ready == 0".to_string();
    let stale_grafana_panel_validation =
        matrixraft_validate_debug_snapshot(&stale_grafana_panel_snapshot);
    assert!(!stale_grafana_panel_validation.ready);
    assert!(stale_grafana_panel_validation
        .issues
        .contains(&"grafana_panel_contract_mismatch".to_string()));

    let mut duplicate_grafana_panel_snapshot = snapshot.clone();
    duplicate_grafana_panel_snapshot.grafana.panels[1].id =
        duplicate_grafana_panel_snapshot.grafana.panels[0].id;
    let duplicate_grafana_panel_validation =
        matrixraft_validate_debug_snapshot(&duplicate_grafana_panel_snapshot);
    assert!(!duplicate_grafana_panel_validation.ready);
    assert!(duplicate_grafana_panel_validation
        .issues
        .contains(&"grafana_panel_id_duplicate".to_string()));

    let mut missing_grafana_panel_snapshot = snapshot.clone();
    missing_grafana_panel_snapshot
        .grafana
        .panels
        .retain(|panel| panel.id != 17);
    let missing_grafana_panel_validation =
        matrixraft_validate_debug_snapshot(&missing_grafana_panel_snapshot);
    assert!(!missing_grafana_panel_validation.ready);
    assert!(missing_grafana_panel_validation
        .issues
        .contains(&"grafana_panel_contract_missing".to_string()));

    let mut stale_runbook_snapshot = snapshot.clone();
    stale_runbook_snapshot.runbook_steps[0].action = "do something else".to_string();
    let stale_runbook_validation = matrixraft_validate_debug_snapshot(&stale_runbook_snapshot);
    assert!(!stale_runbook_validation.ready);
    assert!(stale_runbook_validation
        .issues
        .contains(&"runbook_step_contract_mismatch".to_string()));

    let mut missing_runbook_prometheus_snapshot = snapshot.clone();
    missing_runbook_prometheus_snapshot.runbook_prometheus.text =
        "rustraft_operator_runbook_step_total 1\n".to_string();
    let missing_runbook_prometheus_validation =
        matrixraft_validate_debug_snapshot(&missing_runbook_prometheus_snapshot);
    assert!(!missing_runbook_prometheus_validation.ready);
    assert!(missing_runbook_prometheus_validation
        .issues
        .contains(&"runbook_prometheus_metric_contract_missing".to_string()));
    assert!(missing_runbook_prometheus_validation
        .issues
        .contains(&"runbook_prometheus_step_missing".to_string()));

    let mut missing_runbook_first_step_snapshot = snapshot.clone();
    missing_runbook_first_step_snapshot.runbook_prometheus.text =
        missing_runbook_first_step_snapshot
            .runbook_prometheus
            .text
            .lines()
            .filter(|line| !line.contains("rustraft_operator_runbook_first_step"))
            .collect::<Vec<_>>()
            .join("\n");
    missing_runbook_first_step_snapshot
        .runbook_prometheus
        .metric_count -= 1;
    let missing_runbook_first_step_validation =
        matrixraft_validate_debug_snapshot(&missing_runbook_first_step_snapshot);
    assert!(!missing_runbook_first_step_validation.ready);
    assert!(missing_runbook_first_step_validation
        .issues
        .contains(&"runbook_prometheus_metric_contract_missing".to_string()));
    assert!(missing_runbook_first_step_validation
        .issues
        .contains(&"runbook_prometheus_first_step_missing".to_string()));

    let mut escaped_runbook_snapshot = snapshot.clone();
    escaped_runbook_snapshot
        .runbook_steps
        .push(OperatorRunbookStep {
            id: "step\"with\\escape".to_string(),
            severity: "warning\\level".to_string(),
            target: "runbook\"target\\escaped".to_string(),
            action: "keep escaped runbook labels valid".to_string(),
            validation: "validator still recognizes escaped runbook metrics".to_string(),
        });
    escaped_runbook_snapshot.runbook_prometheus =
        matrixraft_operator_runbook_prometheus(&escaped_runbook_snapshot.runbook_steps, &[]);
    let escaped_runbook_validation = matrixraft_validate_debug_snapshot(&escaped_runbook_snapshot);
    assert!(
        !escaped_runbook_validation
            .issues
            .contains(&"runbook_prometheus_step_missing".to_string()),
        "{escaped_runbook_validation:?}"
    );
    assert!(escaped_runbook_snapshot
        .runbook_prometheus
        .text
        .contains("step=\"step\\\"with\\\\escape\""));
    assert!(escaped_runbook_snapshot
        .runbook_prometheus
        .text
        .contains("target=\"runbook\\\"target\\\\escaped\""));

    let mut extra_runbook_snapshot = snapshot.clone();
    extra_runbook_snapshot
        .runbook_steps
        .push(snapshot.runbook_steps[0].clone());
    extra_runbook_snapshot.runbook_steps[1].id = "unexpected_extra_step".to_string();
    let extra_runbook_validation = matrixraft_validate_debug_snapshot(&extra_runbook_snapshot);
    assert!(!extra_runbook_validation.ready);
    assert!(extra_runbook_validation
        .issues
        .contains(&"runbook_step_count_mismatch".to_string()));

    let mut stale_alert_contract_snapshot = snapshot.clone();
    stale_alert_contract_snapshot.alerts[0].expr = "rustraft_optimization_ready < 0".to_string();
    let stale_alert_contract_validation =
        matrixraft_validate_debug_snapshot(&stale_alert_contract_snapshot);
    assert!(!stale_alert_contract_validation.ready);
    assert!(stale_alert_contract_validation
        .issues
        .contains(&"alert_rule_contract_mismatch".to_string()));

    let mut missing_alert_contract_snapshot = snapshot.clone();
    missing_alert_contract_snapshot
        .alerts
        .retain(|rule| rule.alert != "RustRaftDebugBundleValidationFailed");
    missing_alert_contract_snapshot.triage.alert_rule_count =
        missing_alert_contract_snapshot.alerts.len();
    let missing_alert_contract_validation =
        matrixraft_validate_debug_snapshot(&missing_alert_contract_snapshot);
    assert!(!missing_alert_contract_validation.ready);
    assert!(missing_alert_contract_validation
        .issues
        .contains(&"alert_rule_contract_missing".to_string()));

    let mut stale_top_alert_snapshot = snapshot.clone();
    stale_top_alert_snapshot.triage.top_alert = Some("MissingRustRaftAlert".to_string());
    let stale_top_alert_validation = matrixraft_validate_debug_snapshot(&stale_top_alert_snapshot);
    assert!(!stale_top_alert_validation.ready);
    assert!(stale_top_alert_validation
        .issues
        .contains(&"triage_top_alert_missing".to_string()));

    let mut stale_top_diagnostic_snapshot = snapshot.clone();
    stale_top_diagnostic_snapshot.triage.top_diagnostic_target =
        Some("rustraft.missing".to_string());
    stale_top_diagnostic_snapshot.triage.top_diagnostic_message =
        Some("missing_diagnostic".to_string());
    let stale_top_diagnostic_validation =
        matrixraft_validate_debug_snapshot(&stale_top_diagnostic_snapshot);
    assert!(!stale_top_diagnostic_validation.ready);
    assert!(stale_top_diagnostic_validation
        .issues
        .contains(&"triage_top_diagnostic_missing".to_string()));

    let mut incomplete_top_diagnostic_snapshot = snapshot.clone();
    incomplete_top_diagnostic_snapshot
        .triage
        .top_diagnostic_message = None;
    let incomplete_top_diagnostic_validation =
        matrixraft_validate_debug_snapshot(&incomplete_top_diagnostic_snapshot);
    assert!(!incomplete_top_diagnostic_validation.ready);
    assert!(incomplete_top_diagnostic_validation
        .issues
        .contains(&"triage_top_diagnostic_incomplete".to_string()));

    let mut stale_top_hint_snapshot = snapshot.clone();
    stale_top_hint_snapshot.triage.top_optimization_hint =
        Some("missing_optimization_hint".to_string());
    let stale_top_hint_validation = matrixraft_validate_debug_snapshot(&stale_top_hint_snapshot);
    assert!(!stale_top_hint_validation.ready);
    assert!(stale_top_hint_validation
        .issues
        .contains(&"triage_top_optimization_hint_missing".to_string()));

    let mut stale_severity_snapshot = snapshot.clone();
    stale_severity_snapshot.triage.severity = "warning".to_string();
    let stale_severity_validation = matrixraft_validate_debug_snapshot(&stale_severity_snapshot);
    assert!(!stale_severity_validation.ready);
    assert!(stale_severity_validation
        .issues
        .contains(&"triage_ready_severity_mismatch".to_string()));

    let mut missing_ready_runbook_snapshot = snapshot.clone();
    missing_ready_runbook_snapshot.runbook_steps.clear();
    let missing_ready_runbook_validation =
        matrixraft_validate_debug_snapshot(&missing_ready_runbook_snapshot);
    assert!(!missing_ready_runbook_validation.ready);
    assert!(missing_ready_runbook_validation
        .issues
        .contains(&"runbook_ready_step_missing".to_string()));

    let snapshot_json =
        matrixraft_debug_snapshot_json(&report, &status_surface, &[("service", "raft-a")]);
    let json_validation = matrixraft_validate_debug_snapshot_json(&snapshot_json);
    assert!(json_validation.ready);
    assert_eq!(json_validation.issue_count, 0);
    let invalid_json_validation = matrixraft_validate_debug_snapshot_json("{not-json");
    assert!(!invalid_json_validation.ready);
    assert_eq!(
        invalid_json_validation.issues,
        vec!["json_parse_failed".to_string()]
    );
    let mut stale_contract_json: Value =
        serde_json::from_str(&snapshot_json).expect("debug snapshot json for mutation");
    stale_contract_json["contract"]["schema"] = Value::String("rustraft.debug_snapshot.old".into());
    let stale_contract_validation =
        matrixraft_validate_debug_snapshot_json(&stale_contract_json.to_string());
    assert!(!stale_contract_validation.ready);
    assert!(stale_contract_validation
        .issues
        .contains(&"contract_mismatch".to_string()));
    let parsed_snapshot: Value = serde_json::from_str(&snapshot_json).expect("debug snapshot json");
    assert_eq!(
        parsed_snapshot["contract"]["schema"],
        "rustraft.debug_snapshot.v1"
    );
    assert!(
        parsed_snapshot["generated_at_unix_ms"]
            .as_u64()
            .expect("generated_at_unix_ms")
            > 0
    );
    assert_eq!(
        parsed_snapshot["admin_report"]["cluster_status"]["group_id"],
        5
    );
    assert_eq!(parsed_snapshot["optimization"]["ready"], true);
    assert_eq!(
        parsed_snapshot["grafana"]["uid"],
        "rustraft-runtime-overview"
    );
    assert_eq!(
        parsed_snapshot["alerts"][0]["alert"],
        "RustRaftOptimizationNotReady"
    );
    assert_eq!(
        parsed_snapshot["alerts"][0]["expr"],
        "rustraft_optimization_ready == 0"
    );
    assert_eq!(parsed_snapshot["triage"]["status"], "ready");
    assert_eq!(
        parsed_snapshot["triage"]["first_action"],
        "No immediate operator action is required."
    );
    assert_eq!(
        parsed_snapshot["runbook_steps"][0]["id"],
        "continue_normal_observation"
    );
}
