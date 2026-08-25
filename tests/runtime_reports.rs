// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    rustraft_admin_diagnostic_json_lines, rustraft_admin_diagnostic_log_entries,
    rustraft_admin_status_surface_evidence, rustraft_capability_evidence,
    rustraft_debug_bundle_validation_prometheus, rustraft_debug_snapshot,
    rustraft_debug_snapshot_json, rustraft_debug_snapshot_metadata_prometheus,
    rustraft_diagnostic_log_prometheus, rustraft_operator_runbook_prometheus,
    rustraft_optimization_report, rustraft_optimization_report_prometheus,
    rustraft_runtime_admin_report, rustraft_runtime_local_status_report,
    rustraft_validate_debug_snapshot, rustraft_validate_debug_snapshot_json, RaftCluster,
    RaftHealthStatus, RaftPeerPipelineState, RustRaftAdminStatusSurfaceInput,
    RustRaftDiagnosticLogEntry, RustRaftDiagnosticSeverity, RustRaftOperatorRunbookStep,
    RustRaftOptimizationHint, RustRaftOptimizationHintSeverity, RustRaftPeer,
    RustRaftPeerProgressState, RustRaftReplicaRole, RustRaftRole, RustRaftStatusSnapshot,
};
use serde_json::Value;

fn peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 6_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 7_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn ready_snapshot() -> matrixraft::RustRaftReadinessSnapshot {
    matrixraft::RustRaftReadinessSnapshot {
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
    }
}

fn pipeline_peer(peer_id: u64, match_index: u64, next_index: u64) -> RaftPeerPipelineState {
    RaftPeerPipelineState {
        peer_id,
        progress_state: RustRaftPeerProgressState::Probe,
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
    let status = RustRaftStatusSnapshot {
        group_id: 5,
        node_id: 1,
        role: RustRaftRole::Leader,
        term: 3,
        leader_id: Some(1),
        commit_index: 10,
        applied_index: 9,
        last_log_index: 10,
        last_snapshot_index: 4,
        peers: Vec::new(),
    };
    let pipeline = vec![RaftPeerPipelineState {
        peer_id: 2,
        progress_state: RustRaftPeerProgressState::Replicate,
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

    let report = rustraft_runtime_local_status_report(status, pipeline, ready_snapshot());
    assert_eq!(report.replication_health.status, RaftHealthStatus::Degraded);
    assert_eq!(report.apply_health.status, RaftHealthStatus::Degraded);
    assert!(report.blockers.contains(&"replication_lagging".to_string()));
    assert!(report.blockers.contains(&"apply_lagging".to_string()));
}

#[test]
fn admin_status_surface_evidence_accepts_quorum_progress_with_lagging_peer() {
    let input = RustRaftAdminStatusSurfaceInput {
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
    let evidence = rustraft_admin_status_surface_evidence(&input);
    assert!(evidence.complete, "{evidence:?}");
    assert!(evidence.quorum_peer_progress_observed);
    assert!(evidence.peer_pipeline_runtime_activity_observed);
    assert!(evidence.peer_pipeline_limits_observed);
    assert!(evidence.blockers.is_empty());
}

#[test]
fn admin_status_surface_evidence_fails_closed_on_missing_wal_or_quorum() {
    let input = RustRaftAdminStatusSurfaceInput {
        commit_index: 10,
        max_observed_node_commit_index: 11,
        quorum_size: 2,
        quorum_peer_ids: vec![1, 2, 3],
        peer_pipeline: vec![pipeline_peer(1, 10, 11), pipeline_peer(2, 8, 9)],
        wal_last_log_index: 9,
        wal_segment_lifecycle_present: false,
    };
    let evidence = rustraft_admin_status_surface_evidence(&input);
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

    let report = rustraft_optimization_report(&RustRaftAdminStatusSurfaceInput {
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
            && hint.severity == RustRaftOptimizationHintSeverity::Critical
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
fn optimization_report_is_ready_for_clean_admin_status_surface() {
    let report = rustraft_optimization_report(&RustRaftAdminStatusSurfaceInput {
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
    assert_eq!(report.health, RaftHealthStatus::Healthy);
    assert!(report.ready);
    assert_eq!(report.nodes.len(), 3);
}

#[test]
fn admin_report_genericizes_baseline_raft_parity_evidence_for_rustraft() {
    let mut cluster =
        RaftCluster::new(5, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"x".to_vec()).expect("write");
    let readiness = ready_snapshot();
    let capability_evidence = rustraft_capability_evidence(&readiness);
    let report = rustraft_runtime_admin_report(
        cluster.cluster_status_report().expect("cluster status"),
        readiness,
        capability_evidence,
    );

    assert!(report.ready);
    assert_eq!(report.health, RaftHealthStatus::Healthy);
    assert_eq!(report.public_api.transport_trait, "RaftTransport");
    assert!(report
        .capability_evidence
        .iter()
        .any(|item| item.capability == "leader_write_authority"));
    assert!(report
        .parity
        .baseline_raft_reference_policy
        .feature_reference
        .contains("BaselineRaft"));

    let entries = rustraft_admin_diagnostic_log_entries(&report);
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].target, "rustraft.admin");
    assert_eq!(entries[0].severity, RustRaftDiagnosticSeverity::Info);
    assert!(entries[0]
        .fields
        .contains(&("health".to_string(), "Healthy".to_string())));
    assert!(entries.iter().any(|entry| {
        entry.target == "rustraft.replication" && entry.message == "replication_healthy"
    }));
    assert!(entries
        .iter()
        .any(|entry| entry.target == "rustraft.apply" && entry.message == "apply_healthy"));

    let json_lines = rustraft_admin_diagnostic_json_lines(&report);
    let parsed = json_lines
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("diagnostic json line"))
        .collect::<Vec<_>>();
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[0]["target"], "rustraft.admin");
    assert_eq!(parsed[0]["severity"], "info");

    let status_surface = RustRaftAdminStatusSurfaceInput {
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
    let snapshot = rustraft_debug_snapshot(&report, &status_surface, &[("service", "raft-a")]);
    assert_eq!(snapshot.contract.name, "rustraft_debug_snapshot");
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
    let validation = rustraft_validate_debug_snapshot(&snapshot);
    assert!(validation.ready);
    assert_eq!(validation.issue_count, 0);
    let metadata_prometheus =
        rustraft_debug_snapshot_metadata_prometheus(&snapshot, &[("service", "raft\"a")]);
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
    let invalid_validation = rustraft_validate_debug_snapshot(&invalid_snapshot);
    assert!(!invalid_validation.ready);
    assert_eq!(
        invalid_validation.issues,
        vec!["contract_mismatch".to_string()]
    );
    let invalid_validation_metrics =
        rustraft_debug_bundle_validation_prometheus(&invalid_validation, &[("service", "raft\"a")]);
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
        rustraft_validate_debug_snapshot(&missing_timestamp_snapshot);
    assert!(!missing_timestamp_validation.ready);
    assert!(missing_timestamp_validation
        .issues
        .contains(&"generated_at_missing".to_string()));

    let mut future_timestamp_snapshot = snapshot.clone();
    future_timestamp_snapshot.generated_at_unix_ms = u64::MAX;
    let future_timestamp_validation = rustraft_validate_debug_snapshot(&future_timestamp_snapshot);
    assert!(!future_timestamp_validation.ready);
    assert!(future_timestamp_validation
        .issues
        .contains(&"generated_at_in_future".to_string()));

    let mut stale_timestamp_snapshot = snapshot.clone();
    stale_timestamp_snapshot.generated_at_unix_ms = 1;
    let stale_timestamp_validation = rustraft_validate_debug_snapshot(&stale_timestamp_snapshot);
    assert!(!stale_timestamp_validation.ready);
    assert!(stale_timestamp_validation
        .issues
        .contains(&"generated_at_stale".to_string()));
    let stale_timestamp_validation_metrics = rustraft_debug_bundle_validation_prometheus(
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
    let stale_validation = rustraft_validate_debug_snapshot(&stale_snapshot);
    assert!(!stale_validation.ready);
    assert!(stale_validation
        .issues
        .contains(&"triage_critical_count_mismatch".to_string()));

    let mut stale_diagnostic_snapshot = snapshot.clone();
    stale_diagnostic_snapshot.triage.diagnostic_warning_count = 13;
    let stale_diagnostic_validation = rustraft_validate_debug_snapshot(&stale_diagnostic_snapshot);
    assert!(!stale_diagnostic_validation.ready);
    assert!(stale_diagnostic_validation
        .issues
        .contains(&"triage_diagnostic_warning_count_mismatch".to_string()));

    let mut stale_triage_snapshot = snapshot.clone();
    stale_triage_snapshot.triage.first_action = "stale action".to_string();
    let stale_triage_validation = rustraft_validate_debug_snapshot(&stale_triage_snapshot);
    assert!(!stale_triage_validation.ready);
    assert!(stale_triage_validation
        .issues
        .contains(&"triage_contract_mismatch".to_string()));

    let mut stale_diagnostic_log_snapshot = snapshot.clone();
    stale_diagnostic_log_snapshot.diagnostics[0].message = "stale diagnostic".to_string();
    let stale_diagnostic_log_validation =
        rustraft_validate_debug_snapshot(&stale_diagnostic_log_snapshot);
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
        rustraft_validate_debug_snapshot(&missing_diagnostic_severity_snapshot);
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
        rustraft_validate_debug_snapshot(&missing_diagnostic_entry_snapshot);
    assert!(!missing_diagnostic_entry_validation.ready);
    assert!(missing_diagnostic_entry_validation
        .issues
        .contains(&"diagnostic_prometheus_entry_missing".to_string()));

    let mut escaped_diagnostic_snapshot = snapshot.clone();
    escaped_diagnostic_snapshot
        .diagnostics
        .push(RustRaftDiagnosticLogEntry {
            target: "rustraft.target\"with\\escape".to_string(),
            severity: RustRaftDiagnosticSeverity::Warn,
            message: "diagnostic\"message\\escaped".to_string(),
            fields: vec![(
                "detail".to_string(),
                "escaped labels stay valid".to_string(),
            )],
        });
    escaped_diagnostic_snapshot.diagnostic_prometheus =
        rustraft_diagnostic_log_prometheus(&escaped_diagnostic_snapshot.diagnostics, &[]);
    let escaped_diagnostic_validation =
        rustraft_validate_debug_snapshot(&escaped_diagnostic_snapshot);
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
        rustraft_validate_debug_snapshot(&stale_optimization_hint_count_snapshot);
    assert!(!stale_optimization_hint_count_validation.ready);
    assert!(stale_optimization_hint_count_validation
        .issues
        .contains(&"optimization_hint_count_mismatch".to_string()));

    let mut stale_optimization_warning_count_snapshot = snapshot.clone();
    stale_optimization_warning_count_snapshot
        .optimization
        .warning_count = 99;
    let stale_optimization_warning_count_validation =
        rustraft_validate_debug_snapshot(&stale_optimization_warning_count_snapshot);
    assert!(!stale_optimization_warning_count_validation.ready);
    assert!(stale_optimization_warning_count_validation
        .issues
        .contains(&"optimization_warning_count_mismatch".to_string()));

    let mut stale_optimization_ready_snapshot = snapshot.clone();
    stale_optimization_ready_snapshot
        .optimization
        .hints
        .push(RustRaftOptimizationHint {
            id: "critical_ready_mismatch".to_string(),
            severity: RustRaftOptimizationHintSeverity::Critical,
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
        rustraft_validate_debug_snapshot(&stale_optimization_ready_snapshot);
    assert!(!stale_optimization_ready_validation.ready);
    assert!(stale_optimization_ready_validation
        .issues
        .contains(&"optimization_ready_mismatch".to_string()));

    let mut stale_prometheus_count_snapshot = snapshot.clone();
    stale_prometheus_count_snapshot
        .optimization_prometheus
        .metric_count = 99;
    let stale_prometheus_count_validation =
        rustraft_validate_debug_snapshot(&stale_prometheus_count_snapshot);
    assert!(!stale_prometheus_count_validation.ready);
    assert!(stale_prometheus_count_validation
        .issues
        .contains(&"prometheus_metric_count_mismatch".to_string()));

    let mut missing_prometheus_metric_snapshot = snapshot.clone();
    missing_prometheus_metric_snapshot
        .optimization_prometheus
        .text = "rustraft_optimization_ready 1\n".to_string();
    let missing_prometheus_metric_validation =
        rustraft_validate_debug_snapshot(&missing_prometheus_metric_snapshot);
    assert!(!missing_prometheus_metric_validation.ready);
    assert!(missing_prometheus_metric_validation
        .issues
        .contains(&"prometheus_metric_contract_missing".to_string()));

    let mut missing_prometheus_hint_snapshot = snapshot.clone();
    missing_prometheus_hint_snapshot
        .optimization
        .hints
        .push(RustRaftOptimizationHint {
            id: "stale_missing_hint_metric".to_string(),
            severity: RustRaftOptimizationHintSeverity::Warning,
            component: "observability".to_string(),
            recommendation: "refresh Prometheus hint metrics".to_string(),
            observed_value: 1,
            threshold: 0,
        });
    missing_prometheus_hint_snapshot.optimization.hint_count += 1;
    let missing_prometheus_hint_validation =
        rustraft_validate_debug_snapshot(&missing_prometheus_hint_snapshot);
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
        .push(RustRaftOptimizationHint {
            id: "hint\"with\\escape".to_string(),
            severity: RustRaftOptimizationHintSeverity::Warning,
            component: "observability".to_string(),
            recommendation: "keep escaped labels valid".to_string(),
            observed_value: 1,
            threshold: 0,
        });
    escaped_hint_snapshot.optimization.hint_count += 1;
    escaped_hint_snapshot.optimization.warning_count += 1;
    escaped_hint_snapshot.optimization_prometheus =
        rustraft_optimization_report_prometheus(&escaped_hint_snapshot.optimization, &[]);
    let escaped_hint_validation = rustraft_validate_debug_snapshot(&escaped_hint_snapshot);
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
        rustraft_validate_debug_snapshot(&stale_grafana_contract_snapshot);
    assert!(!stale_grafana_contract_validation.ready);
    assert!(stale_grafana_contract_validation
        .issues
        .contains(&"grafana_contract_mismatch".to_string()));

    let mut stale_grafana_panel_snapshot = snapshot.clone();
    stale_grafana_panel_snapshot.grafana.panels[0].expr = "rustraft_ready == 0".to_string();
    let stale_grafana_panel_validation =
        rustraft_validate_debug_snapshot(&stale_grafana_panel_snapshot);
    assert!(!stale_grafana_panel_validation.ready);
    assert!(stale_grafana_panel_validation
        .issues
        .contains(&"grafana_panel_contract_mismatch".to_string()));

    let mut missing_grafana_panel_snapshot = snapshot.clone();
    missing_grafana_panel_snapshot
        .grafana
        .panels
        .retain(|panel| panel.id != 17);
    let missing_grafana_panel_validation =
        rustraft_validate_debug_snapshot(&missing_grafana_panel_snapshot);
    assert!(!missing_grafana_panel_validation.ready);
    assert!(missing_grafana_panel_validation
        .issues
        .contains(&"grafana_panel_contract_missing".to_string()));

    let mut stale_runbook_snapshot = snapshot.clone();
    stale_runbook_snapshot.runbook_steps[0].action = "do something else".to_string();
    let stale_runbook_validation = rustraft_validate_debug_snapshot(&stale_runbook_snapshot);
    assert!(!stale_runbook_validation.ready);
    assert!(stale_runbook_validation
        .issues
        .contains(&"runbook_step_contract_mismatch".to_string()));

    let mut missing_runbook_prometheus_snapshot = snapshot.clone();
    missing_runbook_prometheus_snapshot.runbook_prometheus.text =
        "rustraft_operator_runbook_step_total 1\n".to_string();
    let missing_runbook_prometheus_validation =
        rustraft_validate_debug_snapshot(&missing_runbook_prometheus_snapshot);
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
        rustraft_validate_debug_snapshot(&missing_runbook_first_step_snapshot);
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
        .push(RustRaftOperatorRunbookStep {
            id: "step\"with\\escape".to_string(),
            severity: "warning\\level".to_string(),
            target: "runbook\"target\\escaped".to_string(),
            action: "keep escaped runbook labels valid".to_string(),
            validation: "validator still recognizes escaped runbook metrics".to_string(),
        });
    escaped_runbook_snapshot.runbook_prometheus =
        rustraft_operator_runbook_prometheus(&escaped_runbook_snapshot.runbook_steps, &[]);
    let escaped_runbook_validation = rustraft_validate_debug_snapshot(&escaped_runbook_snapshot);
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
    let extra_runbook_validation = rustraft_validate_debug_snapshot(&extra_runbook_snapshot);
    assert!(!extra_runbook_validation.ready);
    assert!(extra_runbook_validation
        .issues
        .contains(&"runbook_step_count_mismatch".to_string()));

    let mut stale_alert_contract_snapshot = snapshot.clone();
    stale_alert_contract_snapshot.alerts[0].expr = "rustraft_optimization_ready < 0".to_string();
    let stale_alert_contract_validation =
        rustraft_validate_debug_snapshot(&stale_alert_contract_snapshot);
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
        rustraft_validate_debug_snapshot(&missing_alert_contract_snapshot);
    assert!(!missing_alert_contract_validation.ready);
    assert!(missing_alert_contract_validation
        .issues
        .contains(&"alert_rule_contract_missing".to_string()));

    let mut stale_top_alert_snapshot = snapshot.clone();
    stale_top_alert_snapshot.triage.top_alert = Some("MissingRustRaftAlert".to_string());
    let stale_top_alert_validation = rustraft_validate_debug_snapshot(&stale_top_alert_snapshot);
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
        rustraft_validate_debug_snapshot(&stale_top_diagnostic_snapshot);
    assert!(!stale_top_diagnostic_validation.ready);
    assert!(stale_top_diagnostic_validation
        .issues
        .contains(&"triage_top_diagnostic_missing".to_string()));

    let mut incomplete_top_diagnostic_snapshot = snapshot.clone();
    incomplete_top_diagnostic_snapshot
        .triage
        .top_diagnostic_message = None;
    let incomplete_top_diagnostic_validation =
        rustraft_validate_debug_snapshot(&incomplete_top_diagnostic_snapshot);
    assert!(!incomplete_top_diagnostic_validation.ready);
    assert!(incomplete_top_diagnostic_validation
        .issues
        .contains(&"triage_top_diagnostic_incomplete".to_string()));

    let mut stale_top_hint_snapshot = snapshot.clone();
    stale_top_hint_snapshot.triage.top_optimization_hint =
        Some("missing_optimization_hint".to_string());
    let stale_top_hint_validation = rustraft_validate_debug_snapshot(&stale_top_hint_snapshot);
    assert!(!stale_top_hint_validation.ready);
    assert!(stale_top_hint_validation
        .issues
        .contains(&"triage_top_optimization_hint_missing".to_string()));

    let mut stale_severity_snapshot = snapshot.clone();
    stale_severity_snapshot.triage.severity = "warning".to_string();
    let stale_severity_validation = rustraft_validate_debug_snapshot(&stale_severity_snapshot);
    assert!(!stale_severity_validation.ready);
    assert!(stale_severity_validation
        .issues
        .contains(&"triage_ready_severity_mismatch".to_string()));

    let mut missing_ready_runbook_snapshot = snapshot.clone();
    missing_ready_runbook_snapshot.runbook_steps.clear();
    let missing_ready_runbook_validation =
        rustraft_validate_debug_snapshot(&missing_ready_runbook_snapshot);
    assert!(!missing_ready_runbook_validation.ready);
    assert!(missing_ready_runbook_validation
        .issues
        .contains(&"runbook_ready_step_missing".to_string()));

    let snapshot_json =
        rustraft_debug_snapshot_json(&report, &status_surface, &[("service", "raft-a")]);
    let json_validation = rustraft_validate_debug_snapshot_json(&snapshot_json);
    assert!(json_validation.ready);
    assert_eq!(json_validation.issue_count, 0);
    let invalid_json_validation = rustraft_validate_debug_snapshot_json("{not-json");
    assert!(!invalid_json_validation.ready);
    assert_eq!(
        invalid_json_validation.issues,
        vec!["json_parse_failed".to_string()]
    );
    let mut stale_contract_json: Value =
        serde_json::from_str(&snapshot_json).expect("debug snapshot json for mutation");
    stale_contract_json["contract"]["schema"] = Value::String("rustraft.debug_snapshot.old".into());
    let stale_contract_validation =
        rustraft_validate_debug_snapshot_json(&stale_contract_json.to_string());
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
