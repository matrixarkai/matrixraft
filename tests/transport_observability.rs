// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_baseline_raft_benchmark_metric_names,
    metrics::{
        matrixraft_alert_rules, matrixraft_alert_rules_json, matrixraft_diagnostic_log_prometheus,
        matrixraft_grafana_dashboard, matrixraft_grafana_dashboard_json,
        matrixraft_membership_readiness_metric_names, matrixraft_memory_metric_names,
        matrixraft_metric_names, matrixraft_observability_provisioning,
        matrixraft_observability_provisioning_json,
        matrixraft_observability_provisioning_runbook_steps,
        matrixraft_observability_provisioning_validation_prometheus,
        matrixraft_observability_required_metric_names, matrixraft_operator_runbook_prometheus,
        matrixraft_operator_runbook_steps, matrixraft_operator_runbook_steps_with_diagnostics,
        matrixraft_operator_triage_prometheus, matrixraft_operator_triage_summary,
        matrixraft_optimization_report_prometheus, matrixraft_production_readiness_metric_names,
        matrixraft_queue_pressure_metric_names, matrixraft_runtime_pressure_metric_names,
        matrixraft_scale_metric_names, matrixraft_scale_target_metric_names,
        matrixraft_validate_observability_provisioning,
        matrixraft_validate_observability_provisioning_json,
        matrixraft_validate_required_metric_scrape_texts, matrixraft_wal_lifecycle_metric_names,
    },
    readiness::{
        matrixraft_baseline_raft_parity_surface, matrixraft_parity_report,
        matrixraft_public_api_contract, ReadinessSnapshot,
    },
    status::{
        matrixraft_fatal_blocker_report, BlockerSeverity, DiagnosticLogEntry, DiagnosticSeverity,
        OptimizationHint, OptimizationHintSeverity, OptimizationReport,
    },
    transport::{
        AppendEntriesRequest, InstallSnapshotRequest, PreVoteRequest, PreVoteResponse,
        ReadIndexRequest, SnapshotChunk, VoteRequest,
    },
    DebugBundleValidationReport, InstallSnapshotResponse, LogId, SnapshotMetadata,
};
use serde_json::Value;

fn ready_snapshot() -> ReadinessSnapshot {
    ReadinessSnapshot {
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

#[test]
fn transport_contract_names_owned_rpc_types_including_prevote_and_snapshot_chunks() {
    let append = AppendEntriesRequest {
        group_id: 9,
        term: 4,
        leader_id: 1,
        prev_log_id: Some(LogId { term: 4, index: 9 }),
        entries: Vec::new(),
        leader_commit: 9,
        lease_epoch: 0,
    };
    assert_eq!(append.leader_id, 1);

    let vote = VoteRequest {
        group_id: 9,
        term: 4,
        candidate_id: 2,
        last_log_id: None,
        pre_vote: false,
        force: false,
    };
    assert!(!vote.pre_vote);

    let pre_vote = PreVoteRequest {
        pre_vote: true,
        ..vote.clone()
    };
    assert!(pre_vote.pre_vote);

    let pre_vote_response = PreVoteResponse {
        term: 4,
        vote_granted: true,
        reason: "pre_vote_granted".to_string(),
    };
    assert!(pre_vote_response.vote_granted);

    let chunk = SnapshotChunk {
        meta: SnapshotMetadata {
            snapshot_id: "transport-observability".to_string(),
            last_log_id: LogId { term: 4, index: 10 },
            membership: vec![1, 2, 3],
            members: Vec::new(),
        },
        offset: 0,
        data: b"chunk".to_vec(),
        done: true,
    };
    let install = InstallSnapshotRequest {
        group_id: 9,
        term: 4,
        leader_id: 1,
        chunk,
    };
    assert!(install.chunk.done);
    let install_response = InstallSnapshotResponse {
        term: 4,
        accepted: true,
        next_offset: 5,
        committed_index: 0,
        reason: "installed".to_string(),
    };
    assert!(install_response.accepted);

    let read = ReadIndexRequest {
        group_id: 9,
        requester_id: 1,
        min_commit_index: 10,
        allow_lease_read: true,
    };
    assert!(read.allow_lease_read);
}

#[test]
fn observability_contract_exports_metrics_parity_readiness_and_blocker_reports() {
    let metrics = matrixraft_metric_names();
    assert_eq!(metrics.pre_vote_latency_ms, "rustraft_pre_vote_latency_ms");
    assert_eq!(metrics.blocker_total, "rustraft_blocker_total");
    assert_eq!(metrics.fatal_total, "rustraft_fatal_total");
    assert_eq!(
        metrics.diagnostic_log_total,
        "rustraft_diagnostic_log_total"
    );
    assert_eq!(
        metrics.diagnostic_log_entry_total,
        "rustraft_diagnostic_log_entry_total"
    );
    assert_eq!(metrics.optimization_ready, "rustraft_optimization_ready");
    assert_eq!(
        metrics.optimization_critical_total,
        "rustraft_optimization_critical_total"
    );
    assert_eq!(
        metrics.optimization_warning_total,
        "rustraft_optimization_warning_total"
    );
    assert_eq!(
        metrics.optimization_hint_total,
        "rustraft_optimization_hint_total"
    );
    assert_eq!(
        metrics.optimization_component_hint_total,
        "rustraft_optimization_component_hint_total"
    );
    assert_eq!(
        metrics.operator_triage_status,
        "rustraft_operator_triage_status"
    );
    assert_eq!(
        metrics.operator_triage_diagnostic_error_total,
        "rustraft_operator_triage_diagnostic_error_total"
    );
    assert_eq!(
        metrics.operator_triage_diagnostic_warning_total,
        "rustraft_operator_triage_diagnostic_warning_total"
    );
    assert_eq!(
        metrics.operator_triage_optimization_warning_total,
        "rustraft_operator_triage_optimization_warning_total"
    );
    assert_eq!(
        metrics.operator_triage_alert_rule_total,
        "rustraft_operator_triage_alert_rule_total"
    );
    assert_eq!(
        metrics.operator_triage_top_alert,
        "rustraft_operator_triage_top_alert"
    );
    assert_eq!(
        metrics.operator_triage_first_action,
        "rustraft_operator_triage_first_action"
    );
    assert_eq!(
        metrics.operator_triage_top_diagnostic,
        "rustraft_operator_triage_top_diagnostic"
    );
    assert_eq!(
        metrics.operator_triage_top_optimization_hint,
        "rustraft_operator_triage_top_optimization_hint"
    );
    assert_eq!(
        metrics.operator_runbook_step_total,
        "rustraft_operator_runbook_step_total"
    );
    assert_eq!(
        metrics.operator_runbook_step_present,
        "rustraft_operator_runbook_step_present"
    );
    assert_eq!(
        metrics.operator_runbook_first_step,
        "rustraft_operator_runbook_first_step"
    );
    assert_eq!(
        metrics.debug_snapshot_generated_at_unix_ms,
        "rustraft_debug_snapshot_generated_at_unix_ms"
    );
    assert_eq!(
        metrics.debug_snapshot_age_ms,
        "rustraft_debug_snapshot_age_ms"
    );
    assert_eq!(
        metrics.debug_snapshot_max_age_ms,
        "rustraft_debug_snapshot_max_age_ms"
    );
    assert_eq!(
        metrics.debug_snapshot_stale_after_unix_ms,
        "rustraft_debug_snapshot_stale_after_unix_ms"
    );
    assert_eq!(
        metrics.debug_snapshot_remaining_fresh_ms,
        "rustraft_debug_snapshot_remaining_fresh_ms"
    );
    assert_eq!(
        metrics.debug_snapshot_low_fresh_ms,
        "rustraft_debug_snapshot_low_fresh_ms"
    );
    assert_eq!(
        metrics.debug_snapshot_low_fresh,
        "rustraft_debug_snapshot_low_fresh"
    );
    assert_eq!(
        metrics.debug_snapshot_fresh,
        "rustraft_debug_snapshot_fresh"
    );

    let api = matrixraft_public_api_contract();
    assert!(api.rpc_messages.contains(&"PreVoteRequest".to_string()));
    assert!(api.rpc_messages.contains(&"PreVoteResponse".to_string()));
    assert!(api.rpc_messages.contains(&"SnapshotChunk".to_string()));
    assert!(api
        .rpc_messages
        .contains(&"TransportValidationReport".to_string()));
    assert!(api
        .rpc_messages
        .contains(&"InMemoryRaftTransport".to_string()));
    assert!(api.public_modules.contains(&"mailbox".to_string()));
    assert!(api.public_modules.contains(&"channel_selector".to_string()));
    assert!(api.core_interfaces.contains(&"MailBox".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"MailBoxPressureStats".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"MailBox::try_send_checked".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"MailBox::try_send_many_checked".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"MailBox::pressure_stats_checked".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"MailBox::fetch_checked".to_string()));
    assert!(api.core_interfaces.contains(&"MailChannel".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"MailChannelPressureStats".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"MailChannel::try_send_checked".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"MailChannel::try_send_many_checked".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"MailChannel::pressure_stats_checked".to_string()));
    assert!(api.core_interfaces.contains(&"ChannelSelector".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"ChannelSelector::try_send_many_to_channel_checked".to_string()));
    assert!(api
        .core_interfaces
        .contains(&"ChannelSelector::select_checked".to_string()));
    assert!(api
        .safety_helpers
        .contains(&"matrixraft_fatal_blocker_report".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_scale_target_grafana_panels".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_grafana_dashboard_json".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_queue_pressure_metric_names".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_queue_pressure_prometheus".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_queue_pressure_grafana_panels".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_runtime_pressure_admission_with_queue_pressure".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"QueuePressureMetricNames".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"QueuePressureThresholds".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"QueuePressureDetail".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_node_runtime_status_prometheus".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_node_runtime_grafana_panels".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"ReleasePressureSnapshot".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_release_pressure_snapshot_json".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_release_pressure_snapshot_from_json_bytes".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_release_pressure_snapshot_from_json".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_validate_release_pressure_snapshot".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_read_release_pressure_snapshot".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_write_release_pressure_snapshot_atomic".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_optimization_diagnostic_log_entries".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_local_status_diagnostic_log_entries".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_local_status_diagnostic_json_lines".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_node_runtime_status_diagnostic_log_entries".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_node_runtime_status_diagnostic_json_lines".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_diagnostic_log_prometheus".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_membership_readiness_diagnostic_log_entries".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_membership_readiness_diagnostic_json_lines".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_production_readiness_diagnostic_log_entries".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_production_readiness_diagnostic_json_lines".to_string()));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "Config"
            && mapping.raft_rs_or_tikv_reference.contains("raft::Config")
            && mapping.note.contains("log-buffer")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "AdminCommand::ReleaseMemory"
            && mapping.matrixraft_facade == "MatrixRaftAdminCommandType::ReleaseMemory"
            && mapping.note.contains("memory pressure")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "DiagnosticLogEntry"
            && mapping.raft_rs_or_tikv_reference.contains("structured log")
            && mapping.note.contains("Prometheus counters")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "RuntimePressureAdmission"
            && mapping.raft_rs_or_tikv_reference.contains("backpressure")
            && mapping.note.contains("peer pipeline telemetry")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "QueuePressureMetricNames"
            && mapping
                .byteraft_or_baseline_reference
                .contains("scheduler queue pressure metrics")
            && mapping.note.contains("QPS and memory-pressure dashboards")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_queue_pressure_prometheus"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("mailbox and peer queue scrape")
            && mapping.note.contains("rejected enqueue counters")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_queue_pressure_grafana_panels"
            && mapping
                .byteraft_or_baseline_reference
                .contains("queue pressure dashboard panels")
            && mapping.note.contains("release-scale tuning")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "QueuePressureThresholds"
            && mapping
                .byteraft_or_baseline_reference
                .contains("queue saturation")
            && mapping.note.contains("queue-aware runtime admission")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "QueuePressureDetail"
            && mapping.note.contains("bottleneck ranking")
            && mapping.note.contains("diagnostic logs")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_runtime_pressure_admission_with_queue_pressure"
            && mapping
                .byteraft_or_baseline_reference
                .contains("queue-aware admission gate")
            && mapping.note.contains("fail closed")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "MailBox::try_send_checked"
            && mapping.raft_rs_or_tikv_reference.contains("backpressure")
            && mapping.note.contains("without process aborts")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "MailBox::try_send_many_checked"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("bounded mailbox batch enqueue")
            && mapping.note.contains("high QPS")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "MailBoxPressureStats"
            && mapping
                .byteraft_or_baseline_reference
                .contains("queue pressure snapshot")
            && mapping.note.contains("rejected enqueue count")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "MailBox::pressure_stats_checked"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("checked mailbox pressure read")
            && mapping.note.contains("runtime queue telemetry")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "MailChannel::try_send_many_checked"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("bounded per-peer batch enqueue")
            && mapping.note.contains("memory pressure bounded")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "MailChannelPressureStats"
            && mapping
                .byteraft_or_baseline_reference
                .contains("per-replica queue pressure snapshot")
            && mapping.note.contains("selector-visible total")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "MailChannel::pressure_stats_checked"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("checked per-peer pressure read")
            && mapping.note.contains("per-peer burst pressure")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "ChannelSelector::select_checked"
            && mapping
                .byteraft_or_baseline_reference
                .contains("checked ready-queue selector")
            && mapping.note.contains("deadline-bound polling")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "ChannelSelector::try_send_many_to_channel_checked"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("ready-peer notification")
            && mapping.note.contains("bounded fanout enqueue")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_runtime_pressure_admission_with_scale_targets"
            && mapping.raft_rs_or_tikv_reference.contains("QPS")
            && mapping.note.contains("production parity")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_runtime_pressure_admission_with_pipeline_pressure"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("per-peer Progress")
            && mapping.note.contains("fail-closed production guard")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("scheduler backpressure")
            && mapping.note.contains("pending-tick queue utilization")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_validate_runtime_pressure_admission_evidence_with_policy"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("flow-control policy")
            && mapping.note.contains("fail-closed pressure priority")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_observability_provisioning_runbook_steps"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("runbook checklist")
            && mapping.note.contains("benchmark freshness")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_operator_runbook_steps_with_diagnostics"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("structured logs and alerts")
            && mapping.note.contains("first corrective action")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_operator_runbook_prometheus"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("Prometheus scrape")
            && mapping.note.contains("first-step signals")
    }));

    let surface = matrixraft_baseline_raft_parity_surface();
    assert!(surface.transport_api.contains(&"pre_vote_rpc".to_string()));
    assert!(surface
        .transport_api
        .contains(&"install_snapshot_chunk_rpc".to_string()));
    assert!(surface
        .transport_api
        .contains(&"request_response_validation".to_string()));
    assert!(surface
        .transport_api
        .contains(&"in_memory_transport".to_string()));
    assert!(surface
        .transport_api
        .contains(&"tcp_reference_transport".to_string()));
    assert!(surface
        .observability_api
        .contains(&"blocker_report".to_string()));
    assert!(surface
        .observability_api
        .contains(&"readiness_report".to_string()));

    let readiness = matrixraft_parity_report(&ready_snapshot());
    assert!(readiness.ready);

    let blockers = matrixraft_fatal_blocker_report(
        "rustraft_transport_observability",
        vec!["leader_unavailable".to_string(), "wal_corrupt".to_string()],
        vec!["wal_corrupt".to_string()],
    );
    assert!(!blockers.ready);
    assert!(blockers.fatal);
    assert_eq!(blockers.blocker_count, 2);
    assert_eq!(blockers.fatal_count, 1);
    assert_eq!(
        blockers
            .blockers
            .iter()
            .find(|blocker| blocker.id == "wal_corrupt")
            .expect("fatal blocker")
            .severity,
        BlockerSeverity::Fatal
    );
}

#[test]
fn grafana_dashboard_exports_runtime_metric_panels() {
    let metrics = matrixraft_metric_names();
    let benchmark_metrics = matrixraft_baseline_raft_benchmark_metric_names();
    let runtime_pressure_metrics = matrixraft_runtime_pressure_metric_names();
    let queue_pressure_metrics = matrixraft_queue_pressure_metric_names();
    let membership_readiness_metrics = matrixraft_membership_readiness_metric_names();
    let production_readiness_metrics = matrixraft_production_readiness_metric_names();
    let dashboard = matrixraft_grafana_dashboard();
    assert_eq!(dashboard.uid, "rustraft-runtime-overview");
    assert_eq!(dashboard.refresh, "10s");
    // The count is pinned so a panel cannot appear or vanish unnoticed. It went 53 -> 55 when
    // the support envelope panels landed, 55 -> 61 when scale/QPS panels joined the canonical
    // dashboard, 61 -> 66 when memory panels joined, 66 -> 78 when release scale target
    // panels joined, 78 -> 83 when runtime pressure panels joined, 83 -> 91 when
    // production readiness panels joined, 91 -> 102 when benchmark parity panels
    // joined, 102 -> 103 when scale pressure admission joined, 103 -> 104
    // when peer reorder convergence became visible, 104 -> 108 when
    // benchmark CPU and peak-memory parity ratios became visible,
    // 108 -> 118 when runtime pressure detail panels became visible,
    // 118 -> 132 when snapshot lifecycle evidence panels joined,
    // 132 -> 139 when WAL lifecycle evidence panels joined, and
    // 139 -> 145 when membership readiness panels joined, 145 -> 151
    // when node-runtime timer panels joined, 151 -> 155 when runtime
    // pipeline-pressure admission panels joined, 155 -> 159 when
    // read-backlog admission panels joined, 159 -> 163 when latency-pressure
    // p95 and sample-count panels joined, 163 -> 164 when runtime pressure
    // action-source provenance joined, 164 -> 165 when sustained snapshot
    // transfer completion joined, 165 -> 166 when node-runtime timer
    // utilization joined, 170 -> 171 when runtime-pressure bottleneck
    // scoring joined, 171 -> 172 when production-readiness pressure
    // bottleneck scoring joined, 172 -> 174 when snapshot lifecycle
    // peer-count scale panels joined, 174 -> 176 when WAL slow-fsync
    // compaction count panels joined, 176 -> 187 when runtime-pressure
    // freshness panels joined, 187 -> 194 when benchmark artifact freshness
    // panels joined, 194 -> 202 when public API validation panels joined, and
    // 202 -> 206 when queue-pressure panels joined;
    // naming them keeps the number from being a figure nobody can check.
    assert_eq!(dashboard.panels.len(), 206);
    let panel_ids = dashboard
        .panels
        .iter()
        .map(|panel| panel.id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        panel_ids.len(),
        dashboard.panels.len(),
        "Grafana panel ids must remain unique so debug-bundle validation can match panels deterministically"
    );
    for title in [
        "Support Envelope Status",
        "Support Envelope Severity",
        "Proposal QPS",
        "AppendEntries QPS",
        "ReadIndex QPS",
        "Apply QPS",
        "Replication MB/s",
        "Apply MB/s",
        "Resident Memory",
        "Heap Allocated",
        "Log Cache Memory",
        "Snapshot Buffer Memory",
        "Replication Buffer Memory",
        "Queue Pressure Depth",
        "Queue Pressure Rejections",
        "Queue Pressure Limits",
        "Queue Pressure Max Depth",
        "Runtime Admission Accepted",
        "Runtime Admission Rejected",
        "Runtime Memory Pressure",
        "Runtime Latency Pressure",
        "Runtime Scale Pressure",
        "Runtime Pressure Actions",
        "Runtime Pressure Action Sources",
        "Runtime Memory Pressure Observed",
        "Runtime Memory Pressure Excess",
        "Runtime Memory Pressure Thresholds",
        "Runtime Latency Pressure P99",
        "Runtime Latency Pressure P95",
        "Runtime Latency Pressure Samples",
        "Runtime Latency Pressure Excess",
        "Runtime Latency Pressure Thresholds",
        "Runtime Scale Pressure Observed",
        "Runtime Scale Pressure Target",
        "Runtime Scale Pressure Deficit",
        "Runtime Scale Target Percent",
        "Runtime Read Backlog Pressure",
        "Runtime Read Backlog Detail",
        "Runtime Read Backlog Excess",
        "Runtime Read Backlog Threshold",
        "Snapshot Sender Lifecycle",
        "Snapshot Downloader Lifecycle",
        "Snapshot Sustained Sender Load",
        "Snapshot Sustained Downloader Load",
        "Snapshot Sender Completion",
        "Snapshot Downloader Completion",
        "Snapshot Sustained Transfer Completion",
        "Snapshot Lifecycle Peers",
        "Snapshot Sustained Transfer Completed Peers",
        "Snapshot Rejoin After Compacted Log",
        "WAL Segment Lifecycle",
        "WAL Compaction Observed",
        "WAL Slow Fsync Backpressure",
        "WAL Compaction After Slow Fsync",
        "Membership Readiness Ready",
        "Membership Readiness Satisfied",
        "Membership Readiness Missing",
        "Membership Transition Ready",
        "Membership Transition Missing",
        "Membership Missing Evidence",
        "Public API Contract Ready",
        "Public API Mapping Coverage",
        "Public API Interface Names",
        "Public API Reference Required",
        "Public API Unmapped Required",
        "Public API Unmapped Advertised",
        "Public API Category Coverage",
        "Public API Category Unmapped",
        "Production Readiness Ready",
        "Production Readiness Satisfied",
        "Production Readiness Missing",
        "Production Readiness Blockers",
        "Production Readiness Next Actions",
        "Production Missing Evidence",
        "Production Blocker Detail",
        "Production Next Action Detail",
        "BaselineRaft Benchmark Passed",
        "Benchmark Production Evidence Ready",
        "Benchmark Freshness",
        "Benchmark Age",
        "Benchmark Generated At",
        "Benchmark Fresh",
        "Benchmark Max Age",
        "Benchmark Stale After",
        "Benchmark Freshness Remaining",
        "Benchmark Failed Workloads",
        "Benchmark Blockers",
        "Benchmark Worst P50 Ratio",
        "Benchmark Worst P99 Ratio",
        "Benchmark Worst Throughput Ratio",
        "Benchmark Workload Passed",
        "Benchmark Workload P50 Ratio",
        "Benchmark Workload P99 Ratio",
        "Benchmark Workload Throughput Ratio",
        "Node Runtime Timer Utilization",
        "Peer Reorder Converged Entries",
    ] {
        assert!(
            dashboard.panels.iter().any(|panel| panel.title == title),
            "dashboard must expose the {title} panel"
        );
    }
    assert!(dashboard.tags.contains(&"rustraft".to_string()));

    let expressions = dashboard
        .panels
        .iter()
        .map(|panel| panel.expr.as_str())
        .collect::<Vec<_>>();
    for metric in [
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
        runtime_pressure_metrics.admission_accepted,
        runtime_pressure_metrics.admission_rejected,
        runtime_pressure_metrics.memory_pressure,
        runtime_pressure_metrics.memory_pressure_threshold_value,
        runtime_pressure_metrics.memory_pressure_excess,
        runtime_pressure_metrics.latency_pressure,
        runtime_pressure_metrics.latency_pressure_sample_count,
        runtime_pressure_metrics.latency_pressure_observed_p95_ms,
        runtime_pressure_metrics.latency_pressure_threshold_p99_ms,
        runtime_pressure_metrics.latency_pressure_excess_ms,
        runtime_pressure_metrics.scale_pressure,
        runtime_pressure_metrics.scale_pressure_deficit,
        runtime_pressure_metrics.scale_pressure_target_percent,
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
        queue_pressure_metrics.mailbox_total_len,
        queue_pressure_metrics.mailbox_rejected_send_total,
        queue_pressure_metrics.mail_channel_queued_len,
        queue_pressure_metrics.mail_channel_rejected_send_total,
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
        production_readiness_metrics.next_action_present,
        benchmark_metrics.passed,
        benchmark_metrics.production_evidence_ready,
        benchmark_metrics.failed_workload_total,
        benchmark_metrics.blocker_total,
        benchmark_metrics.worst_p50_ratio,
        benchmark_metrics.worst_p99_ratio,
        benchmark_metrics.worst_throughput_ratio,
        benchmark_metrics.workload_passed,
        benchmark_metrics.workload_p50_ratio,
        benchmark_metrics.workload_p99_ratio,
        benchmark_metrics.workload_throughput_ratio,
    ] {
        assert!(
            expressions.iter().any(|expr| expr.contains(&metric)),
            "dashboard missing metric {metric}"
        );
    }
    assert!(expressions.contains(&"sum by (issue) (rustraft_debug_bundle_validation_issue)"));
    assert!(expressions.contains(
        &"sum by (issue) (rustraft_debug_bundle_validation_issue{artifact=\"support_envelope\"})"
    ));
    assert!(expressions.contains(
        &"sum by (freshness_status) (rustraft_debug_bundle_validation_ready{artifact=\"support_envelope\"})"
    ));
    assert!(expressions
        .contains(&"sum by (target, severity, message) (rustraft_diagnostic_log_entry_total)"));
    assert!(expressions
        .contains(&"sum by (hint, component, severity) (rustraft_optimization_hint_total)"));
    assert!(expressions
        .contains(&"sum by (step, severity, target) (rustraft_operator_runbook_step_present)"));
    assert!(expressions
        .contains(&"sum by (issue) (rustraft_observability_provisioning_validation_issue)"));
    assert!(expressions
        .contains(&"sum by (service, group, workload) (rate(rustraft_proposal_total[1m]))"));
    assert!(expressions
        .contains(&"sum by (service, group, workload) (rate(rustraft_append_entries_total[1m]))"));
    assert!(expressions
        .contains(&"sum by (service, group, workload) (rate(rustraft_read_index_total[1m]))"));
    assert!(expressions
        .contains(&"sum by (service, group, workload) (rate(rustraft_apply_entries_total[1m]))"));
    assert!(expressions.contains(
        &"sum by (service, group, workload) (rate(rustraft_replication_bytes_total[1m])) / 1048576"
    ));
    assert!(expressions.contains(
        &"sum by (service, group, workload) (rate(rustraft_apply_bytes_total[1m])) / 1048576"
    ));

    let json = matrixraft_grafana_dashboard_json();
    let parsed: Value = serde_json::from_str(&json).expect("dashboard json");
    assert_eq!(parsed["title"], "RustRaft Runtime Overview");
    // Same pin, checked through the serialized JSON: the struct and the exported document
    // must agree on how many panels there are.
    // 194 -> 202 when public API validation panels joined, 202 -> 206 when
    // queue-pressure panels joined.
    assert_eq!(parsed["panels"].as_array().expect("panels").len(), 206);
    assert!(json.contains("histogram_quantile(0.99"));
    assert!(json.contains("rustraft_blocker_total"));
    assert!(json.contains("rustraft_fatal_total"));
    assert!(json.contains("rustraft_diagnostic_log_total"));
    assert!(json.contains("rustraft_diagnostic_log_entry_total"));
    assert!(json.contains("rustraft_process_resident_memory_bytes"));
    assert!(json.contains("rustraft_replication_buffer_bytes"));
    assert!(json.contains("rustraft_mailbox_total_len"));
    assert!(json.contains("rustraft_mail_channel_rejected_send_total"));
    assert!(json.contains("rustraft_runtime_pressure_admission_rejected"));
    assert!(json.contains("rustraft_runtime_pressure_bottleneck_score_percent"));
    assert!(json.contains("Runtime Pressure Freshness"));
    assert!(json.contains("rustraft_runtime_pressure_freshness_status"));
    assert!(json.contains("rustraft_runtime_pressure_freshness_low_fresh"));
    assert!(
        json.contains("rustraft_production_readiness_runtime_pressure_bottleneck_score_percent")
    );
    assert!(json.contains("rustraft_runtime_pressure_scale"));
    assert!(json.contains("rustraft_runtime_pressure_pipeline"));
    assert!(json.contains("rustraft_node_runtime_timer_pending_ticks"));
    assert!(json.contains("rustraft_node_runtime_timer_max_pending_ticks"));
    assert!(json.contains("rustraft_node_runtime_timer_accepted_ticks_total"));
    assert!(json.contains("rustraft_node_runtime_timer_completed_ticks_total"));
    assert!(json.contains("rustraft_node_runtime_timer_backpressure"));
    assert!(json.contains("rustraft_node_runtime_timer_rejected_ticks_total"));
    assert!(json.contains("rustraft_node_runtime_timer_utilization_percent"));
    assert!(json.contains("rustraft_peer_reorder_entries_converged_total"));
    assert!(json.contains("rustraft_production_readiness_blocker_total"));
    assert!(json.contains("rustraft_production_readiness_missing_present"));
    assert!(json.contains("rustraft_baseline_raft_benchmark_worst_p99_ratio"));
    assert!(json.contains("rustraft_baseline_raft_benchmark_freshness_status"));
    assert!(json.contains("rustraft_baseline_raft_benchmark_remaining_fresh_ms"));
    assert!(json.contains("rustraft_baseline_raft_benchmark_worst_cpu_ratio"));
    assert!(json.contains("rustraft_baseline_raft_benchmark_workload_peak_resident_memory_ratio"));
    assert!(json.contains("rustraft_baseline_raft_benchmark_workload_throughput_ratio"));
    assert!(json.contains("Benchmark Worst Throughput Ratio"));
    assert!(json.contains("rustraft_scale_target_min_proposal_qps"));
    assert!(json.contains("rustraft_scale_observed_proposal_target_percent"));
    assert!(json.contains("rustraft_optimization_ready"));
    assert!(json.contains("rustraft_optimization_critical_total"));
    assert!(json.contains("rustraft_optimization_warning_total"));
    assert!(json.contains("rustraft_optimization_hint_total"));
    assert!(json.contains("rustraft_optimization_component_hint_total"));
    assert!(json.contains("rustraft_operator_triage_status"));
    assert!(json.contains("rustraft_operator_triage_diagnostic_error_total"));
    assert!(json.contains("rustraft_operator_triage_diagnostic_warning_total"));
    assert!(json.contains("rustraft_operator_triage_optimization_critical_total"));
    assert!(json.contains("rustraft_operator_triage_optimization_warning_total"));
    assert!(json.contains("rustraft_operator_triage_alert_rule_total"));
    assert!(json.contains("rustraft_operator_triage_first_action"));
    assert!(json.contains("rustraft_operator_triage_top_diagnostic"));
    assert!(json.contains("rustraft_operator_triage_top_alert"));
    assert!(json.contains("rustraft_operator_triage_top_optimization_hint"));
    assert!(json.contains("rustraft_operator_runbook_step_total"));
    assert!(json.contains("rustraft_operator_runbook_step_present"));
    assert!(json.contains("rustraft_operator_runbook_first_step"));
    assert!(json.contains("rustraft_debug_snapshot_generated_at_unix_ms"));
    assert!(json.contains("rustraft_debug_snapshot_age_ms"));
    assert!(json.contains("rustraft_debug_snapshot_max_age_ms"));
    assert!(json.contains("rustraft_debug_snapshot_stale_after_unix_ms"));
    assert!(json.contains("rustraft_debug_snapshot_remaining_fresh_ms"));
    assert!(json.contains("rustraft_debug_snapshot_low_fresh_ms"));
    assert!(json.contains("rustraft_debug_snapshot_low_fresh"));
    assert!(json.contains("rustraft_debug_snapshot_fresh"));
    assert!(json.contains("refresh the debug artifact before this deadline"));
    assert!(json.contains("RustRaftDebugSnapshotFreshnessLow warns below the low-fresh threshold"));
    assert!(json.contains("RustRaftDebugSnapshotFreshnessLost can fire"));
    assert!(json.contains("RustRaftDebugSnapshotFreshnessLost fires when this drops to 0"));
    assert!(json.contains("Configured freshness window for debug snapshots"));
    assert!(json.contains("rustraft_debug_bundle_validation_ready"));
    assert!(json.contains("rustraft_debug_bundle_validation_issue_total"));
    assert!(json.contains("rustraft_debug_bundle_validation_issue"));
    assert!(json.contains("rustraft_debug_bundle_validation_first_issue"));
    assert!(json.contains("Support Envelope Freshness Status"));
    assert!(json.contains("sum by (freshness_status)"));
    assert!(json.contains("Support Envelope Status"));
    assert!(json.contains("sum by (support_envelope_status)"));
    assert!(json.contains("Support Envelope Severity"));
    assert!(json.contains("sum by (support_envelope_severity)"));
    assert!(json.contains("Support Envelope Validation Issues"));
    assert!(json.contains("Support Envelope Issue Breakdown"));
    assert!(json.contains("Support Envelope First Issue"));
    assert!(json.contains(
        "rustraft_debug_bundle_validation_issue_total{artifact=\\\"support_envelope\\\"}"
    ));
    assert!(
        json.contains("rustraft_debug_bundle_validation_issue{artifact=\\\"support_envelope\\\"}")
    );
    assert!(json.contains(
        "rustraft_debug_bundle_validation_first_issue{artifact=\\\"support_envelope\\\"}"
    ));
    assert!(json.contains("rustraft_observability_provisioning_validation_ready"));
    assert!(json.contains("rustraft_observability_provisioning_validation_issue_total"));
    assert!(json.contains("rustraft_observability_provisioning_validation_issue"));
    assert!(json.contains("rustraft_observability_provisioning_validation_first_issue"));
}

#[test]
fn alert_rules_export_operator_contract_for_readiness_and_blockers() {
    let rules = matrixraft_alert_rules();
    // Pinned for the same reason as the panel count: 15 -> 16 with the provisioning
    // validation alert, 16 -> 17 with the memory pressure alert, 17 -> 18 with the latency
    // pressure alert, 18 -> 19 with the runtime admission rejection alert, 19 -> 21 with
    // production readiness blocker/missing-evidence alerts, 21 -> 23 with benchmark
    // parity failure/regression alerts, 23 -> 24 with scale pressure alerting, 24 -> 25
    // with node-runtime timer backpressure alerting, 25 -> 27 with snapshot/WAL
    // lifecycle backpressure alerting, 27 -> 28 with peer-pipeline backpressure alerting,
    // 28 -> 29 with membership transition evidence alerting, 29 -> 30 with
    // benchmark CPU/memory resource-ratio alerting, 30 -> 31 with runtime
    // pipeline-pressure admission alerting, 31 -> 32 with read-backlog
    // admission alerting, 32 -> 33 with runtime pressure bottleneck
    // alerting before hard admission rejection, 33 -> 34 with
    // production-readiness runtime-pressure bottleneck alerting, 34 -> 37
    // with runtime-pressure freshness alerting for release-scale evidence, and
    // 37 -> 38 with benchmark artifact freshness alerting.
    assert_eq!(rules.len(), 38);
    assert!(
        rules
            .iter()
            .any(|rule| rule.alert == "RustRaftObservabilityProvisioningValidationFailed"),
        "the provisioning validation alert must be exported"
    );

    let optimization_ready = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftOptimizationNotReady")
        .expect("optimization readiness alert");
    assert_eq!(optimization_ready.expr, "rustraft_optimization_ready == 0");
    assert_eq!(optimization_ready.duration, "5m");
    assert_eq!(optimization_ready.severity, "warning");
    assert!(optimization_ready
        .summary
        .contains("resolve_critical_optimization_hints"));

    let critical_hints = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftOptimizationCriticalHints")
        .expect("critical hint alert");
    assert_eq!(
        critical_hints.expr,
        "rustraft_optimization_critical_total > 0"
    );
    assert_eq!(critical_hints.severity, "critical");
    assert!(critical_hints
        .summary
        .contains("resolve_critical_optimization_hints before rollout"));

    let warning_hints = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftOptimizationWarningHints")
        .expect("warning hint alert");
    assert_eq!(
        warning_hints.expr,
        "rustraft_optimization_warning_total > 0"
    );
    assert_eq!(warning_hints.duration, "10m");
    assert_eq!(warning_hints.severity, "warning");
    assert!(warning_hints.summary.contains("before rollout"));

    let memory_pressure = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftMemoryPressure")
        .expect("memory pressure alert");
    assert_eq!(memory_pressure.duration, "10m");
    assert_eq!(memory_pressure.severity, "warning");
    assert!(memory_pressure
        .expr
        .contains("rustraft_process_resident_memory_bytes >= 8589934592"));
    assert!(memory_pressure
        .expr
        .contains("rustraft_heap_allocated_bytes >= 4294967296"));
    assert!(memory_pressure
        .expr
        .contains("rustraft_replication_buffer_bytes >= 1073741824"));
    assert!(memory_pressure.summary.contains("memory_prometheus"));

    let latency_pressure = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftLatencyPressure")
        .expect("latency pressure alert");
    assert_eq!(latency_pressure.duration, "10m");
    assert_eq!(latency_pressure.severity, "warning");
    assert!(latency_pressure.expr.contains(
        "histogram_quantile(0.99, sum by (le) (rate(rustraft_append_latency_ms_bucket[5m]))) > 100"
    ));
    assert!(latency_pressure.expr.contains(
        "histogram_quantile(0.99, sum by (le) (rate(rustraft_read_index_latency_ms_bucket[5m]))) > 50"
    ));
    assert!(latency_pressure.expr.contains(
        "histogram_quantile(0.99, sum by (le) (rate(rustraft_snapshot_install_latency_ms_bucket[5m]))) > 5000"
    ));
    assert!(latency_pressure.summary.contains("latency_prometheus"));

    let runtime_admission = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftRuntimeAdmissionRejected")
        .expect("runtime admission rejection alert");
    assert_eq!(
        runtime_admission.expr,
        "rustraft_runtime_pressure_admission_rejected > 0"
    );
    assert_eq!(runtime_admission.duration, "1m");
    assert_eq!(runtime_admission.severity, "critical");
    assert!(runtime_admission
        .summary
        .contains("Runtime Pressure Bottlenecks"));
    assert!(runtime_admission
        .summary
        .contains("Runtime Pressure Actions"));
    assert!(runtime_admission
        .summary
        .contains("Runtime Pressure Action Sources"));

    let runtime_bottleneck = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftRuntimePressureBottleneckActive")
        .expect("runtime pressure bottleneck alert");
    assert_eq!(
        runtime_bottleneck.expr,
        "rustraft_runtime_pressure_bottleneck_score_percent > 0"
    );
    assert_eq!(runtime_bottleneck.duration, "5m");
    assert_eq!(runtime_bottleneck.severity, "warning");
    assert!(runtime_bottleneck
        .summary
        .contains("Runtime Pressure Bottlenecks"));
    assert!(runtime_bottleneck
        .summary
        .contains("Runtime Pressure Action Sources"));
    assert!(runtime_bottleneck
        .summary
        .contains("QPS, latency, or memory parity"));

    let runtime_pressure_freshness_low = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftRuntimePressureFreshnessLow")
        .expect("runtime pressure low freshness alert");
    assert_eq!(
        runtime_pressure_freshness_low.expr,
        "rustraft_runtime_pressure_freshness_low_fresh == 0"
    );
    assert_eq!(runtime_pressure_freshness_low.duration, "5m");
    assert_eq!(runtime_pressure_freshness_low.severity, "warning");
    assert!(runtime_pressure_freshness_low
        .summary
        .contains("release-scale QPS, latency, and memory evidence"));

    let runtime_pressure_freshness_lost = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftRuntimePressureFreshnessLost")
        .expect("runtime pressure freshness lost alert");
    assert_eq!(
        runtime_pressure_freshness_lost.expr,
        "rustraft_runtime_pressure_freshness_fresh == 0"
    );
    assert_eq!(runtime_pressure_freshness_lost.duration, "5m");
    assert_eq!(runtime_pressure_freshness_lost.severity, "warning");
    assert!(runtime_pressure_freshness_lost
        .summary
        .contains("trusting parity dashboards"));

    let runtime_pressure_freshness_invalid = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftRuntimePressureFreshnessInvalid")
        .expect("runtime pressure freshness invalid alert");
    assert_eq!(
        runtime_pressure_freshness_invalid.expr,
        "rustraft_runtime_pressure_freshness_issue_total > 0"
    );
    assert_eq!(runtime_pressure_freshness_invalid.duration, "5m");
    assert_eq!(runtime_pressure_freshness_invalid.severity, "warning");
    assert!(runtime_pressure_freshness_invalid
        .summary
        .contains("freshness metadata"));

    let runtime_scale_pressure = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftRuntimeScalePressure")
        .expect("runtime scale pressure alert");
    assert_eq!(
        runtime_scale_pressure.expr,
        "rustraft_runtime_pressure_scale > 0"
    );
    assert_eq!(runtime_scale_pressure.duration, "5m");
    assert_eq!(runtime_scale_pressure.severity, "warning");
    assert!(runtime_scale_pressure
        .summary
        .contains("Runtime Scale Pressure"));

    let node_runtime_timer_backpressure = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftNodeRuntimeTimerBackpressure")
        .expect("node runtime timer backpressure alert");
    assert_eq!(
        node_runtime_timer_backpressure.expr,
        "rustraft_node_runtime_timer_utilization_percent >= 80 or rustraft_node_runtime_timer_backpressure > 0 or rate(rustraft_node_runtime_timer_rejected_ticks_total[1m]) > 0"
    );
    assert_eq!(node_runtime_timer_backpressure.duration, "1m");
    assert_eq!(node_runtime_timer_backpressure.severity, "warning");
    assert!(node_runtime_timer_backpressure
        .summary
        .contains("Node Runtime Timer Utilization"));
    assert!(node_runtime_timer_backpressure
        .summary
        .contains("Rejected Ticks"));

    let snapshot_retry_backpressure = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftSnapshotRetryBackpressure")
        .expect("snapshot retry backpressure alert");
    assert_eq!(
        snapshot_retry_backpressure.expr,
        "rustraft_snapshot_lifecycle_retry_backpressure_present > 0"
    );
    assert_eq!(snapshot_retry_backpressure.duration, "5m");
    assert_eq!(snapshot_retry_backpressure.severity, "warning");
    assert!(snapshot_retry_backpressure
        .summary
        .contains("Snapshot Retry Backpressure"));

    let wal_slow_fsync_backpressure = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftWalSlowFsyncBackpressure")
        .expect("WAL slow fsync backpressure alert");
    assert_eq!(
        wal_slow_fsync_backpressure.expr,
        "rustraft_wal_lifecycle_slow_fsync_backpressure_observed > 0"
    );
    assert_eq!(wal_slow_fsync_backpressure.duration, "5m");
    assert_eq!(wal_slow_fsync_backpressure.severity, "warning");
    assert!(wal_slow_fsync_backpressure
        .summary
        .contains("WAL Slow Fsync Backpressure"));

    let peer_pipeline_backpressure = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftPeerPipelineBackpressure")
        .expect("peer pipeline backpressure alert");
    assert_eq!(
        peer_pipeline_backpressure.expr,
        "sum(rustraft_peer_append_queue_depth) > 0 or sum(rustraft_peer_reorder_queue_depth) > 0"
    );
    assert_eq!(peer_pipeline_backpressure.duration, "5m");
    assert_eq!(peer_pipeline_backpressure.severity, "warning");
    assert!(peer_pipeline_backpressure
        .summary
        .contains("per-peer pipeline panels"));

    let membership_transition_missing = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftMembershipTransitionMissing")
        .expect("membership transition missing alert");
    assert_eq!(
        membership_transition_missing.expr,
        "rustraft_membership_readiness_missing_total > 0 or rustraft_membership_transition_missing_total > 0"
    );
    assert_eq!(membership_transition_missing.duration, "5m");
    assert_eq!(membership_transition_missing.severity, "warning");
    assert!(membership_transition_missing
        .summary
        .contains("joint consensus, learner catch-up, witness"));

    let production_blocked = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftProductionReadinessBlocked")
        .expect("production readiness blocker alert");
    assert_eq!(
        production_blocked.expr,
        "rustraft_production_readiness_blocker_total > 0"
    );
    assert_eq!(production_blocked.duration, "1m");
    assert_eq!(production_blocked.severity, "critical");
    assert!(production_blocked
        .summary
        .contains("Production Blocker Detail"));

    let production_runtime_pressure = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftProductionReadinessRuntimePressureBottleneck")
        .expect("production readiness runtime pressure bottleneck alert");
    assert_eq!(
        production_runtime_pressure.expr,
        "rustraft_production_readiness_runtime_pressure_bottleneck_score_percent > 0"
    );
    assert_eq!(production_runtime_pressure.duration, "1m");
    assert_eq!(production_runtime_pressure.severity, "critical");
    assert!(production_runtime_pressure
        .summary
        .contains("Production Runtime Pressure Bottlenecks"));

    let production_missing = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftProductionReadinessMissingEvidence")
        .expect("production readiness missing evidence alert");
    assert_eq!(
        production_missing.expr,
        "rustraft_production_readiness_missing_total > 0"
    );
    assert_eq!(production_missing.duration, "5m");
    assert_eq!(production_missing.severity, "warning");
    assert!(production_missing
        .summary
        .contains("Production Missing Evidence"));

    let benchmark_failed = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftBaselineRaftBenchmarkFailed")
        .expect("benchmark failure alert");
    assert_eq!(
        benchmark_failed.expr,
        "rustraft_baseline_raft_benchmark_passed == 0 or rustraft_baseline_raft_benchmark_failed_workload_total > 0"
    );
    assert_eq!(benchmark_failed.duration, "5m");
    assert_eq!(benchmark_failed.severity, "critical");
    assert!(benchmark_failed.summary.contains("ratio panels"));

    let benchmark_ratio_regression = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftBaselineRaftBenchmarkRatioRegression")
        .expect("benchmark ratio regression alert");
    assert_eq!(
        benchmark_ratio_regression.expr,
        "rustraft_baseline_raft_benchmark_worst_p50_ratio > 1.1 or rustraft_baseline_raft_benchmark_worst_p99_ratio > 1.1 or rustraft_baseline_raft_benchmark_worst_throughput_ratio < 0.9"
    );
    assert_eq!(benchmark_ratio_regression.duration, "10m");
    assert_eq!(benchmark_ratio_regression.severity, "warning");
    assert!(benchmark_ratio_regression
        .summary
        .contains("workload p50/p99/throughput ratio panels"));

    let benchmark_resource_regression = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftBaselineRaftBenchmarkResourceRegression")
        .expect("benchmark resource regression alert");
    assert_eq!(
        benchmark_resource_regression.expr,
        "rustraft_baseline_raft_benchmark_worst_cpu_ratio > 1.1 or rustraft_baseline_raft_benchmark_worst_peak_resident_memory_ratio > 1.1"
    );
    assert_eq!(benchmark_resource_regression.duration, "10m");
    assert_eq!(benchmark_resource_regression.severity, "warning");
    assert!(benchmark_resource_regression
        .summary
        .contains("CPU and peak resident-memory parity panels"));

    let benchmark_freshness_lost = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftBaselineRaftBenchmarkFreshnessLost")
        .expect("benchmark freshness alert");
    assert_eq!(
        benchmark_freshness_lost.expr,
        "rustraft_baseline_raft_benchmark_fresh == 0"
    );
    assert_eq!(benchmark_freshness_lost.duration, "5m");
    assert_eq!(benchmark_freshness_lost.severity, "warning");
    assert!(benchmark_freshness_lost
        .summary
        .contains("release-mode benchmark parity"));

    let diagnostic_errors = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftDiagnosticErrors")
        .expect("diagnostic error alert");
    assert_eq!(
        diagnostic_errors.expr,
        "rustraft_diagnostic_log_total{severity=\"error\"} > 0"
    );
    assert_eq!(diagnostic_errors.duration, "1m");
    assert_eq!(diagnostic_errors.severity, "critical");
    assert!(diagnostic_errors
        .summary
        .contains("inspect_error_diagnostics"));

    let critical_runbook = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftRunbookCriticalSteps")
        .expect("critical runbook alert");
    assert!(critical_runbook
        .summary
        .contains("operator_runbook_first_step"));

    let debug_bundle_validation = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftDebugBundleValidationFailed")
        .expect("debug bundle validation alert");
    assert_eq!(
        debug_bundle_validation.expr,
        "rustraft_debug_bundle_validation_ready == 0"
    );
    assert_eq!(debug_bundle_validation.duration, "5m");
    assert_eq!(debug_bundle_validation.severity, "warning");

    let support_envelope_validation = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftSupportEnvelopeValidationFailed")
        .expect("support envelope validation alert");
    assert_eq!(
        support_envelope_validation.expr,
        "rustraft_debug_bundle_validation_ready{artifact=\"support_envelope\"} == 0"
    );
    assert_eq!(support_envelope_validation.duration, "5m");
    assert_eq!(support_envelope_validation.severity, "warning");
    assert!(support_envelope_validation
        .summary
        .contains("rustraft_debug_bundle_validation_first_issue{artifact=\"support_envelope\"}"));
    assert!(support_envelope_validation
        .summary
        .contains("rustraft_debug_bundle_validation_issue{artifact=\"support_envelope\"}"));
    assert!(support_envelope_validation
        .summary
        .contains("debug_snapshot_stale"));
    assert!(support_envelope_validation
        .summary
        .contains("debug_snapshot_low_fresh"));
    let support_envelope_critical = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftSupportEnvelopeCritical")
        .expect("support envelope critical alert");
    assert_eq!(
        support_envelope_critical.expr,
        "rustraft_debug_bundle_validation_ready{artifact=\"support_envelope\",support_envelope_severity=\"critical\"} == 0"
    );
    assert_eq!(support_envelope_critical.duration, "1m");
    assert_eq!(support_envelope_critical.severity, "critical");
    assert!(support_envelope_critical
        .summary
        .contains("support_envelope_status"));

    let debug_snapshot_stale = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftDebugSnapshotStale")
        .expect("debug snapshot stale alert");
    assert_eq!(
        debug_snapshot_stale.expr,
        "rustraft_debug_snapshot_age_ms > rustraft_debug_snapshot_max_age_ms"
    );
    assert_eq!(debug_snapshot_stale.duration, "5m");
    assert_eq!(debug_snapshot_stale.severity, "warning");
    assert!(debug_snapshot_stale
        .summary
        .contains("configured freshness window"));

    let debug_snapshot_freshness_low = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftDebugSnapshotFreshnessLow")
        .expect("debug snapshot freshness low alert");
    assert_eq!(
        debug_snapshot_freshness_low.expr,
        "rustraft_debug_snapshot_low_fresh == 0"
    );
    assert_eq!(debug_snapshot_freshness_low.duration, "5m");
    assert_eq!(debug_snapshot_freshness_low.severity, "warning");
    assert!(debug_snapshot_freshness_low
        .summary
        .contains("less than five minutes"));

    let debug_snapshot_freshness = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftDebugSnapshotFreshnessLost")
        .expect("debug snapshot freshness alert");
    assert_eq!(
        debug_snapshot_freshness.expr,
        "rustraft_debug_snapshot_fresh == 0"
    );
    assert_eq!(debug_snapshot_freshness.duration, "5m");
    assert_eq!(debug_snapshot_freshness.severity, "warning");

    let triage_watch = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftOperatorTriageWatch")
        .expect("triage watch alert");
    assert_eq!(
        triage_watch.expr,
        "rustraft_operator_triage_status{status=\"watch\"} > 0"
    );
    assert_eq!(triage_watch.duration, "5m");
    assert_eq!(triage_watch.severity, "warning");

    let triage_attention = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftOperatorTriageNeedsAttention")
        .expect("triage attention alert");
    assert_eq!(
        triage_attention.expr,
        "rustraft_operator_triage_status{status=\"needs_attention\"} > 0"
    );
    assert_eq!(triage_attention.duration, "1m");
    assert_eq!(triage_attention.severity, "critical");

    let runbook_critical = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftRunbookCriticalSteps")
        .expect("critical runbook alert");
    assert_eq!(
        runbook_critical.expr,
        "rustraft_operator_runbook_step_total{severity=\"critical\"} > 0"
    );
    assert_eq!(runbook_critical.duration, "1m");
    assert_eq!(runbook_critical.severity, "critical");

    let provisioning_validation = rules
        .iter()
        .find(|rule| rule.alert == "RustRaftObservabilityProvisioningValidationFailed")
        .expect("provisioning validation alert");
    assert_eq!(
        provisioning_validation.expr,
        "rustraft_observability_provisioning_validation_ready == 0"
    );
    assert_eq!(provisioning_validation.duration, "5m");
    assert_eq!(provisioning_validation.severity, "warning");

    let json = matrixraft_alert_rules_json();
    let parsed: Value = serde_json::from_str(&json).expect("alert rule json");
    assert_eq!(parsed.as_array().expect("alert rules").len(), 38);
    assert!(json.contains("RustRaftOptimizationWarningHints"));
    assert!(json.contains("rustraft_optimization_warning_total > 0"));
    assert!(json.contains("RustRaftMemoryPressure"));
    assert!(json.contains("rustraft_process_resident_memory_bytes >= 8589934592"));
    assert!(json.contains("rustraft_replication_buffer_bytes >= 1073741824"));
    assert!(json.contains("RustRaftLatencyPressure"));
    assert!(json.contains(
        "histogram_quantile(0.99, sum by (le) (rate(rustraft_append_latency_ms_bucket[5m]))) > 100"
    ));
    assert!(json.contains(
        "histogram_quantile(0.99, sum by (le) (rate(rustraft_read_index_latency_ms_bucket[5m]))) > 50"
    ));
    assert!(json.contains("RustRaftRuntimeAdmissionRejected"));
    assert!(json.contains("Runtime Pressure Action Sources"));
    assert!(json.contains("RustRaftRuntimePressureBottleneckActive"));
    assert!(json.contains("rustraft_runtime_pressure_bottleneck_score_percent > 0"));
    assert!(json.contains("RustRaftRuntimePressureFreshnessLow"));
    assert!(json.contains("rustraft_runtime_pressure_freshness_low_fresh == 0"));
    assert!(json.contains("RustRaftRuntimePressureFreshnessLost"));
    assert!(json.contains("rustraft_runtime_pressure_freshness_fresh == 0"));
    assert!(json.contains("RustRaftRuntimePressureFreshnessInvalid"));
    assert!(json.contains("rustraft_runtime_pressure_freshness_issue_total > 0"));
    assert!(json.contains("RustRaftRuntimeScalePressure"));
    assert!(json.contains("rustraft_runtime_pressure_scale > 0"));
    assert!(json.contains("RustRaftRuntimePipelinePressure"));
    assert!(json.contains("rustraft_runtime_pressure_pipeline > 0"));
    assert!(json.contains("RustRaftRuntimeReadBacklogPressure"));
    assert!(json.contains("rustraft_runtime_pressure_read_backlog > 0"));
    assert!(json.contains("rustraft_runtime_pressure_admission_rejected > 0"));
    assert!(json.contains("RustRaftNodeRuntimeTimerBackpressure"));
    assert!(json.contains(
        "rustraft_node_runtime_timer_utilization_percent >= 80 or rustraft_node_runtime_timer_backpressure > 0 or rate(rustraft_node_runtime_timer_rejected_ticks_total[1m]) > 0"
    ));
    assert!(json.contains("RustRaftSnapshotRetryBackpressure"));
    assert!(json.contains("rustraft_snapshot_lifecycle_retry_backpressure_present > 0"));
    assert!(json.contains("RustRaftWalSlowFsyncBackpressure"));
    assert!(json.contains("rustraft_wal_lifecycle_slow_fsync_backpressure_observed > 0"));
    assert!(json.contains("RustRaftPeerPipelineBackpressure"));
    assert!(json.contains(
        "sum(rustraft_peer_append_queue_depth) > 0 or sum(rustraft_peer_reorder_queue_depth) > 0"
    ));
    assert!(json.contains("RustRaftMembershipTransitionMissing"));
    assert!(json.contains(
        "rustraft_membership_readiness_missing_total > 0 or rustraft_membership_transition_missing_total > 0"
    ));
    assert!(json.contains("RustRaftProductionReadinessBlocked"));
    assert!(json.contains("rustraft_production_readiness_blocker_total > 0"));
    assert!(json.contains("RustRaftProductionReadinessRuntimePressureBottleneck"));
    assert!(json
        .contains("rustraft_production_readiness_runtime_pressure_bottleneck_score_percent > 0"));
    assert!(json.contains("RustRaftProductionReadinessMissingEvidence"));
    assert!(json.contains("rustraft_production_readiness_missing_total > 0"));
    assert!(json.contains("RustRaftBaselineRaftBenchmarkFailed"));
    assert!(json.contains(
        "rustraft_baseline_raft_benchmark_passed == 0 or rustraft_baseline_raft_benchmark_failed_workload_total > 0"
    ));
    assert!(json.contains("RustRaftBaselineRaftBenchmarkRatioRegression"));
    assert!(json.contains(
        "rustraft_baseline_raft_benchmark_worst_p50_ratio > 1.1 or rustraft_baseline_raft_benchmark_worst_p99_ratio > 1.1 or rustraft_baseline_raft_benchmark_worst_throughput_ratio < 0.9"
    ));
    assert!(json.contains("RustRaftBaselineRaftBenchmarkResourceRegression"));
    assert!(json.contains(
        "rustraft_baseline_raft_benchmark_worst_cpu_ratio > 1.1 or rustraft_baseline_raft_benchmark_worst_peak_resident_memory_ratio > 1.1"
    ));
    assert!(json.contains("RustRaftBaselineRaftBenchmarkFreshnessLost"));
    assert!(json.contains("rustraft_baseline_raft_benchmark_fresh == 0"));
    assert!(json.contains("RustRaftFatalEvents"));
    assert!(json.contains("rustraft_fatal_total > 0"));
    assert!(json.contains("RustRaftDiagnosticErrors"));
    assert!(json.contains("rustraft_diagnostic_log_total{severity=\\\"error\\\"} > 0"));
    assert!(json.contains("RustRaftBlockersPresent"));
    assert!(json.contains("rustraft_blocker_total > 0"));
    assert!(json.contains("RustRaftDebugBundleValidationFailed"));
    assert!(json.contains("rustraft_debug_bundle_validation_ready == 0"));
    assert!(json.contains("RustRaftSupportEnvelopeValidationFailed"));
    assert!(json.contains(
        "rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\"} == 0"
    ));
    assert!(json.contains("RustRaftSupportEnvelopeCritical"));
    assert!(json.contains(
        "rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\",support_envelope_severity=\\\"critical\\\"} == 0"
    ));
    assert!(json.contains("RustRaftDebugSnapshotStale"));
    assert!(json.contains("rustraft_debug_snapshot_age_ms > rustraft_debug_snapshot_max_age_ms"));
    assert!(json.contains("RustRaftDebugSnapshotFreshnessLow"));
    assert!(json.contains("rustraft_debug_snapshot_low_fresh == 0"));
    assert!(json.contains("RustRaftDebugSnapshotFreshnessLost"));
    assert!(json.contains("rustraft_debug_snapshot_fresh == 0"));
    assert!(json.contains("RustRaftOperatorTriageWatch"));
    assert!(json.contains("rustraft_operator_triage_status{status=\\\"watch\\\"} > 0"));
    assert!(json.contains("RustRaftOperatorTriageNeedsAttention"));
    assert!(json.contains("rustraft_operator_triage_status{status=\\\"needs_attention\\\"} > 0"));
    assert!(json.contains("RustRaftRunbookCriticalSteps"));
    assert!(json.contains("rustraft_operator_runbook_step_total{severity=\\\"critical\\\"} > 0"));
    assert!(json.contains("RustRaftObservabilityProvisioningValidationFailed"));
    assert!(json.contains("rustraft_observability_provisioning_validation_ready == 0"));
}

#[test]
fn observability_required_metric_names_flatten_release_scale_catalog() {
    let required = matrixraft_observability_required_metric_names();
    let provisioning = matrixraft_observability_provisioning();
    assert_eq!(required, provisioning.required_metric_names);

    let unique: std::collections::BTreeSet<_> = required.iter().cloned().collect();
    assert_eq!(
        unique.len(),
        required.len(),
        "required metric catalog must not contain duplicates"
    );

    let benchmark_metrics = matrixraft_baseline_raft_benchmark_metric_names();
    let queue_pressure_metrics = matrixraft_queue_pressure_metric_names();
    for metric_name in [
        benchmark_metrics.passed,
        benchmark_metrics.production_evidence_ready,
        benchmark_metrics.generated_at_unix_ms,
        benchmark_metrics.age_ms,
        benchmark_metrics.max_age_ms,
        benchmark_metrics.stale_after_unix_ms,
        benchmark_metrics.remaining_fresh_ms,
        benchmark_metrics.fresh,
        benchmark_metrics.freshness_status,
        benchmark_metrics.worst_p99_ratio,
        benchmark_metrics.worst_throughput_ratio,
        benchmark_metrics.worst_cpu_ratio,
        benchmark_metrics.worst_peak_resident_memory_ratio,
        benchmark_metrics.workload_throughput_ratio,
        benchmark_metrics.workload_peak_resident_memory_ratio,
    ] {
        assert!(
            required.contains(&metric_name),
            "flattened catalog missing benchmark parity metric {metric_name}"
        );
    }

    for metric_name in [
        "rustraft_scale_target_min_proposal_qps",
        "rustraft_runtime_pressure_latency_observed_p99_ms",
        "rustraft_process_resident_memory_bytes",
        "rustraft_mailbox_total_len",
        "rustraft_mailbox_rejected_send_total",
        "rustraft_mail_channel_queued_len",
        "rustraft_mail_channel_rejected_send_total",
        "rustraft_wal_lifecycle_compaction_after_slow_fsync_observed",
        "rustraft_production_readiness_runtime_pressure_bottleneck_score_percent",
        "rustraft_public_api_contract_ready",
        "rustraft_public_api_mapping_coverage_percent",
        "rustraft_public_api_unmapped_reference_required_total",
    ] {
        assert!(
            required.iter().any(|required| required == metric_name),
            "flattened catalog missing production metric {metric_name}"
        );
    }

    for metric_name in [
        queue_pressure_metrics.mailbox_total_len,
        queue_pressure_metrics.mailbox_high_watermark,
        queue_pressure_metrics.mailbox_max_channel_depth,
        queue_pressure_metrics.mailbox_rejected_send_total,
        queue_pressure_metrics.mail_channel_queued_len,
        queue_pressure_metrics.mail_channel_limit,
        queue_pressure_metrics.mail_channel_max_depth,
        queue_pressure_metrics.mail_channel_rejected_send_total,
    ] {
        assert!(
            required.contains(&metric_name),
            "flattened catalog missing queue pressure metric {metric_name}"
        );
    }
}

#[test]
fn observability_required_metric_scrape_validator_reports_exact_missing_metrics() {
    let provisioning = matrixraft_observability_provisioning();
    let complete_scrape = provisioning
        .required_metric_names
        .iter()
        .chain(provisioning.validation_metric_names.iter())
        .map(|metric| format!("{metric} 1"))
        .collect::<Vec<_>>()
        .join("\n");
    let complete = matrixraft_validate_required_metric_scrape_texts(&[&complete_scrape]);
    assert!(complete.ready, "{complete:#?}");
    assert!(complete.issues.is_empty());

    let partial_scrape = concat!(
        "rustraft_ready 1\n",
        "rustraft_proposal_total{service=\"raft-a\",group=\"g1\",workload=\"release\"} 42\n",
        "rustraft_process_resident_memory_bytes{service=\"raft-a\"} 2048\n",
        "malformed_metric{service=\"raft-a\" 1\n"
    );
    let partial = matrixraft_validate_required_metric_scrape_texts(&[partial_scrape]);
    assert!(!partial.ready);
    assert!(partial
        .issues
        .contains(&"required_metric_missing:rustraft_append_latency_ms".to_string()));
    assert!(partial.issues.contains(
        &"required_metric_missing:rustraft_baseline_raft_benchmark_worst_p99_ratio".to_string()
    ));
    assert!(partial
        .issues
        .contains(&"validation_metric_missing:rustraft_debug_snapshot_age_ms".to_string()));
    assert!(partial.issues.contains(
        &"validation_metric_missing:rustraft_observability_provisioning_validation_ready"
            .to_string()
    ));
    assert!(partial
        .issues
        .contains(&"required_metric_scrape_malformed".to_string()));
}

#[test]
fn observability_provisioning_exports_dashboard_alerts_metrics_and_bundle_contract() {
    let provisioning = matrixraft_observability_provisioning();
    assert_eq!(provisioning.service, "rustraft");
    assert_eq!(provisioning.prometheus_format, "prometheus_text_v0.0.4");
    assert_eq!(provisioning.dashboard.uid, "rustraft-runtime-overview");
    assert_eq!(provisioning.alert_rules, matrixraft_alert_rules());
    assert_eq!(
        provisioning.runbook_steps,
        matrixraft_observability_provisioning_runbook_steps()
    );
    assert!(provisioning
        .runbook_steps
        .iter()
        .any(|step| step.id == "inspect_error_diagnostics"));
    assert!(provisioning
        .runbook_steps
        .iter()
        .any(|step| step.id == "wire_critical_alerts"));
    let review_warning_signals = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "review_warning_signals")
        .expect("review warning signals runbook step");
    assert!(review_warning_signals
        .validation
        .contains("rustraft_operator_triage_diagnostic_warning_total"));
    assert!(review_warning_signals
        .validation
        .contains("rustraft_operator_triage_optimization_warning_total"));
    let resolve_memory_pressure = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "resolve_memory_pressure")
        .expect("memory pressure runbook step");
    assert_eq!(resolve_memory_pressure.target, "memory");
    assert!(resolve_memory_pressure
        .action
        .contains("resident, heap, log-cache, snapshot-buffer"));
    assert!(resolve_memory_pressure
        .validation
        .contains("rustraft_process_resident_memory_bytes"));
    assert!(resolve_memory_pressure
        .validation
        .contains("rustraft_replication_buffer_bytes"));
    let resolve_latency_pressure = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "resolve_latency_pressure")
        .expect("latency pressure runbook step");
    assert_eq!(resolve_latency_pressure.target, "latency");
    assert!(resolve_latency_pressure
        .action
        .contains("read-index, and snapshot-install p99 latency"));
    assert!(resolve_latency_pressure
        .validation
        .contains("QPS and throughput targets remain satisfied"));
    assert!(provisioning
        .runbook_steps
        .iter()
        .any(|step| step.id == "refresh_debug_snapshot"));
    let resolve_node_runtime_timer_backpressure = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "resolve_node_runtime_timer_backpressure")
        .expect("node runtime timer backpressure runbook step");
    assert_eq!(
        resolve_node_runtime_timer_backpressure.target,
        "node_runtime"
    );
    assert!(resolve_node_runtime_timer_backpressure
        .action
        .contains("timer utilization, pending, rejected, accepted, and completed tick panels"));
    assert!(resolve_node_runtime_timer_backpressure
        .validation
        .contains("rustraft_node_runtime_timer_utilization_percent stays below 80"));
    assert!(resolve_node_runtime_timer_backpressure
        .validation
        .contains("rustraft_node_runtime_timer_backpressure is 0"));
    assert!(resolve_node_runtime_timer_backpressure
        .validation
        .contains("rustraft_node_runtime_timer_pending_ticks drains below rustraft_node_runtime_timer_max_pending_ticks"));
    assert!(resolve_node_runtime_timer_backpressure
        .validation
        .contains("rate(rustraft_node_runtime_timer_rejected_ticks_total[1m]) is 0"));
    let resolve_snapshot_retry_backpressure = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "resolve_snapshot_retry_backpressure")
        .expect("snapshot retry backpressure runbook step");
    assert_eq!(
        resolve_snapshot_retry_backpressure.target,
        "snapshot_lifecycle"
    );
    assert!(resolve_snapshot_retry_backpressure
        .action
        .contains("snapshot retry, send-timeout, rate-limit"));
    assert!(resolve_snapshot_retry_backpressure
        .validation
        .contains("rustraft_snapshot_lifecycle_retry_backpressure_present is 0"));
    assert!(resolve_snapshot_retry_backpressure
        .validation
        .contains("sustained sender, downloader, and transfer-completion signals remain 1"));
    let resolve_wal_slow_fsync_backpressure = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "resolve_wal_slow_fsync_backpressure")
        .expect("WAL slow fsync backpressure runbook step");
    assert_eq!(resolve_wal_slow_fsync_backpressure.target, "wal_lifecycle");
    assert!(resolve_wal_slow_fsync_backpressure
        .action
        .contains("WAL slow-fsync, segment lifecycle"));
    assert!(resolve_wal_slow_fsync_backpressure
        .validation
        .contains("rustraft_wal_lifecycle_slow_fsync_backpressure_observed is 0"));
    assert!(resolve_wal_slow_fsync_backpressure
        .validation
        .contains("rustraft_wal_lifecycle_compaction_after_slow_fsync_observed is 1"));
    let resolve_peer_pipeline_backpressure = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "resolve_peer_pipeline_backpressure")
        .expect("peer pipeline backpressure runbook step");
    assert_eq!(resolve_peer_pipeline_backpressure.target, "peer_pipeline");
    assert!(resolve_peer_pipeline_backpressure
        .action
        .contains("per-peer append queue depth"));
    assert!(resolve_peer_pipeline_backpressure.validation.contains(
        "rustraft_peer_append_queue_depth and rustraft_peer_reorder_queue_depth remain 0"
    ));
    assert!(resolve_peer_pipeline_backpressure
        .validation
        .contains("rustraft_peer_reorder_entries_converged_total advances"));
    let resolve_read_backlog_pressure = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "resolve_read_backlog_pressure")
        .expect("read backlog pressure runbook step");
    assert_eq!(resolve_read_backlog_pressure.target, "read_backlog");
    assert!(resolve_read_backlog_pressure
        .action
        .contains("pending ReadIndex, bounded-stale read backlog"));
    assert!(resolve_read_backlog_pressure
        .action
        .contains("release-scale read QPS or random-replica reads"));
    assert!(resolve_read_backlog_pressure
        .validation
        .contains("rustraft_runtime_pressure_read_backlog is 0"));
    assert!(resolve_read_backlog_pressure
        .validation
        .contains("rustraft_runtime_pressure_read_backlog_excess is 0"));
    assert!(resolve_read_backlog_pressure
        .validation
        .contains("bounded-stale replica reads remain deadline-bound"));
    let resolve_membership_transition_evidence = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "resolve_membership_transition_evidence")
        .expect("membership transition evidence runbook step");
    assert_eq!(resolve_membership_transition_evidence.target, "membership");
    assert!(resolve_membership_transition_evidence
        .action
        .contains("learner promotion, witness quorum, joint consensus"));
    assert!(resolve_membership_transition_evidence
        .validation
        .contains("rustraft_membership_readiness_missing_total is 0"));
    assert!(resolve_membership_transition_evidence
        .validation
        .contains("rustraft_membership_transition_missing_total is 0"));
    assert!(resolve_membership_transition_evidence
        .validation
        .contains("joint consensus, learner catch-up, witness"));
    let resolve_benchmark_resource_regression = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "resolve_benchmark_resource_regression")
        .expect("benchmark resource regression runbook step");
    assert_eq!(
        resolve_benchmark_resource_regression.target,
        "benchmark_parity"
    );
    assert!(resolve_benchmark_resource_regression
        .action
        .contains("CPU and peak resident-memory ratio panels"));
    assert!(resolve_benchmark_resource_regression
        .validation
        .contains("rustraft_baseline_raft_benchmark_worst_cpu_ratio is at or below 1.1"));
    assert!(resolve_benchmark_resource_regression.validation.contains(
        "rustraft_baseline_raft_benchmark_worst_peak_resident_memory_ratio is at or below 1.1"
    ));
    let refresh_baseline_raft_benchmark_evidence = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "refresh_baseline_raft_benchmark_evidence")
        .expect("benchmark freshness runbook step");
    assert_eq!(
        refresh_baseline_raft_benchmark_evidence.target,
        "benchmark_parity"
    );
    assert!(refresh_baseline_raft_benchmark_evidence
        .action
        .contains("stale, missing, or future-dated"));
    assert!(refresh_baseline_raft_benchmark_evidence
        .action
        .contains("QPS, latency, CPU, or memory claims"));
    assert!(refresh_baseline_raft_benchmark_evidence
        .validation
        .contains("rustraft_baseline_raft_benchmark_fresh is 1"));
    assert!(refresh_baseline_raft_benchmark_evidence
        .validation
        .contains("rustraft_baseline_raft_benchmark_freshness_status is fresh"));
    let refresh_debug_snapshot = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "refresh_debug_snapshot")
        .expect("refresh debug snapshot runbook step");
    assert!(refresh_debug_snapshot
        .action
        .contains("RustRaftDebugSnapshotFreshnessLow"));
    assert!(refresh_debug_snapshot
        .action
        .contains("RustRaftDebugSnapshotFreshnessLost"));
    assert!(refresh_debug_snapshot
        .validation
        .contains("rustraft_debug_snapshot_fresh is 1"));
    assert!(refresh_debug_snapshot
        .validation
        .contains("rustraft_debug_snapshot_age_ms is below rustraft_debug_snapshot_max_age_ms"));
    assert!(refresh_debug_snapshot.validation.contains(
        "rustraft_debug_snapshot_remaining_fresh_ms is above rustraft_debug_snapshot_low_fresh_ms"
    ));
    assert!(refresh_debug_snapshot
        .validation
        .contains("rustraft_debug_snapshot_low_fresh is 1"));
    assert!(refresh_debug_snapshot
        .validation
        .contains("rustraft_debug_snapshot_stale_after_unix_ms is in the future"));
    let refresh_runtime_pressure_evidence = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "refresh_runtime_pressure_evidence")
        .expect("refresh runtime pressure evidence runbook step");
    assert_eq!(refresh_runtime_pressure_evidence.target, "runtime_pressure");
    assert!(refresh_runtime_pressure_evidence
        .action
        .contains("RustRaftRuntimePressureFreshnessLow"));
    assert!(refresh_runtime_pressure_evidence
        .action
        .contains("RustRaftRuntimePressureFreshnessLost"));
    assert!(refresh_runtime_pressure_evidence
        .action
        .contains("RustRaftRuntimePressureFreshnessInvalid"));
    assert!(refresh_runtime_pressure_evidence
        .validation
        .contains("rustraft_runtime_pressure_freshness_fresh is 1"));
    assert!(refresh_runtime_pressure_evidence
        .validation
        .contains("rustraft_runtime_pressure_freshness_low_fresh is 1"));
    assert!(refresh_runtime_pressure_evidence
        .validation
        .contains("rustraft_runtime_pressure_freshness_issue_total is 0"));
    assert!(refresh_runtime_pressure_evidence
        .validation
        .contains("rustraft_runtime_pressure_freshness_status is fresh"));
    let validate_support_envelope = provisioning
        .runbook_steps
        .iter()
        .find(|step| step.id == "validate_support_envelope")
        .expect("validate support envelope runbook step");
    assert_eq!(validate_support_envelope.target, "support_envelope");
    assert!(validate_support_envelope.validation.contains(
        "rustraft_debug_bundle_validation_first_issue{artifact=\"support_envelope\"} is absent"
    ));
    assert!(validate_support_envelope
        .validation
        .contains("support envelope missing artifact lists are empty"));
    assert!(validate_support_envelope
        .validation
        .contains("debug_snapshot_low_fresh is true"));
    assert!(validate_support_envelope
        .validation
        .contains("debug_snapshot_fresh is true"));
    assert!(validate_support_envelope
        .validation
        .contains("debug_snapshot_freshness_status is fresh"));
    assert!(validate_support_envelope
        .validation
        .contains("support_envelope_status is ready"));
    assert!(validate_support_envelope
        .validation
        .contains("support_envelope_severity is ok"));
    let provisioning_runbook_metrics = matrixraft_operator_runbook_prometheus(
        &provisioning.runbook_steps,
        &[("service", "raft-a")],
    );
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"refresh_debug_snapshot\",severity=\"warning\",target=\"debug_bundle\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"validate_support_envelope\",severity=\"warning\",target=\"support_envelope\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_runtime_pressure_bottleneck\",severity=\"critical\",target=\"runtime_pressure\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"inspect_runtime_pressure_bottleneck_warning\",severity=\"warning\",target=\"runtime_pressure\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"refresh_runtime_pressure_evidence\",severity=\"warning\",target=\"runtime_pressure\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_memory_pressure\",severity=\"warning\",target=\"memory\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_latency_pressure\",severity=\"warning\",target=\"latency\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_node_runtime_timer_backpressure\",severity=\"warning\",target=\"node_runtime\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_snapshot_retry_backpressure\",severity=\"warning\",target=\"snapshot_lifecycle\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_wal_slow_fsync_backpressure\",severity=\"warning\",target=\"wal_lifecycle\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_peer_pipeline_backpressure\",severity=\"warning\",target=\"peer_pipeline\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_read_backlog_pressure\",severity=\"warning\",target=\"read_backlog\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_membership_transition_evidence\",severity=\"warning\",target=\"membership\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"resolve_benchmark_resource_regression\",severity=\"warning\",target=\"benchmark_parity\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"refresh_baseline_raft_benchmark_evidence\",severity=\"warning\",target=\"benchmark_parity\"} 1"
    ));
    assert_eq!(
        provisioning.debug_bundle_contract.schema,
        "rustraft.debug_snapshot.v1"
    );
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_optimization_ready".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_diagnostic_log_entry_total".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_optimization_hint_total".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_optimization_component_hint_total".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_operator_triage_status".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_operator_triage_first_action".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_operator_triage_top_diagnostic".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_operator_triage_top_alert".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_operator_triage_top_optimization_hint".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_operator_runbook_step_total".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_operator_runbook_step_present".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_operator_runbook_first_step".to_string()));
    assert!(provisioning
        .required_metric_names
        .contains(&"rustraft_diagnostic_log_total".to_string()));
    let scale_metrics = matrixraft_scale_metric_names();
    for metric_name in [
        scale_metrics.proposal_qps_total,
        scale_metrics.append_entries_qps_total,
        scale_metrics.read_index_qps_total,
        scale_metrics.apply_entries_qps_total,
        scale_metrics.replication_bytes_total,
        scale_metrics.apply_bytes_total,
    ] {
        assert!(
            provisioning.required_metric_names.contains(&metric_name),
            "provisioning missing scale metric {metric_name}"
        );
        assert!(
            provisioning
                .dashboard
                .panels
                .iter()
                .any(|panel| panel.expr.contains(&metric_name)),
            "dashboard missing scale metric {metric_name}"
        );
    }
    let scale_target_metrics = matrixraft_scale_target_metric_names();
    for metric_name in [
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
    ] {
        assert!(
            provisioning.required_metric_names.contains(&metric_name),
            "provisioning missing scale target metric {metric_name}"
        );
    }
    let memory_metrics = matrixraft_memory_metric_names();
    for metric_name in [
        memory_metrics.process_resident_memory_bytes,
        memory_metrics.heap_allocated_bytes,
        memory_metrics.log_cache_bytes,
        memory_metrics.snapshot_buffer_bytes,
        memory_metrics.replication_buffer_bytes,
    ] {
        assert!(
            provisioning.required_metric_names.contains(&metric_name),
            "provisioning missing memory metric {metric_name}"
        );
        assert!(
            provisioning
                .dashboard
                .panels
                .iter()
                .any(|panel| panel.expr.contains(&metric_name)),
            "dashboard missing memory metric {metric_name}"
        );
    }
    let runtime_pressure_metrics = matrixraft_runtime_pressure_metric_names();
    for metric_name in [
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
    ] {
        assert!(
            provisioning.required_metric_names.contains(&metric_name),
            "provisioning missing runtime pressure metric {metric_name}"
        );
    }
    let wal_lifecycle_metrics = matrixraft_wal_lifecycle_metric_names();
    for metric_name in [
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
    ] {
        assert!(
            provisioning.required_metric_names.contains(&metric_name),
            "provisioning missing WAL lifecycle metric {metric_name}"
        );
        assert!(
            provisioning
                .dashboard
                .panels
                .iter()
                .any(|panel| panel.expr.contains(&metric_name)),
            "dashboard missing WAL lifecycle metric {metric_name}"
        );
    }
    let membership_readiness_metrics = matrixraft_membership_readiness_metric_names();
    for metric_name in [
        membership_readiness_metrics.ready,
        membership_readiness_metrics.satisfied_total,
        membership_readiness_metrics.missing_total,
        membership_readiness_metrics.transition_ready,
        membership_readiness_metrics.transition_missing_total,
        membership_readiness_metrics.transition_missing,
    ] {
        assert!(
            provisioning.required_metric_names.contains(&metric_name),
            "provisioning missing membership readiness metric {metric_name}"
        );
        assert!(
            provisioning
                .dashboard
                .panels
                .iter()
                .any(|panel| panel.expr.contains(&metric_name)),
            "dashboard missing membership readiness metric {metric_name}"
        );
    }
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_debug_snapshot_generated_at_unix_ms".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_debug_snapshot_age_ms".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_debug_snapshot_max_age_ms".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_debug_snapshot_stale_after_unix_ms".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_debug_snapshot_remaining_fresh_ms".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_debug_snapshot_low_fresh_ms".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_debug_snapshot_low_fresh".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_debug_snapshot_fresh".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_debug_bundle_validation_ready".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_debug_bundle_validation_first_issue".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_observability_provisioning_validation_ready".to_string()));
    assert!(provisioning
        .validation_metric_names
        .contains(&"rustraft_observability_provisioning_validation_first_issue".to_string()));
    for artifact_name in [
        "debug_snapshot",
        "debug_snapshot_json",
        "diagnostic_json_lines",
        "runtime_pressure_freshness_diagnostic_json_lines",
        "latency_prometheus",
        "memory_prometheus",
        "scale_prometheus",
        "scale_target_prometheus",
        "snapshot_lifecycle_prometheus",
        "wal_lifecycle_prometheus",
        "membership_readiness_prometheus",
        "public_api_contract_validation",
        "public_api_contract_validation_prometheus",
        "benchmark_prometheus",
        "grafana_dashboard_json",
        "alert_rules_json",
        "observability_provisioning_json",
        "observability_provisioning",
        "validation",
        "provisioning_validation",
        "support_envelope_validation",
        "support_envelope_validation_prometheus",
    ] {
        assert!(
            provisioning
                .debug_artifact_names
                .contains(&artifact_name.to_string()),
            "provisioning missing debug artifact {artifact_name}"
        );
    }
    for artifact_name in [
        "diagnostic_prometheus",
        "latency_prometheus",
        "memory_prometheus",
        "scale_prometheus",
        "scale_target_prometheus",
        "snapshot_lifecycle_prometheus",
        "wal_lifecycle_prometheus",
        "membership_readiness_prometheus",
        "public_api_contract_validation_prometheus",
        "benchmark_prometheus",
        "optimization_prometheus",
        "triage_prometheus",
        "runbook_prometheus",
        "debug_snapshot_metadata_prometheus",
        "validation_prometheus",
        "provisioning_validation_prometheus",
        "provisioning_runbook_prometheus",
        "support_envelope_validation_prometheus",
    ] {
        assert!(
            provisioning
                .prometheus_artifact_names
                .contains(&artifact_name.to_string()),
            "provisioning missing Prometheus artifact {artifact_name}"
        );
    }
    for artifact_name in &provisioning.prometheus_artifact_names {
        assert!(
            provisioning.debug_artifact_names.contains(artifact_name),
            "Prometheus artifact {artifact_name} missing from debug artifact envelope list"
        );
    }
    let dashboard_json = matrixraft_grafana_dashboard_json();
    for metric_name in &provisioning.required_metric_names {
        assert!(
            dashboard_json.contains(metric_name),
            "dashboard missing required metric {metric_name}"
        );
    }
    for metric_name in &provisioning.validation_metric_names {
        assert!(
            dashboard_json.contains(metric_name),
            "dashboard missing validation metric {metric_name}"
        );
    }
    assert!(dashboard_json.contains("Support Envelope Validation Ready"));
    assert!(dashboard_json.contains("Support Envelope Validation Issues"));
    assert!(dashboard_json.contains("Support Envelope Issue Breakdown"));
    assert!(dashboard_json.contains("Support Envelope First Issue"));
    assert!(dashboard_json
        .contains("rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\"}"));
    assert!(dashboard_json.contains(
        "rustraft_debug_bundle_validation_issue_total{artifact=\\\"support_envelope\\\"}"
    ));
    assert!(dashboard_json
        .contains("rustraft_debug_bundle_validation_issue{artifact=\\\"support_envelope\\\"}"));
    assert!(dashboard_json.contains(
        "rustraft_debug_bundle_validation_first_issue{artifact=\\\"support_envelope\\\"}"
    ));
    assert!(dashboard_json.contains("Support Envelope Status"));
    assert!(dashboard_json.contains("sum by (support_envelope_status)"));
    assert!(dashboard_json.contains("Support Envelope Severity"));
    assert!(dashboard_json.contains("sum by (support_envelope_severity)"));
    assert!(dashboard_json.contains("follow inspect_error_diagnostics when errors appear"));
    assert!(dashboard_json.contains("target, severity, and message for inspect_error_diagnostics"));
    assert!(dashboard_json.contains("operator triage summary for inspect_error_diagnostics"));
    assert!(
        dashboard_json.contains("When it drops to 0, follow resolve_critical_optimization_hints")
    );
    assert!(dashboard_json.contains("drive resolve_critical_optimization_hints before rollout"));
    assert!(
        dashboard_json.contains("operator triage summary for resolve_critical_optimization_hints")
    );
    assert_eq!(
        provisioning.sample_artifact_command,
        "cargo run --example debug_artifacts"
    );

    let json = matrixraft_observability_provisioning_json();
    assert!(json.contains("rustraft-runtime-overview"));
    assert!(json.contains("RustRaftDebugBundleValidationFailed"));
    assert!(json.contains("RustRaftSupportEnvelopeValidationFailed"));
    assert!(json.contains("rustraft_diagnostic_log_total"));
    assert!(json.contains("inspect_error_diagnostics"));
    assert!(json.contains("wire_critical_alerts"));
    assert!(json.contains("refresh_debug_snapshot"));
    assert!(json.contains("RustRaftDebugSnapshotFreshnessLow warns"));
    assert!(json.contains("RustRaftDebugSnapshotFreshnessLost"));
    assert!(json.contains("configured freshness window"));
    assert!(json.contains("rustraft_debug_snapshot_fresh is 1"));
    assert!(
        json.contains("rustraft_debug_snapshot_age_ms is below rustraft_debug_snapshot_max_age_ms")
    );
    assert!(json.contains("rustraft_debug_snapshot_stale_after_unix_ms is in the future"));
    assert!(json.contains(
        "rustraft_debug_snapshot_remaining_fresh_ms is above rustraft_debug_snapshot_low_fresh_ms"
    ));
    assert!(json.contains("rustraft_debug_snapshot_low_fresh is 1"));
    assert!(json.contains("validate_support_envelope"));
    assert!(json.contains("rustraft_operator_triage_diagnostic_warning_total"));
    assert!(json.contains("rustraft_operator_triage_optimization_warning_total"));
    assert!(json.contains("rustraft_debug_bundle_validation_ready"));
    assert!(json.contains(
        "rustraft_debug_bundle_validation_first_issue{artifact=\\\"support_envelope\\\"} is absent"
    ));
    assert!(json.contains("debug_snapshot_freshness_status is fresh"));
    assert!(json.contains("support_envelope_status is ready"));
    assert!(json.contains("support_envelope_severity is ok"));
    assert!(json.contains("RustRaftObservabilityProvisioningValidationFailed"));
    assert!(json.contains("rustraft_observability_provisioning_validation_ready"));
    assert!(json.contains("debug_snapshot_json"));
    assert!(json.contains("provisioning_runbook_prometheus"));
    assert!(json.contains("support_envelope_validation"));
    assert!(json.contains("support_envelope_validation_prometheus"));
    assert!(
        json.contains("rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\"}")
    );
    assert!(json.contains("debug_snapshot_low_fresh is true"));
    assert!(json.contains("debug_snapshot_fresh is true"));

    let validation = matrixraft_validate_observability_provisioning(&provisioning);
    assert!(validation.ready);
    assert_eq!(validation.issue_count, 0);
    let validation_metrics = matrixraft_observability_provisioning_validation_prometheus(
        &validation,
        &[("service", "raft\"a")],
    );
    assert_eq!(validation_metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(validation_metrics.metric_count, 2);
    assert!(validation_metrics
        .text
        .contains("rustraft_observability_provisioning_validation_ready{service=\"raft\\\"a\"} 1"));
    let json_validation = matrixraft_validate_observability_provisioning_json(&json);
    assert!(json_validation.ready);

    let mut stale_provisioning = provisioning.clone();
    stale_provisioning.dashboard.uid = "old-runtime-dashboard".to_string();
    let stale_dashboard_validation =
        matrixraft_validate_observability_provisioning(&stale_provisioning);
    assert!(!stale_dashboard_validation.ready);
    assert!(stale_dashboard_validation
        .issues
        .contains(&"observability_provisioning_contract_mismatch".to_string()));
    assert!(stale_dashboard_validation
        .issues
        .contains(&"observability_dashboard_mismatch".to_string()));
    assert!(!stale_dashboard_validation
        .issues
        .contains(&"observability_dashboard_metric_not_advertised".to_string()));
    let stale_dashboard_metrics = matrixraft_observability_provisioning_validation_prometheus(
        &stale_dashboard_validation,
        &[("service", "raft\"a")],
    );
    assert!(stale_dashboard_metrics.text.contains(
        "rustraft_observability_provisioning_validation_issue{service=\"raft\\\"a\",issue=\"observability_dashboard_mismatch\"} 1"
    ));
    assert!(stale_dashboard_metrics.text.contains(
        "rustraft_observability_provisioning_validation_first_issue{service=\"raft\\\"a\",issue=\"observability_provisioning_contract_mismatch\"} 1"
    ));

    let mut stale_dashboard_metric = provisioning.clone();
    stale_dashboard_metric.dashboard.panels[0].expr =
        "rustraft_dashboard_unadvertised_metric".to_string();
    let stale_dashboard_metric_validation =
        matrixraft_validate_observability_provisioning(&stale_dashboard_metric);
    assert!(!stale_dashboard_metric_validation.ready);
    assert!(stale_dashboard_metric_validation
        .issues
        .contains(&"observability_dashboard_mismatch".to_string()));
    assert!(stale_dashboard_metric_validation
        .issues
        .contains(&"observability_dashboard_metric_not_advertised".to_string()));

    let mut duplicate_dashboard_panel = provisioning.clone();
    duplicate_dashboard_panel.dashboard.panels[1].id =
        duplicate_dashboard_panel.dashboard.panels[0].id;
    let duplicate_dashboard_panel_validation =
        matrixraft_validate_observability_provisioning(&duplicate_dashboard_panel);
    assert!(!duplicate_dashboard_panel_validation.ready);
    assert!(duplicate_dashboard_panel_validation
        .issues
        .contains(&"observability_dashboard_panel_id_duplicate".to_string()));

    let mut duplicate_required_metric = provisioning.clone();
    duplicate_required_metric
        .required_metric_names
        .push(duplicate_required_metric.required_metric_names[0].clone());
    let duplicate_required_metric_validation =
        matrixraft_validate_observability_provisioning(&duplicate_required_metric);
    assert!(!duplicate_required_metric_validation.ready);
    assert!(duplicate_required_metric_validation
        .issues
        .contains(&"observability_required_metric_name_duplicate".to_string()));

    let mut duplicate_validation_metric = provisioning.clone();
    duplicate_validation_metric
        .validation_metric_names
        .push(duplicate_validation_metric.validation_metric_names[0].clone());
    let duplicate_validation_metric_validation =
        matrixraft_validate_observability_provisioning(&duplicate_validation_metric);
    assert!(!duplicate_validation_metric_validation.ready);
    assert!(duplicate_validation_metric_validation
        .issues
        .contains(&"observability_validation_metric_name_duplicate".to_string()));

    let mut duplicate_debug_artifact = provisioning.clone();
    duplicate_debug_artifact
        .debug_artifact_names
        .push(duplicate_debug_artifact.debug_artifact_names[0].clone());
    let duplicate_debug_artifact_validation =
        matrixraft_validate_observability_provisioning(&duplicate_debug_artifact);
    assert!(!duplicate_debug_artifact_validation.ready);
    assert!(duplicate_debug_artifact_validation
        .issues
        .contains(&"observability_debug_artifact_name_duplicate".to_string()));

    let mut duplicate_prometheus_artifact = provisioning.clone();
    duplicate_prometheus_artifact
        .prometheus_artifact_names
        .push(duplicate_prometheus_artifact.prometheus_artifact_names[0].clone());
    let duplicate_prometheus_artifact_validation =
        matrixraft_validate_observability_provisioning(&duplicate_prometheus_artifact);
    assert!(!duplicate_prometheus_artifact_validation.ready);
    assert!(duplicate_prometheus_artifact_validation
        .issues
        .contains(&"observability_prometheus_artifact_name_duplicate".to_string()));

    let mut missing_runtime_pressure_panel = provisioning.clone();
    missing_runtime_pressure_panel
        .dashboard
        .panels
        .retain(|panel| panel.title != "Runtime Scale Pressure Deficit");
    let missing_runtime_pressure_panel_validation =
        matrixraft_validate_observability_provisioning(&missing_runtime_pressure_panel);
    assert!(!missing_runtime_pressure_panel_validation.ready);
    assert!(missing_runtime_pressure_panel_validation
        .issues
        .contains(&"observability_dashboard_mismatch".to_string()));
    assert!(missing_runtime_pressure_panel_validation.issues.contains(
        &"observability_runtime_pressure_metric_without_dashboard_panel:rustraft_runtime_pressure_scale_deficit"
            .to_string()
    ));

    let escaped_issue_validation = DebugBundleValidationReport {
        ready: false,
        issue_count: 1,
        issues: vec!["issue\"with\\escape".to_string()],
    };
    let escaped_issue_metrics = matrixraft_observability_provisioning_validation_prometheus(
        &escaped_issue_validation,
        &[("service", "raft\\b")],
    );
    assert!(escaped_issue_metrics.text.contains("service=\"raft\\\\b\""));
    assert!(escaped_issue_metrics
        .text
        .contains("issue=\"issue\\\"with\\\\escape\""));

    let mut stale_metrics = provisioning.clone();
    stale_metrics
        .required_metric_names
        .retain(|name| name != "rustraft_optimization_ready");
    let stale_metrics_validation = matrixraft_validate_observability_provisioning(&stale_metrics);
    assert!(!stale_metrics_validation.ready);
    assert!(stale_metrics_validation
        .issues
        .contains(&"observability_required_metrics_mismatch".to_string()));

    let mut stale_alert_metric = provisioning.clone();
    stale_alert_metric.alert_rules[0].expr = "rustraft_unadvertised_metric > 0".to_string();
    let stale_alert_metric_validation =
        matrixraft_validate_observability_provisioning(&stale_alert_metric);
    assert!(!stale_alert_metric_validation.ready);
    assert!(stale_alert_metric_validation
        .issues
        .contains(&"observability_alert_rules_mismatch".to_string()));
    assert!(stale_alert_metric_validation
        .issues
        .contains(&"observability_alert_metric_not_advertised".to_string()));

    let mut stale_debug_artifacts = provisioning.clone();
    stale_debug_artifacts
        .debug_artifact_names
        .retain(|name| name != "debug_snapshot_json");
    let stale_debug_artifacts_validation =
        matrixraft_validate_observability_provisioning(&stale_debug_artifacts);
    assert!(!stale_debug_artifacts_validation.ready);
    assert!(stale_debug_artifacts_validation
        .issues
        .contains(&"observability_debug_artifacts_mismatch".to_string()));

    let mut stale_artifacts = provisioning.clone();
    stale_artifacts
        .prometheus_artifact_names
        .retain(|name| name != "provisioning_runbook_prometheus");
    let stale_artifacts_validation =
        matrixraft_validate_observability_provisioning(&stale_artifacts);
    assert!(!stale_artifacts_validation.ready);
    assert!(stale_artifacts_validation
        .issues
        .contains(&"observability_prometheus_artifacts_mismatch".to_string()));

    let mut stale_runbook = provisioning.clone();
    stale_runbook.runbook_steps.retain(|step| {
        step.id != "inspect_runtime_pressure_bottleneck_warning"
            && step.id != "resolve_production_readiness_runtime_pressure_bottleneck"
    });
    let mut unexpected_runbook_step = provisioning.runbook_steps[0].clone();
    unexpected_runbook_step.id = "unexpected_pressure_runbook_step".to_string();
    stale_runbook.runbook_steps.push(unexpected_runbook_step);
    let stale_runbook_validation = matrixraft_validate_observability_provisioning(&stale_runbook);
    assert!(!stale_runbook_validation.ready);
    assert!(stale_runbook_validation
        .issues
        .contains(&"observability_runbook_steps_mismatch".to_string()));
    assert!(stale_runbook_validation.issues.contains(
        &"observability_runbook_step_missing:inspect_runtime_pressure_bottleneck_warning"
            .to_string()
    ));
    assert!(stale_runbook_validation.issues.contains(
        &"observability_runbook_step_missing:resolve_production_readiness_runtime_pressure_bottleneck"
            .to_string()
    ));
    assert!(stale_runbook_validation.issues.contains(
        &"observability_runbook_step_unexpected:unexpected_pressure_runbook_step".to_string()
    ));

    let invalid_json_validation = matrixraft_validate_observability_provisioning_json("{not-json");
    assert!(!invalid_json_validation.ready);
    assert!(invalid_json_validation
        .issues
        .contains(&"observability_provisioning_json_parse_error".to_string()));
}

#[test]
fn optimization_report_prometheus_exports_hint_metrics() {
    let report = OptimizationReport {
        ready: false,
        hint_count: 2,
        critical_count: 1,
        warning_count: 1,
        hints: vec![
            OptimizationHint {
                id: "wal_commit_range_missing".to_string(),
                severity: OptimizationHintSeverity::Critical,
                component: "wal".to_string(),
                recommendation: "recover WAL range".to_string(),
                observed_value: 9,
                threshold: 10,
            },
            OptimizationHint {
                id: "append_queue_saturated".to_string(),
                severity: OptimizationHintSeverity::Warning,
                component: "replication_pipeline".to_string(),
                recommendation: "raise append queue capacity".to_string(),
                observed_value: 1,
                threshold: 1,
            },
        ],
    };

    let metrics = matrixraft_optimization_report_prometheus(&report, &[("service", "raft\"a")]);
    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(metrics.metric_count, 7);
    assert!(metrics
        .text
        .contains("rustraft_optimization_ready{service=\"raft\\\"a\"} 0"));
    assert!(metrics
        .text
        .contains("rustraft_optimization_critical_total{service=\"raft\\\"a\"} 1"));
    assert!(metrics
        .text
        .contains("rustraft_optimization_warning_total{service=\"raft\\\"a\"} 1"));
    assert!(metrics.text.contains(
        "rustraft_optimization_hint_total{service=\"raft\\\"a\",hint=\"wal_commit_range_missing\",component=\"wal\",severity=\"critical\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_optimization_hint_total{service=\"raft\\\"a\",hint=\"append_queue_saturated\",component=\"replication_pipeline\",severity=\"warning\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_optimization_component_hint_total{service=\"raft\\\"a\",component=\"wal\",severity=\"critical\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_optimization_component_hint_total{service=\"raft\\\"a\",component=\"replication_pipeline\",severity=\"warning\"} 1"
    ));

    let triage = matrixraft_operator_triage_summary(&[], &report, &matrixraft_alert_rules());
    assert_eq!(triage.status, "needs_attention");
    assert_eq!(triage.severity, "critical");
    assert_eq!(triage.critical_optimization_count, 1);
    assert_eq!(
        triage.top_optimization_hint,
        Some("wal_commit_range_missing".to_string())
    );
    let triage_metrics = matrixraft_operator_triage_prometheus(&triage, &[("service", "raft\"a")]);
    assert_eq!(triage_metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(triage_metrics.metric_count, 9);
    assert!(triage_metrics.text.contains(
        "rustraft_operator_triage_status{service=\"raft\\\"a\",status=\"needs_attention\",severity=\"critical\"} 1"
    ));
    assert!(triage_metrics.text.contains(
        "rustraft_operator_triage_first_action{service=\"raft\\\"a\",action=\"Inspect error diagnostics and critical optimization hints first.\",status=\"needs_attention\",severity=\"critical\"} 1"
    ));
    assert!(triage_metrics
        .text
        .contains("rustraft_operator_triage_optimization_critical_total{service=\"raft\\\"a\"} 1"));
    assert!(!triage_metrics
        .text
        .contains("rustraft_operator_triage_top_diagnostic"));
    assert!(triage_metrics.text.contains(
        "rustraft_operator_triage_top_alert{service=\"raft\\\"a\",alert=\"RustRaftOptimizationCriticalHints\",severity=\"critical\"} 1"
    ));
    assert!(triage_metrics.text.contains(
        "rustraft_operator_triage_top_optimization_hint{service=\"raft\\\"a\",hint=\"wal_commit_range_missing\",severity=\"critical\"} 1"
    ));

    let mut escaped_triage = triage.clone();
    escaped_triage.top_diagnostic_target = Some("rustraft.target\"with\\escape".to_string());
    escaped_triage.top_diagnostic_message = Some("diagnostic\"message\\escaped".to_string());
    escaped_triage.top_alert = Some("RustRaftAlert\"With\\Escape".to_string());
    escaped_triage.top_optimization_hint = Some("hint\"with\\escape".to_string());
    let escaped_triage_metrics =
        matrixraft_operator_triage_prometheus(&escaped_triage, &[("service", "raft\\b")]);
    assert!(escaped_triage_metrics
        .text
        .contains("service=\"raft\\\\b\""));
    assert!(escaped_triage_metrics
        .text
        .contains("target=\"rustraft.target\\\"with\\\\escape\""));
    assert!(escaped_triage_metrics
        .text
        .contains("message=\"diagnostic\\\"message\\\\escaped\""));
    assert!(escaped_triage_metrics
        .text
        .contains("alert=\"RustRaftAlert\\\"With\\\\Escape\""));
    assert!(escaped_triage_metrics
        .text
        .contains("hint=\"hint\\\"with\\\\escape\""));

    let runbook = matrixraft_operator_runbook_steps(&triage, &report, &matrixraft_alert_rules());
    assert!(runbook
        .iter()
        .any(|step| step.id == "resolve_critical_optimization_hints"));
    assert!(runbook.iter().any(|step| step.id == "wire_critical_alerts"));
    let bottleneck_step = runbook
        .iter()
        .find(|step| step.id == "resolve_runtime_pressure_bottleneck")
        .expect("runtime pressure bottleneck step");
    assert_eq!(bottleneck_step.severity, "critical");
    assert_eq!(bottleneck_step.target, "runtime_pressure");
    assert!(bottleneck_step
        .action
        .contains("Runtime Pressure Bottlenecks"));
    assert!(bottleneck_step
        .validation
        .contains("rustraft_runtime_pressure_bottleneck_score_percent"));
    let bottleneck_warning_step = runbook
        .iter()
        .find(|step| step.id == "inspect_runtime_pressure_bottleneck_warning")
        .expect("runtime pressure bottleneck warning step");
    assert_eq!(bottleneck_warning_step.severity, "warning");
    assert_eq!(bottleneck_warning_step.target, "runtime_pressure");
    assert!(bottleneck_warning_step
        .action
        .contains("Runtime Pressure Action Sources"));
    assert!(bottleneck_warning_step
        .validation
        .contains("before release-scale QPS, latency, or memory parity"));
    let production_bottleneck_step = runbook
        .iter()
        .find(|step| step.id == "resolve_production_readiness_runtime_pressure_bottleneck")
        .expect("production readiness runtime pressure bottleneck step");
    assert_eq!(production_bottleneck_step.severity, "critical");
    assert_eq!(production_bottleneck_step.target, "production_readiness");
    assert!(production_bottleneck_step
        .action
        .contains("Production Runtime Pressure Bottlenecks"));
    assert!(production_bottleneck_step
        .validation
        .contains("rustraft_production_readiness_runtime_pressure_bottleneck_score_percent"));
    let runbook_metrics =
        matrixraft_operator_runbook_prometheus(&runbook, &[("service", "raft\"a")]);
    assert_eq!(runbook_metrics.format, "prometheus_text_v0.0.4");
    assert!(runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_total{service=\"raft\\\"a\",severity=\"critical\",target=\"optimization\"}"
    ));
    assert!(runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft\\\"a\",step=\"resolve_critical_optimization_hints\",severity=\"critical\",target=\"optimization\"} 1"
    ));
    assert!(runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft\\\"a\",step=\"resolve_runtime_pressure_bottleneck\",severity=\"critical\",target=\"runtime_pressure\"} 1"
    ));
    assert!(runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft\\\"a\",step=\"resolve_production_readiness_runtime_pressure_bottleneck\",severity=\"critical\",target=\"production_readiness\"} 1"
    ));
    assert!(runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft\\\"a\",step=\"inspect_runtime_pressure_bottleneck_warning\",severity=\"warning\",target=\"runtime_pressure\"} 1"
    ));
    assert!(runbook_metrics.text.contains(
        "rustraft_operator_runbook_first_step{service=\"raft\\\"a\",step=\"resolve_critical_optimization_hints\",severity=\"critical\",target=\"optimization\"} 1"
    ));

    let diagnostics = vec![
        DiagnosticLogEntry {
            target: "rustraft.quorum".to_string(),
            severity: DiagnosticSeverity::Error,
            message: "quorum_not_observed".to_string(),
            fields: vec![("observed".to_string(), "1".to_string())],
        },
        DiagnosticLogEntry {
            target: "rustraft.pipeline".to_string(),
            severity: DiagnosticSeverity::Warn,
            message: "append_queue_pressure".to_string(),
            fields: vec![("depth".to_string(), "9".to_string())],
        },
    ];
    let diagnostic_metrics =
        matrixraft_diagnostic_log_prometheus(&diagnostics, &[("service", "raft\"a")]);
    assert_eq!(diagnostic_metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(diagnostic_metrics.metric_count, 5);
    assert!(diagnostic_metrics
        .text
        .contains("rustraft_diagnostic_log_total{service=\"raft\\\"a\",severity=\"error\"} 1"));
    assert!(diagnostic_metrics.text.contains(
        "rustraft_diagnostic_log_entry_total{service=\"raft\\\"a\",target=\"rustraft.quorum\",severity=\"error\",message=\"quorum_not_observed\"} 1"
    ));
    let diagnostic_triage =
        matrixraft_operator_triage_summary(&diagnostics, &report, &matrixraft_alert_rules());
    assert_eq!(
        diagnostic_triage.top_diagnostic_target.as_deref(),
        Some("rustraft.quorum")
    );
    assert_eq!(
        diagnostic_triage.top_diagnostic_message.as_deref(),
        Some("quorum_not_observed")
    );
    let diagnostic_triage_metrics =
        matrixraft_operator_triage_prometheus(&diagnostic_triage, &[("service", "raft\"a")]);
    assert_eq!(diagnostic_triage_metrics.metric_count, 10);
    assert!(diagnostic_triage_metrics.text.contains(
        "rustraft_operator_triage_top_diagnostic{service=\"raft\\\"a\",target=\"rustraft.quorum\",message=\"quorum_not_observed\",severity=\"critical\"} 1"
    ));

    let production_readiness_diagnostics = vec![
        DiagnosticLogEntry {
            target: "rustraft.production_readiness".to_string(),
            severity: DiagnosticSeverity::Error,
            message: "rustraft production readiness blocked".to_string(),
            fields: vec![("ready".to_string(), "false".to_string())],
        },
        DiagnosticLogEntry {
            target: "rustraft.production_readiness.missing".to_string(),
            severity: DiagnosticSeverity::Warn,
            message: "pipeline:evidence_present".to_string(),
            fields: vec![("ready".to_string(), "false".to_string())],
        },
        DiagnosticLogEntry {
            target: "rustraft.production_readiness.runtime_pressure_evidence".to_string(),
            severity: DiagnosticSeverity::Error,
            message: "runtime_pressure:latency_pressure_detail_sample_count_zero:latency.append"
                .to_string(),
            fields: vec![("ready".to_string(), "false".to_string())],
        },
    ];
    let readiness_triage = matrixraft_operator_triage_summary(
        &production_readiness_diagnostics,
        &OptimizationReport {
            ready: true,
            hint_count: 0,
            critical_count: 0,
            warning_count: 0,
            hints: vec![],
        },
        &matrixraft_alert_rules(),
    );
    let readiness_runbook = matrixraft_operator_runbook_steps_with_diagnostics(
        &production_readiness_diagnostics,
        &readiness_triage,
        &OptimizationReport {
            ready: true,
            hint_count: 0,
            critical_count: 0,
            warning_count: 0,
            hints: vec![],
        },
        &matrixraft_alert_rules(),
    );
    let readiness_step = readiness_runbook
        .iter()
        .find(|step| step.id == "resolve_production_readiness_blockers")
        .expect("production readiness runbook step");
    assert_eq!(readiness_step.severity, "critical");
    assert_eq!(readiness_step.target, "production_readiness");
    assert!(readiness_step
        .validation
        .contains("rustraft_production_readiness_blocker_total is 0"));
    let pressure_evidence_step = readiness_runbook
        .iter()
        .find(|step| step.id == "fix_runtime_pressure_evidence")
        .expect("runtime pressure evidence runbook step");
    assert_eq!(pressure_evidence_step.severity, "critical");
    assert_eq!(pressure_evidence_step.target, "runtime_pressure");
    assert!(pressure_evidence_step.action.contains("release-scale run"));
    let readiness_runbook_metrics =
        matrixraft_operator_runbook_prometheus(&readiness_runbook, &[("service", "raft\"a")]);
    assert!(readiness_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft\\\"a\",step=\"resolve_production_readiness_blockers\",severity=\"critical\",target=\"production_readiness\"} 1"
    ));
    assert!(readiness_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft\\\"a\",step=\"fix_runtime_pressure_evidence\",severity=\"critical\",target=\"runtime_pressure\"} 1"
    ));
}
