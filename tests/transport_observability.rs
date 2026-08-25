// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    metrics::{
        rustraft_alert_rules, rustraft_alert_rules_json, rustraft_diagnostic_log_prometheus,
        rustraft_grafana_dashboard, rustraft_grafana_dashboard_json, rustraft_metric_names,
        rustraft_observability_provisioning, rustraft_observability_provisioning_json,
        rustraft_observability_provisioning_runbook_steps,
        rustraft_observability_provisioning_validation_prometheus,
        rustraft_operator_runbook_prometheus, rustraft_operator_runbook_steps,
        rustraft_operator_triage_prometheus, rustraft_operator_triage_summary,
        rustraft_optimization_report_prometheus, rustraft_validate_observability_provisioning,
        rustraft_validate_observability_provisioning_json,
    },
    readiness::{
        rustraft_baseline_raft_parity_surface, rustraft_parity_report,
        rustraft_public_api_contract, RustRaftReadinessSnapshot,
    },
    status::{
        rustraft_fatal_blocker_report, RustRaftBlockerSeverity, RustRaftDiagnosticLogEntry,
        RustRaftDiagnosticSeverity, RustRaftOptimizationHint, RustRaftOptimizationHintSeverity,
        RustRaftOptimizationReport,
    },
    transport::{
        AppendEntriesRequest, InstallSnapshotRequest, PreVoteRequest, PreVoteResponse,
        ReadIndexRequest, RustRaftSnapshotChunk, VoteRequest,
    },
    RustRaftDebugBundleValidationReport, RustRaftInstallSnapshotResponse, RustRaftLogId,
    RustRaftSnapshotMeta,
};
use serde_json::Value;

fn ready_snapshot() -> RustRaftReadinessSnapshot {
    RustRaftReadinessSnapshot {
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

#[test]
fn transport_contract_names_owned_rpc_types_including_prevote_and_snapshot_chunks() {
    let append = AppendEntriesRequest {
        group_id: 9,
        term: 4,
        leader_id: 1,
        prev_log_id: Some(RustRaftLogId { term: 4, index: 9 }),
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

    let chunk = RustRaftSnapshotChunk {
        meta: RustRaftSnapshotMeta {
            snapshot_id: "transport-observability".to_string(),
            last_log_id: RustRaftLogId { term: 4, index: 10 },
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
    let install_response = RustRaftInstallSnapshotResponse {
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
    let metrics = rustraft_metric_names();
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

    let api = rustraft_public_api_contract();
    assert!(api.rpc_messages.contains(&"PreVoteRequest".to_string()));
    assert!(api.rpc_messages.contains(&"PreVoteResponse".to_string()));
    assert!(api
        .rpc_messages
        .contains(&"RustRaftSnapshotChunk".to_string()));
    assert!(api
        .rpc_messages
        .contains(&"RustRaftTransportValidationReport".to_string()));
    assert!(api
        .rpc_messages
        .contains(&"InMemoryRaftTransport".to_string()));
    assert!(api
        .safety_helpers
        .contains(&"rustraft_fatal_blocker_report".to_string()));

    let surface = rustraft_baseline_raft_parity_surface();
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

    let readiness = rustraft_parity_report(&ready_snapshot());
    assert!(readiness.ready);

    let blockers = rustraft_fatal_blocker_report(
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
        RustRaftBlockerSeverity::Fatal
    );
}

#[test]
fn grafana_dashboard_exports_runtime_metric_panels() {
    let metrics = rustraft_metric_names();
    let dashboard = rustraft_grafana_dashboard();
    assert_eq!(dashboard.uid, "rustraft-runtime-overview");
    assert_eq!(dashboard.refresh, "10s");
    // The count is pinned so a panel cannot appear or vanish unnoticed. It went 53 -> 55 when
    // the support envelope panels landed; naming them keeps the number from being a figure
    // nobody can check.
    assert_eq!(dashboard.panels.len(), 55);
    for title in ["Support Envelope Status", "Support Envelope Severity"] {
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

    let json = rustraft_grafana_dashboard_json();
    let parsed: Value = serde_json::from_str(&json).expect("dashboard json");
    assert_eq!(parsed["title"], "RustRaft Runtime Overview");
    // Same pin, checked through the serialized JSON: the struct and the exported document
    // must agree on how many panels there are.
    assert_eq!(parsed["panels"].as_array().expect("panels").len(), 55);
    assert!(json.contains("histogram_quantile(0.99"));
    assert!(json.contains("rustraft_blocker_total"));
    assert!(json.contains("rustraft_fatal_total"));
    assert!(json.contains("rustraft_diagnostic_log_total"));
    assert!(json.contains("rustraft_diagnostic_log_entry_total"));
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
    let rules = rustraft_alert_rules();
    // Pinned for the same reason as the panel count: 15 -> 16 with the provisioning
    // validation alert.
    assert_eq!(rules.len(), 16);
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

    let json = rustraft_alert_rules_json();
    let parsed: Value = serde_json::from_str(&json).expect("alert rule json");
    assert_eq!(parsed.as_array().expect("alert rules").len(), 16);
    assert!(json.contains("RustRaftOptimizationWarningHints"));
    assert!(json.contains("rustraft_optimization_warning_total > 0"));
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
fn observability_provisioning_exports_dashboard_alerts_metrics_and_bundle_contract() {
    let provisioning = rustraft_observability_provisioning();
    assert_eq!(provisioning.service, "rustraft");
    assert_eq!(provisioning.prometheus_format, "prometheus_text_v0.0.4");
    assert_eq!(provisioning.dashboard.uid, "rustraft-runtime-overview");
    assert_eq!(provisioning.alert_rules, rustraft_alert_rules());
    assert_eq!(
        provisioning.runbook_steps,
        rustraft_observability_provisioning_runbook_steps()
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
    assert!(provisioning
        .runbook_steps
        .iter()
        .any(|step| step.id == "refresh_debug_snapshot"));
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
    let provisioning_runbook_metrics =
        rustraft_operator_runbook_prometheus(&provisioning.runbook_steps, &[("service", "raft-a")]);
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"refresh_debug_snapshot\",severity=\"warning\",target=\"debug_bundle\"} 1"
    ));
    assert!(provisioning_runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft-a\",step=\"validate_support_envelope\",severity=\"warning\",target=\"support_envelope\"} 1"
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
    let dashboard_json = rustraft_grafana_dashboard_json();
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

    let json = rustraft_observability_provisioning_json();
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

    let validation = rustraft_validate_observability_provisioning(&provisioning);
    assert!(validation.ready);
    assert_eq!(validation.issue_count, 0);
    let validation_metrics = rustraft_observability_provisioning_validation_prometheus(
        &validation,
        &[("service", "raft\"a")],
    );
    assert_eq!(validation_metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(validation_metrics.metric_count, 2);
    assert!(validation_metrics
        .text
        .contains("rustraft_observability_provisioning_validation_ready{service=\"raft\\\"a\"} 1"));
    let json_validation = rustraft_validate_observability_provisioning_json(&json);
    assert!(json_validation.ready);

    let mut stale_provisioning = provisioning.clone();
    stale_provisioning.dashboard.uid = "old-runtime-dashboard".to_string();
    let stale_dashboard_validation =
        rustraft_validate_observability_provisioning(&stale_provisioning);
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
    let stale_dashboard_metrics = rustraft_observability_provisioning_validation_prometheus(
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
        rustraft_validate_observability_provisioning(&stale_dashboard_metric);
    assert!(!stale_dashboard_metric_validation.ready);
    assert!(stale_dashboard_metric_validation
        .issues
        .contains(&"observability_dashboard_mismatch".to_string()));
    assert!(stale_dashboard_metric_validation
        .issues
        .contains(&"observability_dashboard_metric_not_advertised".to_string()));

    let escaped_issue_validation = RustRaftDebugBundleValidationReport {
        ready: false,
        issue_count: 1,
        issues: vec!["issue\"with\\escape".to_string()],
    };
    let escaped_issue_metrics = rustraft_observability_provisioning_validation_prometheus(
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
    let stale_metrics_validation = rustraft_validate_observability_provisioning(&stale_metrics);
    assert!(!stale_metrics_validation.ready);
    assert!(stale_metrics_validation
        .issues
        .contains(&"observability_required_metrics_mismatch".to_string()));

    let mut stale_alert_metric = provisioning.clone();
    stale_alert_metric.alert_rules[0].expr = "rustraft_unadvertised_metric > 0".to_string();
    let stale_alert_metric_validation =
        rustraft_validate_observability_provisioning(&stale_alert_metric);
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
        rustraft_validate_observability_provisioning(&stale_debug_artifacts);
    assert!(!stale_debug_artifacts_validation.ready);
    assert!(stale_debug_artifacts_validation
        .issues
        .contains(&"observability_debug_artifacts_mismatch".to_string()));

    let mut stale_artifacts = provisioning.clone();
    stale_artifacts
        .prometheus_artifact_names
        .retain(|name| name != "provisioning_runbook_prometheus");
    let stale_artifacts_validation = rustraft_validate_observability_provisioning(&stale_artifacts);
    assert!(!stale_artifacts_validation.ready);
    assert!(stale_artifacts_validation
        .issues
        .contains(&"observability_prometheus_artifacts_mismatch".to_string()));

    let mut stale_runbook = provisioning.clone();
    stale_runbook.runbook_steps.clear();
    let stale_runbook_validation = rustraft_validate_observability_provisioning(&stale_runbook);
    assert!(!stale_runbook_validation.ready);
    assert!(stale_runbook_validation
        .issues
        .contains(&"observability_runbook_steps_mismatch".to_string()));

    let invalid_json_validation = rustraft_validate_observability_provisioning_json("{not-json");
    assert!(!invalid_json_validation.ready);
    assert!(invalid_json_validation
        .issues
        .contains(&"observability_provisioning_json_parse_error".to_string()));
}

#[test]
fn optimization_report_prometheus_exports_hint_metrics() {
    let report = RustRaftOptimizationReport {
        ready: false,
        hint_count: 2,
        critical_count: 1,
        warning_count: 1,
        hints: vec![
            RustRaftOptimizationHint {
                id: "wal_commit_range_missing".to_string(),
                severity: RustRaftOptimizationHintSeverity::Critical,
                component: "wal".to_string(),
                recommendation: "recover WAL range".to_string(),
                observed_value: 9,
                threshold: 10,
            },
            RustRaftOptimizationHint {
                id: "append_queue_saturated".to_string(),
                severity: RustRaftOptimizationHintSeverity::Warning,
                component: "replication_pipeline".to_string(),
                recommendation: "raise append queue capacity".to_string(),
                observed_value: 1,
                threshold: 1,
            },
        ],
    };

    let metrics = rustraft_optimization_report_prometheus(&report, &[("service", "raft\"a")]);
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

    let triage = rustraft_operator_triage_summary(&[], &report, &rustraft_alert_rules());
    assert_eq!(triage.status, "needs_attention");
    assert_eq!(triage.severity, "critical");
    assert_eq!(triage.critical_optimization_count, 1);
    assert_eq!(
        triage.top_optimization_hint,
        Some("wal_commit_range_missing".to_string())
    );
    let triage_metrics = rustraft_operator_triage_prometheus(&triage, &[("service", "raft\"a")]);
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
        rustraft_operator_triage_prometheus(&escaped_triage, &[("service", "raft\\b")]);
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

    let runbook = rustraft_operator_runbook_steps(&triage, &report, &rustraft_alert_rules());
    assert!(runbook
        .iter()
        .any(|step| step.id == "resolve_critical_optimization_hints"));
    assert!(runbook.iter().any(|step| step.id == "wire_critical_alerts"));
    let runbook_metrics = rustraft_operator_runbook_prometheus(&runbook, &[("service", "raft\"a")]);
    assert_eq!(runbook_metrics.format, "prometheus_text_v0.0.4");
    assert!(runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_total{service=\"raft\\\"a\",severity=\"critical\",target=\"optimization\"}"
    ));
    assert!(runbook_metrics.text.contains(
        "rustraft_operator_runbook_step_present{service=\"raft\\\"a\",step=\"resolve_critical_optimization_hints\",severity=\"critical\",target=\"optimization\"} 1"
    ));
    assert!(runbook_metrics.text.contains(
        "rustraft_operator_runbook_first_step{service=\"raft\\\"a\",step=\"resolve_critical_optimization_hints\",severity=\"critical\",target=\"optimization\"} 1"
    ));

    let diagnostics = vec![
        RustRaftDiagnosticLogEntry {
            target: "rustraft.quorum".to_string(),
            severity: RustRaftDiagnosticSeverity::Error,
            message: "quorum_not_observed".to_string(),
            fields: vec![("observed".to_string(), "1".to_string())],
        },
        RustRaftDiagnosticLogEntry {
            target: "rustraft.pipeline".to_string(),
            severity: RustRaftDiagnosticSeverity::Warn,
            message: "append_queue_pressure".to_string(),
            fields: vec![("depth".to_string(), "9".to_string())],
        },
    ];
    let diagnostic_metrics =
        rustraft_diagnostic_log_prometheus(&diagnostics, &[("service", "raft\"a")]);
    assert_eq!(diagnostic_metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(diagnostic_metrics.metric_count, 5);
    assert!(diagnostic_metrics
        .text
        .contains("rustraft_diagnostic_log_total{service=\"raft\\\"a\",severity=\"error\"} 1"));
    assert!(diagnostic_metrics.text.contains(
        "rustraft_diagnostic_log_entry_total{service=\"raft\\\"a\",target=\"rustraft.quorum\",severity=\"error\",message=\"quorum_not_observed\"} 1"
    ));
    let diagnostic_triage =
        rustraft_operator_triage_summary(&diagnostics, &report, &rustraft_alert_rules());
    assert_eq!(
        diagnostic_triage.top_diagnostic_target.as_deref(),
        Some("rustraft.quorum")
    );
    assert_eq!(
        diagnostic_triage.top_diagnostic_message.as_deref(),
        Some("quorum_not_observed")
    );
    let diagnostic_triage_metrics =
        rustraft_operator_triage_prometheus(&diagnostic_triage, &[("service", "raft\"a")]);
    assert_eq!(diagnostic_triage_metrics.metric_count, 10);
    assert!(diagnostic_triage_metrics.text.contains(
        "rustraft_operator_triage_top_diagnostic{service=\"raft\\\"a\",target=\"rustraft.quorum\",message=\"quorum_not_observed\",severity=\"critical\"} 1"
    ));
}
