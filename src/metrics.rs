// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Metric-name contract for RustRaft observability.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub use crate::matrixraft_baseline_raft_runtime_capability_prometheus;
use crate::status::{
    matrixraft_admin_diagnostic_log_entries, matrixraft_optimization_report,
    AdminStatusSurfaceInput, DiagnosticLogEntry, DiagnosticSeverity, OptimizationHintSeverity,
    OptimizationReport, RuntimeAdminReport,
};

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
pub struct PrometheusMetricSet {
    pub format: String,
    pub metric_count: u64,
    pub text: String,
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

pub fn matrixraft_alert_rules() -> Vec<AlertRule> {
    let metrics = matrixraft_metric_names();
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

pub fn matrixraft_alert_rules_json() -> String {
    serde_json::to_string_pretty(&matrixraft_alert_rules())
        .expect("RustRaft alert rules must serialize")
}

pub fn matrixraft_observability_provisioning() -> ObservabilityProvisioning {
    let metrics = matrixraft_metric_names();
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
            "diagnostic_prometheus".to_string(),
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
    if provisioning.alert_rules != expected.alert_rules {
        issues.push("observability_alert_rules_mismatch".to_string());
    }
    if matrixraft_alert_rules_have_unadvertised_metrics(provisioning) {
        issues.push("observability_alert_metric_not_advertised".to_string());
    }
    if provisioning.runbook_steps != expected.runbook_steps {
        issues.push("observability_runbook_steps_mismatch".to_string());
    }
    if provisioning.debug_bundle_contract != expected.debug_bundle_contract {
        issues.push("observability_debug_bundle_contract_mismatch".to_string());
    }
    if provisioning.sample_artifact_command != expected.sample_artifact_command {
        issues.push("observability_sample_artifact_command_mismatch".to_string());
    }

    matrixraft_debug_bundle_validation_report(issues)
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
        matrixraft_alert_expr_metric_name(&rule.expr)
            .is_none_or(|metric| !advertised_metrics.contains(metric))
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

fn matrixraft_expr_references_metric(expr: &str, metric: &str) -> bool {
    if let Some(start) = expr.find(metric) {
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
    } else {
        false
    }
}

fn matrixraft_alert_expr_metric_name(expr: &str) -> Option<&str> {
    let start = expr.find(|ch: char| ch.is_ascii_alphabetic() || ch == '_')?;
    let end = expr[start..]
        .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .map(|offset| start + offset)
        .unwrap_or(expr.len());
    Some(&expr[start..end])
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
        if !snapshot
            .optimization_prometheus
            .text
            .contains(required_metric)
        {
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
    if snapshot.diagnostics != matrixraft_admin_diagnostic_log_entries(&snapshot.admin_report) {
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
        if !snapshot
            .diagnostic_prometheus
            .text
            .contains(required_metric)
        {
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
    let optimization_hint_count = snapshot.optimization.hints.len() as u64;
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
        if !snapshot.runbook_prometheus.text.contains(required_metric) {
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
    let expected_runbook_steps = matrixraft_operator_runbook_steps(
        &snapshot.triage,
        &snapshot.optimization,
        &snapshot.alerts,
    );
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
    match snapshot.triage.status.as_str() {
        "ready" => {
            if snapshot.triage.severity != "info" {
                issues.push("triage_ready_severity_mismatch".to_string());
            }
            if !snapshot
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
    if triage.diagnostic_warning_count > 0 || optimization.warning_count > 0 {
        steps.push(matrixraft_runbook_step(
            "review_warning_signals",
            "warning",
            "diagnostics",
            "Review warning diagnostics and optimization hints for trend risk.",
            "rustraft_operator_triage_diagnostic_warning_total and rustraft_operator_triage_optimization_warning_total are 0 or acknowledged before production claims.",
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
    let optimization = matrixraft_optimization_report(status_surface);
    let diagnostics = matrixraft_admin_diagnostic_log_entries(admin_report);
    let diagnostic_prometheus = matrixraft_diagnostic_log_prometheus(&diagnostics, labels);
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
        optimization_prometheus: matrixraft_optimization_report_prometheus(&optimization, labels),
        optimization,
        grafana: matrixraft_grafana_dashboard(),
        alerts,
        triage,
        runbook_prometheus,
        runbook_steps,
    }
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
        panels: vec![
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
        ],
    }
}

pub fn matrixraft_grafana_dashboard_json() -> String {
    serde_json::to_string_pretty(&matrixraft_grafana_dashboard())
        .expect("RustRaft Grafana dashboard must serialize")
}

fn percentile_expr(metric: &str, quantile: &str) -> String {
    format!("histogram_quantile({quantile}, sum by (le) (rate({metric}_bucket[5m])))")
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
