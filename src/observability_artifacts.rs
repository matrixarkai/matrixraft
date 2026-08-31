// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Deployable observability artifacts, rendered from the in-crate model.
//!
//! [`crate::matrixraft_grafana_dashboard`] describes the dashboard in a shape
//! that suits validation: a flat list of panels, each with one expression. That
//! shape is not what Grafana imports -- a Grafana panel needs a `gridPos`, a
//! datasource, and its query under `targets`, none of which the model carries.
//! So [`matrixraft_grafana_dashboard_import_json`] renders the model into the
//! schema Grafana actually accepts, rather than the model being serialised
//! directly and rejected at import.
//!
//! Everything here is derived from the model, never written alongside it, so a
//! panel or an alert added in `metrics.rs` cannot go missing from what gets
//! deployed. `tests/observability_artifacts.rs` asserts the checked-in files
//! match what these functions render.

use serde_json::json;

use crate::metrics::{
    matrixraft_alert_rules, matrixraft_grafana_dashboard, AlertRule, GrafanaDashboard,
};

/// One rendered file: where it belongs, and what goes in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityArtifact {
    pub path: String,
    pub contents: String,
}

/// Panels are laid out three to a row on Grafana's 24-column grid.
const PANEL_WIDTH: u32 = 8;
const PANEL_HEIGHT: u32 = 8;
const PANELS_PER_ROW: u32 = 3;

/// The datasource variable the dashboard is parameterised on, so an import can
/// pick a Prometheus without the JSON naming one.
const DATASOURCE_VARIABLE: &str = "${DS_PROMETHEUS}";

/// Quotes a scalar for YAML, escaping what a double-quoted scalar must escape.
///
/// Alert summaries are prose and contain apostrophes and colons, either of
/// which turns an unquoted scalar into a parse error or a map.
fn yaml_quoted(value: &str) -> String {
    // Written with unicode escapes rather than literal backslashes so the
    // intent stays readable: a backslash becomes two, a quote becomes
    // backslash-quote, and the whole scalar is wrapped in quotes.
    let escaped = value
        .replace('\u{5c}', "\u{5c}\u{5c}")
        .replace('\u{22}', "\u{5c}\u{22}");
    format!("\u{22}{escaped}\u{22}")
}

/// Renders the dashboard model into the schema Grafana imports.
pub fn matrixraft_grafana_dashboard_import_json() -> String {
    let dashboard: GrafanaDashboard = matrixraft_grafana_dashboard();
    let datasource = json!({ "type": "prometheus", "uid": DATASOURCE_VARIABLE });

    let panels: Vec<_> = dashboard
        .panels
        .iter()
        .enumerate()
        .map(|(position, panel)| {
            let position = position as u32;
            json!({
                "id": panel.id,
                "title": panel.title,
                "type": panel.panel_type,
                "description": panel.description,
                "datasource": datasource,
                "gridPos": {
                    "h": PANEL_HEIGHT,
                    "w": PANEL_WIDTH,
                    "x": (position % PANELS_PER_ROW) * PANEL_WIDTH,
                    "y": (position / PANELS_PER_ROW) * PANEL_HEIGHT,
                },
                "fieldConfig": {
                    "defaults": { "unit": panel.unit },
                    "overrides": [],
                },
                "targets": [{
                    "refId": "A",
                    "expr": panel.expr,
                    "datasource": datasource,
                    "legendFormat": "__auto",
                }],
            })
        })
        .collect();

    let rendered = json!({
        "uid": dashboard.uid,
        "title": dashboard.title,
        "timezone": dashboard.timezone,
        "schemaVersion": dashboard.schema_version,
        "refresh": dashboard.refresh,
        "tags": dashboard.tags,
        "editable": true,
        "time": { "from": "now-6h", "to": "now" },
        "templating": {
            "list": [{
                "name": "DS_PROMETHEUS",
                "label": "Data source",
                "type": "datasource",
                "query": "prometheus",
                "hide": 0,
                "refresh": 1,
            }],
        },
        "panels": panels,
    });

    format!(
        "{}\n",
        serde_json::to_string_pretty(&rendered).expect("dashboard must serialize")
    )
}

/// Renders the alert rules as a Prometheus rule file.
pub fn matrixraft_prometheus_alert_rules_yaml() -> String {
    let rules: Vec<AlertRule> = matrixraft_alert_rules();
    let mut out = String::from("groups:\n  - name: matrixraft\n    rules:\n");
    for rule in &rules {
        out.push_str(&format!("      - alert: {}\n", rule.alert));
        out.push_str(&format!("        expr: {}\n", yaml_quoted(&rule.expr)));
        out.push_str(&format!("        for: {}\n", rule.duration));
        out.push_str("        labels:\n");
        out.push_str(&format!(
            "          severity: {}\n",
            yaml_quoted(&rule.severity)
        ));
        out.push_str("        annotations:\n");
        out.push_str(&format!(
            "          summary: {}\n",
            yaml_quoted(&rule.summary)
        ));
    }
    out
}

/// Where Prometheus looks for the node's metrics text.
///
/// The crate does not serve this itself -- it renders Prometheus text from
/// `metrics.rs` and leaves exposing it to the embedder. `examples/metrics_exporter`
/// is a runnable one, and is what this target points at.
pub const DEFAULT_SCRAPE_TARGET: &str = "host.docker.internal:9464";

/// Renders the Prometheus server configuration.
pub fn matrixraft_prometheus_config_yaml() -> String {
    format!(
        "global:\n  \
           scrape_interval: 15s\n  \
           evaluation_interval: 15s\n\
         rule_files:\n  \
           - /etc/prometheus/rules/matrixraft-alerts.yaml\n\
         scrape_configs:\n  \
           - job_name: matrixraft\n    \
               metrics_path: /metrics\n    \
               static_configs:\n      \
                 - targets: [{}]\n",
        yaml_quoted(DEFAULT_SCRAPE_TARGET)
    )
}

/// Renders the Grafana datasource provisioning file.
pub fn matrixraft_grafana_datasource_provisioning_yaml() -> String {
    "apiVersion: 1\n\
     datasources:\n  \
       - name: Prometheus\n    \
           uid: matrixraft-prometheus\n    \
           type: prometheus\n    \
           access: proxy\n    \
           url: http://prometheus:9090\n    \
           isDefault: true\n"
        .to_string()
}

/// Renders the Grafana dashboard provisioning file.
pub fn matrixraft_grafana_dashboard_provisioning_yaml() -> String {
    "apiVersion: 1\n\
     providers:\n  \
       - name: matrixraft\n    \
           orgId: 1\n    \
           folder: MatrixRaft\n    \
           type: file\n    \
           disableDeletion: false\n    \
           updateIntervalSeconds: 30\n    \
           allowUiUpdates: true\n    \
           options:\n      \
             path: /etc/grafana/provisioning/dashboards/matrixraft\n      \
             foldersFromFilesStructure: false\n"
        .to_string()
}

/// Renders a Compose file that brings up Prometheus and Grafana already
/// provisioned with the dashboard and the alert rules.
pub fn matrixraft_observability_compose_yaml() -> String {
    "services:\n  \
       prometheus:\n    \
           image: prom/prometheus:v2.54.1\n    \
           ports:\n      \
             - \"9090:9090\"\n    \
           volumes:\n      \
             - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro\n      \
             - ./prometheus/rules:/etc/prometheus/rules:ro\n    \
           extra_hosts:\n      \
             - \"host.docker.internal:host-gateway\"\n  \
       grafana:\n    \
           image: grafana/grafana:11.1.4\n    \
           depends_on:\n      \
             - prometheus\n    \
           ports:\n      \
             - \"3000:3000\"\n    \
           environment:\n      \
             GF_AUTH_ANONYMOUS_ENABLED: \"true\"\n      \
             GF_AUTH_ANONYMOUS_ORG_ROLE: Admin\n    \
           volumes:\n      \
             - ./grafana/provisioning/datasources:/etc/grafana/provisioning/datasources:ro\n      \
             - ./grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards:ro\n      \
             - ./grafana/dashboards:/etc/grafana/provisioning/dashboards/matrixraft:ro\n"
        .to_string()
}

/// Every artifact, with the path it belongs at under the observability root.
///
/// One list, used by the renderer that writes the files and by the test that
/// asserts the checked-in copies still match. Adding an artifact here is enough
/// to have it written and guarded.
pub fn matrixraft_observability_artifacts() -> Vec<ObservabilityArtifact> {
    vec![
        ObservabilityArtifact {
            path: "grafana/dashboards/matrixraft-runtime-overview.json".to_string(),
            contents: matrixraft_grafana_dashboard_import_json(),
        },
        ObservabilityArtifact {
            path: "grafana/provisioning/datasources/prometheus.yaml".to_string(),
            contents: matrixraft_grafana_datasource_provisioning_yaml(),
        },
        ObservabilityArtifact {
            path: "grafana/provisioning/dashboards/matrixraft.yaml".to_string(),
            contents: matrixraft_grafana_dashboard_provisioning_yaml(),
        },
        ObservabilityArtifact {
            path: "prometheus/prometheus.yml".to_string(),
            contents: matrixraft_prometheus_config_yaml(),
        },
        ObservabilityArtifact {
            path: "prometheus/rules/matrixraft-alerts.yaml".to_string(),
            contents: matrixraft_prometheus_alert_rules_yaml(),
        },
        ObservabilityArtifact {
            path: "docker-compose.yml".to_string(),
            contents: matrixraft_observability_compose_yaml(),
        },
    ]
}
