// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The checked-in observability artifacts have to stay what the model renders.
//!
//! `metrics.rs` is the source of the panels and the alert rules. If someone
//! adds a panel there and does not regenerate, the dashboard that gets deployed
//! silently lacks it -- these tests are what turns that into a failure.

use std::path::Path;

use matrixraft::matrixraft_alert_rules;
use matrixraft::matrixraft_grafana_dashboard;
use matrixraft::observability_artifacts::{
    matrixraft_grafana_dashboard_import_json, matrixraft_observability_artifacts,
};

#[test]
fn the_checked_in_artifacts_match_what_the_model_renders() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("observability");
    for artifact in matrixraft_observability_artifacts() {
        let path = root.join(&artifact.path);
        let found = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("{} is missing: {err}", artifact.path));
        assert_eq!(
            found, artifact.contents,
            "{} drifted from the model; run `cargo run --example render_observability_artifacts`",
            artifact.path
        );
    }
}

/// The model's own JSON is not importable -- it has no `gridPos`, no
/// `targets`, and no datasource. This asserts the rendered form has them for
/// every panel, since a dashboard missing them fails at import rather than
/// looking wrong.
#[test]
fn every_rendered_panel_carries_what_grafana_needs_to_import_it() {
    let rendered: serde_json::Value =
        serde_json::from_str(&matrixraft_grafana_dashboard_import_json()).expect("valid json");
    let panels = rendered["panels"].as_array().expect("panels array");

    assert_eq!(
        panels.len(),
        matrixraft_grafana_dashboard().panels.len(),
        "every modelled panel has to reach the dashboard"
    );
    assert!(!panels.is_empty(), "a dashboard with no panels is not one");

    for panel in panels {
        let title = panel["title"].as_str().unwrap_or("<untitled>");
        assert!(
            panel["gridPos"]["w"].is_number() && panel["gridPos"]["h"].is_number(),
            "panel {title} has no gridPos, so Grafana would stack it at the origin"
        );
        let targets = panel["targets"].as_array().expect("targets array");
        assert_eq!(targets.len(), 1, "panel {title} should carry its one query");
        let expr = targets[0]["expr"].as_str().unwrap_or_default();
        assert!(
            !expr.is_empty(),
            "panel {title} has an empty query and would draw nothing"
        );
        assert_eq!(
            panel["datasource"]["type"], "prometheus",
            "panel {title} must name a Prometheus datasource"
        );
    }
}

#[test]
fn every_alert_rule_reaches_the_prometheus_rule_file() {
    let rendered = matrixraft_observability_artifacts()
        .into_iter()
        .find(|artifact| artifact.path.ends_with("matrixraft-alerts.yaml"))
        .expect("the alert rule file is rendered");
    let rules = matrixraft_alert_rules();
    assert!(!rules.is_empty());
    for rule in &rules {
        assert!(
            rendered
                .contents
                .contains(&format!("- alert: {}", rule.alert)),
            "{} is modelled but missing from the rule file",
            rule.alert
        );
        // The expression is a quoted YAML scalar, so a matcher like
        // severity="error" is stored escaped. Comparing against the raw
        // expression would fail on exactly the rules that have label
        // matchers, so this asserts the escaped scalar the file must carry.
        let quoted = rule
            .expr
            .replace('\u{5c}', "\u{5c}\u{5c}")
            .replace('\u{22}', "\u{5c}\u{22}");
        assert!(
            rendered
                .contents
                .contains(&format!("expr: \u{22}{quoted}\u{22}")),
            "{} is in the rule file without its expression",
            rule.alert
        );
    }
}
