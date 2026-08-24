// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    cluster::{RaftCluster, RustRaftConsensus, RustRaftReadIndexRequest},
    config::{RaftConfig, RustRaftConfig},
    membership::{
        JointConsensusMembership, RaftMembershipExecutor, RaftMembershipOperation, RustRaftPeer,
        RustRaftReplicaRole,
    },
    metrics::rustraft_metric_names,
    node::{RaftNodeRuntime, RustRaftNodeOptions},
    readiness::{
        rustraft_open_source_surface, rustraft_parity_report, rustraft_public_api_contract,
        rustraft_standalone_readiness_report, rustraft_temporalstore_adapter_shape,
        RustRaftReadinessSnapshot,
    },
    snapshot::{
        PersistentRaftSnapshotStoreOptions, RaftSnapshot, RustRaftApplySnapshotFence,
        RustRaftSnapshotMeta,
    },
    status::{rustraft_cluster_status_report, RaftHealthStatus},
    transport::{AppendEntriesRequest, ReadIndexRequest, RustRaftTransport, VoteRequest},
    wal::{PersistentRaftWalOptions, RaftHardState, RaftWalRecord},
};

fn peer(node_id: u64, role: RustRaftReplicaRole) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 23_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 24_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn module_cluster() -> RaftCluster {
    RaftCluster::new(
        707,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("module cluster")
}

#[test]
fn public_modules_expose_temporalstore_consumption_boundary() {
    let mut cluster = module_cluster();
    RustRaftConsensus::start(&mut cluster).expect("start through cluster module");
    let log_id =
        RustRaftConsensus::propose(&mut cluster, b"module-write".to_vec(), Default::default())
            .expect("propose through cluster module");
    assert_eq!(log_id.index, 2);

    let read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 707,
            requester_id: 1,
            min_commit_index: 2,
            allow_lease_read: true,
        })
        .expect("read index through cluster module");
    assert!(read.safe);
    assert!(read.lease_read);
    assert!(cluster.lease_read_eligible(1, 2).expect("lease eligible"));

    cluster
        .campaign(2, false)
        .expect("campaign/pre-vote surface");
    cluster.transfer_leader(1).expect("leader transfer surface");

    let mut executor = RaftMembershipExecutor::new();
    executor
        .execute(
            &mut cluster,
            RaftMembershipOperation::AddLearner(peer(4, RustRaftReplicaRole::Voter)),
        )
        .expect("add learner through membership module");
    assert!(cluster.membership().learners.contains(&4));
    cluster
        .set_node_healthy(4, false)
        .expect("isolate learner after immediate catch-up");
    let catchup_log_id = cluster
        .propose(b"module-write-after-add".to_vec())
        .expect("write after add");
    cluster.compact_logs_through(catchup_log_id.index);
    cluster
        .set_node_healthy(4, true)
        .expect("restore learner for snapshot");

    let snapshot = RaftSnapshot {
        group_id: 707,
        meta: RustRaftSnapshotMeta {
            snapshot_id: "module-contract-catchup".to_string(),
            last_log_id: catchup_log_id.clone(),
            membership: vec![1, 2, 3, 4],
            members: Vec::new(),
        },
        payload: b"snapshot".to_vec(),
    };
    cluster
        .install_snapshot_to(
            4,
            snapshot,
            RustRaftApplySnapshotFence {
                applied_index: catchup_log_id.index,
                commit_index: catchup_log_id.index,
                installed_snapshot_index: catchup_log_id.index,
                first_retained_log_index: catchup_log_id.index + 1,
            },
        )
        .expect("snapshot catch-up through snapshot module");

    executor
        .execute_all(
            &mut cluster,
            vec![
                RaftMembershipOperation::Promote(4),
                RaftMembershipOperation::AddWitness(peer(5, RustRaftReplicaRole::Voter)),
                RaftMembershipOperation::Remove(3),
            ],
        )
        .expect("membership workflow through module boundary");
    let membership = cluster.membership();
    assert!(membership.voters.contains(&4));
    assert!(membership.witnesses.contains(&5));
    assert!(!membership.voters.contains(&3));

    let report = rustraft_cluster_status_report(
        cluster.group_id,
        cluster.leader_id(),
        cluster.leader_transfer_state(),
        vec![cluster.status(1).expect("node status")],
    );
    assert_eq!(report.health, RaftHealthStatus::Healthy);
    assert_eq!(report.leader_id, Some(1));

    let readiness = RustRaftReadinessSnapshot {
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
    };
    assert!(rustraft_parity_report(&readiness).ready);
    assert!(!rustraft_metric_names().append_latency_ms.is_empty());
}

#[test]
fn standalone_readiness_report_covers_non_temporalstore_embedding_status() {
    let report = rustraft_standalone_readiness_report();
    assert!(report.standalone, "{:?}", report.missing);
    assert_eq!(
        report.production_status,
        matrixraft::RustRaftProductionStatus::ProductionReady
    );
    assert!(report.missing.is_empty());

    let expected = [
        "node_lifecycle",
        "replication",
        "election_pre_vote",
        "membership",
        "wal_recovery",
        "snapshots",
        "read_index_lease_read",
        "status_metrics_readiness",
    ];
    let actual = report
        .capabilities
        .iter()
        .map(|capability| capability.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert!(report
        .capabilities
        .iter()
        .all(|capability| capability.ready));
    assert!(report
        .capabilities
        .iter()
        .all(|capability| !capability.evidence.is_empty()));
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("RaftNodeRuntime")));
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("AppendEntriesRequest")));
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("PersistentRaftWalOptions")));
    assert!(report
        .evidence
        .iter()
        .all(|item| !item.contains("TemporalStore")));

    let api = rustraft_public_api_contract();
    assert!(api
        .compatibility_reports
        .contains(&"rustraft_standalone_readiness_report".to_string()));
    let surface = rustraft_open_source_surface();
    assert!(surface
        .compatibility_reports
        .contains(&"rustraft_standalone_readiness_report".to_string()));
}

#[test]
fn public_modules_export_runtime_storage_wal_snapshot_and_transport_types() {
    let _node_options = std::mem::size_of::<RustRaftNodeOptions>();
    let _node_runtime = std::mem::size_of::<RaftNodeRuntime>();
    let _config = RustRaftConfig::default();
    let _joint = std::mem::size_of::<JointConsensusMembership>();

    let wal_options =
        PersistentRaftWalOptions::new(std::env::temp_dir().join("rustraft-module-wal"));
    assert!(wal_options.validate().is_ok());
    let _hard_state = std::mem::size_of::<RaftHardState>();
    let _wal_record = std::mem::size_of::<RaftWalRecord>();

    let snapshot_options =
        PersistentRaftSnapshotStoreOptions::new(std::env::temp_dir().join("rustraft-module-snap"));
    assert!(snapshot_options.chunk_size > 0);

    let _append = std::mem::size_of::<AppendEntriesRequest>();
    let _vote = std::mem::size_of::<VoteRequest>();
    let _read_index_alias = std::mem::size_of::<ReadIndexRequest>();
    let _transport = std::mem::size_of::<&dyn RustRaftTransport>();
}

#[test]
fn open_source_surface_names_modules_examples_reports_and_adapter_boundary() {
    let api = rustraft_public_api_contract();
    for module in [
        "node",
        "cluster",
        "membership",
        "wal",
        "snapshot",
        "transport",
        "status",
        "metrics",
        "readiness",
    ] {
        assert!(api.public_modules.contains(&module.to_string()));
    }
    assert!(api
        .embedding_examples
        .contains(&"examples/debug_artifacts.rs".to_string()));
    assert!(api
        .embedding_examples
        .contains(&"examples/open_source_surface.rs".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"RustRaftBenchmarkRunner".to_string()));
    assert!(api
        .compatibility_reports
        .contains(&"rustraft_production_readiness_report".to_string()));

    let surface = rustraft_open_source_surface();
    assert_eq!(surface.crate_name, "rustraft");
    assert!(surface.public_modules.contains(&"wal".to_string()));
    assert!(surface.embedding_docs.contains(&"README.md".to_string()));
    assert!(surface
        .baseline_raft_parity_matrix
        .contains(&"leader_election".to_string()));
    assert!(surface
        .benchmark_harness_interface
        .contains(&"rustraft_run_baseline_raft_parity_benchmark".to_string()));
    assert!(surface
        .compatibility_reports
        .contains(&"rustraft_public_api_contract".to_string()));
    assert!(surface
        .temporalstore_adapter_boundary
        .iter()
        .any(|item| item.contains("TemporalStore command codecs")));
    let adapter_shape = rustraft_temporalstore_adapter_shape();
    assert_eq!(adapter_shape.node_field, "node");
    assert!(adapter_shape.node_runtime_type.contains("RaftNodeRuntime"));
    assert!(adapter_shape
        .temporalstore_owned
        .contains(&"apply semantics".to_string()));
}

#[test]
fn debug_artifacts_example_exports_complete_support_envelope() {
    let example = include_str!("../examples/debug_artifacts.rs");
    for required in [
        "\"debug_snapshot\"",
        "\"debug_snapshot_json\"",
        "\"debug_snapshot_metadata_prometheus\"",
        "\"diagnostic_json_lines\"",
        "\"diagnostic_prometheus\"",
        "\"optimization_prometheus\"",
        "\"triage_prometheus\"",
        "\"runbook_prometheus\"",
        "\"grafana_dashboard_json\"",
        "\"alert_rules_json\"",
        "\"observability_provisioning_json\"",
        "\"observability_provisioning\"",
        "\"validation\"",
        "\"validation_prometheus\"",
        "\"provisioning_validation\"",
        "\"provisioning_validation_prometheus\"",
        "\"provisioning_runbook_prometheus\"",
        "\"support_envelope_validation\"",
        "\"support_envelope_validation_prometheus\"",
    ] {
        assert!(
            example.contains(required),
            "debug_artifacts example missing envelope field {required}"
        );
    }
    assert!(example.contains("serde_json::to_string_pretty(&snapshot)"));
    assert!(example.contains(".diagnostics"));
    assert!(example.contains("snapshot.diagnostic_prometheus"));
    assert!(example.contains("snapshot.optimization_prometheus"));
    assert!(example.contains("snapshot.runbook_prometheus"));
    assert!(example.contains("rustraft_operator_runbook_prometheus(&provisioning.runbook_steps"));
    assert!(example.contains("missing_debug_artifacts"));
    assert!(example.contains("extra_debug_artifacts"));
    assert!(example.contains("debug_artifact_unadvertised"));
    assert!(example.contains("extra_prometheus_artifacts"));
    assert!(example.contains("prometheus_artifact_unadvertised"));
    assert!(example.contains("artifact_inventory_status"));
    assert!(example.contains("\"complete\""));
    assert!(example.contains("\"drift\""));
    assert!(example.contains("support_envelope_issues"));
    assert!(example.contains("\"schema\""));
    assert!(example.contains("rustraft.support_envelope_validation.v1"));
    assert!(example.contains("\"artifact\": \"support_envelope\""));
    assert!(example.contains("\"service\": \"rustraft-example\""));
    assert!(example.contains("\"validation_checked_at_unix_ms\""));
    assert!(example.contains("let validation_checked_at_unix_ms = now_unix_ms()"));
    assert!(example.contains("\"debug_snapshot_generated_at_unix_ms\""));
    assert!(example.contains("snapshot.generated_at_unix_ms"));
    assert!(example.contains("\"debug_snapshot_age_ms\""));
    assert!(example.contains("saturating_sub(snapshot.generated_at_unix_ms)"));
    assert!(example.contains("DEBUG_SNAPSHOT_MAX_AGE_MS"));
    assert!(example.contains("\"debug_snapshot_max_age_ms\""));
    assert!(example.contains("DEBUG_SNAPSHOT_LOW_FRESH_MS"));
    assert!(example.contains("\"debug_snapshot_low_fresh_ms\""));
    assert!(example.contains("\"debug_snapshot_low_fresh_after_unix_ms\""));
    assert!(example.contains("DEBUG_SNAPSHOT_MAX_AGE_MS.saturating_sub(DEBUG_SNAPSHOT_LOW_FRESH_MS)"));
    assert!(example.contains("\"debug_snapshot_stale_after_unix_ms\""));
    assert!(example.contains("saturating_add(DEBUG_SNAPSHOT_MAX_AGE_MS)"));
    assert!(example.contains("\"debug_snapshot_freshness_status\""));
    assert!(example.contains("\"refresh_soon\""));
    assert!(example.contains("\"fresh\""));
    assert!(example.contains("\"stale\""));
    assert!(example.contains("\"debug_snapshot_fresh\""));
    assert!(example.contains("debug_snapshot_age_ms <= DEBUG_SNAPSHOT_MAX_AGE_MS"));
    assert!(example.contains("support_envelope_issues.push(\"debug_snapshot_stale\")"));
    assert!(example.contains("\"debug_snapshot_remaining_fresh_ms\""));
    assert!(example.contains("saturating_sub(debug_snapshot_age_ms)"));
    assert!(example.contains("\"debug_snapshot_low_fresh\""));
    assert!(example.contains("debug_snapshot_remaining_fresh_ms"));
    assert!(example.contains(">= DEBUG_SNAPSHOT_LOW_FRESH_MS"));
    assert!(example.contains("support_envelope_issues.push(\"debug_snapshot_low_fresh\")"));
    assert!(example.contains("let support_envelope_status = if support_envelope_issues.is_empty()"));
    assert!(example.contains("\"support_envelope_status\""));
    assert!(example.contains("\"needs_attention\""));
    assert!(example.contains("let support_envelope_severity = if support_envelope_issues.is_empty()"));
    assert!(example.contains("\"support_envelope_severity\""));
    assert!(example.contains("\"critical\""));
    assert!(example.contains("\"ready_metric\""));
    assert!(example
        .contains("rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\"}"));
    assert!(example.contains("\"issue_total_metric\""));
    assert!(example.contains(
        "rustraft_debug_bundle_validation_issue_total{artifact=\\\"support_envelope\\\"}"
    ));
    assert!(example.contains("\"issue_breakdown_metric\""));
    assert!(example
        .contains("rustraft_debug_bundle_validation_issue{artifact=\\\"support_envelope\\\"}"));
    assert!(example.contains("\"first_issue\""));
    assert!(example.contains("\"first_issue_metric\""));
    assert!(example.contains(
        "rustraft_debug_bundle_validation_first_issue{artifact=\\\"support_envelope\\\"}"
    ));
    assert!(example.contains("(\"freshness_status\", debug_snapshot_freshness_status)"));
    assert!(example.contains("(\"support_envelope_status\", support_envelope_status)"));
    assert!(example.contains("(\"support_envelope_severity\", support_envelope_severity)"));
    assert!(example.contains("\"alert_links\""));
    assert!(example.contains("RustRaftSupportEnvelopeValidationFailed"));
    assert!(example.contains("RustRaftSupportEnvelopeCritical"));
    assert!(example.contains("RustRaftDebugBundleValidationFailed"));
    assert!(example.contains("RustRaftObservabilityProvisioningValidationFailed"));
    assert!(example.contains("RustRaftDebugSnapshotStale"));
    assert!(example.contains("RustRaftDebugSnapshotFreshnessLow"));
    assert!(example.contains("RustRaftDebugSnapshotFreshnessLost"));
    assert!(example.contains("\"critical_alert_links\""));
    assert!(example.contains("RustRaftSupportEnvelopeCritical"));
    assert!(example.contains("\"alert_runbook_map\""));
    assert!(example.contains("\"RustRaftSupportEnvelopeValidationFailed\": \"validate_support_envelope\""));
    assert!(example.contains("\"RustRaftSupportEnvelopeCritical\": \"wire_critical_alerts\""));
    assert!(example.contains("\"RustRaftDiagnosticErrors\": \"inspect_error_diagnostics\""));
    assert!(example.contains(
        "\"RustRaftOptimizationCriticalHints\": \"resolve_critical_optimization_hints\""
    ));
    assert!(example.contains("\"RustRaftDebugSnapshotFreshnessLost\": \"refresh_debug_snapshot\""));
    assert!(example.contains("\"runbook_evidence_map\""));
    assert!(example.contains("\"validate_support_envelope\""));
    assert!(example.contains("rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\"}"));
    assert!(example.contains("\"wire_critical_alerts\""));
    assert!(example.contains("alert_rules_json"));
    assert!(example.contains("\"inspect_error_diagnostics\""));
    assert!(example.contains("diagnostic_json_lines"));
    assert!(example.contains("\"resolve_critical_optimization_hints\""));
    assert!(example.contains("rustraft_optimization_critical_total"));
    assert!(example.contains("\"refresh_debug_snapshot\""));
    assert!(example.contains("rustraft_debug_snapshot_fresh"));
    assert!(example.contains("\"operator_handoff_sequence\""));
    assert!(example.contains(
        "\"validate_support_envelope\",\n            \"wire_critical_alerts\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\",\n            \"resolve_critical_optimization_hints\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\",\n            \"refresh_debug_snapshot\""
    ));
    assert!(example.contains("\"handoff_command_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"cargo test --test module_contract debug_artifacts_example_exports_complete_support_envelope\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"cargo run --example debug_artifacts --quiet | rg RustRaftSupportEnvelopeCritical\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"cargo run --example debug_artifacts --quiet | rg rustraft_diagnostic_log_total\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"cargo run --example debug_artifacts --quiet | rg rustraft_optimization_critical_total\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"cargo run --example debug_artifacts --quiet | rg rustraft_debug_snapshot_fresh\""
    ));
    assert!(example.contains("\"handoff_success_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"support envelope ready is true and first issue is absent\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"critical alert rule is present and routed to the support envelope runbook\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"diagnostic error metric is present with zero unexpected error logs\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"critical optimization total is zero after applying the top hint\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"debug snapshot freshness metric is one and freshness status is fresh\""
    ));
    assert!(example.contains("\"handoff_dashboard_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": [\n                \"Support Envelope Validation Ready\""
    ));
    assert!(example.contains("\"Support Envelope First Issue\""));
    assert!(example.contains(
        "\"wire_critical_alerts\": [\n                \"Support Envelope Severity\""
    ));
    assert!(example.contains("\"Support Envelope Issue Breakdown\""));
    assert!(example.contains("\"Diagnostic Errors\""));
    assert!(example.contains("\"Diagnostic Log Rate\""));
    assert!(example.contains("\"Optimization Critical Hints\""));
    assert!(example.contains("\"Triage Top Optimization Hint\""));
    assert!(example.contains("\"Support Envelope Freshness Status\""));
    assert!(example.contains("\"Debug Snapshot Fresh\""));
    assert!(example.contains("\"handoff_log_stream_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": [\n                \"support_envelope_validation\""
    ));
    assert!(example.contains("\"support_envelope_validation_prometheus\""));
    assert!(example.contains(
        "rustraft_debug_bundle_validation_first_issue{artifact=\\\"support_envelope\\\"}"
    ));
    assert!(example.contains("\"diagnostic_log_prometheus\""));
    assert!(example.contains("\"triage_prometheus\""));
    assert!(example.contains("\"validation_prometheus\""));
    assert!(example.contains("\"handoff_owner_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-observability-oncall\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-runtime-incident-commander\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-owner\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-performance-owner\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-runtime-owner\""));
    assert!(example.contains("\"handoff_priority_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"P0\""));
    assert!(example.contains("\"wire_critical_alerts\": \"P0\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"P1\""));
    assert!(example.contains("\"resolve_critical_optimization_hints\": \"P1\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"P2\""));
    assert!(example.contains("\"handoff_response_time_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"immediate\""));
    assert!(example.contains("\"wire_critical_alerts\": \"immediate\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"within 5 minutes\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"within 15 minutes\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"within 30 minutes\""));
    assert!(example.contains("\"handoff_escalation_trigger_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"support envelope ready is false or first issue is present\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"critical support envelope alert is missing or unrouted\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"diagnostic error metric remains nonzero after first inspection\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"critical optimization total remains nonzero after mitigation\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"debug snapshot freshness metric remains zero after refresh\""
    ));
    assert!(example.contains("\"handoff_recovery_action_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"open support envelope first issue and apply the matching remediation check\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"rebuild alert rules JSON and verify critical support envelope routing\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"capture diagnostic JSON lines and isolate the first repeated error target\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"apply the top optimization hint and recheck critical total\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"regenerate debug snapshot artifacts and rerun validation Prometheus checks\""
    ));
    assert!(example.contains("\"handoff_closure_check_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"support envelope ready is true and first issue is absent\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"critical alert link is present and runbook target is wire_critical_alerts\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"diagnostic error total is zero for the inspected target\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_optimization_critical_total is zero\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"rustraft_debug_snapshot_fresh is one and validation Prometheus is present\""
    ));
    assert!(example.contains("\"handoff_retained_artifact_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"support_envelope_validation\""));
    assert!(example.contains("\"wire_critical_alerts\": \"alert_rules_json\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"diagnostic_json_lines\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"optimization_prometheus\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"debug_snapshot_json\""));
    assert!(example.contains("\"handoff_audit_note_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"records final support envelope readiness and first issue state\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"records the alert rule and runbook route used during escalation\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"records the repeated diagnostic target and error evidence\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"records the optimization hint and critical-total recovery evidence\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"records the refreshed debug snapshot and validation scrape\""
    ));
    assert!(example.contains("\"handoff_review_question_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"which support envelope issue proved the incident was resolved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"which critical alert route confirmed on-call coverage\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"which diagnostic target repeated before recovery\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"which optimization hint removed the critical total\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"which refreshed snapshot proved current debug evidence\""
    ));
    assert!(example.contains("\"handoff_metric_probe_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"rustraft_support_envelope_ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"rustraft_support_envelope_critical_alert_total\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"rustraft_diagnostic_log_errors\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_optimization_critical_total\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"rustraft_debug_snapshot_fresh\""
    ));
    assert!(example.contains("\"handoff_triage_signal_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"rustraft_operator_triage_status\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"rustraft_operator_triage_top_alert\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"rustraft_operator_triage_top_diagnostic\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_operator_triage_top_optimization_hint\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"rustraft_operator_triage_first_action\""
    ));
    assert!(example.contains("\"handoff_validation_gate_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"support_envelope_validation.ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"support_envelope_validation.alert_links_present\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"debug_snapshot_validation.diagnostic_log_contract\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"debug_snapshot_validation.optimization_prometheus_contract\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"debug_snapshot_validation.freshness_contract\""
    ));
    assert!(example.contains("\"handoff_promql_query_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\"} == 1\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"rustraft_support_envelope_critical_alert_total > 0\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"rustraft_diagnostic_log_errors == 0\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_optimization_critical_total == 0\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"rustraft_debug_snapshot_fresh == 1\""
    ));
    assert!(example.contains("\"handoff_log_query_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"diagnostic_json_lines | rg support_envelope\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"alert_rules_json | rg RustRaftSupportEnvelopeCritical\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"diagnostic_json_lines | rg rustraft.summary\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"triage_prometheus | rg rustraft_operator_triage_top_optimization_hint\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"debug_snapshot_json | rg generated_at_unix_ms\""
    ));
    assert!(example.contains("\"handoff_annotation_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"RustRaft support envelope validated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"RustRaft critical alert route verified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"RustRaft diagnostic evidence inspected\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"RustRaft optimization critical total cleared\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"RustRaft debug snapshot refreshed\""
    ));
    assert!(example.contains("\"handoff_correlation_key_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"rustraft.support_envelope.validation\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"rustraft.support_envelope.alert_route\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"rustraft.diagnostics.error_target\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft.optimization.critical_hint\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"rustraft.debug_snapshot.refresh\""
    ));
    assert!(example.contains("\"handoff_retention_window_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"retain for 30 days\""));
    assert!(example.contains("\"wire_critical_alerts\": \"retain for 30 days\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"retain for 14 days\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"retain for 14 days\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"retain until next successful refresh\""
    ));
    assert!(example.contains("\"handoff_cleanup_guard_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"do not clean until support envelope validation is archived\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"do not clean until alert route annotation is archived\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"do not clean until diagnostic JSON lines are archived\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"do not clean until optimization Prometheus is archived\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"do not clean until replacement debug snapshot is validated\""
    ));
    assert!(example.contains("\"handoff_final_summary_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"summarize support envelope readiness and first issue\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"summarize alert route, owner, and annotation key\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"summarize diagnostic target, severity, and log query\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"summarize optimization hint, PromQL result, and retained artifact\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"summarize snapshot age, freshness status, and cleanup guard\""
    ));
    assert!(example.contains("\"handoff_reopen_trigger_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"reopen if support envelope ready flips false\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"reopen if critical alert route disappears or owner is empty\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"reopen if diagnostic error logs reappear for the same target\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"reopen if critical optimization total rises above zero\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"reopen if snapshot freshness becomes stale or refresh soon\""
    ));
    assert!(example.contains("\"handoff_prevention_check_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"keep support envelope validation ready in the next scrape\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"keep critical alert route and owner populated in alert rules\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"keep diagnostic error log total at zero for the target\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"keep optimization critical total at zero for two scrapes\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"keep debug snapshot freshness fresh after the next refresh window\""
    ));
    assert!(example.contains("\"handoff_verification_owner_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-observability-oncall\""));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-runtime-incident-commander\""
    ));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-owner\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-performance-owner\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-runtime-owner\""));
    assert!(example.contains("\"handoff_verification_evidence_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\"}\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"alert_rules_json includes RustRaftSupportEnvelopeCritical owner\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"rustraft_diagnostic_log_errors == 0\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_optimization_critical_total == 0\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"rustraft_debug_snapshot_fresh == 1\""));
    assert!(example.contains("\"handoff_verification_cadence_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"verify on the next Prometheus scrape\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"verify before leaving the incident bridge\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"verify after 5 minutes without repeated errors\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"verify across two consecutive optimization scrapes\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"verify before the next low-freshness window\""
    ));
    assert!(example.contains("\"handoff_verification_failure_action_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"reopen validate_support_envelope and capture the first support issue\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"reopen wire_critical_alerts and rebuild alert routing evidence\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"reopen inspect_error_diagnostics and retain the repeated error logs\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"reopen resolve_critical_optimization_hints and keep the critical PromQL result\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"reopen refresh_debug_snapshot and regenerate the debug bundle\""
    ));
    assert!(example.contains("\"handoff_verification_audit_trail_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"append support_envelope_validation readiness and first issue\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"append alert_rules_json route owner and critical alert name\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"append diagnostic_json_lines target and error count\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"append optimization_prometheus critical total and hint\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"append debug_snapshot_json generated timestamp and freshness status\""
    ));
    assert!(example.contains("\"handoff_verification_output_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"support envelope verification note\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"critical alert route verification note\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"diagnostic error verification note\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"optimization recovery verification note\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"debug snapshot freshness verification note\""
    ));
    assert!(example.contains("\"handoff_verification_delivery_channel_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"support envelope incident timeline\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"critical alert routing review\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"diagnostic investigation log\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"optimization follow-up report\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"debug snapshot refresh record\""
    ));
    assert!(example.contains("\"handoff_verification_acknowledgement_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"timeline entry acknowledged by raft-observability-oncall\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"routing review acknowledged by raft-runtime-incident-commander\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"diagnostic log acknowledged by raft-diagnostics-owner\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"optimization report acknowledged by raft-performance-owner\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"refresh record acknowledged by raft-runtime-owner\""
    ));
    assert!(example.contains("\"handoff_verification_closeout_status_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"closed: support envelope verification acknowledged\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"closed: critical alert route verification acknowledged\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"closed: diagnostic verification acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"closed: optimization verification acknowledged\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"closed: debug snapshot refresh verification acknowledged\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_action_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"reopen support envelope validation with missing closeout evidence\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"reopen alert routing handoff with unresolved escalation\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"reopen diagnostics handoff with missing error context\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"reopen optimization handoff with pending critical hint\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"reopen debug snapshot handoff with stale refresh evidence\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_check_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"confirm support envelope evidence is attached before reclosing\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"confirm alert escalation is resolved before reclosing\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"confirm diagnostic context is complete before reclosing\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"confirm critical optimization hint is cleared before reclosing\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"confirm debug snapshot refresh is current before reclosing\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_owner_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-observability-oncall\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-runtime-incident-commander\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-owner\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-performance-owner\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-runtime-owner\""));
    assert!(example.contains("\"handoff_verification_reopen_notification_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-observability-oncall/reopen-support-envelope\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-runtime-incident-commander/reopen-alert-routing\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-owner/reopen-diagnostics\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-performance-owner/reopen-optimization\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-runtime-owner/reopen-debug-snapshot\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_sla_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"reopened support envelope must be reviewed within 15m\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"reopened alert route must be reviewed within 10m\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"reopened diagnostic context must be reviewed within 20m\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"reopened optimization hint must be reviewed within 30m\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"reopened debug snapshot must be refreshed within 15m\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_breach_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"escalate overdue support envelope reopen to raft-runtime-incident-commander\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"escalate overdue alert route reopen to raft-runtime-incident-commander\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"escalate overdue diagnostic reopen to raft-observability-oncall\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"escalate overdue optimization reopen to raft-performance-owner\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"escalate overdue debug snapshot reopen to raft-runtime-owner\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_resolution_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"resolve reopen after support envelope evidence is accepted\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"resolve reopen after alert route escalation is cleared\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"resolve reopen after diagnostic context is complete\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"resolve reopen after critical optimization hint is cleared\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"resolve reopen after debug snapshot evidence is refreshed\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_audit_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"audit reopened support envelope resolution in raft-support-envelope-log\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"audit reopened alert route resolution in raft-alert-handoff-log\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"audit reopened diagnostic resolution in raft-diagnostics-log\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"audit reopened optimization resolution in raft-optimization-log\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"audit reopened debug snapshot resolution in raft-debug-snapshot-log\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_dashboard_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"RustRaft Support Envelope Reopen Resolution\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"RustRaft Alert Route Reopen Resolution\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"RustRaft Diagnostics Reopen Resolution\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"RustRaft Optimization Reopen Resolution\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"RustRaft Debug Snapshot Reopen Resolution\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_metric_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"rustraft_handoff_reopen_support_envelope_total\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"rustraft_handoff_reopen_alert_route_total\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"rustraft_handoff_reopen_diagnostics_total\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_handoff_reopen_optimization_total\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"rustraft_handoff_reopen_debug_snapshot_total\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_promql_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"sum(rate(rustraft_handoff_reopen_support_envelope_total[5m]))\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"sum(rate(rustraft_handoff_reopen_alert_route_total[5m]))\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"sum(rate(rustraft_handoff_reopen_diagnostics_total[5m]))\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"sum(rate(rustraft_handoff_reopen_optimization_total[5m]))\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"sum(rate(rustraft_handoff_reopen_debug_snapshot_total[5m]))\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_alert_threshold_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"page when support envelope reopens exceed zero for 10m\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"page when alert route reopens exceed zero for 10m\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"review when diagnostic reopens exceed two for 30m\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"review when optimization reopens exceed two for 30m\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"review when debug snapshot reopens exceed one for 30m\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_notification_route_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-page\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-page\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-review\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-review\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-review\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_escalation_policy_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-incident-lead-immediate-escalation\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-incident-lead-immediate-escalation\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-owner-next-business-cycle\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-owner-next-business-cycle\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-owner-next-business-cycle\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_log_query_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft_logs{event=\\\"support_envelope_reopened\\\"}\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft_logs{event=\\\"alert_route_reopened\\\"}\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft_logs{event=\\\"diagnostic_reopened\\\"}\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft_logs{event=\\\"optimization_reopened\\\"}\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft_logs{event=\\\"debug_snapshot_reopened\\\"}\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_correlation_key_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft.support_envelope.reopen_id\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft.alert_route.reopen_id\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft.diagnostics.reopen_id\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft.optimization.reopen_id\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft.debug_snapshot.reopen_id\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_retention_window_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"retain reopen metric, log, and evidence for 30d\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"retain reopen metric, log, and evidence for 30d\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"retain diagnostic reopen context for 14d\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"retain optimization reopen context for 14d\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"retain debug snapshot reopen context for 7d\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_cleanup_guard_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"cleanup only after support envelope reopen evidence is archived\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"cleanup only after alert route reopen page is resolved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"cleanup only after diagnostic reopen owner signs off\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"cleanup only after optimization reopen owner signs off\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"cleanup only after debug snapshot reopen bundle is archived\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_final_summary_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-reopen-closeout\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-reopen-closeout\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-reopen-closeout\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-reopen-closeout\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-reopen-closeout\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-reopen-ack\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-reopen-ack\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-reopen-ack\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-reopen-ack\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-reopen-ack\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_delivery_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-closeout-feed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-closeout-feed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-closeout-feed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-closeout-feed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-closeout-feed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_source_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-reopen-replay\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-reopen-replay\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-reopen-replay\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-reopen-replay\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-reopen-replay\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_check_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"replay support envelope readiness, first issue, and retained validation metrics\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"replay alert route, escalation policy, and page delivery evidence\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"replay diagnostic log query, correlation key, and retained error context\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"replay optimization hint metric, PromQL result, and owner signoff\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"replay debug snapshot timestamp, freshness metric, and archived bundle\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_result_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"replay passes when support envelope ready stays true and first issue is absent\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"replay passes when alert route, escalation, and delivery evidence all match\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"replay passes when diagnostic context resolves to the retained correlation key\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"replay passes when optimization PromQL stays clear with owner signoff\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"replay passes when refreshed snapshot remains fresh and archived bundle matches\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_failure_action_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"reopen support envelope validation and attach failed replay metrics\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"reopen alert routing and page the incident commander with replay mismatch\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"reopen diagnostics with retained correlation key and failed log query\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"reopen optimization handoff with failed PromQL and missing signoff\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"reopen debug snapshot refresh with stale replay bundle evidence\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_evidence_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-replay-escalation-note\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-replay-page-record\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-replay-escalation-log\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-replay-owner-ticket\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-replay-refresh-ticket\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-replay-escalation-ack\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-replay-page-ack\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-replay-escalation-ack\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-replay-owner-ack\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-replay-refresh-ack\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_closeout_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-replay-escalation-closeout\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-replay-page-closeout\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-replay-escalation-closeout\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-replay-owner-closeout\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-replay-refresh-closeout\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_delivery_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-replay-closeout-feed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-replay-closeout-feed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-replay-closeout-feed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-replay-closeout-feed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-replay-closeout-feed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_retention_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"retain replay closeout feed for 30d\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"retain replay alert closeout feed for 30d\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"retain replay diagnostics closeout feed for 14d\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"retain replay optimization closeout feed for 14d\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"retain replay debug snapshot closeout feed for 7d\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_review_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"review replay closeout feed on day 25\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"review replay alert closeout feed on day 25\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"review replay diagnostics closeout feed on day 10\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"review replay optimization closeout feed on day 10\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"review replay debug snapshot closeout feed on day 5\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_disposition_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"archive replay support closeout after clean review\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"keep replay alert closeout until next oncall audit\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"purge replay diagnostics after correlation export\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"archive replay optimization closeout after owner signoff\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"replace replay debug snapshot with latest bundle after review\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-replay-support-expiry-audit-record\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-replay-alert-expiry-audit-record\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-replay-diagnostics-expiry-audit-record\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-replay-optimization-expiry-audit-record\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-replay-debug-snapshot-expiry-audit-record\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_owner_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-audit-owner\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-audit-owner\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-audit-owner\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-audit-owner\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-audit-owner\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-expiry-audit-signoff\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-expiry-audit-signoff\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-audit-signoff\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-audit-signoff\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-audit-signoff\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_delivery_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-expiry-signoff-feed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-expiry-signoff-feed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-signoff-feed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-signoff-feed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-signoff-feed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-expiry-signoff-ack\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-expiry-signoff-ack\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-signoff-ack\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-signoff-ack\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-signoff-ack\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-expiry-signoff-closeout\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-expiry-signoff-closeout\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-signoff-closeout\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-signoff-closeout\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-signoff-closeout\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_delivery_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-expiry-closeout-feed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-expiry-closeout-feed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-closeout-feed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-closeout-feed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-closeout-feed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_retention_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"retain replay expiry closeout feed for 30d\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"retain replay alert expiry closeout feed for 30d\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"retain replay diagnostics expiry closeout feed for 14d\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"retain replay optimization expiry closeout feed for 14d\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"retain replay debug snapshot expiry closeout feed for 7d\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_guard_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"block cleanup until raft support expiry closeout feed is archived\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"block cleanup until raft alert expiry closeout page proof is archived\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"block cleanup until raft diagnostics expiry closeout logs are archived\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"block cleanup until raft optimization expiry closeout report is archived\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"block cleanup until raft debug snapshot expiry closeout refresh is archived\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_evidence_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-closeout-archive-proof\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-closeout-page-proof\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-closeout-log-proof\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-closeout-report-proof\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-closeout-refresh-proof\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_approval_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-closeout-cleanup-approved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-closeout-cleanup-approved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-closeout-cleanup-approved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-closeout-cleanup-approved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-closeout-cleanup-approved\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_execution_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-closeout-cleanup-executed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-closeout-cleanup-executed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-closeout-cleanup-executed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-closeout-cleanup-executed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-closeout-cleanup-executed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_verification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-cleanup-verified-with-archive\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-verified-with-page-proof\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-cleanup-verified-with-log-archive\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-cleanup-verified-with-report-archive\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-cleanup-verified-with-refresh-archive\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_notification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-cleanup-notified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-notified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-cleanup-notified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-cleanup-notified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-cleanup-notified\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-cleanup-notification-ack\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-notification-ack\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-cleanup-notification-ack\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-cleanup-notification-ack\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-cleanup-notification-ack\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_closure_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-cleanup-loop-closed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-loop-closed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-cleanup-loop-closed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-cleanup-loop-closed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-cleanup-loop-closed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_final_summary_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-cleanup-final-summary\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-final-summary\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-cleanup-final-summary\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-cleanup-final-summary\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-cleanup-final-summary\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_index_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-cleanup-archive-index\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-archive-index\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-cleanup-archive-index\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-cleanup-archive-index\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-cleanup-archive-index\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_validation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-cleanup-archive-index-validated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-archive-index-validated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-cleanup-archive-index-validated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-cleanup-archive-index-validated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-cleanup-archive-index-validated\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_owner_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-cleanup-archive-owner\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-archive-owner\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-cleanup-archive-owner\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-cleanup-archive-owner\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-cleanup-archive-owner\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_retention_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"retain raft support cleanup archive for 30d\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"retain raft alert cleanup archive for 30d\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"retain raft diagnostics cleanup archive for 14d\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"retain raft optimization cleanup archive for 14d\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"retain raft debug snapshot cleanup archive for 7d\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_retention_review_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"review raft support cleanup archive in support envelope Grafana panel\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"review raft alert cleanup archive in critical alert Grafana panel\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"review raft diagnostics cleanup archive in debugging diagnostics panel\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"review raft optimization cleanup archive in optimization hint panel\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"review raft debug snapshot cleanup archive in debug snapshot panel\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_retention_review_signoff_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-review-signed-off\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-review-signed-off\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-review-signed-off\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-review-signed-off\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-review-signed-off\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_review_ready_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-review-ready-for-closeout\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-review-ready-for-closeout\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-review-ready-for-closeout\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-review-ready-for-closeout\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-review-ready-for-closeout\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_premerge_evidence_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-premerge-evidence\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-premerge-evidence\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-premerge-evidence\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-premerge-evidence\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-premerge-evidence\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_premerge_verification_status_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-premerge-verified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-premerge-verified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-premerge-verified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-premerge-verified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-premerge-verified\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_gate_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-gate-open\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-gate-open\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-gate-open\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-gate-open\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-gate-open\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_audit_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-audited\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-audited\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-audited\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-audited\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-audited\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_summary_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-summary\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-summary\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-summary\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-summary\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-summary\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_handoff_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-handoff\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-handoff\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-handoff\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-handoff\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-handoff\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_followup_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-followup\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-followup\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-followup\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-followup\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-followup\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_closure_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-closed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-closed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-closed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-closed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-closed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_verification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-verified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-verified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-verified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-verified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-verified\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_acceptance_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-accepted\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-accepted\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-accepted\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-accepted\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-accepted\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_attestation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-attested\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-attested\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-attested\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-attested\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-attested\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_certification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-certified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-certified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-certified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-certified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-certified\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_release_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-released\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-released\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-released\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-released\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-released\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_distribution_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-distributed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-distributed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-distributed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-distributed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-distributed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_ingestion_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-ingested\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-ingested\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-ingested\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-ingested\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-ingested\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_indexing_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-indexed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-indexed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-indexed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-indexed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-indexed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_query_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-queryable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-queryable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-queryable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-queryable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-queryable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_retrieval_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-retrievable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-retrievable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-retrievable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-retrievable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-retrievable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_consumption_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-consumable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-consumable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-consumable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-consumable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-consumable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_application_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-applicable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-applicable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-applicable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-applicable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-applicable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_activation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-active\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-active\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-active\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-active\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-active\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_operationalization_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-operational\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-operational\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-operational\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-operational\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-operational\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_readiness_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-ready\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-ready\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-ready\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-ready\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_availability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-available\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-available\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-available\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-available\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-available\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_accessibility_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-accessible\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-accessible\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-accessible\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-accessible\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-accessible\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_usability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-usable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-usable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-usable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-usable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-usable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_reliability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-reliable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-reliable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-reliable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-reliable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-reliable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_durability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-durable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-durable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-durable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-durable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-durable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_resilience_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-resilient\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-resilient\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-resilient\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-resilient\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-resilient\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_recoverability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-recoverable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-recoverable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-recoverable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-recoverable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-recoverable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_maintainability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-maintainable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-maintainable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-maintainable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-maintainable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-maintainable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_operability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-operable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-operable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-operable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-operable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-operable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_serviceability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-serviceable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-serviceable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-serviceable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-serviceable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-serviceable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_diagnosability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-diagnosable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-diagnosable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-diagnosable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-diagnosable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-diagnosable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_traceability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-traceable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-traceable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-traceable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-traceable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-traceable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_auditability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-auditable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-auditable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-auditable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-auditable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-auditable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_verifiability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-verifiable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-verifiable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-verifiable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-verifiable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-verifiable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_reproducibility_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-reproducible\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-reproducible\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-reproducible\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-reproducible\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-reproducible\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_consistency_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-consistent\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-consistent\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-consistent\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-consistent\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-consistent\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_comparability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-comparable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-comparable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-comparable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-comparable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-comparable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_correlation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-correlated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-correlated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-correlated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-correlated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-correlated\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_causality_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-causal\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-causal\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-causal\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-causal\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-causal\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_explainability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-explainable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-explainable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-explainable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-explainable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-explainable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_accountability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-accountable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-accountable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-accountable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-accountable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-accountable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_ownership_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-owned\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-owned\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-owned\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-owned\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-owned\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_responsibility_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-responsible\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-responsible\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-responsible\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-responsible\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-responsible\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_stewardship_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-stewarded\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-stewarded\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-stewarded\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-stewarded\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-stewarded\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_governance_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-governed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-governed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-governed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-governed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-governed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_compliance_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-compliant\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-compliant\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-compliant\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-compliant\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-compliant\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_conformance_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-conformant\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-conformant\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-conformant\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-conformant\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-conformant\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_adherence_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-adherent\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-adherent\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-adherent\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-adherent\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-adherent\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_alignment_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-aligned\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-aligned\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-aligned\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-aligned\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-aligned\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_synchronization_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-synchronized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-synchronized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-synchronized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-synchronized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-synchronized\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_coordination_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-coordinated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-coordinated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-coordinated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-coordinated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-coordinated\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_orchestration_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-orchestrated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-orchestrated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-orchestrated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-orchestrated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-orchestrated\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_integration_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-integrated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-integrated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-integrated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-integrated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-integrated\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_readiness_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-ready\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-ready\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-ready\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-ready\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_availability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-available\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-available\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-available\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-available\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-available\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_accessibility_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-accessible\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-accessible\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-accessible\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-accessible\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-accessible\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_usability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-usable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-usable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-usable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-usable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-usable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_reliability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-reliable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-reliable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-reliable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-reliable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-reliable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_durability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-durable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-durable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-durable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-durable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-durable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_resilience_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-resilient\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-resilient\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-resilient\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-resilient\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-resilient\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_recoverability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-recoverable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-recoverable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-recoverable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-recoverable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-recoverable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_maintainability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-maintainable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-maintainable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-maintainable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-maintainable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-maintainable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_operability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-operable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-operable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-operable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-operable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-operable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_serviceability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-serviceable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-serviceable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-serviceable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-serviceable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-serviceable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_diagnosability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-diagnosable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-diagnosable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-diagnosable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-diagnosable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-diagnosable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_traceability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-traceable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-traceable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-traceable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-traceable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-traceable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_auditability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-auditable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-auditable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-auditable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-auditable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-auditable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_verifiability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-verifiable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-verifiable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-verifiable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-verifiable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-verifiable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_reproducibility_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-reproducible\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-reproducible\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-reproducible\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-reproducible\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-reproducible\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_consistency_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-consistent\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-consistent\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-consistent\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-consistent\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-consistent\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_comparability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-comparable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-comparable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-comparable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-comparable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-comparable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_correlation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-correlated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-correlated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-correlated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-correlated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-correlated\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_causality_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-causal\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-causal\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-causal\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-causal\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-causal\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_explainability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-explainable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-explainable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-explainable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-explainable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-explainable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_accountability_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-accountable\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-accountable\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-accountable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-accountable\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-accountable\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_ownership_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-owned\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-owned\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-owned\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-owned\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-owned\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_responsibility_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-responsible\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-responsible\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-responsible\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-responsible\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-responsible\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_stewardship_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-stewarded\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-stewarded\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-stewarded\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-stewarded\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-stewarded\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_governance_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-governed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-governed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-governed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-governed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-governed\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_compliance_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-compliant\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-compliant\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-compliant\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-compliant\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-compliant\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_conformance_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-conformant\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-conformant\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-conformant\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-conformant\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-conformant\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_adherence_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-adherent\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-adherent\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-adherent\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-adherent\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-adherent\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_alignment_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-aligned\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-aligned\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-aligned\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-aligned\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-aligned\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_synchronization_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-synchronized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-synchronized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-synchronized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-synchronized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-synchronized\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_coordination_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-coordinated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-coordinated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-coordinated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-coordinated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-coordinated\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_orchestration_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-orchestrated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-orchestrated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-orchestrated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-orchestrated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-orchestrated\""
    ));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_archive_publication_postclosure_integration_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-cleanup-archive-publication-postclosure-integrated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-postclosure-integrated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-cleanup-archive-publication-postclosure-integrated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-cleanup-archive-publication-postclosure-integrated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-debug-snapshot-cleanup-archive-publication-postclosure-integrated\""
    ));
    assert!(example.contains("\"support_envelope_operator_handoff_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"support_envelope_validation\""
    ));
    assert!(example.contains("\"wire_critical_alerts\": \"critical_alert_handoff\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"diagnostic_log_prometheus\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"optimization_handoff\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"debug_snapshot_json\""));
    assert!(example.contains("\"support_envelope_operator_verification_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"support_envelope_validation_prometheus\""
    ));
    assert!(example.contains("\"wire_critical_alerts\": \"alert_rules_json\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"diagnostic_json_lines\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"optimization_prometheus\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"validation_prometheus\""
    ));
    assert!(example.contains("\"support_envelope_operator_dashboard_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"Support Envelope Validation Ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"Support Envelope Severity\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"Support Envelope First Issue\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"Triage Top Optimization Hint\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"Support Envelope Freshness Status\""
    ));
    assert!(example.contains("\"support_envelope_operator_runbook_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"validate_support_envelope\""
    ));
    assert!(example.contains("\"wire_critical_alerts\": \"wire_critical_alerts\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"inspect_error_diagnostics\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"resolve_critical_optimization_hints\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"refresh_debug_snapshot\""));
    assert!(example.contains("\"support_envelope_operator_collection_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"cargo test --test module_contract debug_artifacts_example_exports_complete_support_envelope\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"cargo run --example debug_artifacts --quiet | rg RustRaftSupportEnvelopeCritical\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"cargo run --example debug_artifacts --quiet | rg rustraft_diagnostic_log_total\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"cargo run --example debug_artifacts --quiet | rg rustraft_optimization_critical_total\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"cargo run --example debug_artifacts --quiet | rg rustraft_debug_snapshot_fresh\""
    ));
    assert!(example.contains("\"support_envelope_operator_execution_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"run contract validation before handoff\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"confirm critical alert routing evidence\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"inspect diagnostic log error totals\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"triage critical optimization hint totals\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"confirm debug snapshot freshness evidence\""
    ));
    assert!(example.contains("\"support_envelope_operator_acceptance_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"support envelope contract test passes\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"critical alert is present and routed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"diagnostic error totals are inspectable\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"critical optimization total is visible\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"fresh debug snapshot signal is present\""
    ));
    assert!(example.contains("\"support_envelope_operator_escalation_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"escalate failed validation to raft-observability-oncall\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"escalate missing critical alert route to raft-runtime-incident-commander\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"escalate diagnostic error spikes to raft-observability-oncall\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"escalate critical optimization totals to raft-runtime-incident-commander\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"escalate stale debug snapshots to raft-observability-oncall\""
    ));
    assert!(example.contains("\"support_envelope_operator_notification_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"notify raft-observability-oncall with validation evidence\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"notify raft-runtime-incident-commander with alert route evidence\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"notify raft-observability-oncall with diagnostic error totals\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"notify raft-runtime-incident-commander with optimization totals\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"notify raft-observability-oncall with snapshot freshness evidence\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-observability-oncall acknowledges validation evidence\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-runtime-incident-commander acknowledges alert route evidence\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-observability-oncall acknowledges diagnostic error totals\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-runtime-incident-commander acknowledges optimization totals\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-observability-oncall acknowledges snapshot freshness evidence\""
    ));
    assert!(example.contains("\"support_envelope_operator_closure_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"close validation handoff with contract evidence attached\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"close alert route handoff with critical alert evidence attached\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"close diagnostic handoff with error totals attached\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"close optimization handoff with critical totals attached\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"close snapshot handoff with freshness evidence attached\""
    ));
    assert!(example.contains("\"support_envelope_operator_archive_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"archive validation handoff as raft-support-envelope-validation-evidence\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"archive alert route handoff as raft-critical-alert-route-evidence\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"archive diagnostic handoff as raft-error-diagnostic-totals\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"archive optimization handoff as raft-critical-optimization-totals\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"archive snapshot handoff as raft-debug-snapshot-freshness-evidence\""
    ));
    assert!(example.contains("\"support_envelope_operator_retention_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"retain raft-support-envelope-validation-evidence for 30d\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"retain raft-critical-alert-route-evidence for 30d\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"retain raft-error-diagnostic-totals for 14d\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"retain raft-critical-optimization-totals for 14d\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"retain raft-debug-snapshot-freshness-evidence for 7d\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_guard_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"block cleanup until raft validation evidence retention proof exists\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"block cleanup until raft alert route evidence retention proof exists\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"block cleanup until raft diagnostic totals retention proof exists\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"block cleanup until raft optimization totals retention proof exists\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"block cleanup until raft snapshot freshness retention proof exists\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_evidence_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-retention-proof\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-retention-proof\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-retention-proof\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-retention-proof\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-retention-proof\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_approval_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-approved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-approved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-approved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-approved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-approved\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_execution_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-executed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-executed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-executed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-executed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-executed\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_verification_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-verified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-verified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-verified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-verified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-verified\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_notification_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-notified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-notified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-notified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-notified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-notified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-acknowledged\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-acknowledged\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-acknowledged\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-acknowledged\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_closure_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-closed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-closed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-closed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-closed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-closed\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_summary_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-summary\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-summary\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-summary\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-summary\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-summary\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_archive_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-archived\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-archived\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-archived\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-archived\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-archived\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retained\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retained\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retained\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retained\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retained\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_review_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_approval_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-approved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-approved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-approved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-approved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-approved\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_execution_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-executed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-executed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-executed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-executed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-executed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_verification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-verified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-verified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-verified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-verified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-verified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_notification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-notified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-notified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-notified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-notified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-notified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_closure_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-closed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-closed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-closed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-closed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-closed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_summary_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-summarized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-summarized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-summarized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-summarized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-summarized\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_archive_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-archived\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-archived\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-archived\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-archived\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-archived\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_review_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-reviewed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-reviewed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-reviewed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-reviewed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-reviewed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_approval_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-approved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-approved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-approved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-approved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-approved\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_execution_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-executed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-executed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-executed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-executed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-executed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_verification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-verified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-verified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-verified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-verified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-verified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_notification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-notified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-notified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-notified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-notified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-notified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-acknowledged\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-acknowledged\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-acknowledged\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-acknowledged\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_closure_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-closed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-closed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-closed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-closed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-closed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_summary_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-summarized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-summarized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-summarized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-summarized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-summarized\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_archive_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-archived\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-archived\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-archived\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-archived\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-archived\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_review_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-reviewed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-reviewed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-reviewed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-reviewed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-reviewed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_approval_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-approved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-approved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-approved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-approved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-approved\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_execution_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-executed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-executed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-executed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-executed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-executed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_verification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-verified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-verified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-verified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-verified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-verified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_notification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-notified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-notified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-notified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-notified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-notified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_handoff_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-handed-off\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-handed-off\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-handed-off\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-handed-off\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-handed-off\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_readiness_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-ready\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-ready\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-ready\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-ready\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_route_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-routed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-routed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-routed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-routed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-routed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_ack_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-acknowledged\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-acknowledged\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-acknowledged\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-acknowledged\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_delivery_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-delivered\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-delivered\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-delivered\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-delivered\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-delivered\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_confirmation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-confirmed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-confirmed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-confirmed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-confirmed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-confirmed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_closeout_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-closed-out\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-closed-out\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-closed-out\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-closed-out\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-closed-out\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_archive_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-archived\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-archived\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-archived\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-archived\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-archived\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_retention_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-retained\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-retained\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-retained\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-retained\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-retained\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_audit_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-audited\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-audited\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-audited\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-audited\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-audited\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_signoff_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-signed-off\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-signed-off\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-signed-off\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-signed-off\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-signed-off\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_publication_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-published\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-published\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-published\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-published\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-published\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_distribution_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-distributed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-distributed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-distributed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-distributed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-distributed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_ingestion_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-ingested\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-ingested\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-ingested\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-ingested\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-ingested\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_indexing_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-indexed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-indexed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-indexed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-indexed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-indexed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_querying_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-queried\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-queried\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-queried\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-queried\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-queried\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_fetching_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-fetched\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-fetched\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-fetched\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-fetched\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-fetched\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_materialization_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-materialized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-materialized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-materialized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-materialized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-materialized\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_correlation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-correlated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-correlated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-correlated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-correlated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-correlated\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_aggregation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-aggregated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-aggregated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-aggregated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-aggregated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-aggregated\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_summarization_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-summarized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-summarized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-summarized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-summarized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-summarized\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_normalization_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-normalized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-normalized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-normalized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-normalized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-normalized\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_validation_state_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-validated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-validated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-validated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-validated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-validated\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_certification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-certified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-certified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-certified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-certified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-certified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_attestation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-attested\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-attested\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-attested\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-attested\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-attested\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_sealing_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-sealed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-sealed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-sealed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-sealed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-sealed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-released\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-released\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-released\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-released\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-released\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_distribution_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-distributed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-distributed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-distributed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-distributed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-distributed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_ingestion_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-ingested\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-ingested\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-ingested\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-ingested\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-ingested\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_indexing_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-indexed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-indexed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-indexed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-indexed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-indexed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_query_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-queried\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-queried\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-queried\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-queried\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-queried\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_retrieval_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-retrieved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-retrieved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-retrieved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-retrieved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-retrieved\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_consumption_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-consumed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-consumed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-consumed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-consumed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-consumed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_application_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-applied\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-applied\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-applied\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-applied\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-applied\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_activation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-activated\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-activated\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-activated\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-activated\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-activated\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_operationalization_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-operationalized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-operationalized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-operationalized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-operationalized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-operationalized\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_readiness_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-ready\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-ready\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-ready\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-ready\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_acceptance_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-accepted\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-accepted\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-accepted\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-accepted\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-accepted\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_attestation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-attested\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-attested\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-attested\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-attested\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-attested\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_certification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-certified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-certified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-certified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-certified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-certified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_publication_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-published\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-published\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-published\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-published\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-published\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handed-off\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handed-off\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handed-off\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handed-off\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handed-off\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_ack_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-acknowledged\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-acknowledged\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-acknowledged\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-acknowledged\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_closure_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-closed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-closed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-closed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-closed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-closed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_archive_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-archived\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-archived\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-archived\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-archived\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-archived\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retained\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retained\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retained\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retained\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retained\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_review_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_approval_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-approved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-approved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-approved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-approved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-approved\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_execution_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-executed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-executed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-executed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-executed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-executed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_verification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-verified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-verified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-verified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-verified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-verified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_notification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-notified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-notified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-notified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-notified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-notified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_closure_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-closed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-closed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-closed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-closed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-closed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_summary_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-summarized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-summarized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-summarized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-summarized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-summarized\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_archive_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-archived\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-archived\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-archived\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-archived\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-archived\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retained\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retained\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retained\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retained\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retained\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_review_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_approval_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-approved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-approved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-approved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-approved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-approved\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_ownership_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-owned\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-owned\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-owned\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-owned\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-owned\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-acknowledged\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-acknowledged\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-acknowledged\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-acknowledged\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_ready_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-ready\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-ready\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-ready\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-ready\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_executed_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-executed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-executed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-executed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-executed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-executed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_verified_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-verified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-verified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-verified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-verified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-verified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_retained_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-retained\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-retained\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-retained\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-retained\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-retained\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_reviewed_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-reviewed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-reviewed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-reviewed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-reviewed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-reviewed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_approval_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-approved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-approved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-approved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-approved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-approved\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_publication_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-published\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-published\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-published\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-published\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-published\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_distribution_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-distributed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-distributed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-distributed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-distributed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-distributed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_acknowledgment_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-acknowledged\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-acknowledged\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-acknowledged\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-acknowledged\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_acceptance_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-accepted\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-accepted\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-accepted\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-accepted\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-accepted\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_readiness_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-ready\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-ready\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-ready\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-ready\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_activation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-active\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-active\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-active\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-active\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-active\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_execution_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-executing\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-executing\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-executing\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-executing\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-executing\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_completion_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-completed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-completed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-completed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-completed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-completed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_verification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-verified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-verified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-verified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-verified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-verified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_closure_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-closed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-closed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-closed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-closed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-closed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_archive_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-archived\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-archived\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-archived\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-archived\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-archived\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_retention_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-retention\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-retention\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-retention\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-retention\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-retention\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_preservation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-preserved\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-preserved\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-preserved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-preserved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-preserved\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_restoration_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-restored\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-restored\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-restored\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-restored\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-restored\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_reconciliation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-reconciled\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-reconciled\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-reconciled\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-reconciled\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-reconciled\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_finalization_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalized\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_certification_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-certified\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-certified\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-certified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-certified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-certified\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_attestation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-attested\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-attested\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-attested\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-attested\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-attested\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_authorization_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-authorized\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-authorized\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-authorized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-authorized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-authorized\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_release_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-released\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-released\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-released\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-released\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-released\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_dispatch_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-dispatched\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-dispatched\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-dispatched\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-dispatched\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-dispatched\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_delivery_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-delivered\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-delivered\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-delivered\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-delivered\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-delivered\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_receipt_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-received\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-received\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-received\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-received\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-received\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_confirmation_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-confirmed\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-confirmed\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-confirmed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-confirmed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-confirmed\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_completion_ready_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-completion-ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-completion-ready\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-completion-ready\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-completion-ready\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-completion-ready\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_finalization_ready_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalization-ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalization-ready\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalization-ready\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalization-ready\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalization-ready\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_finalization_complete_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalization-complete\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalization-complete\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalization-complete\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalization-complete\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-finalization-complete\""
    ));
    assert!(example.contains(
        "\"support_envelope_operator_cleanup_retention_retention_retention_escalation_release_handoff_retention_retention_critical_alert_handoff_archival_ready_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-archival-ready\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-archival-ready\""
    ));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-archival-ready\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-archival-ready\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained-retained-escalation-release-handoff-retention-retention-critical-alert-handoff-archival-ready\""
    ));
    assert!(example.contains("\"critical_alert_handoff\""));
    assert!(example.contains("raft-observability-oncall"));
    assert!(example.contains("raft-runtime-incident-commander"));
    assert!(example.contains("support_envelope_validation"));
    assert!(example.contains("\"optimization_handoff\""));
    assert!(example.contains("optimization_prometheus"));
    assert!(example.contains("resolve_critical_optimization_hints"));
    assert!(example.contains("rustraft_optimization_ready"));
    assert!(example.contains("rustraft_optimization_critical_total"));
    assert!(example.contains("Triage Top Optimization Hint"));
    assert!(example.contains("\"dashboard_panels\""));
    assert!(example.contains("Support Envelope Validation Ready"));
    assert!(example.contains("Support Envelope Validation Issues"));
    assert!(example.contains("Support Envelope Issue Breakdown"));
    assert!(example.contains("Support Envelope First Issue"));
    assert!(example.contains("Support Envelope Freshness Status"));
    assert!(example.contains("Support Envelope Status"));
    assert!(example.contains("Support Envelope Severity"));
    assert!(example.contains("Provisioning Validation Ready"));
    assert!(example.contains("Provisioning Validation Issues"));
    assert!(example.contains("Provisioning Issue Breakdown"));
    assert!(example.contains("Provisioning First Issue"));
    assert!(example.contains("\"runbook_steps\""));
    assert!(example.contains("refresh_debug_snapshot"));
    assert!(example.contains("inspect_error_diagnostics"));
    assert!(example.contains("wire_critical_alerts"));
    assert!(example.contains("validate_support_envelope"));
    assert!(example.contains("\"collection_commands\""));
    assert!(example.contains("cargo run --example debug_artifacts --quiet"));
    assert!(example.contains(
        "cargo test --test module_contract debug_artifacts_example_exports_complete_support_envelope"
    ));
    assert!(example.contains("\"operator_handoff_artifacts\""));
    assert!(example.contains("validation_prometheus"));
    assert!(example.contains("optimization_prometheus"));
    assert!(example.contains("triage_prometheus"));
    assert!(example.contains("diagnostic_log_prometheus"));
    assert!(example.contains("provisioning_validation"));
    assert!(example.contains("provisioning_validation_prometheus"));
    assert!(example.contains("provisioning_runbook_prometheus"));
    assert!(example.contains("support_envelope_validation"));
    assert!(example.contains("alert_rules_json"));
    assert!(example.contains("observability_provisioning_json"));
    assert!(example.contains("\"remediation_checks\""));
    assert!(example.contains("ready is true"));
    assert!(example.contains("first_issue is null"));
    assert!(example.contains("debug snapshot validation ready is true"));
    assert!(example.contains("observability provisioning validation ready is true"));
    assert!(example.contains("debug_snapshot_fresh is true"));
    assert!(example.contains("debug_snapshot_low_fresh is true"));
    assert!(example.contains("debug_snapshot_freshness_status is fresh"));
    assert!(example.contains("missing_debug_artifacts is empty"));
    assert!(example.contains("missing_prometheus_artifacts is empty"));
    assert!(example.contains("extra_debug_artifacts is empty"));
    assert!(example.contains("extra_prometheus_artifacts is empty"));
    assert!(example.contains("\"issue_remediation_map\""));
    assert!(example.contains("\"debug_snapshot_validation_failed\""));
    assert!(example.contains("\"observability_provisioning_validation_failed\""));
    assert!(example.contains("RustRaftSupportEnvelopeCritical"));
    assert!(example.contains("\"debug_artifact_missing\""));
    assert!(example.contains("\"prometheus_artifact_missing\""));
    assert!(example.contains("\"debug_artifact_unadvertised\""));
    assert!(example.contains("\"prometheus_artifact_unadvertised\""));
    assert!(example.contains("\"debug_snapshot_stale\""));
    assert!(example.contains("\"debug_snapshot_low_fresh\""));
    assert!(example.contains("\"advertised_debug_artifacts\""));
    assert!(example.contains("\"advertised_prometheus_artifacts\""));
    assert!(example.contains("\"emitted_artifacts\""));
    assert!(example.contains("artifact\", \"support_envelope"));
    assert!(example.contains("serde_json::to_string_pretty(&provisioning.dashboard)"));
    assert!(example.contains("serde_json::to_string_pretty(&provisioning.alert_rules)"));
    assert!(example.contains("serde_json::to_string_pretty(&provisioning)"));
}
