// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;

use matrixraft::{
    cluster::{Consensus, RaftCluster, ReadIndexRequest},
    config::Config,
    membership::{
        JointConsensusMembership, MembershipExecutor, MembershipOperation, Peer, ReplicaRole,
    },
    metrics::matrixraft_metric_names,
    node::{NodeOptions, NodeRuntime},
    readiness::{
        matrixraft_api_name_mappings, matrixraft_core_interface_names,
        matrixraft_evidence_interface_names, matrixraft_open_source_surface,
        matrixraft_parity_report, matrixraft_public_api_contract,
        matrixraft_public_api_contract_validation_prometheus,
        matrixraft_reference_mapped_interface_names, matrixraft_standalone_readiness_report,
        matrixraft_temporalstore_adapter_shape, matrixraft_validate_public_api_contract,
        ReadinessSnapshot,
    },
    snapshot::{
        ApplySnapshotFence, PersistentRaftSnapshotStoreOptions, RaftSnapshot, SnapshotMetadata,
    },
    status::{matrixraft_cluster_status_report, HealthStatus, RuntimeTimerStatus},
    // `ReadIndexRequest` is imported above from `cluster`; dropping the
    // `RustRaft` prefix made the two module paths name the same item.
    transport::{AppendEntriesRequest, Transport, VoteRequest},
    wal::{HardState, PersistentRaftWalOptions, WalRecord},
};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
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
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("module cluster")
}

#[test]
fn debug_artifacts_support_envelope_operator_maps_keep_runtime_pressure_lane() {
    let source = include_str!("../examples/debug_artifacts.rs");
    let mut missing_maps = Vec::new();
    let mut current_map: Option<String> = None;
    let mut has_runtime_pressure_lane = false;

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("\"support_envelope_operator_") && trimmed.ends_with("{") {
            current_map = trimmed
                .split_once('"')
                .and_then(|(_, rest)| rest.split_once('"'))
                .map(|(name, _)| name.to_string());
            has_runtime_pressure_lane = false;
            continue;
        }

        if current_map.is_some()
            && trimmed.contains("\"inspect_runtime_pressure_bottleneck_warning\"")
        {
            has_runtime_pressure_lane = true;
        }

        if trimmed == "}," {
            if let Some(map_name) = current_map.take() {
                if !has_runtime_pressure_lane {
                    missing_maps.push(map_name);
                }
            }
        }
    }

    assert!(
        missing_maps.is_empty(),
        "support envelope operator maps missing runtime pressure lane: {missing_maps:?}"
    );
}

#[test]
fn public_modules_expose_temporalstore_consumption_boundary() {
    let mut cluster = module_cluster();
    Consensus::start(&mut cluster).expect("start through cluster module");
    let log_id = Consensus::propose(&mut cluster, b"module-write".to_vec(), Default::default())
        .expect("propose through cluster module");
    assert_eq!(log_id.index, 2);

    let read = cluster
        .read_index(ReadIndexRequest {
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

    let mut executor = MembershipExecutor::new();
    executor
        .execute(
            &mut cluster,
            MembershipOperation::AddLearner(peer(4, ReplicaRole::Voter)),
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
        meta: SnapshotMetadata {
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
            ApplySnapshotFence {
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
                MembershipOperation::Promote(4),
                MembershipOperation::AddWitness(peer(5, ReplicaRole::Voter)),
                MembershipOperation::Remove(3),
            ],
        )
        .expect("membership workflow through module boundary");
    let membership = cluster.membership();
    assert!(membership.voters.contains(&4));
    assert!(membership.witnesses.contains(&5));
    assert!(!membership.voters.contains(&3));

    let report = matrixraft_cluster_status_report(
        cluster.group_id,
        cluster.leader_id(),
        cluster.leader_transfer_state(),
        vec![cluster.status(1).expect("node status")],
    );
    assert_eq!(report.health, HealthStatus::Healthy);
    assert_eq!(report.leader_id, Some(1));

    let readiness = ReadinessSnapshot {
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
    };
    assert!(matrixraft_parity_report(&readiness).ready);
    assert!(!matrixraft_metric_names().append_latency_ms.is_empty());
}

#[test]
fn standalone_readiness_report_covers_non_temporalstore_embedding_status() {
    let report = matrixraft_standalone_readiness_report();
    assert!(report.standalone, "{:?}", report.missing);
    assert_eq!(
        report.production_status,
        matrixraft::ProductionStatus::ProductionReady
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
        .any(|item| item.contains("NodeRuntime")));
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

    let api = matrixraft_public_api_contract();
    assert!(api
        .compatibility_reports
        .contains(&"matrixraft_standalone_readiness_report".to_string()));
    let surface = matrixraft_open_source_surface();
    assert!(surface
        .compatibility_reports
        .contains(&"matrixraft_standalone_readiness_report".to_string()));
}

#[test]
fn public_modules_export_runtime_storage_wal_snapshot_and_transport_types() {
    let _node_options = std::mem::size_of::<NodeOptions>();
    let _node_runtime = std::mem::size_of::<NodeRuntime>();
    let _runtime_timer_status = std::mem::size_of::<RuntimeTimerStatus>();
    let _config = Config::default();
    let _joint = std::mem::size_of::<JointConsensusMembership>();

    let wal_options =
        PersistentRaftWalOptions::new(std::env::temp_dir().join("rustraft-module-wal"));
    assert!(wal_options.validate().is_ok());
    let _hard_state = std::mem::size_of::<HardState>();
    let _wal_record = std::mem::size_of::<WalRecord>();

    let snapshot_options =
        PersistentRaftSnapshotStoreOptions::new(std::env::temp_dir().join("rustraft-module-snap"));
    assert!(snapshot_options.chunk_size > 0);

    let _append = std::mem::size_of::<AppendEntriesRequest>();
    let _vote = std::mem::size_of::<VoteRequest>();
    let _read_index_alias = std::mem::size_of::<ReadIndexRequest>();
    let _transport = std::mem::size_of::<&dyn Transport>();
}

#[test]
fn open_source_surface_names_modules_examples_reports_and_adapter_boundary() {
    let api = matrixraft_public_api_contract();
    assert_eq!(api.api_name_mappings, matrixraft_api_name_mappings());
    assert_eq!(api.core_interfaces, matrixraft_core_interface_names());
    let validation = matrixraft_validate_public_api_contract(&api);
    assert!(
        validation.ready,
        "public API contract drift must fail closed: {:#?}",
        validation.blockers
    );
    assert!(validation
        .mapped_canonical_names
        .contains(&"AppendEntriesRequest".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"RuntimePressureAdmission".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"LatencyPressureDetail".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"NodeRuntimeTimerPressureDetail".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"NodeRuntimeTimerThresholds".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"SnapshotLifecycleEvidence".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"NodeRuntime".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"RuntimeTimerStatus".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_runtime_pressure_admission_with_scale_targets".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_runtime_pressure_admission_with_pipeline_pressure".to_string()));
    assert!(validation.mapped_canonical_names.contains(
        &"matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure".to_string()
    ));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_runtime_pressure_bottleneck_summary".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_runtime_pressure_freshness_report".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_runtime_pressure_freshness_prometheus".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_runtime_pressure_freshness_diagnostic_log_entries".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_runtime_pressure_freshness_diagnostic_json_lines".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_public_api_contract_validation_prometheus".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_snapshot_lifecycle_evidence_prometheus".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_wal_lifecycle_evidence_prometheus".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_membership_readiness_prometheus".to_string()));
    assert!(validation
        .unmapped_advertised_names
        .contains(&"matrixraft_public_api_contract".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_runtime_local_status_report".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_runtime_admin_report".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_admin_diagnostic_json_lines".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_diagnostic_log_prometheus".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_observability_provisioning_runbook_steps".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_operator_runbook_steps_with_diagnostics".to_string()));
    assert!(validation
        .mapped_canonical_names
        .contains(&"matrixraft_operator_runbook_prometheus".to_string()));
    assert!(
        validation.unmapped_reference_required_names.is_empty(),
        "healthy public API contract must map every fail-closed reference-required name"
    );
    assert!(!validation
        .unmapped_advertised_names
        .contains(&"matrixraft_runtime_admin_report".to_string()));
    assert!(!validation
        .unmapped_advertised_names
        .contains(&"matrixraft_grafana_dashboard_json".to_string()));
    assert!(!validation
        .unmapped_advertised_names
        .contains(&"matrixraft_snapshot_lifecycle_evidence_prometheus".to_string()));
    assert!(!validation
        .unmapped_advertised_names
        .contains(&"matrixraft_wal_lifecycle_evidence_prometheus".to_string()));
    assert!(!validation
        .unmapped_advertised_names
        .contains(&"matrixraft_membership_readiness_prometheus".to_string()));
    assert!(!validation
        .unmapped_advertised_names
        .contains(&"matrixraft_public_api_contract_validation_prometheus".to_string()));
    assert!(
        validation.api_mapping_coverage_percent > 0
            && validation.api_mapping_coverage_percent < 100
    );
    assert_eq!(validation.mapping_coverage_by_category.len(), 12);
    assert!(validation
        .mapping_coverage_by_category
        .iter()
        .any(|coverage| coverage.category == "embedding_examples"
            && coverage.advertised_name_count == api.embedding_examples.len()));
    assert!(validation
        .mapping_coverage_by_category
        .iter()
        .any(|coverage| coverage.category == "evidence_interfaces"
            && coverage.advertised_name_count == api.evidence_interfaces.len()
            && coverage.mapped_name_count == api.evidence_interfaces.len()));
    let mapped_by_category = validation
        .mapping_coverage_by_category
        .iter()
        .map(|coverage| coverage.mapped_name_count)
        .sum::<usize>();
    assert!(mapped_by_category >= validation.mapped_canonical_names.len());
    let unique_mapped_by_category = validation
        .mapping_coverage_by_category
        .iter()
        .flat_map(|coverage| coverage.mapped_names.iter().cloned())
        .collect::<BTreeSet<_>>();
    let unique_advertised_by_category = validation
        .mapping_coverage_by_category
        .iter()
        .flat_map(|coverage| {
            coverage
                .mapped_names
                .iter()
                .chain(coverage.unmapped_names.iter())
                .cloned()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        unique_mapped_by_category.len(),
        validation.mapped_canonical_names.len()
    );
    assert_eq!(
        unique_mapped_by_category,
        validation
            .mapped_canonical_names
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(
        validation.api_mapping_coverage_percent,
        unique_mapped_by_category.len() * 100 / unique_advertised_by_category.len()
    );
    let compatibility_coverage = validation
        .mapping_coverage_by_category
        .iter()
        .find(|coverage| coverage.category == "compatibility_reports")
        .expect("compatibility coverage must be reported");
    assert!(compatibility_coverage
        .unmapped_names
        .contains(&"matrixraft_public_api_contract".to_string()));
    assert!(!compatibility_coverage
        .unmapped_names
        .contains(&"matrixraft_runtime_admin_report".to_string()));
    let diagnostic_coverage = validation
        .mapping_coverage_by_category
        .iter()
        .find(|coverage| coverage.category == "diagnostic_interfaces")
        .expect("diagnostic coverage must be reported");
    assert!(diagnostic_coverage.mapped_name_count >= 7);
    assert!(diagnostic_coverage.coverage_percent > 0);
    assert!(!diagnostic_coverage
        .unmapped_names
        .contains(&"matrixraft_diagnostic_log_prometheus".to_string()));
    let observability_coverage = validation
        .mapping_coverage_by_category
        .iter()
        .find(|coverage| coverage.category == "observability_interfaces")
        .expect("observability coverage must be reported");
    assert!(!observability_coverage
        .unmapped_names
        .contains(&"matrixraft_snapshot_lifecycle_evidence_prometheus".to_string()));
    assert!(!observability_coverage
        .unmapped_names
        .contains(&"matrixraft_wal_lifecycle_evidence_prometheus".to_string()));
    assert!(!observability_coverage
        .unmapped_names
        .contains(&"matrixraft_membership_readiness_prometheus".to_string()));
    assert!(!observability_coverage
        .unmapped_names
        .contains(&"matrixraft_public_api_contract_validation_prometheus".to_string()));
    for required in [
        "matrixraft_public_api_contract_validation_prometheus",
        "matrixraft_snapshot_lifecycle_evidence_prometheus",
        "matrixraft_wal_lifecycle_evidence_prometheus",
        "matrixraft_membership_readiness_prometheus",
    ] {
        assert!(
            matrixraft_reference_mapped_interface_names().contains(&required.to_string()),
            "{required} must stay in the fail-closed reference-mapped API subset"
        );
    }
    assert_eq!(
        validation.reference_required_names,
        matrixraft_reference_mapped_interface_names()
    );
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_runtime_pressure_admission_with_scale_targets".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_runtime_pressure_admission_with_pipeline_pressure".to_string()));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure".to_string()
    ));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_runtime_pressure_bottleneck_summary".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_runtime_pressure_freshness_report".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_runtime_pressure_freshness_prometheus".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_runtime_pressure_freshness_diagnostic_log_entries".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_runtime_pressure_freshness_diagnostic_json_lines".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_public_api_contract_validation_prometheus".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_release_benchmark_runtime_timer_status".to_string()));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_input_with_runtime_pressure_evidence".to_string()
    ));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_benchmark_runtime_pressure_readiness_artifact".to_string()));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog".to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact".to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
            .to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
            .to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_validate_benchmark_runtime_pressure_readiness_artifact".to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
            .to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact".to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
            .to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
            .to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts"
            .to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts"
            .to_string()
    ));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_production_readiness_input_with_benchmark_artifacts".to_string()));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_input_with_asserted_benchmark_artifacts".to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts"
            .to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_artifacts"
            .to_string()
    ));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_production_readiness_report_with_benchmark_artifacts".to_string()));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_report_with_asserted_benchmark_artifacts".to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_debug_snapshot_with_runtime_pressure_evidence".to_string()));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence".to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts".to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(validation
        .reference_required_names
        .contains(&"BenchmarkRunner".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_grafana_dashboard".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_grafana_dashboard_json".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_alert_rules".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_alert_rules_json".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_observability_provisioning".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_observability_provisioning_json".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_observability_required_metric_names".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_validate_required_metric_scrape_texts".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_validate_observability_provisioning".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_validate_observability_provisioning_json".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_observability_provisioning_validation_prometheus".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_operator_runbook_steps".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_operator_runbook_steps_with_diagnostics".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_operator_runbook_prometheus".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_observability_provisioning_runbook_steps".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_runtime_local_status_report".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_runtime_admin_report".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_production_readiness_report".to_string()));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_report_with_runtime_pressure_policy".to_string()
    ));
    assert!(validation.reference_required_names.contains(
        &"matrixraft_production_readiness_report_with_runtime_pressure_policy_and_freshness"
            .to_string()
    ));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_admin_diagnostic_log_entries".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_admin_diagnostic_json_lines".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_local_status_diagnostic_log_entries".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_local_status_diagnostic_json_lines".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_node_runtime_status_diagnostic_log_entries".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_node_runtime_status_diagnostic_json_lines".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"matrixraft_diagnostic_log_prometheus".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"SnapshotLifecycleEvidence".to_string()));
    assert!(validation
        .reference_required_names
        .contains(&"PipelineEvidence".to_string()));
    assert!(validation.reference_required_names.contains(
        &"PipelineEvidence::packet_loss_reorder_all_faulted_peers_recovered".to_string()
    ));
    assert_eq!(
        validation.interface_name_count,
        api.public_modules.len()
            + api.core_interfaces.len()
            + api.rpc_messages.len()
            + api.safety_helpers.len()
            + api.embedding_examples.len()
            + api.parity_reports.len()
            + api.benchmark_interfaces.len()
            + api.observability_interfaces.len()
            + api.diagnostic_interfaces.len()
            + api.evidence_interfaces.len()
            + api.compatibility_reports.len()
    );
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "Storage"
            && mapping.matrixraft_facade == "MatrixRaftStorage"
            && mapping.raft_rs_or_tikv_reference == "raft::Storage"
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "ReadIndexRequest"
            && mapping.raft_rs_or_tikv_reference.contains("MsgReadIndex")
            && mapping
                .byteraft_or_baseline_reference
                .contains("lease_read")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_read_safety_decision"
            && mapping.raft_rs_or_tikv_reference.contains("ReadIndex")
            && mapping.byteraft_or_baseline_reference.contains("safe read")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_append_safety_decision"
            && mapping.raft_rs_or_tikv_reference.contains("append")
            && mapping
                .byteraft_or_baseline_reference
                .contains("AppendEntries")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_learner_promotion_decision"
            && mapping.raft_rs_or_tikv_reference.contains("learner")
            && mapping
                .byteraft_or_baseline_reference
                .contains("auto-promote")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_parity_report"
            && mapping
                .byteraft_or_baseline_reference
                .contains("BaselineRaft")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_baseline_raft_parity_matrix"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("feature parity matrix")
            && mapping
                .byteraft_or_baseline_reference
                .contains("parity matrix")
    }));
    assert!(validation
        .mapping_coverage_by_category
        .iter()
        .all(|coverage| { coverage.advertised_name_count == 0 || coverage.mapped_name_count > 0 }));
    assert_eq!(
        api.evidence_interfaces,
        matrixraft_evidence_interface_names()
    );
    assert!(api
        .evidence_interfaces
        .contains(&"PipelineEvidence".to_string()));
    assert!(api.evidence_interfaces.contains(
        &"PipelineEvidence::packet_loss_reorder_all_faulted_peers_recovered".to_string()
    ));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "PeerProgress"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("ProgressTracker")
            && mapping
                .byteraft_or_baseline_reference
                .contains("replication pipeline")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "PipelineEvidence"
            && mapping
                .byteraft_or_baseline_reference
                .contains("replication pipeline readiness")
            && mapping.note.contains("per-peer fault recovery")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "PipelineEvidence::packet_loss_reorder_all_faulted_peers_recovered"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("all-peer Progress")
            && mapping
                .byteraft_or_baseline_reference
                .contains("all faulted peers recovered")
            && mapping.note.contains("fail-closed production signal")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "NodeRuntime"
            && mapping.raft_rs_or_tikv_reference.contains("RaftRouter")
            && mapping.note.contains("timer backpressure")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "RuntimeTimerStatus"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("scheduler delay")
            && mapping.note.contains("rejected tick")
            && mapping.note.contains("latency/QPS")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_runtime_local_status_report"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("ProgressTracker")
            && mapping.note.contains("bounded-stale read")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_runtime_admin_report"
            && mapping
                .byteraft_or_baseline_reference
                .contains("GetInfo/admin")
            && mapping.note.contains("QPS/latency")
            && mapping.note.contains("peer-pipeline lag")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_production_readiness_report"
            && mapping
                .byteraft_or_baseline_reference
                .contains("ByteRaft release gate")
            && mapping.note.contains("QPS/latency/memory parity")
            && mapping.note.contains("ranked runtime-pressure bottlenecks")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_production_readiness_report_with_runtime_pressure_policy"
            && mapping
                .byteraft_or_baseline_reference
                .contains("fail-closed runtime-pressure deployment gate")
            && mapping
                .note
                .contains("configured fail-closed admission policy")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_report_with_runtime_pressure_policy_and_freshness"
            && mapping
                .byteraft_or_baseline_reference
                .contains("runtime-pressure freshness deployment gate")
            && mapping
                .note
                .contains("future-dated runtime-pressure evidence")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_admin_diagnostic_json_lines"
            && mapping.raft_rs_or_tikv_reference.contains("JSON")
            && mapping.note.contains("Grafana")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_local_status_diagnostic_json_lines"
            && mapping
                .byteraft_or_baseline_reference
                .contains("local replica")
            && mapping.note.contains("random-replica read lag")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_node_runtime_status_diagnostic_json_lines"
            && mapping.raft_rs_or_tikv_reference.contains("scheduler JSON")
            && mapping.note.contains("timer backlog")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_diagnostic_log_prometheus"
            && mapping.raft_rs_or_tikv_reference.contains("Prometheus")
            && mapping.note.contains("peer-pipeline")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_runtime_pressure_bottleneck_summary"
            && mapping
                .byteraft_or_baseline_reference
                .contains("QPS/latency bottleneck")
            && mapping.note.contains("excess or deficit percent")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_runtime_pressure_freshness_report"
            && mapping
                .byteraft_or_baseline_reference
                .contains("QPS/latency/memory evidence freshness gate")
            && mapping.note.contains("stale")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_runtime_pressure_freshness_prometheus"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("Prometheus freshness scrape")
            && mapping.note.contains("Grafana")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_runtime_pressure_freshness_diagnostic_log_entries"
            && mapping.note.contains("log-only release gates")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_runtime_pressure_freshness_diagnostic_json_lines"
            && mapping.note.contains("centralized log queries")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "InstallSnapshotResponse"
            && mapping
                .byteraft_or_baseline_reference
                .contains("install_snapshot_response")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "SnapshotLifecycleEvidence"
            && mapping.raft_rs_or_tikv_reference.contains("raftstore")
            && mapping
                .byteraft_or_baseline_reference
                .contains("sender/downloader lifecycle")
            && mapping.note.contains("coherent transfer completion")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "BenchmarkRunner"
            && mapping
                .byteraft_or_baseline_reference
                .contains("QPS/latency parity")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_release_benchmark_runtime_timer_status"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("scheduler/timer pressure")
            && mapping
                .byteraft_or_baseline_reference
                .contains("no-pressure timer evidence")
            && mapping.note.contains("producer and consumer")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_benchmark_runtime_pressure_readiness_artifact"
            && mapping.raft_rs_or_tikv_reference.contains("Prometheus")
            && mapping
                .byteraft_or_baseline_reference
                .contains("diagnostic logs")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
            && mapping.raft_rs_or_tikv_reference.contains("ReadIndex")
            && mapping
                .byteraft_or_baseline_reference
                .contains("read backlog metrics")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
            && mapping.raft_rs_or_tikv_reference.contains("scheduler pressure")
            && mapping
                .byteraft_or_baseline_reference
                .contains("timer metrics")
            && mapping.note.contains("node-runtime timer pressure")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact"
            && mapping.raft_rs_or_tikv_reference.contains("Prometheus")
            && mapping.byteraft_or_baseline_reference.contains("schema")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
            && mapping.raft_rs_or_tikv_reference.contains("ReadIndex")
            && mapping
                .byteraft_or_baseline_reference
                .contains("read backlog pressure evidence")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
            && mapping.raft_rs_or_tikv_reference.contains("scheduler pressure")
            && mapping
                .byteraft_or_baseline_reference
                .contains("timer pressure evidence")
            && mapping.note.contains("drop node-runtime timer pressure")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts"
            && mapping
                .byteraft_or_baseline_reference
                .contains("BaselineRaft release-scale benchmark artifacts")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts"
            && mapping
                .byteraft_or_baseline_reference
                .contains("production-clean benchmark artifacts")
            && mapping.note.contains("deriving QPS scale targets")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_production_readiness_input_with_benchmark_artifacts"
            && mapping
                .byteraft_or_baseline_reference
                .contains("diagnostic benchmark artifacts")
            && mapping.note.contains("failed QPS")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_input_with_asserted_benchmark_artifacts"
            && mapping.raft_rs_or_tikv_reference.contains("fail-closed")
            && mapping.note.contains("matched-but-failing")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
            && mapping.raft_rs_or_tikv_reference.contains("ReadIndex")
            && mapping
                .byteraft_or_baseline_reference
                .contains("read backlog admission")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts"
            && mapping.raft_rs_or_tikv_reference.contains("ReadIndex")
            && mapping.note.contains("pending-read backlog evidence")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts"
            && mapping.raft_rs_or_tikv_reference.contains("scheduler")
            && mapping.note.contains("node-runtime timer evidence")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts"
            && mapping
                .byteraft_or_baseline_reference
                .contains("BaselineRaft release-scale benchmark artifacts")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_artifacts"
            && mapping
                .byteraft_or_baseline_reference
                .contains("runtime-pressure gated readiness report")
            && mapping.note.contains("latency")
            && mapping.note.contains("scale-target admission")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_production_readiness_report_with_benchmark_artifacts"
            && mapping
                .byteraft_or_baseline_reference
                .contains("readiness report")
            && mapping.note.contains("operator triage")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_report_with_asserted_benchmark_artifacts"
            && mapping
                .byteraft_or_baseline_reference
                .contains("production-clean")
            && mapping.note.contains("QPS")
            && mapping.note.contains("memory parity")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
            && mapping.raft_rs_or_tikv_reference.contains("ReadIndex")
            && mapping
                .byteraft_or_baseline_reference
                .contains("read backlog gated")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts"
            && mapping.raft_rs_or_tikv_reference.contains("ReadIndex")
            && mapping.note.contains("hard-gate semantics")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts"
            && mapping.raft_rs_or_tikv_reference.contains("scheduler")
            && mapping.note.contains("hard-gate semantics")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_grafana_dashboard"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("Grafana raftstore dashboard")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_grafana_dashboard_json"
            && mapping.raft_rs_or_tikv_reference.contains("JSON")
            && mapping
                .byteraft_or_baseline_reference
                .contains("dashboard JSON")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_alert_rules_json"
            && mapping.raft_rs_or_tikv_reference.contains("alertmanager")
            && mapping
                .byteraft_or_baseline_reference
                .contains("alert rule JSON")
            && mapping.note.contains("QPS")
            && mapping.note.contains("latency")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_observability_provisioning"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("observability bundle")
            && mapping
                .byteraft_or_baseline_reference
                .contains("production observability bundle")
            && mapping.note.contains("runbook")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_observability_required_metric_names"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("required metric catalog")
            && mapping
                .byteraft_or_baseline_reference
                .contains("production benchmark")
            && mapping.note.contains("QPS")
            && mapping.note.contains("memory")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_validate_required_metric_scrape_texts"
            && mapping
                .raft_rs_or_tikv_reference
                .contains("Prometheus scrape")
            && mapping
                .byteraft_or_baseline_reference
                .contains("scrape completeness")
            && mapping.note.contains("latency")
            && mapping.note.contains("benchmark")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_validate_observability_provisioning"
            && mapping
                .byteraft_or_baseline_reference
                .contains("provisioning drift validation")
            && mapping.note.contains("fails closed")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_observability_provisioning_validation_prometheus"
            && mapping.raft_rs_or_tikv_reference.contains("Prometheus")
            && mapping.note.contains("provisioning drift")
            && mapping.note.contains("QPS")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure"
            && mapping
                .byteraft_or_baseline_reference
                .contains("read backlog admission")
            && mapping.note.contains("fail-closed production decision")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence"
            && mapping
                .byteraft_or_baseline_reference
                .contains("read backlog pressure")
            && mapping.note.contains("ReadIndex")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical
            == "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
            && mapping.raft_rs_or_tikv_reference.contains("ReadIndex")
            && mapping
                .byteraft_or_baseline_reference
                .contains("read backlog pressure")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "LatencyPressureDetail"
            && mapping
                .byteraft_or_baseline_reference
                .contains("p95/p99 latency")
            && mapping.note.contains("sample count")
            && mapping.note.contains("production gates")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_validate_runtime_pressure_admission_evidence"
            && mapping
                .byteraft_or_baseline_reference
                .contains("runtime-pressure evidence validation")
            && mapping.note.contains("memory")
            && mapping.note.contains("scale")
            && mapping.note.contains("production gates")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "matrixraft_validate_runtime_pressure_admission_evidence_with_policy"
            && mapping
                .byteraft_or_baseline_reference
                .contains("fail-closed runtime-pressure rejection validation")
            && mapping
                .note
                .contains("configured fail-closed pressure priority")
            && mapping.note.contains("production gates")
    }));
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
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "examples/debug_artifacts.rs"
            && mapping.raft_rs_or_tikv_reference.contains("Prometheus")
            && mapping
                .byteraft_or_baseline_reference
                .contains("support bundle")
    }));
    assert!(api.api_name_mappings.iter().any(|mapping| {
        mapping.canonical == "examples/baseline_raft_parity_benchmark.rs"
            && mapping
                .byteraft_or_baseline_reference
                .contains("BaselineRaft-vs-RustRaft")
            && mapping.note.contains("QPS")
            && mapping.note.contains("memory")
    }));
    assert!(api
        .benchmark_interfaces
        .contains(&"BenchmarkRunner".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_scale_optimization_inputs_from_benchmark_report".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_validate_benchmark_scale_optimization_inputs".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_release_benchmark_runtime_timer_status".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_baseline_raft_benchmark_summary_prometheus".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_baseline_raft_benchmark_metric_names".to_string()));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_baseline_raft_benchmark_grafana_panels".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_observability_required_metric_names".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_validate_required_metric_scrape_texts".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_debug_snapshot_with_benchmark_summary".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_debug_snapshot_with_benchmark_artifacts".to_string()));
    assert!(api.observability_interfaces.contains(
        &"matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts".to_string()
    ));
    assert!(api.observability_interfaces.contains(
        &"matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_benchmark_runtime_pressure_readiness_artifact".to_string()));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog".to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer".to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact".to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_validate_benchmark_runtime_pressure_readiness_artifact".to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact".to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts".to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts"
            .to_string()
    ));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_production_readiness_input_with_benchmark_artifacts".to_string()));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_input_with_asserted_benchmark_artifacts".to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_artifacts"
            .to_string()
    ));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_production_readiness_report_with_benchmark_artifacts".to_string()));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_report_with_asserted_benchmark_artifacts".to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts"
            .to_string()
    ));
    assert!(api.benchmark_interfaces.contains(
        &"matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts"
            .to_string()
    ));
    assert!(api
        .benchmark_interfaces
        .contains(&"matrixraft_benchmark_runbook_steps".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_scale_target_metrics_prometheus".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_production_readiness_metric_names".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_production_readiness_grafana_panels".to_string()));
    assert!(api.observability_interfaces.contains(
        &"matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure"
            .to_string()
    ));
    assert!(api.observability_interfaces.contains(
        &"matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence".to_string()
    ));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_runtime_pressure_freshness_report".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_runtime_pressure_freshness_prometheus".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"LatencyPressureDetail".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_validate_runtime_pressure_admission_evidence".to_string()));
    assert!(api.observability_interfaces.contains(
        &"matrixraft_validate_runtime_pressure_admission_evidence_with_policy".to_string()
    ));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_snapshot_lifecycle_evidence_prometheus".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_snapshot_lifecycle_grafana_panels".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_wal_lifecycle_evidence_prometheus".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_wal_lifecycle_grafana_panels".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_membership_readiness_metric_names".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_membership_readiness_prometheus".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_membership_readiness_grafana_panels".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_observability_provisioning".to_string()));
    assert!(api
        .observability_interfaces
        .contains(&"matrixraft_observability_provisioning_runbook_steps".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_optimization_diagnostic_json_lines".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_membership_readiness_diagnostic_log_entries".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_membership_readiness_diagnostic_json_lines".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_production_readiness_diagnostic_json_lines".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_operator_runbook_steps_with_diagnostics".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_operator_runbook_prometheus".to_string()));
    assert!(api
        .diagnostic_interfaces
        .contains(&"matrixraft_validate_debug_snapshot".to_string()));
    assert!(api
        .compatibility_reports
        .contains(&"matrixraft_production_readiness_report".to_string()));
    assert!(api
        .compatibility_reports
        .contains(&"matrixraft_api_name_mappings".to_string()));
    assert!(api
        .compatibility_reports
        .contains(&"matrixraft_validate_public_api_contract".to_string()));

    let surface = matrixraft_open_source_surface();
    assert_eq!(surface.crate_name, "rustraft");
    assert!(surface.public_modules.contains(&"wal".to_string()));
    assert!(surface.embedding_docs.contains(&"README.md".to_string()));
    assert!(surface
        .baseline_raft_parity_matrix
        .contains(&"leader_election".to_string()));
    assert!(surface
        .benchmark_harness_interface
        .contains(&"matrixraft_run_baseline_raft_parity_benchmark".to_string()));
    assert!(surface
        .compatibility_reports
        .contains(&"matrixraft_public_api_contract".to_string()));
    assert!(surface
        .temporalstore_adapter_boundary
        .iter()
        .any(|item| item.contains("TemporalStore command codecs")));
    let adapter_shape = matrixraft_temporalstore_adapter_shape();
    assert_eq!(adapter_shape.node_field, "node");
    assert!(adapter_shape.node_runtime_type.contains("NodeRuntime"));
    assert!(adapter_shape
        .temporalstore_owned
        .contains(&"apply semantics".to_string()));
}

#[test]
fn public_api_contract_validation_prometheus_exports_mapping_drift_metrics() {
    let api = matrixraft_public_api_contract();
    let validation = matrixraft_validate_public_api_contract(&api);
    assert!(validation.ready);

    let prometheus = matrixraft_public_api_contract_validation_prometheus(
        &validation,
        &[("service", "raft\"a"), ("environment", "prod\\west")],
    );
    assert_eq!(prometheus.format, "prometheus_text_v0.0.4");
    assert_eq!(
        prometheus.metric_count,
        6 + validation.mapping_coverage_by_category.len() as u64 * 2
    );
    assert!(prometheus.text.contains(
        "rustraft_public_api_contract_ready{service=\"raft\\\"a\",environment=\"prod\\\\west\"} 1"
    ));
    assert!(prometheus
        .text
        .contains("rustraft_public_api_mapping_coverage_percent"));
    assert!(prometheus
        .text
        .contains("rustraft_public_api_unmapped_reference_required_total"));
    assert!(prometheus.text.contains(
        "rustraft_public_api_mapping_category_coverage_percent{service=\"raft\\\"a\",environment=\"prod\\\\west\",category=\"observability_interfaces\"}"
    ));
    assert!(!prometheus
        .text
        .contains("rustraft_public_api_blocker_present"));

    let mut broken = api;
    broken.api_name_mappings.retain(|mapping| {
        mapping.canonical != "ReadIndexRequest"
            && mapping.canonical != "matrixraft_public_api_contract_validation_prometheus"
    });
    let broken_validation = matrixraft_validate_public_api_contract(&broken);
    assert!(!broken_validation.ready);
    assert!(broken_validation
        .unmapped_reference_required_names
        .contains(&"ReadIndexRequest".to_string()));
    assert!(broken_validation
        .unmapped_reference_required_names
        .contains(&"matrixraft_public_api_contract_validation_prometheus".to_string()));

    let broken_prometheus = matrixraft_public_api_contract_validation_prometheus(
        &broken_validation,
        &[("service", "raft-a")],
    );
    assert!(broken_prometheus
        .text
        .contains("rustraft_public_api_contract_ready{service=\"raft-a\"} 0"));
    assert!(broken_prometheus
        .text
        .contains("rustraft_public_api_unmapped_reference_required_total{service=\"raft-a\"} 2"));
    assert!(broken_prometheus.text.contains(
        "rustraft_public_api_blocker_present{service=\"raft-a\",blocker=\"api_mapping:missing_required_canonical:ReadIndexRequest\"} 1"
    ));
    assert!(broken_prometheus.text.contains(
        "rustraft_public_api_blocker_present{service=\"raft-a\",blocker=\"api_mapping:missing_required_canonical:matrixraft_public_api_contract_validation_prometheus\"} 1"
    ));
    assert_eq!(
        broken_prometheus.metric_count,
        6 + broken_validation.mapping_coverage_by_category.len() as u64 * 2
            + broken_validation.blockers.len() as u64
    );
}

#[test]
fn public_api_contract_validation_rejects_duplicate_and_unmapped_names() {
    let mut api = matrixraft_public_api_contract();
    api.storage_trait = "MatrixRaftStorage".to_string();
    api.transport_trait = "MatrixRaftTransport".to_string();
    api.benchmark_interfaces.push("BenchmarkRunner".to_string());
    api.core_interfaces
        .retain(|name| name != "AdminCommand::ReleaseMemory");
    api.api_name_mappings.retain(|mapping| {
        mapping.canonical != "ReadIndexRequest" && mapping.canonical != "BenchmarkRunner"
    });
    api.api_name_mappings[0].raft_rs_or_tikv_reference.clear();

    let validation = matrixraft_validate_public_api_contract(&api);
    assert!(!validation.ready);
    assert!(validation
        .blockers
        .contains(&"storage_trait:non_canonical:MatrixRaftStorage:expected:Storage".to_string()));
    assert!(validation.blockers.contains(
        &"transport_trait:non_canonical:MatrixRaftTransport:expected:Transport".to_string()
    ));
    assert!(validation
        .blockers
        .contains(&"benchmark_interfaces:duplicate_name:BenchmarkRunner".to_string()));
    assert!(validation
        .blockers
        .contains(&"api_mapping:missing_required_canonical:ReadIndexRequest".to_string()));
    assert!(validation
        .blockers
        .contains(&"api_mapping:missing_required_canonical:BenchmarkRunner".to_string()));
    assert_eq!(
        validation.unmapped_reference_required_names,
        vec![
            "ReadIndexRequest".to_string(),
            "BenchmarkRunner".to_string()
        ]
    );
    assert!(validation
        .blockers
        .iter()
        .any(|blocker| { blocker == "api_mapping:Storage:missing_raft_rs_or_tikv_reference" }));
    assert!(validation.blockers.iter().any(|blocker| {
        blocker == "api_mapping:unadvertised_canonical:AdminCommand::ReleaseMemory"
    }));
    assert!(validation
        .unmapped_advertised_names
        .contains(&"BenchmarkRunner".to_string()));
    assert!(validation
        .unmapped_advertised_names
        .contains(&"ReadIndexRequest".to_string()));
    assert!(!validation
        .unmapped_advertised_names
        .contains(&"Storage".to_string()));
    assert!(validation.api_mapping_coverage_percent < 100);
}

#[test]
fn public_api_contract_validation_rejects_unmapped_public_categories() {
    let mut api = matrixraft_public_api_contract();
    let diagnostic_names = api
        .diagnostic_interfaces
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    api.api_name_mappings
        .retain(|mapping| !diagnostic_names.contains(mapping.canonical.as_str()));

    let validation = matrixraft_validate_public_api_contract(&api);
    assert!(!validation.ready);
    assert!(validation.blockers.contains(
        &"api_mapping:category_without_reference_mapping:diagnostic_interfaces".to_string()
    ));
    assert!(validation
        .mapping_coverage_by_category
        .iter()
        .any(|coverage| {
            coverage.category == "diagnostic_interfaces"
                && coverage.advertised_name_count == api.diagnostic_interfaces.len()
                && coverage.mapped_name_count == 0
                && coverage.coverage_percent == 0
        }));

    let mut api = matrixraft_public_api_contract();
    let example_names = api
        .embedding_examples
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    api.api_name_mappings
        .retain(|mapping| !example_names.contains(mapping.canonical.as_str()));

    let validation = matrixraft_validate_public_api_contract(&api);
    assert!(!validation.ready);
    assert!(validation.blockers.contains(
        &"api_mapping:category_without_reference_mapping:embedding_examples".to_string()
    ));
    assert!(validation
        .mapping_coverage_by_category
        .iter()
        .any(|coverage| {
            coverage.category == "embedding_examples"
                && coverage.advertised_name_count == api.embedding_examples.len()
                && coverage.mapped_name_count == 0
                && coverage.coverage_percent == 0
        }));

    let mut api = matrixraft_public_api_contract();
    let evidence_names = api
        .evidence_interfaces
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    api.api_name_mappings
        .retain(|mapping| !evidence_names.contains(mapping.canonical.as_str()));

    let validation = matrixraft_validate_public_api_contract(&api);
    assert!(!validation.ready);
    assert!(validation.blockers.contains(
        &"api_mapping:category_without_reference_mapping:evidence_interfaces".to_string()
    ));
    assert!(validation
        .mapping_coverage_by_category
        .iter()
        .any(|coverage| {
            coverage.category == "evidence_interfaces"
                && coverage.advertised_name_count == api.evidence_interfaces.len()
                && coverage.mapped_name_count == 0
                && coverage.coverage_percent == 0
        }));
}

#[test]
fn debug_artifacts_example_exports_complete_support_envelope() {
    let example = include_str!("../examples/debug_artifacts.rs");
    for required in [
        "\"debug_snapshot\"",
        "\"debug_snapshot_json\"",
        "\"debug_snapshot_metadata_prometheus\"",
        "\"diagnostic_json_lines\"",
        "\"local_status_diagnostic_json_lines\"",
        "\"runtime_pressure_freshness_diagnostic_json_lines\"",
        "\"diagnostic_prometheus\"",
        "\"peer_pipeline_prometheus\"",
        "\"runtime_pressure_freshness_prometheus\"",
        "\"snapshot_lifecycle_prometheus\"",
        "\"wal_lifecycle_prometheus\"",
        "\"membership_readiness_prometheus\"",
        "\"benchmark_prometheus\"",
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
    assert!(example.contains("matrixraft_local_status_diagnostic_json_lines"));
    assert!(example.contains("matrixraft_runtime_pressure_freshness_diagnostic_json_lines"));
    assert!(example.contains("matrixraft_peer_pipeline_metrics_prometheus"));
    assert!(example.contains("matrixraft_snapshot_lifecycle_evidence_prometheus"));
    assert!(example.contains("matrixraft_wal_lifecycle_evidence_prometheus"));
    assert!(example.contains("matrixraft_membership_readiness_prometheus"));
    assert!(example.contains("snapshot.diagnostic_prometheus"));
    assert!(example.contains("matrixraft_baseline_raft_benchmark_failure_summary"));
    assert!(example.contains("matrixraft_debug_snapshot_with_benchmark_artifacts"));
    assert!(example.contains("matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts"));
    assert!(example.contains(
        "matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts"
    ));
    assert!(example.contains("benchmark_prometheus"));
    assert!(example.contains("runtime_pressure_freshness_prometheus"));
    assert!(example.contains("snapshot_lifecycle_prometheus"));
    assert!(example.contains("wal_lifecycle_prometheus"));
    assert!(example.contains("membership_readiness_prometheus"));
    assert!(example.contains("snapshot.optimization_prometheus"));
    assert!(example.contains("snapshot.runbook_prometheus"));
    assert!(example.contains("matrixraft_operator_runbook_prometheus(&provisioning.runbook_steps"));
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
    assert!(
        example.contains("DEBUG_SNAPSHOT_MAX_AGE_MS.saturating_sub(DEBUG_SNAPSHOT_LOW_FRESH_MS)")
    );
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
    assert!(
        example.contains("let support_envelope_severity = if support_envelope_issues.is_empty()")
    );
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
    assert!(example
        .contains("\"RustRaftSupportEnvelopeValidationFailed\": \"validate_support_envelope\""));
    assert!(example.contains("\"RustRaftSupportEnvelopeCritical\": \"wire_critical_alerts\""));
    assert!(example.contains("\"RustRaftDiagnosticErrors\": \"inspect_error_diagnostics\""));
    assert!(example.contains(
        "\"RustRaftOptimizationCriticalHints\": \"resolve_critical_optimization_hints\""
    ));
    assert!(example.contains(
        "\"RustRaftRuntimePressureBottleneckActive\": \"inspect_runtime_pressure_bottleneck_warning\""
    ));
    assert!(example.contains(
        "\"RustRaftProductionReadinessRuntimePressureBottleneck\": \"resolve_production_readiness_runtime_pressure_bottleneck\""
    ));
    assert!(example.contains(
        "\"RustRaftRuntimePressureFreshnessLow\": \"refresh_runtime_pressure_evidence\""
    ));
    assert!(example.contains(
        "\"RustRaftRuntimePressureFreshnessLost\": \"refresh_runtime_pressure_evidence\""
    ));
    assert!(example.contains(
        "\"RustRaftRuntimePressureFreshnessInvalid\": \"refresh_runtime_pressure_evidence\""
    ));
    assert!(example.contains(
        "\"RustRaftBaselineRaftBenchmarkFreshnessLost\": \"refresh_baseline_raft_benchmark_evidence\""
    ));
    assert!(example.contains("\"RustRaftDebugSnapshotFreshnessLost\": \"refresh_debug_snapshot\""));
    assert!(example.contains("\"runbook_evidence_map\""));
    assert!(example.contains("\"validate_support_envelope\""));
    assert!(example
        .contains("rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\"}"));
    assert!(example.contains("\"wire_critical_alerts\""));
    assert!(example.contains("alert_rules_json"));
    assert!(example.contains("\"inspect_error_diagnostics\""));
    assert!(example.contains("diagnostic_json_lines"));
    assert!(example.contains("\"resolve_critical_optimization_hints\""));
    assert!(example.contains("rustraft_optimization_critical_total"));
    assert!(example.contains("\"inspect_runtime_pressure_bottleneck_warning\""));
    assert!(example.contains("rustraft_runtime_pressure_bottleneck_score_percent"));
    assert!(example.contains("\"resolve_production_readiness_runtime_pressure_bottleneck\""));
    assert!(
        example.contains("rustraft_production_readiness_runtime_pressure_bottleneck_score_percent")
    );
    assert!(example.contains("rustraft_production_readiness_blocker_total"));
    assert!(example.contains("\"refresh_runtime_pressure_evidence\""));
    assert!(example.contains("rustraft_runtime_pressure_freshness_fresh"));
    assert!(example.contains("rustraft_runtime_pressure_freshness_issue_total"));
    assert!(example.contains("\"refresh_baseline_raft_benchmark_evidence\""));
    assert!(example.contains("rustraft_baseline_raft_benchmark_fresh"));
    assert!(example.contains("rustraft_baseline_raft_benchmark_freshness_status"));
    assert!(example.contains("\"refresh_debug_snapshot\""));
    assert!(example.contains("rustraft_debug_snapshot_fresh"));
    assert!(example.contains("\"operator_handoff_sequence\""));
    assert!(
        example.contains("\"validate_support_envelope\",\n            \"wire_critical_alerts\"")
    );
    assert!(example.contains(
        "\"inspect_error_diagnostics\",\n            \"resolve_critical_optimization_hints\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\",\n            \"inspect_runtime_pressure_bottleneck_warning\""
    ));
    assert!(example.contains(
        "\"inspect_runtime_pressure_bottleneck_warning\",\n            \"resolve_production_readiness_runtime_pressure_bottleneck\""
    ));
    assert!(example.contains(
        "\"resolve_production_readiness_runtime_pressure_bottleneck\",\n            \"refresh_runtime_pressure_evidence\""
    ));
    assert!(example.contains(
        "\"refresh_runtime_pressure_evidence\",\n            \"refresh_baseline_raft_benchmark_evidence\""
    ));
    assert!(example.contains(
        "\"refresh_baseline_raft_benchmark_evidence\",\n            \"refresh_debug_snapshot\""
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
        "\"inspect_runtime_pressure_bottleneck_warning\": \"cargo run --example debug_artifacts --quiet | rg rustraft_runtime_pressure_bottleneck_score_percent\""
    ));
    assert!(example.contains(
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"cargo run --example debug_artifacts --quiet | rg rustraft_production_readiness_runtime_pressure_bottleneck_score_percent\""
    ));
    assert!(example.contains(
        "\"refresh_runtime_pressure_evidence\": \"cargo run --example debug_artifacts --quiet | rg rustraft_runtime_pressure_freshness_fresh\""
    ));
    assert!(example.contains(
        "\"refresh_baseline_raft_benchmark_evidence\": \"cargo run --example debug_artifacts --quiet | rg rustraft_baseline_raft_benchmark_fresh\""
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
        "\"inspect_runtime_pressure_bottleneck_warning\": \"runtime pressure bottleneck score is visible and can be cleared before parity claims\""
    ));
    assert!(example.contains(
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"production-readiness runtime pressure bottleneck score is visible and cleared before release claims\""
    ));
    assert!(example.contains(
        "\"refresh_runtime_pressure_evidence\": \"runtime-pressure freshness metric is one, issue total is zero, and status is fresh\""
    ));
    assert!(example.contains(
        "\"refresh_baseline_raft_benchmark_evidence\": \"benchmark freshness metric is one and status is fresh before QPS, latency, CPU, or memory claims\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"debug snapshot freshness metric is one and freshness status is fresh\""
    ));
    assert!(example.contains("\"handoff_dashboard_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": [\n                \"Support Envelope Validation Ready\""
    ));
    assert!(example.contains("\"Support Envelope First Issue\""));
    assert!(example
        .contains("\"wire_critical_alerts\": [\n                \"Support Envelope Severity\""));
    assert!(example.contains("\"Support Envelope Issue Breakdown\""));
    assert!(example.contains("\"Diagnostic Errors\""));
    assert!(example.contains("\"Diagnostic Log Rate\""));
    assert!(example.contains("\"Optimization Critical Hints\""));
    assert!(example.contains("\"Triage Top Optimization Hint\""));
    assert!(example.contains("\"Production Runtime Pressure Bottlenecks\""));
    assert!(example.contains("\"Production Readiness Blockers\""));
    assert!(example.contains("\"Runtime Pressure Freshness\""));
    assert!(example.contains("\"Runtime Pressure Freshness Issues\""));
    assert!(example.contains("\"Benchmark Freshness\""));
    assert!(example.contains("\"Benchmark Freshness Remaining\""));
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
    assert!(example.contains("\"provisioning_runbook_prometheus\""));
    assert!(example.contains("\"validation_prometheus\""));
    assert!(example.contains("\"handoff_owner_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-observability-oncall\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-runtime-incident-commander\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-owner\""));
    assert!(example.contains("\"resolve_critical_optimization_hints\": \"raft-performance-owner\""));
    assert!(example.contains(
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"raft-production-readiness-owner\""
    ));
    assert!(example.contains("\"refresh_runtime_pressure_evidence\": \"raft-performance-owner\""));
    assert!(example
        .contains("\"refresh_baseline_raft_benchmark_evidence\": \"raft-performance-owner\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-runtime-owner\""));
    assert!(example.contains("\"handoff_priority_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"P0\""));
    assert!(example.contains("\"wire_critical_alerts\": \"P0\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"P1\""));
    assert!(example.contains("\"resolve_critical_optimization_hints\": \"P1\""));
    assert!(
        example.contains("\"resolve_production_readiness_runtime_pressure_bottleneck\": \"P0\"")
    );
    assert!(example.contains("\"refresh_runtime_pressure_evidence\": \"P1\""));
    assert!(example.contains("\"refresh_baseline_raft_benchmark_evidence\": \"P1\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"P2\""));
    assert!(example.contains("\"handoff_response_time_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"immediate\""));
    assert!(example.contains("\"wire_critical_alerts\": \"immediate\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"within 5 minutes\""));
    assert!(example.contains("\"resolve_critical_optimization_hints\": \"within 15 minutes\""));
    assert!(example
        .contains("\"resolve_production_readiness_runtime_pressure_bottleneck\": \"immediate\""));
    assert!(example.contains("\"refresh_runtime_pressure_evidence\": \"within 15 minutes\""));
    assert!(example.contains("\"refresh_baseline_raft_benchmark_evidence\": \"within 15 minutes\""));
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
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"production-readiness runtime pressure bottleneck score remains nonzero\""
    ));
    assert!(example.contains(
        "\"refresh_runtime_pressure_evidence\": \"runtime-pressure freshness metric remains zero or issue total remains nonzero\""
    ));
    assert!(example.contains(
        "\"refresh_baseline_raft_benchmark_evidence\": \"benchmark freshness metric remains zero or status is not fresh\""
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
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"inspect production-readiness pressure sources and reduce every blocking runtime pressure score\""
    ));
    assert!(example.contains(
        "\"refresh_runtime_pressure_evidence\": \"rerun release-scale runtime-pressure capture and recheck freshness Prometheus\""
    ));
    assert!(example.contains(
        "\"refresh_baseline_raft_benchmark_evidence\": \"rerun release-mode BaselineRaft parity benchmarks and recheck benchmark freshness Prometheus\""
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
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"rustraft_production_readiness_runtime_pressure_bottleneck_score_percent is zero\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"rustraft_debug_snapshot_fresh is one and validation Prometheus is present\""
    ));
    assert!(example.contains(
        "\"refresh_baseline_raft_benchmark_evidence\": \"rustraft_baseline_raft_benchmark_fresh is one and freshness status is fresh\""
    ));
    assert!(example.contains("\"handoff_retained_artifact_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"support_envelope_validation\""));
    assert!(example.contains("\"wire_critical_alerts\": \"alert_rules_json\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"diagnostic_json_lines\""));
    assert!(
        example.contains("\"resolve_critical_optimization_hints\": \"optimization_prometheus\"")
    );
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
    assert!(example.contains("\"validate_support_envelope\": \"rustraft_support_envelope_ready\""));
    assert!(example
        .contains("\"wire_critical_alerts\": \"rustraft_support_envelope_critical_alert_total\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"rustraft_diagnostic_log_errors\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_optimization_critical_total\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"rustraft_debug_snapshot_fresh\""));
    assert!(example.contains("\"handoff_triage_signal_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"rustraft_operator_triage_status\""));
    assert!(example.contains("\"wire_critical_alerts\": \"rustraft_operator_triage_top_alert\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"rustraft_operator_triage_top_diagnostic\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_operator_triage_top_optimization_hint\""
    ));
    assert!(
        example.contains("\"refresh_debug_snapshot\": \"rustraft_operator_triage_first_action\"")
    );
    assert!(example.contains("\"handoff_validation_gate_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"support_envelope_validation.ready\"")
    );
    assert!(example
        .contains("\"wire_critical_alerts\": \"support_envelope_validation.alert_links_present\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"debug_snapshot_validation.diagnostic_log_contract\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"debug_snapshot_validation.optimization_prometheus_contract\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"debug_snapshot_validation.freshness_contract\""));
    assert!(example.contains("\"handoff_promql_query_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\"} == 1\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"rustraft_support_envelope_critical_alert_total > 0\""
    ));
    assert!(
        example.contains("\"inspect_error_diagnostics\": \"rustraft_diagnostic_log_errors == 0\"")
    );
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_optimization_critical_total == 0\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"rustraft_debug_snapshot_fresh == 1\""));
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
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"debug_snapshot_json | rg generated_at_unix_ms\""));
    assert!(example.contains("\"handoff_annotation_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"RustRaft support envelope validated\"")
    );
    assert!(
        example.contains("\"wire_critical_alerts\": \"RustRaft critical alert route verified\"")
    );
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"RustRaft diagnostic evidence inspected\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"RustRaft optimization critical total cleared\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"RustRaft debug snapshot refreshed\""));
    assert!(example.contains("\"handoff_correlation_key_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"rustraft.support_envelope.validation\"")
    );
    assert!(example.contains("\"wire_critical_alerts\": \"rustraft.support_envelope.alert_route\""));
    assert!(
        example.contains("\"inspect_error_diagnostics\": \"rustraft.diagnostics.error_target\"")
    );
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft.optimization.critical_hint\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"rustraft.debug_snapshot.refresh\""));
    assert!(example.contains("\"handoff_retention_window_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"retain for 30 days\""));
    assert!(example.contains("\"wire_critical_alerts\": \"retain for 30 days\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"retain for 14 days\""));
    assert!(example.contains("\"resolve_critical_optimization_hints\": \"retain for 14 days\""));
    assert!(
        example.contains("\"refresh_debug_snapshot\": \"retain until next successful refresh\"")
    );
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
    assert!(example.contains("\"wire_critical_alerts\": \"raft-runtime-incident-commander\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-owner\""));
    assert!(example.contains("\"resolve_critical_optimization_hints\": \"raft-performance-owner\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-runtime-owner\""));
    assert!(example.contains("\"handoff_verification_evidence_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"rustraft_debug_bundle_validation_ready{artifact=\\\"support_envelope\\\"}\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"alert_rules_json includes RustRaftSupportEnvelopeCritical owner\""
    ));
    assert!(
        example.contains("\"inspect_error_diagnostics\": \"rustraft_diagnostic_log_errors == 0\"")
    );
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_optimization_critical_total == 0\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"rustraft_debug_snapshot_fresh == 1\""));
    assert!(example.contains("\"handoff_verification_cadence_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"verify on the next Prometheus scrape\"")
    );
    assert!(
        example.contains("\"wire_critical_alerts\": \"verify before leaving the incident bridge\"")
    );
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"verify after 5 minutes without repeated errors\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"verify across two consecutive optimization scrapes\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"verify before the next low-freshness window\""));
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
    assert!(
        example.contains("\"validate_support_envelope\": \"support envelope verification note\"")
    );
    assert!(
        example.contains("\"wire_critical_alerts\": \"critical alert route verification note\"")
    );
    assert!(
        example.contains("\"inspect_error_diagnostics\": \"diagnostic error verification note\"")
    );
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"optimization recovery verification note\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"debug snapshot freshness verification note\""));
    assert!(example.contains("\"handoff_verification_delivery_channel_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"support envelope incident timeline\"")
    );
    assert!(example.contains("\"wire_critical_alerts\": \"critical alert routing review\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"diagnostic investigation log\""));
    assert!(example
        .contains("\"resolve_critical_optimization_hints\": \"optimization follow-up report\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"debug snapshot refresh record\""));
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
    assert!(example.contains("\"resolve_critical_optimization_hints\": \"raft-performance-owner\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-runtime-owner\""));
    assert!(example.contains("\"handoff_verification_reopen_notification_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-observability-oncall/reopen-support-envelope\""
    ));
    assert!(example.contains(
        "\"wire_critical_alerts\": \"raft-runtime-incident-commander/reopen-alert-routing\""
    ));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostics-owner/reopen-diagnostics\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-performance-owner/reopen-optimization\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-runtime-owner/reopen-debug-snapshot\""));
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
    assert!(
        example.contains("\"wire_critical_alerts\": \"RustRaft Alert Route Reopen Resolution\"")
    );
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"RustRaft Diagnostics Reopen Resolution\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"RustRaft Optimization Reopen Resolution\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"RustRaft Debug Snapshot Reopen Resolution\""));
    assert!(example.contains("\"handoff_verification_reopen_metric_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"rustraft_handoff_reopen_support_envelope_total\""
    ));
    assert!(
        example.contains("\"wire_critical_alerts\": \"rustraft_handoff_reopen_alert_route_total\"")
    );
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"rustraft_handoff_reopen_diagnostics_total\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"rustraft_handoff_reopen_optimization_total\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"rustraft_handoff_reopen_debug_snapshot_total\""));
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
    assert!(example.contains("\"handoff_verification_reopen_alert_threshold_map\""));
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
    assert!(example.contains("\"handoff_verification_reopen_notification_route_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-support-envelope-page\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-page\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-review\""));
    assert!(
        example.contains("\"resolve_critical_optimization_hints\": \"raft-optimization-review\"")
    );
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-review\""));
    assert!(example.contains("\"handoff_verification_reopen_escalation_policy_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-incident-lead-immediate-escalation\""));
    assert!(
        example.contains("\"wire_critical_alerts\": \"raft-incident-lead-immediate-escalation\"")
    );
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostics-owner-next-business-cycle\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-owner-next-business-cycle\""
    ));
    assert!(
        example.contains("\"refresh_debug_snapshot\": \"raft-debug-owner-next-business-cycle\"")
    );
    assert!(example.contains("\"handoff_verification_reopen_log_query_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft_logs{event=\\\"support_envelope_reopened\\\"}\""
    ));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft_logs{event=\\\"alert_route_reopened\\\"}\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft_logs{event=\\\"diagnostic_reopened\\\"}\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft_logs{event=\\\"optimization_reopened\\\"}\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft_logs{event=\\\"debug_snapshot_reopened\\\"}\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_correlation_key_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft.support_envelope.reopen_id\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft.alert_route.reopen_id\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft.diagnostics.reopen_id\""));
    assert!(example
        .contains("\"resolve_critical_optimization_hints\": \"raft.optimization.reopen_id\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft.debug_snapshot.reopen_id\""));
    assert!(example.contains("\"handoff_verification_reopen_retention_window_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"retain reopen metric, log, and evidence for 30d\""
    ));
    assert!(example
        .contains("\"wire_critical_alerts\": \"retain reopen metric, log, and evidence for 30d\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"retain diagnostic reopen context for 14d\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"retain optimization reopen context for 14d\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"retain debug snapshot reopen context for 7d\""));
    assert!(example.contains("\"handoff_verification_reopen_cleanup_guard_map\""));
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
    assert!(example.contains("\"handoff_verification_reopen_final_summary_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-envelope-reopen-closeout\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-reopen-closeout\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-reopen-closeout\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-reopen-closeout\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-reopen-closeout\""));
    assert!(example.contains("\"handoff_verification_reopen_acknowledgement_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-support-envelope-reopen-ack\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-reopen-ack\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-reopen-ack\""));
    assert!(example
        .contains("\"resolve_critical_optimization_hints\": \"raft-optimization-reopen-ack\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-reopen-ack\""));
    assert!(example.contains("\"handoff_verification_reopen_delivery_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"raft-support-envelope-closeout-feed\"")
    );
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-closeout-feed\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-closeout-feed\""));
    assert!(example
        .contains("\"resolve_critical_optimization_hints\": \"raft-optimization-closeout-feed\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-closeout-feed\""));
    assert!(example.contains("\"handoff_verification_reopen_replay_source_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"raft-support-envelope-reopen-replay\"")
    );
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-reopen-replay\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-reopen-replay\""));
    assert!(example
        .contains("\"resolve_critical_optimization_hints\": \"raft-optimization-reopen-replay\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-reopen-replay\""));
    assert!(example.contains("\"handoff_verification_reopen_replay_check_map\""));
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
    assert!(example.contains("\"handoff_verification_reopen_replay_result_map\""));
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
    assert!(example.contains("\"handoff_verification_reopen_replay_failure_action_map\""));
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
    assert!(example.contains("\"handoff_verification_reopen_replay_escalation_evidence_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-replay-escalation-note\""
    ));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-replay-page-record\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostics-replay-escalation-log\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-replay-owner-ticket\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-replay-refresh-ticket\""));
    assert!(
        example.contains("\"handoff_verification_reopen_replay_escalation_acknowledgement_map\"")
    );
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-replay-escalation-ack\""
    ));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-replay-page-ack\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostics-replay-escalation-ack\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-replay-owner-ack\""
    ));
    assert!(
        example.contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-replay-refresh-ack\"")
    );
    assert!(example.contains("\"handoff_verification_reopen_replay_escalation_closeout_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-replay-escalation-closeout\""
    ));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-replay-page-closeout\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostics-replay-escalation-closeout\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-replay-owner-closeout\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-replay-refresh-closeout\""));
    assert!(example.contains("\"handoff_verification_reopen_replay_escalation_delivery_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-envelope-replay-closeout-feed\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-replay-closeout-feed\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostics-replay-closeout-feed\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-replay-closeout-feed\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-replay-closeout-feed\""));
    assert!(example.contains("\"handoff_verification_reopen_replay_escalation_retention_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"retain replay closeout feed for 30d\"")
    );
    assert!(
        example.contains("\"wire_critical_alerts\": \"retain replay alert closeout feed for 30d\"")
    );
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"retain replay diagnostics closeout feed for 14d\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"retain replay optimization closeout feed for 14d\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"retain replay debug snapshot closeout feed for 7d\""
    ));
    assert!(example.contains("\"handoff_verification_reopen_replay_escalation_expiry_review_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"review replay closeout feed on day 25\""));
    assert!(example
        .contains("\"wire_critical_alerts\": \"review replay alert closeout feed on day 25\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"review replay diagnostics closeout feed on day 10\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"review replay optimization closeout feed on day 10\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"review replay debug snapshot closeout feed on day 5\""
    ));
    assert!(example
        .contains("\"handoff_verification_reopen_replay_escalation_expiry_disposition_map\""));
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
    assert!(example.contains("\"handoff_verification_reopen_replay_escalation_expiry_audit_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-replay-support-expiry-audit-record\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-replay-alert-expiry-audit-record\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-replay-diagnostics-expiry-audit-record\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-replay-optimization-expiry-audit-record\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-replay-debug-snapshot-expiry-audit-record\""
    ));
    assert!(example
        .contains("\"handoff_verification_reopen_replay_escalation_expiry_audit_owner_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"raft-support-envelope-audit-owner\"")
    );
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-audit-owner\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-audit-owner\""));
    assert!(example
        .contains("\"resolve_critical_optimization_hints\": \"raft-optimization-audit-owner\""));
    assert!(example.contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-audit-owner\""));
    assert!(example
        .contains("\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-envelope-expiry-audit-signoff\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-expiry-audit-signoff\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-audit-signoff\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-audit-signoff\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-audit-signoff\""));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_delivery_map\""
    ));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-envelope-expiry-signoff-feed\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-expiry-signoff-feed\""));
    assert!(
        example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-signoff-feed\"")
    );
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-signoff-feed\""
    ));
    assert!(
        example.contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-signoff-feed\"")
    );
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_acknowledgement_map\""
    ));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-envelope-expiry-signoff-ack\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-expiry-signoff-ack\""));
    assert!(
        example.contains("\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-signoff-ack\"")
    );
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-signoff-ack\""
    ));
    assert!(
        example.contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-signoff-ack\"")
    );
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-envelope-expiry-signoff-closeout\""
    ));
    assert!(
        example.contains("\"wire_critical_alerts\": \"raft-alert-route-expiry-signoff-closeout\"")
    );
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-signoff-closeout\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-signoff-closeout\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-signoff-closeout\""));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_delivery_map\""
    ));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-envelope-expiry-closeout-feed\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-expiry-closeout-feed\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-closeout-feed\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-closeout-feed\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-closeout-feed\""));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_retention_map\""
    ));
    assert!(example
        .contains("\"validate_support_envelope\": \"retain replay expiry closeout feed for 30d\""));
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
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-expiry-closeout-archive-proof\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-expiry-closeout-page-proof\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-closeout-log-proof\""));
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
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-expiry-closeout-cleanup-approved\""));
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
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-expiry-closeout-cleanup-executed\""));
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
    assert!(
        example.contains("\"validate_support_envelope\": \"raft-support-expiry-cleanup-notified\"")
    );
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-notified\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostics-expiry-cleanup-notified\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-expiry-cleanup-notified\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-debug-snapshot-expiry-cleanup-notified\""));
    assert!(example.contains(
        "\"handoff_verification_reopen_replay_escalation_expiry_audit_signoff_closeout_cleanup_acknowledgement_map\""
    ));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-support-expiry-cleanup-notification-ack\""
    ));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-notification-ack\""));
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
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-expiry-cleanup-loop-closed\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-loop-closed\""));
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
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-expiry-cleanup-final-summary\""));
    assert!(
        example.contains("\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-final-summary\"")
    );
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
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-expiry-cleanup-archive-index\""));
    assert!(
        example.contains("\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-archive-index\"")
    );
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
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-support-expiry-cleanup-archive-owner\""));
    assert!(
        example.contains("\"wire_critical_alerts\": \"raft-alert-expiry-cleanup-archive-owner\"")
    );
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
    assert!(
        example.contains("\"wire_critical_alerts\": \"retain raft alert cleanup archive for 30d\"")
    );
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
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-cleanup-archive-review-signed-off\""));
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
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-cleanup-archive-premerge-evidence\""));
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
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-cleanup-archive-premerge-verified\""));
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
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-audited\""));
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
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-summary\""));
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
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-handoff\""));
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
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-followup\""));
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
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-cleanup-archive-publication-closed\""));
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
    assert!(example.contains("\"validate_support_envelope\": \"support_envelope_validation\""));
    assert!(example.contains("\"wire_critical_alerts\": \"critical_alert_handoff\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"diagnostic_log_prometheus\""));
    assert!(example.contains("\"resolve_critical_optimization_hints\": \"optimization_handoff\""));
    assert!(example.contains(
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"production_readiness_runtime_pressure_handoff\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"debug_snapshot_json\""));
    assert!(example.contains("\"support_envelope_operator_verification_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"support_envelope_validation_prometheus\""));
    assert!(example.contains("\"wire_critical_alerts\": \"alert_rules_json\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"diagnostic_json_lines\""));
    assert!(
        example.contains("\"resolve_critical_optimization_hints\": \"optimization_prometheus\"")
    );
    assert!(example.contains(
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"provisioning_runbook_prometheus\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"validation_prometheus\""));
    assert!(example.contains("\"support_envelope_operator_dashboard_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"Support Envelope Validation Ready\"")
    );
    assert!(example.contains("\"wire_critical_alerts\": \"Support Envelope Severity\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"Support Envelope First Issue\""));
    assert!(example
        .contains("\"resolve_critical_optimization_hints\": \"Triage Top Optimization Hint\""));
    assert!(example.contains(
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"Production Runtime Pressure Bottlenecks\""
    ));
    assert!(example.contains("\"refresh_debug_snapshot\": \"Support Envelope Freshness Status\""));
    assert!(example.contains("\"support_envelope_operator_runbook_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"validate_support_envelope\""));
    assert!(example.contains("\"wire_critical_alerts\": \"wire_critical_alerts\""));
    assert!(example.contains("\"inspect_error_diagnostics\": \"inspect_error_diagnostics\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"resolve_critical_optimization_hints\""
    ));
    assert!(example.contains(
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"resolve_production_readiness_runtime_pressure_bottleneck\""
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
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"cargo run --example debug_artifacts --quiet | rg rustraft_production_readiness_runtime_pressure_bottleneck_score_percent\""
    ));
    assert!(example.contains(
        "\"inspect_runtime_pressure_bottleneck_warning\": \"cargo run --example debug_artifacts --quiet | rg rustraft_runtime_pressure_bottleneck_score_percent\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"cargo run --example debug_artifacts --quiet | rg rustraft_debug_snapshot_fresh\""
    ));
    assert!(example.contains("\"support_envelope_operator_execution_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"run contract validation before handoff\""));
    assert!(
        example.contains("\"wire_critical_alerts\": \"confirm critical alert routing evidence\"")
    );
    assert!(
        example.contains("\"inspect_error_diagnostics\": \"inspect diagnostic log error totals\"")
    );
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"triage critical optimization hint totals\""
    ));
    assert!(example.contains(
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"resolve production-readiness runtime pressure bottleneck totals\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"confirm debug snapshot freshness evidence\""));
    assert!(example.contains("\"support_envelope_operator_acceptance_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"support envelope contract test passes\""));
    assert!(example.contains("\"wire_critical_alerts\": \"critical alert is present and routed\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"diagnostic error totals are inspectable\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"critical optimization total is visible\""
    ));
    assert!(example.contains(
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"production-readiness runtime pressure bottleneck is visible\""
    ));
    assert!(
        example.contains("\"refresh_debug_snapshot\": \"fresh debug snapshot signal is present\"")
    );
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
        "\"resolve_production_readiness_runtime_pressure_bottleneck\": \"escalate production-readiness pressure to raft-production-readiness-owner before release signoff\""
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
    assert!(example.contains("\"support_envelope_operator_acknowledgement_map\""));
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
    assert!(example.contains("\"validate_support_envelope\": \"raft-validation-retention-proof\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-retention-proof\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostic-totals-retention-proof\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-retention-proof\""
    ));
    assert!(
        example.contains("\"refresh_debug_snapshot\": \"raft-snapshot-freshness-retention-proof\"")
    );
    assert!(example.contains("\"support_envelope_operator_cleanup_approval_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-validation-cleanup-approved\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-approved\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-approved\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-approved\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-approved\""));
    assert!(example.contains("\"support_envelope_operator_cleanup_execution_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-validation-cleanup-executed\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-executed\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-executed\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-executed\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-executed\""));
    assert!(example.contains("\"support_envelope_operator_cleanup_verification_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-validation-cleanup-verified\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-verified\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-verified\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-verified\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-verified\""));
    assert!(example.contains("\"support_envelope_operator_cleanup_notification_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-validation-cleanup-notified\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-notified\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-notified\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-notified\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-notified\""));
    assert!(example.contains("\"support_envelope_operator_cleanup_acknowledgement_map\""));
    assert!(
        example.contains("\"validate_support_envelope\": \"raft-validation-cleanup-acknowledged\"")
    );
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-acknowledged\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-acknowledged\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-acknowledged\""));
    assert!(example.contains("\"support_envelope_operator_cleanup_closure_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-validation-cleanup-closed\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-closed\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-closed\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-closed\""
    ));
    assert!(
        example.contains("\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-closed\"")
    );
    assert!(example.contains("\"support_envelope_operator_cleanup_summary_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-validation-cleanup-summary\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-summary\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-summary\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-summary\""
    ));
    assert!(
        example.contains("\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-summary\"")
    );
    assert!(example.contains("\"support_envelope_operator_cleanup_archive_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-validation-cleanup-archived\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-archived\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-archived\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-archived\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-archived\""));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_map\""));
    assert!(example.contains("\"validate_support_envelope\": \"raft-validation-cleanup-retained\""));
    assert!(example.contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retained\""));
    assert!(example
        .contains("\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retained\""));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retained\""
    ));
    assert!(example
        .contains("\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retained\""));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_review_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-validation-cleanup-retention-reviewed\""));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-reviewed\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-reviewed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-reviewed\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_approval_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-validation-cleanup-retention-approved\""));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-approved\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-approved\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-approved\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-approved\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_execution_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-validation-cleanup-retention-executed\""));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-executed\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-executed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-executed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-executed\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_verification_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-validation-cleanup-retention-verified\""));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-verified\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-verified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-verified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-verified\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_notification_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-validation-cleanup-retention-notified\""));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-notified\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-notified\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-notified\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-notified\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_acknowledgement_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-acknowledged\""
    ));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-acknowledged\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-acknowledged\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-acknowledged\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_closure_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-validation-cleanup-retention-closed\""));
    assert!(
        example.contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-closed\"")
    );
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-closed\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-closed\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-closed\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_summary_map\""));
    assert!(example.contains(
        "\"validate_support_envelope\": \"raft-validation-cleanup-retention-summarized\""
    ));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-summarized\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-summarized\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-summarized\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-summarized\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_archive_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-validation-cleanup-retention-archived\""));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-archived\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-archived\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-archived\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-archived\""
    ));
    assert!(example.contains("\"support_envelope_operator_cleanup_retention_retention_map\""));
    assert!(example
        .contains("\"validate_support_envelope\": \"raft-validation-cleanup-retention-retained\""));
    assert!(example
        .contains("\"wire_critical_alerts\": \"raft-alert-route-cleanup-retention-retained\""));
    assert!(example.contains(
        "\"inspect_error_diagnostics\": \"raft-diagnostic-totals-cleanup-retention-retained\""
    ));
    assert!(example.contains(
        "\"resolve_critical_optimization_hints\": \"raft-optimization-totals-cleanup-retention-retained\""
    ));
    assert!(example.contains(
        "\"refresh_debug_snapshot\": \"raft-snapshot-freshness-cleanup-retention-retained\""
    ));
    assert!(
        example.contains("\"support_envelope_operator_cleanup_retention_retention_review_map\"")
    );
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
    assert!(
        example.contains("\"support_envelope_operator_cleanup_retention_retention_approval_map\"")
    );
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
    assert!(
        example.contains("\"support_envelope_operator_cleanup_retention_retention_execution_map\"")
    );
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
    assert!(example
        .contains("\"support_envelope_operator_cleanup_retention_retention_verification_map\""));
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
    assert!(example
        .contains("\"support_envelope_operator_cleanup_retention_retention_notification_map\""));
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
    assert!(example
        .contains("\"support_envelope_operator_cleanup_retention_retention_acknowledgement_map\""));
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
    assert!(
        example.contains("\"support_envelope_operator_cleanup_retention_retention_closure_map\"")
    );
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
    assert!(
        example.contains("\"support_envelope_operator_cleanup_retention_retention_summary_map\"")
    );
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
    assert!(
        example.contains("\"support_envelope_operator_cleanup_retention_retention_archive_map\"")
    );
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
    assert!(
        example.contains("\"support_envelope_operator_cleanup_retention_retention_retention_map\"")
    );
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
    assert!(example.contains("\"local_status_diagnostic_json_lines\""));
    assert!(example.contains("\"peer_pipeline_prometheus\""));
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
