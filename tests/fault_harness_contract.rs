// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::fault::{
    rustraft_fault_harness_readiness_report, rustraft_reference_raft_fault_scenarios,
    RustRaftFaultScenario, RustRaftFaultScenarioEvidence,
};

fn passing_evidence(scenario: RustRaftFaultScenario) -> RustRaftFaultScenarioEvidence {
    let observed_acceptance = rustraft_reference_raft_fault_scenarios()
        .into_iter()
        .find(|requirement| requirement.scenario == scenario)
        .expect("scenario requirement")
        .acceptance;
    RustRaftFaultScenarioEvidence {
        scenario,
        process_path_observed: true,
        spawned_process_count: 3,
        observed_process_ids: vec![10_001, 10_002, 10_003],
        scenario_runtime_ms: 1_500,
        client_operation_count: 256,
        injected_fault_count: 3,
        independent_wal_dirs_observed: true,
        independent_snapshot_dirs_observed: true,
        safety_observed: true,
        recovery_observed: true,
        metrics_observed: true,
        observed_acceptance,
        report_path: Some(format!("reports/{}.json", scenario.id())),
    }
}

#[test]
fn reference_raft_fault_contract_names_required_process_scenarios() {
    let scenarios = rustraft_reference_raft_fault_scenarios()
        .into_iter()
        .map(|item| item.scenario.id())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        scenarios,
        [
            "packet_loss_majority",
            "partition_heal",
            "slow_wal_fsync",
            "snapshot_during_membership_change",
            "leader_transfer_under_load",
            "follower_rejoin_compacted_logs",
            "rolling_restart_joint_consensus",
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
    );
}

#[test]
fn fault_harness_readiness_fails_closed_on_missing_process_evidence() {
    let report = rustraft_fault_harness_readiness_report(&[]);

    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"packet_loss_majority:evidence_missing".to_string()));
    assert!(report
        .missing
        .contains(&"partition_heal:evidence_missing".to_string()));
    assert!(report
        .missing
        .contains(&"rolling_restart_joint_consensus:evidence_missing".to_string()));
}

#[test]
fn fault_harness_readiness_requires_independent_stores_safety_recovery_and_metrics() {
    let report = rustraft_fault_harness_readiness_report(&[RustRaftFaultScenarioEvidence {
        scenario: RustRaftFaultScenario::PacketLossMajority,
        process_path_observed: true,
        spawned_process_count: 3,
        observed_process_ids: vec![10_001, 10_002, 10_003],
        scenario_runtime_ms: 1_500,
        client_operation_count: 256,
        injected_fault_count: 3,
        independent_wal_dirs_observed: false,
        independent_snapshot_dirs_observed: false,
        safety_observed: false,
        recovery_observed: false,
        metrics_observed: false,
        observed_acceptance: Vec::new(),
        report_path: Some("reports/packet-loss.json".to_string()),
    }]);

    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"packet_loss_majority:independent_wal_dirs_observed".to_string()));
    assert!(report
        .missing
        .contains(&"packet_loss_majority:safety_observed".to_string()));
    assert!(report
        .missing
        .contains(&"packet_loss_majority:metrics_observed".to_string()));
}

#[test]
fn fault_harness_readiness_requires_nontrivial_workload_and_fault_injection() {
    let mut evidence = passing_evidence(RustRaftFaultScenario::PacketLossMajority);
    evidence.scenario_runtime_ms = 0;
    evidence.client_operation_count = 0;
    evidence.injected_fault_count = 0;

    let report = rustraft_fault_harness_readiness_report(&[evidence]);

    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"packet_loss_majority:scenario_runtime_ms_at_least_1000".to_string()));
    assert!(report
        .missing
        .contains(&"packet_loss_majority:client_operation_count_at_least_1".to_string()));
    assert!(report
        .missing
        .contains(&"packet_loss_majority:injected_fault_count_at_least_1".to_string()));
}

#[test]
fn fault_harness_readiness_requires_distinct_real_process_evidence() {
    let mut evidence = passing_evidence(RustRaftFaultScenario::SlowWalFsync);
    evidence.spawned_process_count = 1;
    evidence.observed_process_ids = vec![10_001, 10_001, 10_001];
    evidence.report_path = None;

    let report = rustraft_fault_harness_readiness_report(&[evidence]);

    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"slow_wal_fsync:spawned_process_count_at_least_3".to_string()));
    assert!(report
        .missing
        .contains(&"slow_wal_fsync:distinct_process_ids_at_least_3".to_string()));
    assert!(report
        .missing
        .contains(&"slow_wal_fsync:process_fault_report_path".to_string()));
}

#[test]
fn fault_harness_readiness_requires_exact_reference_raft_acceptance_markers() {
    let mut evidence = passing_evidence(RustRaftFaultScenario::PartitionHeal);
    evidence
        .observed_acceptance
        .retain(|item| item != "read_eligible_after_heal_catchup");

    let report = rustraft_fault_harness_readiness_report(&[evidence]);

    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"partition_heal:acceptance:read_eligible_after_heal_catchup".to_string()));
}

#[test]
fn fault_harness_readiness_accepts_complete_reference_raft_style_evidence() {
    let evidence = rustraft_reference_raft_fault_scenarios()
        .into_iter()
        .map(|requirement| passing_evidence(requirement.scenario))
        .collect::<Vec<_>>();
    let report = rustraft_fault_harness_readiness_report(&evidence);

    assert!(report.ready, "{report:#?}");
    assert!(report.missing.is_empty());
    assert!(report.results.iter().all(|result| result.ready));
}

#[test]
fn leader_transfer_under_load_requires_exact_once_report_path() {
    let mut evidence = passing_evidence(RustRaftFaultScenario::LeaderTransferUnderLoad);
    evidence.report_path = None;

    let report = rustraft_fault_harness_readiness_report(&[evidence]);

    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"leader_transfer_under_load:exact_once_report_path".to_string()));
}
