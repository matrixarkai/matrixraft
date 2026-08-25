// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_baseline_raft_parity_matrix, matrixraft_baseline_raft_reference_policy,
    matrixraft_parity_report, RustRaftBaselineRaftParityStatus, RustRaftReadinessSnapshot,
};

fn ready_snapshot() -> RustRaftReadinessSnapshot {
    RustRaftReadinessSnapshot {
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
fn baseline_raft_parity_matrix_tracks_all_required_capabilities() {
    let matrix = matrixraft_baseline_raft_parity_matrix(&ready_snapshot());
    let ids = matrix
        .iter()
        .map(|item| item.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        ids,
        [
            "leader_election",
            "leader_transfer",
            "learner_promotion",
            "lease_read",
            "log_compaction",
            "log_replication",
            "membership_changes",
            "observability_status",
            "pre_vote",
            "read_index",
            "restart_recovery",
            "snapshot_trigger_install",
            "witness_quorum_behavior",
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
    );
    assert!(matrix.iter().all(|item| item.required));
    assert!(matrix.iter().all(|item| !item.evidence.is_empty()));
}

#[test]
fn baseline_raft_parity_report_tracks_gaps_and_intentional_differences() {
    let mut snapshot = ready_snapshot();
    snapshot.matrixraft_rpc_transport_contract_present = false;
    snapshot.learner_catchup_promotion_present = false;

    let report = matrixraft_parity_report(&snapshot);

    assert!(report
        .baseline_raft_gaps
        .contains(&"log_replication".to_string()));
    assert!(report.baseline_raft_gaps.contains(&"pre_vote".to_string()));
    assert!(report
        .baseline_raft_gaps
        .contains(&"learner_promotion".to_string()));
    assert!(report
        .baseline_raft_intentional_differences
        .contains(&"leader_transfer".to_string()));

    let leader_transfer = report
        .baseline_raft_parity_matrix
        .iter()
        .find(|item| item.id == "leader_transfer")
        .expect("leader transfer parity item");
    assert_eq!(
        leader_transfer.status,
        RustRaftBaselineRaftParityStatus::IntentionalDifference
    );
    assert!(leader_transfer.note.contains("consuming runtime"));
}

#[test]
fn ready_baseline_raft_matrix_has_only_declared_runtime_split_difference() {
    let report = matrixraft_parity_report(&ready_snapshot());

    assert!(report.baseline_raft_gaps.is_empty(), "{report:#?}");
    assert_eq!(
        report.baseline_raft_intentional_differences,
        vec!["leader_transfer".to_string()]
    );
    assert!(
        report
            .baseline_raft_parity_matrix
            .iter()
            .filter(|item| item.status == RustRaftBaselineRaftParityStatus::Satisfied)
            .count()
            >= 12
    );
}

#[test]
fn baseline_raft_is_feature_and_performance_reference_but_rust_api_can_be_idiomatic() {
    let policy = matrixraft_baseline_raft_reference_policy();
    assert!(policy.feature_reference.contains("BaselineRaft"));
    assert!(policy.performance_reference.contains("BaselineRaft"));
    assert!(policy.performance_reference.contains("p50/p99"));
    assert!(policy.rust_api_policy.contains("idiomatic Rust"));
    assert!(policy
        .temporalstore_consumption_boundary
        .contains("DataRaftConsensusBackend"));

    let report = matrixraft_parity_report(&ready_snapshot());
    assert_eq!(report.baseline_raft_reference_policy, policy);
}
