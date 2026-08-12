// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    rustraft_parity_report, rustraft_reference_raft_parity_matrix,
    rustraft_reference_raft_reference_policy, RustRaftReadinessSnapshot,
    RustRaftReferenceRaftParityStatus,
};

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
fn reference_raft_parity_matrix_tracks_all_required_capabilities() {
    let matrix = rustraft_reference_raft_parity_matrix(&ready_snapshot());
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
fn reference_raft_parity_report_tracks_gaps_and_intentional_differences() {
    let mut snapshot = ready_snapshot();
    snapshot.rustraft_rpc_transport_contract_present = false;
    snapshot.learner_catchup_promotion_present = false;

    let report = rustraft_parity_report(&snapshot);

    assert!(report
        .reference_raft_gaps
        .contains(&"log_replication".to_string()));
    assert!(report.reference_raft_gaps.contains(&"pre_vote".to_string()));
    assert!(report
        .reference_raft_gaps
        .contains(&"learner_promotion".to_string()));
    assert!(report
        .reference_raft_intentional_differences
        .contains(&"leader_transfer".to_string()));

    let leader_transfer = report
        .reference_raft_parity_matrix
        .iter()
        .find(|item| item.id == "leader_transfer")
        .expect("leader transfer parity item");
    assert_eq!(
        leader_transfer.status,
        RustRaftReferenceRaftParityStatus::IntentionalDifference
    );
    assert!(leader_transfer.note.contains("consuming runtime"));
}

#[test]
fn ready_reference_raft_matrix_has_only_declared_runtime_split_difference() {
    let report = rustraft_parity_report(&ready_snapshot());

    assert!(report.reference_raft_gaps.is_empty(), "{report:#?}");
    assert_eq!(
        report.reference_raft_intentional_differences,
        vec!["leader_transfer".to_string()]
    );
    assert!(
        report
            .reference_raft_parity_matrix
            .iter()
            .filter(|item| item.status == RustRaftReferenceRaftParityStatus::Satisfied)
            .count()
            >= 12
    );
}

#[test]
fn reference_raft_is_feature_and_performance_reference_but_rust_api_can_be_idiomatic() {
    let policy = rustraft_reference_raft_reference_policy();
    assert!(policy.feature_reference.contains("ReferenceRaft"));
    assert!(policy.performance_reference.contains("ReferenceRaft"));
    assert!(policy.performance_reference.contains("p50/p99"));
    assert!(policy.rust_api_policy.contains("idiomatic Rust"));
    assert!(policy
        .temporalstore_consumption_boundary
        .contains("DataRaftConsensusBackend"));

    let report = rustraft_parity_report(&ready_snapshot());
    assert_eq!(report.reference_raft_reference_policy, policy);
}
