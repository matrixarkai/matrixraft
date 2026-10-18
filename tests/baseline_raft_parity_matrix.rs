// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_baseline_raft_capability_coverage, matrixraft_baseline_raft_parity_matrix,
    matrixraft_baseline_raft_parity_surface, matrixraft_baseline_raft_reference_policy,
    matrixraft_parity_report, BaselineRaftParityStatus, ReadinessSnapshot,
};

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
fn baseline_raft_parity_matrix_tracks_all_required_capabilities() {
    let matrix = matrixraft_baseline_raft_parity_matrix(&ready_snapshot());
    let ids = matrix
        .iter()
        .map(|item| item.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        ids,
        [
            "campaign",
            "leader_election",
            "leader_transfer",
            "learner_promotion",
            "lease_read",
            "log_compaction",
            "log_replication",
            "membership_changes",
            "node_lifecycle",
            "observability_status",
            "pre_vote",
            "propose_expected_term",
            "read_index",
            "restart_recovery",
            "snapshot_trigger_install",
            "transport_contract_validation",
            "wal_hard_state",
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
        BaselineRaftParityStatus::IntentionalDifference
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
            .filter(|item| item.status == BaselineRaftParityStatus::Satisfied)
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

#[test]
fn baseline_raft_capability_coverage_is_total() {
    // The comparison surface names the capabilities the baseline engine exposes,
    // and the matrix is what tracks them. Nothing connected the two: the surface
    // named about forty capabilities, the matrix carried thirteen items, and a
    // capability could be missing from the matrix entirely without any test
    // noticing. That is the one failure a tracking matrix cannot be allowed --
    // it reports completeness it has not checked.
    //
    // So the mapping is asserted to be total in both directions.
    let surface = matrixraft_baseline_raft_parity_surface();
    let coverage = matrixraft_baseline_raft_capability_coverage();
    let matrix = matrixraft_baseline_raft_parity_matrix(&ready_snapshot());

    let families: Vec<(&str, &Vec<String>)> = vec![
        ("node_lifecycle", &surface.node_lifecycle),
        ("transport_api", &surface.transport_api),
        ("write_api", &surface.write_api),
        ("read_api", &surface.read_api),
        ("membership_api", &surface.membership_api),
        ("durability_api", &surface.durability_api),
        ("observability_api", &surface.observability_api),
    ];

    // Not vacuous: a surface that had gone empty, or a mapping that had, would
    // satisfy every containment check below.
    let surface_count: usize = families.iter().map(|(_, names)| names.len()).sum();
    assert!(
        surface_count >= 30,
        "the surface names only {surface_count} capabilities, which is too few for this \
         assertion to be testing anything"
    );
    assert_eq!(
        coverage.len(),
        surface_count,
        "the mapping has {} entries against {surface_count} named capabilities",
        coverage.len()
    );

    // Every named capability is tracked, exactly once.
    for (family, names) in &families {
        for name in names.iter() {
            let matches = coverage
                .iter()
                .filter(|entry| entry.family == *family && entry.capability == *name)
                .count();
            assert_eq!(
                matches, 1,
                "{family}/{name} is named by the surface and has {matches} mapping entries"
            );
        }
    }

    // And every item the mapping names exists in the matrix, so the mapping
    // cannot point at a tracker that is not there.
    for entry in &coverage {
        assert!(
            matrix.iter().any(|item| item.id == entry.matrix_item),
            "{}/{} is mapped to matrix item {:?}, which the matrix does not contain",
            entry.family,
            entry.capability,
            entry.matrix_item
        );
        assert!(
            families
                .iter()
                .any(|(family, names)| *family == entry.family
                    && names.iter().any(|name| name == &entry.capability)),
            "the mapping names {}/{}, which the surface does not",
            entry.family,
            entry.capability
        );
    }

    // Every matrix item earns its place: an item nothing maps to is tracking a
    // capability the surface does not claim.
    for item in &matrix {
        assert!(
            coverage.iter().any(|entry| entry.matrix_item == item.id),
            "matrix item {:?} answers for no capability the surface names",
            item.id
        );
    }

    // On a ready snapshot the five items added with this mapping are satisfied,
    // so the mapping did not become total by admitting untracked shortfalls.
    for id in [
        "node_lifecycle",
        "transport_contract_validation",
        "propose_expected_term",
        "campaign",
        "wal_hard_state",
    ] {
        let item = matrix
            .iter()
            .find(|item| item.id == id)
            .unwrap_or_else(|| panic!("the matrix should contain {id}"));
        assert_eq!(
            item.status,
            BaselineRaftParityStatus::Satisfied,
            "{id} should be satisfied on a ready snapshot"
        );
        assert!(
            !item.evidence.is_empty(),
            "{id} should name the evidence it rests on"
        );
    }
}
