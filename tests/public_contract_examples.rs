// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_parity_report, matrixraft_read_safety_decision,
    matrixraft_temporalstore_extraction_plan, readiness::matrixraft_temporalstore_adapter_shape,
    ExtractionStatus, ProductionStatus, ReadIndexRequest, ReadinessSnapshot, StateRole,
    StatusSnapshot,
};

#[test]
fn production_readiness_snapshot_reports_ready_when_all_evidence_is_present() {
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

    let report = matrixraft_parity_report(&readiness);
    assert!(report.ready);
    assert!(report.missing.is_empty());
    assert_eq!(report.production_status, ProductionStatus::ProductionReady);
}

#[test]
fn read_safety_example_rejects_reads_ahead_of_applied_index() {
    let status = StatusSnapshot {
        group_id: 7,
        node_id: 1,
        role: StateRole::Leader,
        term: 9,
        leader_id: Some(1),
        commit_index: 42,
        applied_index: 42,
        last_log_index: 42,
        last_snapshot_index: 30,
        peers: Vec::new(),
    };

    let decision = matrixraft_read_safety_decision(
        &status,
        &ReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 43,
            allow_lease_read: true,
        },
    );

    assert!(!decision.safe);
    assert_eq!(decision.reason, "apply_lag");
}

#[test]
fn extraction_plan_keeps_reusable_raft_logic_out_of_temporalstore() {
    let plan = matrixraft_temporalstore_extraction_plan();
    assert!(plan.policy.contains("RustRaft owns reusable consensus"));
    assert!(plan
        .slices
        .iter()
        .any(|slice| slice.id == "read_safety" && slice.status == ExtractionStatus::InLibrary));
    assert!(plan
        .slices
        .iter()
        .any(|slice| slice.id == "replication_pipeline_runtime"
            && slice.status == ExtractionStatus::PendingMigration));
    assert!(plan
        .slices
        .iter()
        .any(|slice| slice.id == "domain_fsm_adapters"
            && slice.status == ExtractionStatus::AdapterOnly
            && slice.temporalstore_boundary.contains("TemporalStore owns")));
}

#[test]
fn temporalstore_adapter_shape_keeps_consensus_inside_rustraft_runtime() {
    let shape = matrixraft_temporalstore_adapter_shape();
    assert_eq!(shape.backend_type, "TemporalRaftConsensusBackend");
    assert_eq!(shape.node_field, "node");
    assert_eq!(
        shape.node_runtime_type,
        "matrixraft::node::NodeRuntime<TemporalStoreStateMachine, TemporalTransport>"
    );
    assert_eq!(shape.codec_field, "codec: TemporalCommandCodec");
    assert_eq!(shape.engine_field, "engine: TemporalEngine");
    assert!(shape
        .matrixraft_owned
        .iter()
        .any(|item| item.contains("consensus node runtime")));
    for temporalstore_owned in [
        "command encoding",
        "apply semantics",
        "storage engine",
        "process/admin integration",
    ] {
        assert!(shape
            .temporalstore_owned
            .contains(&temporalstore_owned.to_string()));
    }
    assert!(shape
        .example
        .contains("struct TemporalRaftConsensusBackend"));
    assert!(shape.example.contains("NodeRuntime"));
}
