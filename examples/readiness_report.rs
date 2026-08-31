// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{matrixraft_parity_report, ProductionStatus, ReadinessSnapshot};

fn main() {
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
    assert_eq!(report.production_status, ProductionStatus::ProductionReady);
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
