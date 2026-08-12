// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{rustraft_parity_report, RustRaftProductionStatus, RustRaftReadinessSnapshot};

fn main() {
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

    let report = rustraft_parity_report(&readiness);
    assert_eq!(
        report.production_status,
        RustRaftProductionStatus::ProductionReady
    );
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
