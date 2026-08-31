// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{matrixraft_read_safety_decision, ReadIndexRequest, StateRole, StatusSnapshot};

fn main() {
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
            min_commit_index: 40,
            allow_lease_read: true,
        },
    );

    assert!(decision.safe);
    assert!(decision.lease_read);
    println!("{}", serde_json::to_string_pretty(&decision).unwrap());
}
