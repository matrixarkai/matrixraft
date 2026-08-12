// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    rustraft_read_safety_decision, RustRaftReadIndexRequest, RustRaftRole, RustRaftStatusSnapshot,
};

fn main() {
    let status = RustRaftStatusSnapshot {
        group_id: 7,
        node_id: 1,
        role: RustRaftRole::Leader,
        term: 9,
        leader_id: Some(1),
        commit_index: 42,
        applied_index: 42,
        last_log_index: 42,
        last_snapshot_index: 30,
        peers: Vec::new(),
    };

    let decision = rustraft_read_safety_decision(
        &status,
        &RustRaftReadIndexRequest {
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
