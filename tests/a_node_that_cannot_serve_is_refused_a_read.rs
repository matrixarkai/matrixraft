// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A read is refused when the node is not alive or its role cannot serve data.
//!
//! `matrixraft_read_safety_runtime_decision` is a public decision helper: a host
//! hands it a node's situation and it answers whether a read or write may be
//! served. Its first refusal is the most basic one:
//!
//! ```text
//! if !input.node_alive || !input.role_can_serve_data {
//!     return ..reason: "node_unavailable"..;
//! }
//! ```
//!
//! **Both clauses were held by nothing** across 1,027 tests, and the reason is
//! structural rather than an oversight. The only caller inside the crate is the
//! evidence-artifact builder, which fixes seven scenarios -- stale leader lease,
//! lagging follower read, stale follower write, bounded-stale accept and reject,
//! minority partition read and write -- and **none of them is this arm**. Every
//! input it builds passes `node_alive: true, role_can_serve_data: true`, so the
//! arm had no scenario to exercise it.
//!
//! Which is the same shape as a validator tested only on input it should accept:
//! the refusing half is the half that does the work, and it had nothing.
//!
//! A witness is the role this matters for -- it participates in the quorum but
//! holds no data, so `role_can_serve_data` is what stops a read being answered
//! from a replica that has none of it.

use matrixraft::{
    matrixraft_read_safety_runtime_decision, ReadSafetyOperation, ReadSafetyRuntimeInput,
};

/// The crate's own `bounded_stale_read_accept` scenario, which is allowed, so
/// each test below changes exactly one field away from a known-good input.
fn serving_node() -> ReadSafetyRuntimeInput {
    ReadSafetyRuntimeInput {
        operation: ReadSafetyOperation::BoundedStaleRead,
        node_id: 2,
        leader_id: 1,
        node_alive: true,
        role_can_serve_data: true,
        leader_lease_valid: true,
        has_majority: true,
        node_commit_index: 9,
        leader_commit_index: 10,
        max_stale_index_lag: 1,
    }
}

#[test]
fn a_node_that_is_not_alive_is_refused() {
    let decision = matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
        node_alive: false,
        ..serving_node()
    });

    assert!(!decision.allowed, "a node that is not alive cannot serve");
    assert_eq!(decision.reason, "node_unavailable");
}

#[test]
fn a_role_that_cannot_serve_data_is_refused() {
    // The witness case: in the quorum, holds no data.
    let decision = matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
        role_can_serve_data: false,
        ..serving_node()
    });

    assert!(
        !decision.allowed,
        "a replica holding no data cannot answer a read from it"
    );
    assert_eq!(decision.reason, "node_unavailable");
}

#[test]
fn the_same_node_serving_and_able_is_allowed() {
    // The positive control, and it is what makes the two tests above
    // attributable: the input differs from each of them by exactly one field,
    // so that field is what the refusal is about. Without it, a decision wired
    // to refuse everything would satisfy both.
    let decision = matrixraft_read_safety_runtime_decision(serving_node());

    assert!(decision.allowed, "refused instead: {}", decision.reason);
    assert_ne!(decision.reason, "node_unavailable");
}
