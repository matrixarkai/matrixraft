// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `submit_apply_result` refuses an applied index below the confirmed frontier.
//!
//! The confirmed frontier, `safety_applied_index`, is what read safety is
//! measured against. `submit_apply_result` bounds an incoming index on both
//! sides:
//!
//! ```text
//! if applied_index < node.safety_applied_index || applied_index > node.applied_index {
//!     return Err(..);
//! }
//! ```
//!
//! Planted one clause at a time, the upper bound -- claiming more applied than
//! was applied -- is caught by
//! `matrixraft_multi_raft_server_controls_storage_apply_and_witness_by_group`.
//! **The lower bound was caught by nothing** across 1,025 tests.
//!
//! That one is load-bearing for an argument made elsewhere. The doc comment on
//! [`RaftCluster::peer_can_catch_up_from_log`] explains that `release_memory`'s
//! pressure tier floors at `safety_applied_index` rather than `applied_index`,
//! and that this is safe because the two "coincide in every reachable state --
//! `submit_apply_result` refuses a value below the safety index, so the
//! confirmed frontier cannot be held behind the eligible one". If the lower
//! bound went, that premise would go with it: the frontier could fall behind,
//! the two indexes would stop being the same number, and the pressure tier
//! could compact below the point read safety is measured from.
//!
//! MEASURED: with the frontier at 3, `submit_apply_result(leader, 2, false)`
//! answers `applied_index 2 outside safety/applied range 3..3` and the frontier
//! is unchanged.

use matrixraft::{Peer, RaftCluster, ReplicaRole};

const GROUP: u64 = 89;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 42_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 43_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// A started group with two entries proposed, its leader, and the frontier that
/// applying them left behind.
fn cluster_with_a_frontier() -> (RaftCluster, u64, u64) {
    let mut cluster = RaftCluster::new(GROUP, Default::default(), vec![peer(1), peer(2), peer(3)])
        .expect("build the cluster");
    cluster.start().expect("start the cluster");
    cluster.propose(b"a".to_vec()).expect("propose a");
    cluster.propose(b"b".to_vec()).expect("propose b");
    let leader = cluster.leader_id().expect("a leader after start");
    let frontier = cluster
        .safety_applied_index(leader)
        .expect("the confirmed frontier");
    assert!(
        frontier > 1,
        "the premise: there has to be a frontier to fall behind, got {frontier}"
    );
    (cluster, leader, frontier)
}

#[test]
fn an_index_below_the_confirmed_frontier_is_refused() {
    let (mut cluster, leader, frontier) = cluster_with_a_frontier();

    let error = cluster
        .submit_apply_result(leader, frontier - 1, false)
        .expect_err("the frontier must not be moved backwards");
    assert!(
        error.to_string().contains("outside safety/applied range"),
        "the error names the range it refused against: {error}"
    );
    assert_eq!(
        cluster
            .safety_applied_index(leader)
            .expect("frontier after the refusal"),
        frontier,
        "and the refusal leaves the frontier where it was"
    );
}

#[test]
fn an_index_at_the_confirmed_frontier_is_accepted() {
    // The positive control. The rule is about moving BACKWARDS, not about
    // refusing a repeat -- and without this, a guard that refused every submit
    // would pass the test above.
    let (mut cluster, leader, frontier) = cluster_with_a_frontier();

    cluster
        .submit_apply_result(leader, frontier, false)
        .expect("re-confirming the frontier is allowed");
    assert_eq!(
        cluster
            .safety_applied_index(leader)
            .expect("frontier after"),
        frontier
    );
}
