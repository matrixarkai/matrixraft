// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A read path report is not safe once the live quorum is gone.
//!
//! `read_path_report` is the live path for a host's bounded-stale read -- the
//! node runtime answers `bounded_stale_read_index` with it -- and its verdict
//! is a conjunction:
//!
//! ```text
//! let safe = read_index.safe
//!     && quorum.reached
//!     && applied_index_fence.passed
//!     && bounded_stale.map(|r| r.allowed).unwrap_or(true);
//! ```
//!
//! Those clauses are mutually redundant, which is why planting them one at a
//! time proves nothing here: `read_index.safe` comes from `read_index()`, which
//! runs its own quorum and fence checks, so dropping any single clause leaves
//! the others producing the right verdict. **Planted in pairs**, the picture
//! separates:
//!
//! | planted away together | caught by |
//! |---|---|
//! | `read_index.safe` + `applied_index_fence.passed` | 2 tests, so the fence is held |
//! | `read_index.safe` + `quorum.reached` | **nothing** |
//!
//! So nothing held the one that matters most: a report answering `safe: true`
//! with no live quorum tells a host it may serve a read that a partitioned
//! leader has no right to serve.
//!
//! The existing `read_path_report_tracks_quorum_lease_fence_and_bounded_stale`
//! does assert `quorum.reached`, but only where it is **true**. It never puts
//! the report in a cluster that has lost its quorum, so it pins the sub-report
//! and not the verdict's dependence on it.

use matrixraft::{Peer, RaftCluster, ReadIndexRequest, ReadPathReport, ReplicaRole};

const GROUP: u64 = 88;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 40_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 41_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// A started three-voter group with one entry committed, and its leader.
fn cluster_with_an_entry() -> (RaftCluster, u64) {
    let mut cluster = RaftCluster::new(GROUP, Default::default(), vec![peer(1), peer(2), peer(3)])
        .expect("build the cluster");
    cluster.start().expect("start the cluster");
    cluster.propose(b"set a=1".to_vec()).expect("propose");
    let leader = cluster.leader_id().expect("a leader after start");
    (cluster, leader)
}

fn report(cluster: &mut RaftCluster, leader: u64) -> ReadPathReport {
    cluster
        .read_path_report(
            ReadIndexRequest {
                group_id: GROUP,
                requester_id: leader,
                min_commit_index: 1,
                allow_lease_read: false,
            },
            0,
        )
        .expect("the read path report is produced")
}

#[test]
fn a_report_is_not_safe_once_the_live_quorum_is_gone() {
    let (mut cluster, leader) = cluster_with_an_entry();

    cluster.set_node_healthy(2, false).expect("follower 2 down");
    cluster.set_node_healthy(3, false).expect("follower 3 down");

    let report = report(&mut cluster, leader);
    assert!(
        !report.quorum.reached,
        "the premise: two of three voters are down, so the quorum of {} is not met",
        report.quorum.required
    );
    assert!(
        !report.safe,
        "a read must not be reported safe without a live quorum"
    );
    assert_eq!(
        report.reason, "no_live_quorum",
        "and the report says which of its clauses refused"
    );
}

#[test]
fn a_report_is_safe_while_the_quorum_holds() {
    // The positive control. Without it, a verdict wired to `false` would pass
    // the test above, and it also establishes that the only thing changed
    // between the two cases is the quorum.
    let (mut cluster, leader) = cluster_with_an_entry();

    let report = report(&mut cluster, leader);
    assert!(report.quorum.reached, "all three voters are healthy");
    assert!(report.safe, "so the read is safe: {}", report.reason);
    assert_eq!(report.reason, "read_index_quorum");
}
