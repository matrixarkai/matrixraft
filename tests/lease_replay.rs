// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A lease confirmation that has already been counted cannot be counted again.
//!
//! `leader_lease_confirmation_epochs` is the per-node monotonic counter that
//! refuses a confirmation no newer than one already seen from that node. #183
//! established, with a test, that `expire_leader_lease` must not clear it: a
//! promise already spent must not be spendable twice.
//!
//! `reset_leader_lease_epoch` and `invalidate_leader_lease` did clear it, so
//! the same replay worked across a leadership change:
//!
//! ```text
//! confirmation epoch 10 accepted: true
//! the SAME confirmation again, no reset: false
//! the same confirmation after a leadership change: true
//! ```
//!
//! The counter's job does not depend on the epoch. "Do not accept a
//! confirmation older than one already seen from this node" is true for the
//! life of the group, so a new epoch does not make an old promise fresh.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn three_voters() -> RaftCluster {
    let mut cluster = RaftCluster::new(
        7,
        Config {
            election_timeout_ms: 1_000,
            heartbeat_interval_ms: 100,
            leader_lease_ms: 500,
            ..Config::default()
        },
        vec![peer(1), peer(2), peer(3)],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster.campaign(1, false).ok();
    cluster
}

#[test]
fn a_counted_confirmation_cannot_be_replayed_after_a_leadership_change() {
    let mut cluster = three_voters();
    assert!(
        cluster.receive_leader_lease_confirmation(2, 10),
        "the first confirmation is counted"
    );
    assert!(
        !cluster.receive_leader_lease_confirmation(2, 10),
        "and an immediate repeat is refused"
    );

    cluster
        .campaign(3, true)
        .expect("leadership moves to node 3, which resets the lease epoch");

    assert!(
        !cluster.receive_leader_lease_confirmation(2, 10),
        "the same confirmation must still be refused -- a new epoch does not \
         make an old promise fresh, and counting it again extends the lease on \
         a promise already spent"
    );
}

#[test]
fn a_counted_confirmation_cannot_be_replayed_after_the_lease_is_invalidated() {
    // The other route that used to clear the counters. Losing quorum
    // invalidates the lease, and the engine reaches `invalidate_leader_lease`
    // from a dozen places.
    let mut cluster = three_voters();
    assert!(cluster.receive_leader_lease_confirmation(2, 10));

    // Resigning invalidates the lease directly.
    // `step_down_leader_if_lost_quorum` returns early while the lease is still
    // valid -- which it is, having just been confirmed -- so it is the wrong
    // door for this.
    assert!(
        cluster
            .resign_leader("rolling restart")
            .expect("the leader resigns"),
        "the resignation has to happen for this to exercise the invalidation path"
    );

    assert!(
        !cluster.receive_leader_lease_confirmation(2, 10),
        "a confirmation already counted stays refused across an invalidation"
    );
}

#[test]
fn a_newer_confirmation_is_still_accepted_after_a_leadership_change() {
    // The counter must not become a one-way door: keeping it across an epoch
    // change has to leave a genuinely newer confirmation acceptable, or a
    // group that changed leaders could never hold a lease again.
    let mut cluster = three_voters();
    assert!(cluster.receive_leader_lease_confirmation(2, 10));

    cluster
        .campaign(3, true)
        .expect("leadership moves to node 3");

    assert!(
        cluster.receive_leader_lease_confirmation(2, 11),
        "a newer confirmation is a new promise and has to be accepted"
    );
}
