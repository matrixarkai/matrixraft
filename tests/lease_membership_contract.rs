// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A membership change takes the leader lease with it.
//!
//! A leader lease is held by a **quorum of the current membership**. Change the
//! membership and the quorum it was computed against no longer exists: a
//! removed peer's confirmation would be counting toward a group it has left, an
//! added voter raises the bar the lease was measured by, and a promoted learner
//! changes which confirmations are eligible at all.
//!
//! `RaftCluster` handles this the strong way -- `reset_leader_lease_epoch`
//! clears every confirmation and invalidates the lease, so it has to be earned
//! again from the new membership. That is more conservative than adjusting the
//! member set in place, which is what the comparable engine does with its
//! lease's own `UpdateMember`.
//!
//! **None of it was tested.** Deleting the reset from `add_peer`, `remove_peer`
//! or `promote_peer` left all 755 tests passing, so the next person to decide
//! that clearing every confirmation looks wasteful would have had nothing stop
//! them. These are the tests that stop them.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 91_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 92_000 + node_id),
        role,
        auto_promote: false,
    }
}

/// A started cluster whose leader holds its lease.
fn leased(extra: Vec<Peer>) -> (RaftCluster, u64) {
    let mut members = vec![
        peer(1, ReplicaRole::Voter),
        peer(2, ReplicaRole::Voter),
        peer(3, ReplicaRole::Voter),
    ];
    members.extend(extra);
    let mut cluster = RaftCluster::new(7, Config::default(), members).expect("cluster");
    cluster.start().expect("start");
    let leader = cluster.leader_id().expect("leader");

    // One heartbeat round is what earns the lease: it collects a confirmation
    // from every healthy follower and evaluates the quorum once at the end.
    cluster.broadcast_heartbeat().expect("heartbeat");
    assert!(
        cluster.lease_read_eligible(leader, 0).unwrap_or(false),
        "the setup must start from a held lease, or these tests prove nothing"
    );
    (cluster, leader)
}

fn held(cluster: &RaftCluster, leader: u64) -> bool {
    cluster.lease_read_eligible(leader, 0).unwrap_or(false)
}

#[test]
fn adding_a_peer_drops_the_lease_until_it_is_earned_again() {
    // An added voter raises the quorum the lease has to reach. Keeping the old
    // confirmations would let a lease measured against three nodes stand for a
    // group of four.
    let (mut cluster, leader) = leased(Vec::new());
    cluster
        .add_peer(peer(4, ReplicaRole::Voter))
        .expect("add a voter");
    assert!(
        !held(&cluster, leader),
        "adding a voter must drop the lease until the new membership confirms it"
    );

    // And it can be earned again, so this is an invalidation rather than a
    // one-way door.
    cluster.broadcast_heartbeat().expect("heartbeat");
    assert!(
        held(&cluster, leader),
        "a round against the new membership should earn it back"
    );
}

#[test]
fn removing_a_peer_drops_the_lease_until_it_is_earned_again() {
    // A removed peer's confirmation would otherwise count toward a group it
    // has left.
    let (mut cluster, leader) = leased(Vec::new());
    let victim = (1..=3u64).find(|id| *id != leader).expect("a follower");
    cluster.remove_peer(victim).expect("remove a voter");
    assert!(
        !held(&cluster, leader),
        "removing a peer must drop the lease: its confirmation belonged to the \
         membership that included it"
    );

    cluster.broadcast_heartbeat().expect("heartbeat");
    assert!(
        held(&cluster, leader),
        "a round against the smaller membership should earn it back"
    );
}

#[test]
fn promoting_a_learner_drops_the_lease_until_it_is_earned_again() {
    // A promotion changes which confirmations are eligible: the lease quorum
    // requires a confirmation's recorded role to match the node's current one,
    // so a learner's confirmation cannot stand in for a voter's.
    let (mut cluster, leader) = leased(vec![peer(4, ReplicaRole::Learner)]);
    cluster.promote_peer(4).expect("promote the learner");
    assert!(
        !held(&cluster, leader),
        "promoting a learner to voter must drop the lease"
    );

    cluster.broadcast_heartbeat().expect("heartbeat");
    assert!(
        held(&cluster, leader),
        "a round against the promoted membership should earn it back"
    );
}

#[test]
fn the_lease_quorum_is_computed_from_the_current_members() {
    // The property underneath all three: the quorum is counted over the nodes
    // the cluster holds **now**, not over a member list recorded when the
    // lease was taken. So a confirmation from a node that is gone cannot be
    // reached at all, whatever is left in the confirmation map.
    //
    // Asserted through behaviour rather than internals: remove a follower from
    // a three-node group and the remaining two still make a quorum, so one
    // round re-establishes the lease. If the quorum were measured against the
    // old three-node membership with a stale confirmation counted, the
    // invalidation above would be unnecessary -- and if it were measured
    // against the old size *without* it, this round could not reach it.
    let (mut cluster, leader) = leased(Vec::new());
    let victim = (1..=3u64).find(|id| *id != leader).expect("a follower");
    cluster.remove_peer(victim).expect("remove a voter");
    assert!(!held(&cluster, leader));

    cluster.broadcast_heartbeat().expect("heartbeat");
    assert!(
        held(&cluster, leader),
        "two of two is a quorum of the current membership"
    );
    assert_eq!(
        cluster.membership().voters.len(),
        2,
        "and the membership really is the smaller one"
    );
}
