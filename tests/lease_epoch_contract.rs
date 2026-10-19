// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A confirmation from a previous lease epoch does not count.
//!
//! The leader-lease renewal used to `clear()` the confirmations before
//! refilling them, which meant a stale confirmation could not exist and the
//! epoch check in the quorum was belt-and-braces. The clear is gone -- it freed
//! the map's only node and allocated a fresh one on every proposal -- so two
//! lines that were redundant are now **load-bearing**:
//!
//! 1. the renewal bumping `leader_lease_epoch`, and
//! 2. the quorum counting a confirmation only when its epoch matches.
//!
//! Remove either and a leader holds a lease earned in a previous round by peers
//! that have since gone quiet. Both plants **survived** the suite before these
//! tests existed, which is the whole reason they do: removing a redundancy
//! moves the weight onto whatever made it redundant, and that was undefended.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 51_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 52_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// Five voters, started, with the lease earned from a live quorum.
///
/// One heartbeat round is what earns it: it collects a confirmation from every
/// healthy follower and evaluates the quorum once at the end. The renewal is
/// then asserted on top, because it is the call these tests drive and a lease
/// that a heartbeat holds but a renewal does not would make every later
/// assertion meaningless.
fn leased() -> (RaftCluster, u64) {
    let members = (1..=5u64).map(peer).collect::<Vec<_>>();
    let mut cluster = RaftCluster::new(7, Config::default(), members).expect("cluster");
    cluster.start().expect("start");
    let leader = cluster.leader_id().expect("leader");
    cluster.broadcast_heartbeat().expect("heartbeat");
    assert!(
        cluster.lease_read_eligible(leader, 0).unwrap_or(false),
        "the setup must start from a held lease, or these tests prove nothing"
    );
    assert!(
        cluster.renew_leader_lease_from_live_quorum(),
        "and a renewal against the same live quorum has to hold it too"
    );
    (cluster, leader)
}

fn held(cluster: &RaftCluster, leader: u64) -> bool {
    cluster.lease_read_eligible(leader, 0).unwrap_or(false)
}

#[test]
fn a_confirmation_from_the_previous_epoch_does_not_hold_the_lease() {
    let (mut cluster, leader) = leased();
    assert!(held(&cluster, leader));

    // Three of the four followers go quiet. The next renewal collects
    // acknowledgements from the leader and one follower -- two of five, short
    // of the quorum of three -- and the three confirmations still in the map
    // are from the epoch before it.
    for node_id in [3, 4, 5] {
        cluster
            .set_node_healthy(node_id, false)
            .expect("mark a peer down");
    }

    assert!(
        !cluster.renew_leader_lease_from_live_quorum(),
        "two of five is not a quorum, so the renewal must not report a lease"
    );
    assert!(
        !held(&cluster, leader),
        "and a stale confirmation must not hold the lease for a peer that has gone quiet"
    );
}

#[test]
fn the_lease_is_earned_again_when_the_peers_come_back() {
    // The other half: the epoch check is a filter, not a one-way door. A test
    // that only checked the drop would be satisfied by code that broke the
    // lease permanently.
    let (mut cluster, leader) = leased();
    for node_id in [3, 4, 5] {
        cluster
            .set_node_healthy(node_id, false)
            .expect("mark a peer down");
    }
    assert!(!cluster.renew_leader_lease_from_live_quorum());
    assert!(!held(&cluster, leader));

    for node_id in [3, 4, 5] {
        cluster
            .set_node_healthy(node_id, true)
            .expect("bring a peer back");
    }
    assert!(
        cluster.renew_leader_lease_from_live_quorum(),
        "one round against the live quorum earns it back"
    );
    assert!(held(&cluster, leader));
}

#[test]
fn renewing_twice_keeps_the_lease() {
    // What the removed `clear()` used to guarantee by emptying the map: an
    // acknowledgement in the new epoch has to **overwrite** the entry left from
    // the old one. If the stale entry were kept instead of replaced, its epoch
    // would never match again and a live quorum could never hold the lease.
    let (mut cluster, leader) = leased();
    for round in 0..5 {
        assert!(
            cluster.renew_leader_lease_from_live_quorum(),
            "round {round} of a live quorum has to hold the lease"
        );
        assert!(
            held(&cluster, leader),
            "and a read stays eligible through round {round}"
        );
    }
}
