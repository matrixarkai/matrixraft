// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A new leader does not inherit the previous period's witness policy.
//!
//! Witnesses take part in a quorum but hold no data, so counting them in the
//! commit quorum trades durability for availability. The engine only does it
//! while a leader cannot reach a voters-only majority:
//! `refresh_witness_commit_quorum_policy` decides from the live voter and
//! witness counts.
//!
//! That refresh ran from **one** place, the leader-lease tick. Taking office
//! did not refresh it, so a new leader inherited whatever the policy had been.
//! Measured before the fix, five voters and two witnesses:
//!
//! ```text
//! fresh:     count_witness=false
//! degraded:  count_witness=true
//! new leader took office, voters back: count_witness=true   <-- stale
//! after one tick: count_witness=false
//! ```
//!
//! The window is one lease tick and it lands in the **common** case: a new
//! leader is usually elected because the old one lost quorum, which puts the
//! new leader on the healthy side. For that tick an entry could be committed
//! on a quorum completed by replicas holding none of it.
//!
//! **Recomputed on taking office, not cleared.** Clearing is the simpler
//! change and would stall the degraded case for a tick, since a leader taking
//! office while it still cannot reach a voters-only majority needs witnesses
//! counted straight away. Both arms are here, and a clearing fix fails the
//! second.
//!
//! **Why five voters and not three, and why `take_office` asserts.** `campaign`
//! returns early for a node that is already the leader, so campaigning the
//! sitting leader tests nothing about taking office. A first version of this
//! file did exactly that in three of five tests: they passed on the value the
//! tick had already set, and a plant that cleared the policy instead of
//! recomputing it passed all five. Five voters leave a healthy non-leader to
//! promote while three are down, and `take_office` asserts the leader actually
//! moved so the early return cannot hide again.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

const GROUP_ID: u64 = 221;

fn node(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 48_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 48_500 + node_id),
        role,
        auto_promote: false,
    }
}

/// Five voters and two witnesses: the voters-only quorum is 3, and the
/// witness-inclusive quorum is 4.
fn started() -> RaftCluster {
    let mut peers: Vec<Peer> = (1..=5).map(|id| node(id, ReplicaRole::Voter)).collect();
    peers.push(node(6, ReplicaRole::Witness));
    peers.push(node(7, ReplicaRole::Witness));
    let mut cluster =
        RaftCluster::new(GROUP_ID, Config::default(), peers).expect("build the cluster");
    cluster.start().expect("start the cluster");
    assert_eq!(
        cluster.leader_id(),
        Some(1),
        "the tests below promote node 2, which depends on node 1 holding office first"
    );
    cluster
}

/// Takes three of the five voters away, so a voters-only majority is out of
/// reach and witnesses have to be counted. Node 2 stays up to be promoted.
fn degrade(cluster: &mut RaftCluster) {
    for down in [3u64, 4, 5] {
        cluster
            .set_node_healthy(down, false)
            .expect("a voter goes down");
    }
    cluster.tick_leader_lease(1);
    assert!(
        cluster.count_witness_in_commit_quorum(),
        "the premise: two live voters of five cannot form a voters-only quorum"
    );
}

fn heal(cluster: &mut RaftCluster) {
    for up in [3u64, 4, 5] {
        cluster.set_node_healthy(up, true).expect("a voter returns");
    }
}

/// Promotes `node_id` and asserts office actually changed hands, because
/// `campaign` returns early for a node that already holds it.
fn take_office(cluster: &mut RaftCluster, node_id: u64) {
    let before = cluster.leader_id();
    assert_ne!(
        before,
        Some(node_id),
        "campaign returns early for the sitting leader, so this would test nothing"
    );
    cluster
        .campaign(node_id, true)
        .expect("the new leader takes office");
    assert_eq!(
        cluster.leader_id(),
        Some(node_id),
        "office has to have changed hands for this test to mean anything"
    );
}

#[test]
fn a_fresh_group_does_not_count_witnesses() {
    let cluster = started();
    assert!(
        !cluster.count_witness_in_commit_quorum(),
        "counting witnesses is the exception, not the resting state"
    );
}

#[test]
fn a_new_leader_does_not_inherit_a_degraded_policy() {
    let mut cluster = started();
    degrade(&mut cluster);
    // The voters come back and a new leader takes office, with no lease tick
    // in between -- the window the policy used to be stale in.
    heal(&mut cluster);
    take_office(&mut cluster, 2);

    assert!(
        !cluster.count_witness_in_commit_quorum(),
        "a leader that can reach a voters-only majority must not commit on a \
         quorum witnesses can complete, and must not wait a lease tick to stop"
    );
}

#[test]
fn a_new_leader_that_is_still_degraded_counts_witnesses_at_once() {
    // The arm that separates recomputing from clearing: a fix that cleared the
    // policy on taking office passes the test above and fails this one.
    let mut cluster = started();
    degrade(&mut cluster);
    // No healing: node 2 is the other live voter, and it takes office.
    take_office(&mut cluster, 2);

    assert!(
        cluster.count_witness_in_commit_quorum(),
        "a leader taking office while it still cannot reach a voters-only \
         majority needs witnesses counted immediately, not one tick later"
    );
}

#[test]
fn the_tick_still_governs_the_policy_after_office_changes() {
    // Taking office must recompute the policy, not pin it.
    let mut cluster = started();
    degrade(&mut cluster);
    take_office(&mut cluster, 2);
    assert!(cluster.count_witness_in_commit_quorum());

    heal(&mut cluster);
    cluster.tick_leader_lease(1);
    assert!(
        !cluster.count_witness_in_commit_quorum(),
        "once the voters are back the policy returns to ignoring witnesses"
    );
}

#[test]
fn ignoring_witnesses_outright_still_wins() {
    // `ignore_witness` is the operator's switch and outranks the dynamic
    // policy, so taking office must not resurrect witness counting under it.
    let mut cluster = started();
    degrade(&mut cluster);
    cluster.set_ignore_witness(true);
    assert!(
        !cluster.count_witness_in_commit_quorum(),
        "the switch clears the policy immediately"
    );

    take_office(&mut cluster, 2);
    assert!(
        !cluster.count_witness_in_commit_quorum(),
        "and taking office must not undo it, however degraded the group is"
    );
}
