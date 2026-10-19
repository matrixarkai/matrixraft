// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A caller can tell an accepted membership change from a downgraded one.
//!
//! Raft allows only one configuration change to be pending -- in the log and
//! not yet applied -- at a time. This cluster **downgrades** a second one to an
//! ordinary entry rather than refusing it, which is a deliberate choice pinned
//! by `membership_proposal_downgrades_second_unapplied_change`.
//!
//! What that cost was: `propose_with_options` returns only a `LogId`, so a
//! caller got `Ok` either way. A host that proposed a change, saw success and
//! waited for the membership to move would wait for something that was never
//! going to happen. The comparable engine avoids the question by refusing the
//! proposal outright -- "config changes are only allowed to be proposed if the
//! leader's applied index is greater than this value" -- but reversing a
//! deliberate decision is not the same as making it legible.
//!
//! `propose_membership_change` reports which of the two happened, in the shape
//! `transfer_leader_outcome` already uses for the same reason.
//!
//! **Every test here holds the first apply in flight**, with
//! `mark_apply_task_inflight`. A `RaftCluster` models every replica in one
//! process and applies as it commits, so without that the first change is
//! already applied by the time the second is proposed and there is nothing to
//! downgrade -- the first version of these tests saw `Accepted` twice and was
//! measuring an empty scenario.

use matrixraft::{
    Config, MembershipProposalOutcome, Peer, ProposeOptions, RaftCluster, ReplicaRole,
};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 95_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 96_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started() -> (RaftCluster, u64) {
    let mut cluster =
        RaftCluster::new(7, Config::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    let leader = cluster.leader_id().expect("leader");
    (cluster, leader)
}

#[test]
fn the_first_membership_change_is_accepted_and_the_second_is_downgraded() {
    let (mut cluster, leader) = started();

    let first = cluster
        .propose_membership_change(b"add-peer-4".to_vec())
        .expect("first proposal");
    assert!(first.is_accepted(), "the first one is accepted: {first:?}");
    let first_index = first.index();
    assert_eq!(
        cluster.pending_membership_change_index(),
        Some(first_index),
        "and it is the pending one"
    );

    // Hold its apply in flight, so it is in the log and not yet applied --
    // which is the state the one-at-a-time rule is about.
    cluster
        .mark_apply_task_inflight(leader, first_index)
        .expect("hold the first apply in flight");

    let second = cluster
        .propose_membership_change(b"add-peer-5".to_vec())
        .expect("the second is appended, not refused");
    assert!(
        !second.is_accepted(),
        "the second one must not report acceptance: {second:?}"
    );
    match second {
        MembershipProposalOutcome::Downgraded { index, pending_at } => {
            assert_eq!(index, first_index + 1, "it still took the next index");
            assert_eq!(
                pending_at, first_index,
                "and it says which change is in its way"
            );
        }
        other => panic!("expected a downgrade, got {other:?}"),
    }

    // The membership did not move for it, and the pending change is still the
    // first one.
    assert_eq!(
        cluster.pending_membership_change_index(),
        Some(first_index),
        "a downgraded proposal must not become the pending change"
    );
}

#[test]
fn a_change_is_accepted_again_once_the_pending_one_has_been_applied() {
    // The downgrade is a wait, not a wall: it lasts exactly as long as the
    // leader has not applied past the pending change.
    let (mut cluster, leader) = started();
    let first = cluster
        .propose_membership_change(b"add-peer-4".to_vec())
        .expect("first proposal");
    let first_index = first.index();
    assert!(first.is_accepted());

    cluster
        .mark_apply_task_inflight(leader, first_index)
        .expect("hold the first apply in flight");
    assert!(
        !cluster
            .propose_membership_change(b"too-soon".to_vec())
            .expect("appended")
            .is_accepted(),
        "still pending, so still downgraded"
    );

    // Now finish the apply.
    cluster
        .submit_apply_result(leader, first_index, false)
        .expect("applied");

    let next = cluster
        .propose_membership_change(b"add-peer-5".to_vec())
        .expect("proposal after the apply");
    assert!(
        next.is_accepted(),
        "once the pending change is applied the next one is accepted: {next:?}"
    );
    assert_eq!(
        cluster.pending_membership_change_index(),
        Some(next.index())
    );
}

#[test]
fn the_old_call_still_downgrades_silently() {
    // Pinning what was NOT changed. `propose_with_options` keeps its deliberate
    // behaviour: the second proposal is appended as an ordinary entry and the
    // caller gets an indistinguishable `Ok`. That is the whole reason the
    // reporting call exists, and a change to it here would make this file's
    // premise false.
    let (mut cluster, leader) = started();
    let options = ProposeOptions {
        expected_term: None,
        is_command: false,
        is_membership_change: true,
    };
    let first = cluster
        .propose_with_options(b"add-peer-4".to_vec(), options.clone())
        .expect("first");
    cluster
        .mark_apply_task_inflight(leader, first.index)
        .expect("hold the first apply in flight");
    let second = cluster
        .propose_with_options(b"add-peer-5".to_vec(), options)
        .expect("second is Ok, which is the point");
    assert_eq!(second.index, first.index + 1);
    assert_eq!(
        cluster.pending_membership_change_index(),
        Some(first.index),
        "the second did not become the pending change, and Ok did not say so"
    );
}
