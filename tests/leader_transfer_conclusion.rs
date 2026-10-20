// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The record of a leader transfer survives the transfer ending.
//!
//! Every way a transfer could end used to throw its record away. The abort
//! paths wrote the reason into a `Box` they dropped on the next line: a probe
//! that wrote a sentinel there instead left all 826 tests passing, so nothing
//! read it. The completion paths emptied the slot and wrote nothing at all. A
//! host that asked for a transfer and then saw none in flight could not tell
//! "leadership moved" from "it timed out" from "the transferee was removed".
//!
//! The record now stays in the slot it already occupied, marked with how it
//! ended, which is why every "is a transfer in flight?" question has to ask the
//! conclusion rather than whether the slot is occupied. That distinction is
//! what these tests are about: each one names a caller that would get the wrong
//! answer from the slot alone.

use matrixraft::{
    Config, LeaderTransferConclusion, LeaderTransferOutcome, LeaderTransferState, Peer,
    RaftCluster, ReplicaRole,
};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn three_node_cluster() -> RaftCluster {
    RaftCluster::new(
        7,
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("valid cluster")
}

/// A cluster whose transfer to `transferee` is accepted and then sits in flight
/// rather than completing at once, because the transferee is held back and so
/// cannot be campaigned for yet.
fn cluster_with_a_transfer_in_flight(transferee: u64) -> RaftCluster {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .set_node_healthy(transferee, false)
        .expect("hold the transferee back");
    cluster
        .propose(b"before-transfer".to_vec())
        .expect("propose");
    cluster
        .begin_leader_transfer(transferee)
        .expect("begin transfer")
        .expect("a transfer is in flight");
    assert!(
        cluster.leader_transfer_state().is_some(),
        "the transfer has to be in flight for the test to be about anything"
    );
    cluster
}

/// Tick until the transfer ends, with a bound.
///
/// An unbounded loop here is a plant trap: anything that makes
/// `leader_transfer_state` keep answering `Some` turns the loop into an
/// infinite one, and a test binary that never exits reports nothing. The bound
/// turns that into a named failure. It returns the number of ticks it took, so
/// a caller can assert on it rather than assume it.
fn tick_until_the_transfer_ends(cluster: &mut RaftCluster) -> u64 {
    let timeout_ticks = cluster
        .leader_transfer_state()
        .expect("a transfer is in flight")
        .timeout_ticks;
    let mut ticks = 0;
    while cluster.leader_transfer_state().is_some() {
        cluster.tick_leader_transfer();
        ticks += 1;
        assert!(
            ticks <= timeout_ticks + 1,
            "the transfer did not end within its own timeout of {timeout_ticks} ticks"
        );
    }
    ticks
}

fn concluded(cluster: &RaftCluster) -> LeaderTransferState {
    cluster
        .last_leader_transfer()
        .expect("the last transfer is recorded")
}

#[test]
fn a_transfer_that_times_out_says_so_afterwards() {
    let mut cluster = cluster_with_a_transfer_in_flight(2);
    let timeout_ticks = cluster
        .leader_transfer_state()
        .expect("in flight")
        .timeout_ticks;
    assert!(timeout_ticks > 0, "a zero timeout would end it immediately");

    let mut ticks = 0;
    while cluster.leader_transfer_state().is_some() {
        cluster.tick_leader_transfer();
        ticks += 1;
        assert!(ticks <= timeout_ticks + 1, "the tick loop has to terminate");
    }

    let record = concluded(&cluster);
    assert_eq!(record.conclusion, Some(LeaderTransferConclusion::Aborted));
    assert_eq!(record.reason, "transfer_timeout");
    assert_eq!(record.transferee_id, 2);
    // The elapsed count is read off the record rather than assumed, so a
    // transfer that aged the wrong number of ticks before giving up is visible.
    assert_eq!(record.elapsed_ticks, timeout_ticks);
    assert_eq!(ticks, timeout_ticks);
}

#[test]
fn a_transfer_that_completes_says_so_afterwards() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster
        .propose(b"before-transfer".to_vec())
        .expect("propose");
    let outcome = cluster
        .transfer_leader_outcome(2)
        .expect("transfer to a caught-up voter");
    assert_eq!(outcome, LeaderTransferOutcome::Transferred);
    assert_eq!(cluster.leader_id(), Some(2));

    let record = concluded(&cluster);
    assert_eq!(record.conclusion, Some(LeaderTransferConclusion::Completed));
    assert!(
        record.conclusion.expect("concluded").is_completed(),
        "the completed conclusion has to answer is_completed"
    );
    assert_eq!(record.transferee_id, 2);
}

/// The two conclusions have to be distinguishable, which they are not if the
/// same state produces both. Driving one of each and comparing them is the
/// independent quantity: a change that collapses them fails here even when each
/// half still looks right on its own.
#[test]
fn a_timeout_and_a_completion_are_not_the_same_record() {
    let mut timed_out = cluster_with_a_transfer_in_flight(2);
    tick_until_the_transfer_ends(&mut timed_out);

    let mut completed = three_node_cluster();
    completed.start().expect("cluster starts");
    completed.propose(b"x".to_vec()).expect("propose");
    completed
        .transfer_leader(2)
        .expect("transfer to a caught-up voter");

    let a = concluded(&timed_out);
    let b = concluded(&completed);
    assert_eq!(a.transferee_id, b.transferee_id, "same transferee");
    assert_ne!(
        a.conclusion, b.conclusion,
        "a timeout and a completion must not record the same conclusion"
    );
    assert_ne!(a.reason, b.reason, "and must not record the same reason");
}

#[test]
fn a_concluded_record_is_not_a_transfer_in_flight() {
    let mut cluster = cluster_with_a_transfer_in_flight(2);
    tick_until_the_transfer_ends(&mut cluster);

    // The surfaces that answer "what is happening now" stay empty. They meant
    // in-flight before the record was retained and they still do, which is what
    // makes the retention safe to add behind them.
    assert!(cluster.leader_transfer_state().is_none());
    assert!(cluster
        .cluster_status_report()
        .expect("status report")
        .leader_transfer
        .is_none());

    // And the record itself is there, which is the new part.
    assert!(cluster.last_leader_transfer().is_some());
}

#[test]
fn aborting_a_concluded_transfer_does_nothing_and_counts_nothing() {
    let mut cluster = cluster_with_a_transfer_in_flight(2);
    assert!(cluster.abort_leader_transfer("first_abort"));
    let after_first = concluded(&cluster);

    // A second abort has nothing to abort. While the slot was emptied this fell
    // out for free; with the record in place it is the conclusion that has to
    // stop it.
    assert!(!cluster.abort_leader_transfer("second_abort"));
    let after_second = concluded(&cluster);
    assert_eq!(after_first, after_second, "the record did not move");
    assert_eq!(after_second.reason, "first_abort");

    // The abort count is read off a fresh transfer's record rather than from a
    // getter that might share the bug, so a second abort that was counted shows
    // up as a count of two here.
    let next = cluster
        .begin_leader_transfer(3)
        .expect("begin another transfer")
        .expect("in flight");
    assert_eq!(
        next.aborted_transfers, 1,
        "one transfer has been aborted, not two"
    );
}

#[test]
fn a_concluded_transfer_is_not_aged() {
    let mut cluster = cluster_with_a_transfer_in_flight(2);
    assert!(cluster.abort_leader_transfer("ended_for_the_test"));
    let elapsed_at_conclusion = concluded(&cluster).elapsed_ticks;

    for _ in 0..50 {
        assert!(
            !cluster.tick_leader_transfer(),
            "ticking a concluded transfer does nothing"
        );
    }
    assert_eq!(
        concluded(&cluster).elapsed_ticks,
        elapsed_at_conclusion,
        "a transfer that has ended does not keep accruing ticks"
    );
}

/// The admission check compares a request against the transfer currently aimed
/// somewhere. Asking the slot instead would read a *finished* transfer to node
/// 2 as a duplicate of a *new* request to node 2 and decline it, which is a
/// cluster that can transfer to a node once and never again.
#[test]
fn a_new_transfer_to_the_same_node_is_not_a_duplicate_of_a_finished_one() {
    let mut cluster = cluster_with_a_transfer_in_flight(2);
    tick_until_the_transfer_ends(&mut cluster);
    assert_eq!(concluded(&cluster).transferee_id, 2);

    let again = cluster
        .begin_leader_transfer(2)
        .expect("begin a second transfer to the same node")
        .expect("accepted rather than declined as a duplicate");
    assert_eq!(again.transferee_id, 2);
    assert_eq!(
        again.conclusion, None,
        "a transfer born in flight has no conclusion"
    );
    assert_eq!(again.elapsed_ticks, 0, "and starts its clock again");
    assert!(cluster.leader_transfer_state().is_some());
}

/// Each abort path has to be distinguishable from the others, or the record
/// says only "it did not complete" -- which is what the abort counter already
/// said.
#[test]
fn the_abort_paths_record_different_reasons() {
    let mut removed = cluster_with_a_transfer_in_flight(2);
    removed.remove_peer(2).expect("remove the transferee");
    assert_eq!(concluded(&removed).reason, "transferee_removed");

    let mut timed_out = cluster_with_a_transfer_in_flight(2);
    tick_until_the_transfer_ends(&mut timed_out);
    assert_eq!(concluded(&timed_out).reason, "transfer_timeout");

    let mut resigned = cluster_with_a_transfer_in_flight(2);
    assert!(resigned
        .resign_leader("rolling restart")
        .expect("the leader resigns"));
    assert_eq!(concluded(&resigned).reason, "resign_leader");

    // All three ended the same way as far as the conclusion is concerned, and
    // the reason is the only thing separating them. Asserting both halves stops
    // a change that drops the reason from passing on the conclusion alone.
    for cluster in [&removed, &timed_out, &resigned] {
        assert_eq!(
            concluded(cluster).conclusion,
            Some(LeaderTransferConclusion::Aborted)
        );
    }
}

#[test]
fn a_record_written_before_the_conclusion_existed_loads_as_in_flight() {
    // A document without the field describes a transfer that was in flight when
    // it was written, so that is what it has to load as.
    let without: LeaderTransferState = serde_json::from_str(
        r#"{"transferee_id":4,"elapsed_ticks":2,"timeout_ticks":9,"aborted_transfers":0,"duplicate_requests":0,"reason":"transfer_ready"}"#,
    )
    .expect("a document without the conclusion still loads");
    assert_eq!(without.conclusion, None);
    assert_eq!(without.transferee_id, 4);

    // And a concluded record keeps its conclusion across a round trip, or a
    // status report that crosses a process boundary loses the answer.
    let mut cluster = cluster_with_a_transfer_in_flight(2);
    cluster.abort_leader_transfer("round_trip");
    let record = concluded(&cluster);
    let round_tripped: LeaderTransferState =
        serde_json::from_str(&serde_json::to_string(&record).expect("serialize"))
            .expect("deserialize");
    assert_eq!(round_tripped, record);
    assert_eq!(
        round_tripped.conclusion,
        Some(LeaderTransferConclusion::Aborted)
    );
}
