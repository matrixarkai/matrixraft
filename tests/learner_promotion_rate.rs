// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A learner is promoted on its measured rate, not on one instant of zero lag.
//!
//! `matrixraft_learner_promotion_decision` already answers the lag question: is
//! the learner within `max_lag` of the commit index. That is a threshold on
//! **entries**, and a threshold on entries is a guess about time -- 1,000
//! behind on a fast link is ten milliseconds, and 1,000 behind on a slow one is
//! ten seconds. The two get the same answer.
//!
//! `matrixraft_learner_promotion_estimate` asks the question the threshold is
//! standing in for: at the rate just measured, how long would the rest take,
//! and is that under one election timeout? A voter that cannot catch up within
//! one is a voter that raises the quorum bar without being able to meet it.
//! The comparable engine makes exactly this decision at the end of each
//! catch-up turn.

use matrixraft::{matrixraft_learner_promotion_estimate, Config, Peer, RaftCluster, ReplicaRole};

const ELECTION_TIMEOUT_MS: u64 = 1_000;

fn estimate(
    learner_match_index: u64,
    leader_last_index: u64,
    turn_entries: u64,
    turn_elapsed_ms: u64,
) -> matrixraft::LearnerPromotionEstimate {
    matrixraft_learner_promotion_estimate(
        4,
        true,
        learner_match_index,
        leader_last_index,
        turn_entries,
        turn_elapsed_ms,
        ELECTION_TIMEOUT_MS,
    )
}

#[test]
fn a_caught_up_learner_is_promoted_whatever_the_rate_was() {
    let report = estimate(100, 100, 1, 10_000);
    assert_eq!(report.remaining_entries, 0);
    assert_eq!(report.estimated_catch_up_ms, 0);
    assert!(report.promote_now, "{report:?}");
    assert_eq!(report.reason, "learner_caught_up");
}

#[test]
fn a_learner_whose_rate_cannot_close_the_lag_in_time_is_not_promoted() {
    // 100 entries took 500ms, so 5ms an entry. 500 left is 2,500ms, which is
    // two and a half election timeouts. (The first version of this test said
    // five seconds and the arithmetic caught it.)
    let report = estimate(100, 600, 100, 500);
    assert_eq!(report.remaining_entries, 500);
    assert_eq!(report.estimated_catch_up_ms, 2_500);
    assert!(!report.within_election_timeout, "{report:?}");
    assert!(!report.promote_now, "{report:?}");
    assert_eq!(report.reason, "estimated_catch_up_exceeds_election_timeout");
}

#[test]
fn the_same_lag_on_a_faster_link_is_promoted() {
    // The same 500 entries behind, measured at 1ms per entry rather than 10.
    // A lag threshold cannot tell these two apart; this is the whole point.
    let slow = estimate(100, 600, 100, 500);
    let fast = estimate(100, 600, 100, 50);
    assert_eq!(slow.remaining_entries, fast.remaining_entries);
    assert!(!slow.promote_now);
    assert_eq!(fast.estimated_catch_up_ms, 250);
    assert!(fast.promote_now, "{fast:?}");
    assert_eq!(fast.reason, "estimated_catch_up_within_election_timeout");
}

#[test]
fn the_boundary_is_inclusive() {
    // Exactly one election timeout is within it: the bound is "cannot catch up
    // within one", not "takes less than one".
    let exact = estimate(100, 200, 100, ELECTION_TIMEOUT_MS);
    assert_eq!(exact.estimated_catch_up_ms, ELECTION_TIMEOUT_MS);
    assert!(exact.promote_now, "{exact:?}");

    let over = estimate(100, 200, 100, ELECTION_TIMEOUT_MS + 1);
    assert_eq!(over.estimated_catch_up_ms, ELECTION_TIMEOUT_MS + 1);
    assert!(!over.promote_now, "{over:?}");
}

#[test]
fn a_learner_that_has_matched_nothing_is_not_promoted() {
    let report = estimate(0, 1_000, 100, 1);
    assert!(!report.promote_now, "{report:?}");
    assert_eq!(report.reason, "learner_has_no_matched_log");
}

#[test]
fn a_turn_that_moved_nothing_measures_nothing() {
    // No rate to extrapolate from, and no dividing by zero either.
    let report = estimate(100, 600, 0, 500);
    assert_eq!(report.remaining_entries, 500);
    assert_eq!(report.estimated_catch_up_ms, 0);
    assert!(!report.promote_now, "{report:?}");
    assert_eq!(report.reason, "no_turn_to_estimate_from");
}

#[test]
fn a_node_that_is_not_a_learner_is_not_promoted() {
    let report =
        matrixraft_learner_promotion_estimate(4, false, 100, 600, 100, 50, ELECTION_TIMEOUT_MS);
    assert!(!report.promote_now, "{report:?}");
    assert_eq!(report.reason, "node_is_not_learner");
}

#[test]
fn the_multiplication_comes_before_the_division() {
    // 100 entries in 1ms with 100,000 still to go. The rate first is
    // 1/100 = 0 in integer arithmetic, which would estimate **zero** and
    // promote a learner that needs a second. Multiplying first gives 1,000ms,
    // which is the election timeout exactly -- so this case sits on the
    // boundary and is promoted, while the truncated form would promote it for
    // the wrong reason and promote the ten-times-worse case too.
    let report = estimate(1, 100_001, 100, 1);
    assert_eq!(report.remaining_entries, 100_000);
    assert_eq!(
        report.estimated_catch_up_ms, 1_000,
        "the truncating order would have said 0"
    );
    assert!(report.promote_now, "{report:?}");

    // Ten times the backlog, where the truncated form still says zero.
    let worse = estimate(1, 1_000_001, 100, 1);
    assert_eq!(worse.estimated_catch_up_ms, 10_000);
    assert!(!worse.promote_now, "{worse:?}");
}

#[test]
fn an_enormous_turn_does_not_wrap() {
    // u64 milliseconds times u64 entries overflows u64; the product is taken
    // in u128 so a pathological measurement saturates instead of wrapping to
    // something small and promoting on it.
    let report = estimate(1, u64::MAX, u64::MAX, u64::MAX);
    assert!(
        report.estimated_catch_up_ms > ELECTION_TIMEOUT_MS,
        "{report:?}"
    );
    assert!(!report.promote_now, "{report:?}");
}

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 71_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 72_000 + node_id),
        role,
        auto_promote: false,
    }
}

#[test]
fn the_cluster_fills_in_the_last_index_and_the_election_timeout() {
    let config = Config {
        election_timeout_ms: ELECTION_TIMEOUT_MS,
        ..Default::default()
    };
    let members = vec![
        peer(1, ReplicaRole::Voter),
        peer(2, ReplicaRole::Voter),
        peer(3, ReplicaRole::Voter),
    ];
    let mut cluster = RaftCluster::new(81, config, members).expect("cluster");
    cluster.start().expect("start");
    cluster
        .add_learner(peer(4, ReplicaRole::Learner))
        .expect("add a learner");
    for _ in 0..4 {
        cluster.propose(b"write".to_vec()).expect("propose");
    }

    // A real lag, made deterministically: an unhealthy node is handed no
    // entries, so the leader's log runs ahead of it.
    cluster
        .set_node_healthy(4, false)
        .expect("the learner stops receiving");
    for _ in 0..3 {
        cluster.propose(b"while-behind".to_vec()).expect("propose");
    }
    let catchup = cluster.catchup_report(4).expect("catchup report");
    assert!(
        catchup.lag > 0,
        "the learner has to be behind for this to be about anything: {catchup:?}"
    );

    let report = cluster
        .learner_promotion_estimate(4, 1, 10_000)
        .expect("estimate");
    assert_eq!(
        report.election_timeout_ms, ELECTION_TIMEOUT_MS,
        "the timeout comes from the cluster's own config"
    );
    assert_eq!(report.learner_id, 4);

    // **Asserted against the lag, not against itself.** `promote_now ==
    // (remaining == 0)` is satisfied by any bug that zeroes the remaining
    // count -- a plant that read the leader's own index as the learner's match
    // index survived exactly that way.
    assert!(
        report.remaining_entries >= catchup.lag,
        "the estimate measures against the leader's last index, which is at least \
         the commit index the catch-up report used: {report:?} against {catchup:?}"
    );
    assert!(
        report.remaining_entries > 0,
        "the learner is behind, so there is something left to send: {report:?}"
    );
    assert!(
        !report.promote_now,
        "one entry per ten seconds does not close that lag inside an election \
         timeout: {report:?}"
    );

    assert!(
        cluster.learner_promotion_estimate(99, 1, 1).is_err(),
        "a node that is not in the group is an error, not a false"
    );
}
