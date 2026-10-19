// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A voter that grants a vote restarts its own election timer.
//!
//! Raft's rule, and the one thing a voter owes a candidate beyond the vote
//! itself: having given its vote away, it must not then time out and campaign
//! against the candidate it voted for. The vote is already spent, so the
//! competing election cannot win it back -- it only splits the round and
//! delays whoever was going to win.
//!
//! The timer lives in the node runtime, not in `RaftCluster`: the runtime's
//! loop is what accumulates `election_elapsed_ms` and what fires an election
//! when it passes `election_timeout_ms`. So the reset belongs on the step that
//! granted the vote, which is where these tests look for it.
//!
//! **A granted pre-vote does not reset it.** A pre-vote promises nothing and
//! moves no state -- the voter has not given anything away and has no reason
//! to hold its own election back. Note that a pre-vote arrives as
//! `Message::Vote` with `pre_vote: true` and comes back as
//! `StepResult::Vote`, exactly like a binding vote: the step result alone
//! cannot tell them apart, which is why the request is what decides.

use matrixraft::{
    Config, LogId, Message, NodeOptions, NodeRuntime, Peer, ReplicaRole, StepResult, VoteRequest,
};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 77;
/// The heartbeat interval the runtime below runs at. `election_elapsed_ms`
/// grows by exactly this much per heartbeat tick, so it is also the most the
/// timer can have regained between a reset and the status read that checks it.
const HEARTBEAT_INTERVAL_MS: u64 = 50;
/// What the timer must reach before a test does anything, in multiples of the
/// heartbeat interval. The gap between this and `HEARTBEAT_INTERVAL_MS` is what
/// separates "was reset" from "was not": a reset leaves at most one interval on
/// the clock, no reset leaves at least this.
const ACCUMULATED_MS: u64 = 4 * HEARTBEAT_INTERVAL_MS;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 12_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 13_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn temp_runtime_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-election-timer-{name}-{}-{nonce}",
        std::process::id()
    ))
}

/// Node 2 of a three-voter group, with an election timeout far enough out that
/// no election fires during a test and `enable_lease_read` off so a leader
/// lease cannot be what refuses the vote.
fn voter_options(name: &str) -> NodeOptions {
    let base_dir = temp_runtime_dir(name);
    let config = Config {
        heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
        election_timeout_ms: 10_000,
        leader_lease_ms: 20,
        enable_lease_read: false,
        ..Default::default()
    };
    NodeOptions {
        group_id: GROUP_ID,
        node_id: 2,
        raft_addr: "127.0.0.1:12002".to_string(),
        snapshot_addr: "127.0.0.1:13002".to_string(),
        wal_dir: base_dir.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base_dir.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config,
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

fn election_elapsed_ms(runtime: &NodeRuntime) -> u64 {
    let status = runtime.status().expect("runtime status");
    let row = status
        .peer_runtime
        .first()
        .expect("the runtime reports a peer row");
    row.election_elapsed_ms
}

/// Starts node 2, stops the remembered leader looking live so the election
/// timer is free to accumulate, and waits until it has.
///
/// The wait is what makes these tests about the reset rather than about
/// timing: the timer only grows, so polling until it passes a threshold either
/// succeeds or the test fails having asserted nothing.
fn runtime_with_an_accumulated_timer(name: &str) -> NodeRuntime {
    let mut runtime = NodeRuntime::create(voter_options(name)).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime
        .set_node_healthy(1, false)
        .expect("the remembered leader stops looking live");

    let deadline = Instant::now() + Duration::from_secs(10);
    while election_elapsed_ms(&runtime) < ACCUMULATED_MS {
        assert!(
            Instant::now() < deadline,
            "the election timer never reached {ACCUMULATED_MS}ms; it was at {}ms",
            election_elapsed_ms(&runtime)
        );
        std::thread::sleep(Duration::from_millis(HEARTBEAT_INTERVAL_MS));
    }
    runtime
}

fn vote_request(pre_vote: bool) -> VoteRequest {
    VoteRequest {
        group_id: GROUP_ID,
        term: 2,
        candidate_id: 3,
        last_log_id: Some(LogId { term: 1, index: 1 }),
        pre_vote,
        force: false,
    }
}

#[test]
fn granting_a_vote_restarts_the_election_timer() {
    let runtime = runtime_with_an_accumulated_timer("granted");
    let before = election_elapsed_ms(&runtime);

    let granted = runtime
        .step(Message::Vote {
            target: 2,
            request: vote_request(false),
        })
        .expect("step the vote request");
    assert!(
        matches!(&granted, StepResult::Vote(response) if response.vote_granted),
        "the vote has to be granted for this test to be about anything: {granted:?}"
    );

    let after = election_elapsed_ms(&runtime);
    assert!(
        after <= HEARTBEAT_INTERVAL_MS,
        "granting a vote has to restart the election timer: it was {before}ms and is {after}ms, \
         which is more than the one heartbeat interval a restart could have regained"
    );
    assert!(
        after < before,
        "the timer went backwards or it did not restart: {before}ms -> {after}ms"
    );
}

#[test]
fn granting_a_vote_in_a_batch_restarts_the_election_timer() {
    let runtime = runtime_with_an_accumulated_timer("batched");
    let before = election_elapsed_ms(&runtime);

    let results = runtime
        .step_batch(vec![
            Message::PreVote { candidate_id: 3 },
            Message::Vote {
                target: 2,
                request: vote_request(false),
            },
        ])
        .expect("step the batch");
    assert_eq!(results.len(), 2);
    assert!(
        matches!(&results[1], StepResult::Vote(response) if response.vote_granted),
        "the batched vote has to be granted: {:?}",
        results[1]
    );

    let after = election_elapsed_ms(&runtime);
    assert!(
        after <= HEARTBEAT_INTERVAL_MS,
        "a vote granted inside a batch restarts the timer too: {before}ms -> {after}ms"
    );
}

#[test]
fn a_batch_that_grants_a_pre_vote_but_refuses_the_vote_leaves_the_timer_alone() {
    let runtime = runtime_with_an_accumulated_timer("mixed-batch");
    let before = election_elapsed_ms(&runtime);

    // The pre-vote is granted; the binding vote is at a term the voter has
    // already reached while it still remembers a leader, so it is refused.
    // This is the batch that tells a paired check from the cheap one: asking
    // only whether the batch held a binding vote *and* granted some vote
    // resets the timer here, on a vote that was never given.
    let results = runtime
        .step_batch(vec![
            Message::Vote {
                target: 2,
                request: vote_request(true),
            },
            Message::Vote {
                target: 2,
                request: VoteRequest {
                    term: 1,
                    ..vote_request(false)
                },
            },
        ])
        .expect("step the mixed batch");
    assert_eq!(results.len(), 2);
    assert!(
        matches!(&results[0], StepResult::Vote(response) if response.vote_granted),
        "the pre-vote has to be granted for this test to be about anything: {:?}",
        results[0]
    );
    assert!(
        matches!(&results[1], StepResult::Vote(response) if !response.vote_granted),
        "and the binding vote has to be refused: {:?}",
        results[1]
    );

    let after = election_elapsed_ms(&runtime);
    assert!(
        after >= before,
        "no vote was given, so nothing should hold this voter's election back: \
         {before}ms -> {after}ms"
    );
}

#[test]
fn granting_a_pre_vote_leaves_the_election_timer_alone() {
    let runtime = runtime_with_an_accumulated_timer("pre-vote");
    let before = election_elapsed_ms(&runtime);

    // A pre-vote carried by `Message::Vote`, which is how one arrives over the
    // wire, and which comes back as `StepResult::Vote` just like a binding
    // vote. If the reset keyed off the result variant this would reset too.
    let granted = runtime
        .step(Message::Vote {
            target: 2,
            request: vote_request(true),
        })
        .expect("step the pre-vote request");
    assert!(
        matches!(&granted, StepResult::Vote(response) if response.vote_granted),
        "the pre-vote has to be granted for this test to be about anything: {granted:?}"
    );

    let after = election_elapsed_ms(&runtime);
    assert!(
        after >= before,
        "a pre-vote promises nothing, so it must not hold the voter's own election back: \
         {before}ms -> {after}ms"
    );
}
