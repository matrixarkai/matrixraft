// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A node remembers the term it is in and the vote it has cast.
//!
//! A campaign votes for the candidate **itself**, which binds it exactly as a
//! vote granted to a peer does. The vote-durability fix covered grants, because
//! it keyed on a vote *message*; a campaign is not one. It arrives as an admin
//! command, as a `TimeoutNow`, or from the election tick, and none of those
//! looked like a vote.
//!
//! So this keys on the state instead of the message: the node's own term and
//! vote are compared across a step, and any change to them is written. That
//! covers a campaign, a term moved by a peer's message, and whatever else
//! changes them later.
//!
//! **Why it matters.** Suppose A self-votes in term T, B grants T, and A leads
//! with two of three. A restarts and comes back at a term below T with no
//! record of its own vote. C now campaigns in T: B refuses, because B's grant
//! was written, but A grants -- it has forgotten. C has its own vote and A's,
//! and leads term T as well. Two leaders in one term, from the one vote that
//! was never written down.
//!
//! **Starting is deliberately not persisted**, and the last test pins that.
//! The start election is deterministic -- it always votes for this node at the
//! recovered term plus one -- so a restart re-derives the same vote and refuses
//! any other candidate in that term. Nothing is forgotten, so nothing needs
//! writing, and writing it would cost one fsync per group start: a durable
//! append is its fsync and very little else, measured in this tree at 224 a
//! second against 85,014 with fsync off, against a documented mass start of
//! 1.83s for 16,384 groups.
//!
//! The terms that do need writing are the ones a restart cannot re-derive: a
//! campaign can walk the term several steps past the record, and a node back at
//! `recovered + 1` would grant terms it had already voted in.

use matrixraft::{
    AppendEntriesRequest, Config, LogEntry, LogId, Message, NodeOptions, NodeRuntime, Peer,
    ReplicaRole,
};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 262;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 53_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 53_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn base_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-hard-state-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn options(base: &Path) -> NodeOptions {
    NodeOptions {
        group_id: GROUP_ID,
        node_id: 1,
        raft_addr: "127.0.0.1:53001".to_string(),
        snapshot_addr: "127.0.0.1:53501".to_string(),
        wal_dir: base.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config: Config::default(),
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

fn heartbeat(term: u64) -> AppendEntriesRequest {
    AppendEntriesRequest {
        group_id: GROUP_ID,
        term,
        leader_id: 2,
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: 0,
        lease_epoch: 0,
    }
}

fn entry_append(term: u64) -> AppendEntriesRequest {
    AppendEntriesRequest {
        group_id: GROUP_ID,
        term,
        leader_id: 2,
        prev_log_id: None,
        entries: vec![LogEntry {
            log_id: LogId { term, index: 1 },
            payload: b"x".to_vec(),
            is_command: false,
        }],
        leader_commit: 1,
        lease_epoch: 0,
    }
}

/// `(surviving records, (term, voted_for))` as a restart recovers them.
fn recovered(base: &Path) -> (usize, Option<(u64, Option<u64>)>) {
    let mut restarted = NodeRuntime::create(options(base)).expect("re-create the group");
    restarted.start().expect("re-start it");
    let report = restarted.wal_recovery_report().expect("a recovery report");
    let surviving = report
        .as_ref()
        .map(|report| report.surviving_records)
        .unwrap_or(0);
    let hard = report
        .as_ref()
        .and_then(|report| report.recovered.as_ref())
        .map(|record| (record.hard_state.current_term, record.hard_state.voted_for));
    let _ = restarted.shutdown();
    (surviving, hard)
}

/// Makes the node a follower at `term` by way of an append it keeps, so a
/// campaign afterwards reaches its term bump instead of returning early for the
/// sitting leader.
fn demote_to_follower_at(runtime: &NodeRuntime, term: u64) {
    runtime
        .step(Message::AppendEntries {
            target: 1,
            request: entry_append(term),
        })
        .expect("the append is answered");
}

#[test]
fn a_campaign_past_the_recorded_term_is_written_down() {
    let base = base_dir("campaign");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create the group");
        runtime.start().expect("start it");
        demote_to_follower_at(&runtime, 9);
        runtime
            .campaign(true)
            .expect("a node that is not the leader can campaign");
        runtime.shutdown().expect("shut it down");
    }

    let (_, hard) = recovered(&base);
    assert_eq!(
        hard,
        Some((10, Some(1))),
        "the campaign moved the term to 10 and voted for this node, and a \
         restart has to come back knowing both -- otherwise it can grant term \
         10 to somebody else"
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_campaign_through_a_batch_is_written_down_too() {
    // `step_batch` has its own persistence condition, so a host that batches
    // its commands must not lose the term either.
    let base = base_dir("batch");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        demote_to_follower_at(&runtime, 9);
        let results = runtime
            .step_batch(vec![Message::Admin {
                command: matrixraft::AdminCommand::Campaign {
                    candidate_id: 1,
                    forced: true,
                },
            }])
            .expect("the batch is answered");
        assert!(!results.is_empty(), "the batch produced a result");
        runtime.shutdown().expect("shutdown");
    }

    let (_, hard) = recovered(&base);
    let term = hard.map(|(term, _)| term).unwrap_or_default();
    assert!(
        term >= 10,
        "a batched campaign has to leave the moved term on disk: {hard:?}"
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_term_moved_by_a_peer_is_written_down() {
    // Not only campaigns. A heartbeat carrying a higher term moves this node's
    // term, and that is durable state whether or not anything was appended.
    let base = base_dir("peerterm");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        let _ = runtime.step(Message::AppendEntries {
            target: 1,
            request: heartbeat(9),
        });
        runtime.shutdown().expect("shutdown");
    }

    let (surviving, hard) = recovered(&base);
    assert_eq!(surviving, 1, "the term move is one record");
    assert_eq!(
        hard.map(|(term, _)| term),
        Some(9),
        "and it is the term the peer carried: {hard:?}"
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_second_message_at_the_same_term_writes_nothing() {
    // The discriminator against persisting on every step. Only a CHANGE to the
    // term or the vote is worth a record, and a steady stream of heartbeats at
    // one term is the normal case: a record each would be an fsync per
    // interval per group.
    let base = base_dir("sameterm");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        for _ in 0..4 {
            let _ = runtime.step(Message::AppendEntries {
                target: 1,
                request: heartbeat(9),
            });
        }
        runtime.shutdown().expect("shutdown");
    }

    let (surviving, _) = recovered(&base);
    assert_eq!(
        surviving, 1,
        "four heartbeats at one term move the term once, so they are worth one \
         record between them"
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn starting_alone_writes_nothing() {
    // Pins the decision not to persist the start election, which is the
    // expensive half of this and buys nothing: the start election is
    // deterministic, so a restart re-derives the same self-vote and refuses any
    // other candidate in that term. If this ever starts writing, mass startup
    // pays an fsync per group for a vote that was never at risk.
    let base = base_dir("start");
    let _ = std::fs::remove_dir_all(&base);
    {
        let mut runtime = NodeRuntime::create(options(&base)).expect("create");
        runtime.start().expect("start");
        runtime.shutdown().expect("shutdown");
    }

    let (surviving, hard) = recovered(&base);
    assert_eq!(surviving, 0, "starting is not a promise: {hard:?}");
    let _ = std::fs::remove_dir_all(&base);
}
