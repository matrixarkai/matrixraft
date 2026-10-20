// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A granted vote is on disk before the grant is returned.
//!
//! Raft requires the voter's term and vote to be durable *before* it answers,
//! because a candidate counts the grant immediately. Only the proposal path
//! wrote a WAL record -- three `append_built` calls in the whole runtime, all
//! three behind a proposal -- so a voter that granted and then restarted came
//! back with whatever vote the last proposal happened to carry, which for a
//! group that had not yet proposed anything was **no record at all**. Measured
//! on the unfixed runtime: `records on disk: 0`, so the vote was gone, and the
//! node granted the same term again to a different candidate.
//!
//! What these tests assert is the **durable record**, not the outcome of a
//! second vote request after a restart. That outcome is confounded here: this
//! cluster holds all three peers in one process, so a recovered node elects a
//! leader and the second request is refused as `known_leader` before the
//! `already_voted` rule is ever reached. The record is the property raft
//! actually requires, and it is the one thing a restart cannot confound.
//!
//! Both edges of the condition are pinned, because the fix is a condition and
//! not a call: a refused vote and a granted **pre**-vote must still write
//! nothing. A pre-vote promises nothing and moves no state, so persisting one
//! would be an fsync per election round for no guarantee.

use matrixraft::{
    Config, LogId, Message, NodeOptions, NodeRuntime, Peer, PersistentRaftWal,
    PersistentRaftWalOptions, ReplicaRole, VoteRequest, WalRecord,
};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 77;
const TERM: u64 = 7;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 12_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 13_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-vote-durability-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn options(base: &Path) -> NodeOptions {
    NodeOptions {
        group_id: GROUP_ID,
        node_id: 1,
        raft_addr: "127.0.0.1:12001".to_string(),
        snapshot_addr: "127.0.0.1:13001".to_string(),
        wal_dir: base.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config: Config::default(),
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

fn vote(candidate_id: u64, pre_vote: bool) -> VoteRequest {
    VoteRequest {
        group_id: GROUP_ID,
        term: TERM,
        candidate_id,
        last_log_id: Some(LogId { term: 1, index: 1 }),
        pre_vote,
        force: false,
    }
}

/// The records the group left on disk, read back through a fresh WAL.
fn records_on_disk(base: &Path) -> Vec<WalRecord> {
    PersistentRaftWal::open(PersistentRaftWalOptions {
        dir: base.join("wal"),
        max_records_per_segment: 1_000,
        max_segment_bytes: 1 << 20,
        min_keep_segments: 1,
        fsync_on_append: false,
    })
    .expect("re-open the WAL the group wrote")
    .records()
}

fn carries_the_vote(records: &[WalRecord], candidate_id: u64) -> bool {
    records.iter().any(|record| {
        record.hard_state.current_term == TERM && record.hard_state.voted_for == Some(candidate_id)
    })
}

fn recorded(records: &[WalRecord]) -> Vec<(u64, Option<u64>)> {
    records
        .iter()
        .map(|record| (record.hard_state.current_term, record.hard_state.voted_for))
        .collect()
}

#[test]
fn a_granted_vote_is_on_disk_once_the_grant_has_been_returned() {
    let base = dir("granted");
    let _ = std::fs::remove_dir_all(&base);
    let mut runtime = NodeRuntime::create(options(&base)).expect("create the group");
    runtime.start().expect("start it");

    let granted = runtime.vote_to(1, vote(2, false)).expect("a binding vote");
    assert!(granted.vote_granted, "reason: {}", granted.reason);
    runtime.shutdown().expect("shut it down");

    let records = records_on_disk(&base);
    assert!(
        carries_the_vote(&records, 2),
        "the grant was returned, so a record carrying term {TERM} voted_for 2 has to be on \
         disk; found {} record(s): {:?}",
        records.len(),
        recorded(&records)
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_granted_vote_through_the_batch_path_is_on_disk_too() {
    // The batch path persisted for a batch that held a proposal; a batch that
    // granted a binding vote and nothing else wrote nothing.
    let base = dir("batch");
    let _ = std::fs::remove_dir_all(&base);
    let mut runtime = NodeRuntime::create(options(&base)).expect("create the group");
    runtime.start().expect("start it");

    runtime
        .step_batch(vec![Message::Vote {
            target: 1,
            request: vote(2, false),
        }])
        .expect("a batch holding one binding vote");
    runtime.shutdown().expect("shut it down");

    let records = records_on_disk(&base);
    assert!(
        carries_the_vote(&records, 2),
        "a vote granted through the batch path is as binding as one granted on its own: {:?}",
        recorded(&records)
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_granted_pre_vote_writes_nothing() {
    // A pre-vote promises nothing and moves no state, so there is nothing to
    // make durable -- and persisting one would be an fsync per election round
    // for no guarantee. This is the edge a fix written against "a vote was
    // granted" rather than "a *binding* vote was granted" would cross.
    let base = dir("prevote");
    let _ = std::fs::remove_dir_all(&base);
    let mut runtime = NodeRuntime::create(options(&base)).expect("create the group");
    runtime.start().expect("start it");

    let pre_vote = runtime.vote_to(1, vote(2, true)).expect("a pre-vote");
    assert!(
        pre_vote.vote_granted,
        "the pre-vote has to be granted for this to say anything: {}",
        pre_vote.reason
    );
    runtime.shutdown().expect("shut it down");

    // Nothing written at all, not merely nothing naming candidate 2: a record
    // whose vote happens not to be 2 would satisfy the weaker reading while
    // the fsync it cost was still paid.
    let records = records_on_disk(&base);
    assert!(
        records.is_empty(),
        "a granted pre-vote binds nothing, so the group had nothing to persist: {:?}",
        recorded(&records)
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn a_refused_vote_writes_nothing() {
    // The other edge: the condition is "granted", not "a vote was stepped".
    // Candidate 9 is not a member of this group, so the request is refused.
    let base = dir("refused");
    let _ = std::fs::remove_dir_all(&base);
    let mut runtime = NodeRuntime::create(options(&base)).expect("create the group");
    runtime.start().expect("start it");

    let refused = runtime
        .vote_to(1, vote(9, false))
        .expect("the vote is answered");
    assert!(
        !refused.vote_granted,
        "candidate 9 is not a member of this group, so the vote cannot be granted"
    );
    runtime.shutdown().expect("shut it down");

    // Again the record count, not the recorded candidate. A refused vote never
    // sets `voted_for` to 9, so asking whether any record names 9 is true
    // whether or not the write happened -- that reading pinned nothing.
    let records = records_on_disk(&base);
    assert!(
        records.is_empty(),
        "a refused vote gave nothing away, so there is nothing to persist: {:?}",
        recorded(&records)
    );
    let _ = std::fs::remove_dir_all(&base);
}
