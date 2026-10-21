// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A log index at the ceiling is refused at the boundary, not overflowed inside.
//!
//! `validate_log_id` bounded an index from below -- it must be greater than
//! zero -- and not from above. Three places then add one to an index that came
//! off the wire, and `u64::MAX` made each of them fault:
//!
//! | site | expression |
//! |---|---|
//! | `transport.rs` | `prev_log_id.index + 1`, the expected first index |
//! | `transport.rs` | `entry.log_id.index + 1`, the running expectation |
//! | `pipeline.rs` | `next_index = entry.log_id.index + 1` |
//!
//! The first two are **inside the validator** -- the function whose job is to
//! reject malformed input faulted on it, before it could return a report.
//!
//! The third is reachable through a door left deliberately open: a peer's
//! append response claiming `match_index = u64::MAX` is *stored*, which
//! `tests/append_response_bounds.rs` asserts on purpose, so `next_index` sits
//! at the ceiling and an entry arriving at it had no representable successor.
//!
//! **The case that matters most is the one that looked valid.** A request with
//! `prev_log_id.index = u64::MAX - 1` and one entry at `u64::MAX` is perfectly
//! contiguous. Nothing rejected it, so it was accepted -- and the fault landed
//! later, in the follower's pipeline, on an index the protocol had already
//! promised to a leader.
//!
//! So the rule is at the boundary: an index must be below `u64::MAX`, because
//! the protocol needs the index after it to exist -- a follower's next index, a
//! leader's next entry at `last + 1`, and a snapshot's first retained index at
//! `snapshot_index + 1`. The pipeline keeps its own check as well, so a path
//! that never ran the validator cannot fault either.

use matrixraft::{
    matrixraft_validate_append_entries_request, AppendEntriesRequest, AppendEntriesResponse,
    LogEntry, LogId, PipelineLimits, ReplicationPipeline, SnapshotState,
};

fn request(prev: Option<LogId>, entries: Vec<LogEntry>) -> AppendEntriesRequest {
    AppendEntriesRequest {
        group_id: 1,
        term: 3,
        leader_id: 1,
        prev_log_id: prev,
        entries,
        leader_commit: 1,
        lease_epoch: 0,
    }
}

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: b"x".to_vec(),
        is_command: false,
    }
}

fn response(match_index: u64) -> AppendEntriesResponse {
    AppendEntriesResponse {
        term: 1,
        success: true,
        match_index,
        rejection_hint: None,
        rejected_index: None,
        require_snapshot: None,
        snapshot_state: SnapshotState::None,
        lease_confirmation_epoch: 0,
        lease_duration_ms: 0,
    }
}

fn mentions_the_ceiling(blockers: &[String], field: &str) -> bool {
    blockers
        .iter()
        .any(|blocker| blocker.starts_with(field) && blocker.contains("the index after it"))
}

#[test]
fn the_validator_refuses_a_previous_index_at_the_ceiling() {
    let report = matrixraft_validate_append_entries_request(&request(
        Some(LogId {
            term: 1,
            index: u64::MAX,
        }),
        Vec::new(),
    ));
    assert!(
        mentions_the_ceiling(&report.blockers, "prev_log_id"),
        "the validator has to answer with a report rather than faulting: {:?}",
        report.blockers
    );
}

#[test]
fn the_validator_refuses_an_entry_at_the_ceiling() {
    let report = matrixraft_validate_append_entries_request(&request(
        Some(LogId { term: 1, index: 7 }),
        vec![entry(u64::MAX)],
    ));
    assert!(
        mentions_the_ceiling(&report.blockers, "entries[].log_id"),
        "{:?}",
        report.blockers
    );
}

#[test]
fn a_contiguous_request_ending_at_the_ceiling_is_still_refused() {
    // The case that used to get through. `MAX - 1` then `MAX` is contiguous, so
    // no contiguity blocker fires and the request reads as well formed -- the
    // fault then landed in the follower's pipeline instead, on an index the
    // leader had already been told was accepted.
    let report = matrixraft_validate_append_entries_request(&request(
        Some(LogId {
            term: 1,
            index: u64::MAX - 1,
        }),
        vec![entry(u64::MAX)],
    ));
    assert!(
        mentions_the_ceiling(&report.blockers, "entries[].log_id"),
        "a contiguous run into the ceiling has to be refused on the index \
         itself, since contiguity has nothing to complain about: {:?}",
        report.blockers
    );
}

#[test]
fn an_ordinary_request_still_validates() {
    // Positive control. A validator that refused everything would satisfy all
    // three tests above.
    let report = matrixraft_validate_append_entries_request(&request(
        Some(LogId { term: 1, index: 7 }),
        vec![entry(8), entry(9)],
    ));
    assert!(
        report.blockers.is_empty(),
        "a well formed request has nothing to answer for: {:?}",
        report.blockers
    );
}

#[test]
fn the_validator_still_catches_a_hole_in_the_entries() {
    // The other positive control: contiguity is still enforced, and is still
    // reported as contiguity rather than swallowed by the new rule.
    let report = matrixraft_validate_append_entries_request(&request(
        Some(LogId { term: 1, index: 7 }),
        vec![entry(8), entry(10)],
    ));
    assert!(
        report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("must be contiguous")),
        "{:?}",
        report.blockers
    );
}

#[test]
fn the_pipeline_refuses_an_append_at_the_ceiling() {
    // Defence in depth: this does not depend on the validator having run.
    let mut pipeline = ReplicationPipeline::new(2, 1, PipelineLimits::default());
    pipeline
        .handle_append_response(&response(u64::MAX))
        .expect("the claim is handled");
    let next_index = pipeline.status().next_index;
    assert_eq!(
        next_index,
        u64::MAX,
        "the setup depends on the stored claim putting the next index at the \
         ceiling, which is what the append-response bounds pin deliberately"
    );

    let error = pipeline
        .receive_out_of_order(&entry(next_index))
        .expect_err("an entry with no representable successor has to be refused");
    assert!(
        error.to_string().contains("largest representable index"),
        "{error}"
    );
}

#[test]
fn the_pipeline_still_takes_an_ordinary_append() {
    // Positive control for the pipeline arm.
    let mut pipeline = ReplicationPipeline::new(2, 1, PipelineLimits::default());
    pipeline
        .receive_out_of_order(&entry(1))
        .expect("an in-order append at the next index is taken");
    assert_eq!(
        pipeline.status().next_index,
        2,
        "and it still moves the next index one past what arrived"
    );
}
