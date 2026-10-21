// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A public validator must not fault on the input it exists to judge.
//!
//! `matrixraft_validate_snapshot_tail_catchup` computed
//! `snapshot.last_log_id.index + 1 + offset` as the first thing in its loop,
//! before the overlap check that would have turned the entry away. Snapshot
//! metadata can come from a peer, so `last_log_id.index = u64::MAX` was
//! "attempt to add with overflow" at `snapshot.rs:138` -- the screening
//! function faulting on exactly what it screens, in any build with overflow
//! checks.
//!
//! The computation now runs after the overlap check and saturates. Both halves
//! matter and are tested: an ordinary contiguous tail still passes, and a real
//! hole is still refused as a hole rather than being swallowed by a saturated
//! expectation.

use matrixraft::{
    snapshot::matrixraft_validate_snapshot_tail_catchup, LogEntry, LogId, SnapshotMetadata,
};

fn meta(index: u64) -> SnapshotMetadata {
    SnapshotMetadata {
        snapshot_id: "snapshot".to_string(),
        last_log_id: LogId { term: 1, index },
        membership: vec![1, 2, 3],
        members: Vec::new(),
        payload_bytes: 0,
    }
}

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: b"entry".to_vec(),
        is_command: true,
    }
}

#[test]
fn metadata_at_the_top_of_the_range_is_refused_rather_than_fatal() {
    // The test returning at all is the assertion: this used to abort.
    let refused = matrixraft_validate_snapshot_tail_catchup(&meta(u64::MAX), &[entry(7)])
        .expect_err("a snapshot ending at the top of the range admits no tail at all");
    assert!(
        refused.to_string().contains("overlaps installed snapshot"),
        "and the overlap is what is wrong with it: {refused}"
    );
}

#[test]
fn an_ordinary_contiguous_tail_still_passes() {
    // The positive control: a validator that refused everything would satisfy
    // the test above.
    matrixraft_validate_snapshot_tail_catchup(&meta(10), &[entry(11), entry(12), entry(13)])
        .expect("a tail that continues the snapshot is valid");
}

#[test]
fn a_hole_in_the_tail_is_still_refused_as_a_hole() {
    // The expectation is what detects a hole, so saturating it must not blunt
    // that: 11 then 13 skips 12.
    let refused = matrixraft_validate_snapshot_tail_catchup(&meta(10), &[entry(11), entry(13)])
        .expect_err("a tail with a hole is not contiguous");
    assert!(
        refused.to_string().contains("not contiguous"),
        "and it is reported as a hole rather than an overlap: {refused}"
    );
}

#[test]
fn a_tail_below_the_snapshot_is_still_refused_as_an_overlap() {
    let refused = matrixraft_validate_snapshot_tail_catchup(&meta(10), &[entry(9)])
        .expect_err("an entry the snapshot already covers is an overlap");
    assert!(
        refused.to_string().contains("overlaps installed snapshot"),
        "{refused}"
    );
}

#[test]
fn the_offset_is_bounded_too_not_just_the_snapshot_index() {
    // Moving the computation after the overlap check is not the whole fix, and
    // planting showed it: with the check first, `last_log_id.index` is always
    // below u64::MAX by the time the add runs, so `index + 1` is safe on its
    // own. `+ offset` is not. A snapshot ending one below the top, with two
    // entries above it, overflows at offset 1 -- and that arm stayed green
    // until this test existed.
    let refused = matrixraft_validate_snapshot_tail_catchup(
        &meta(u64::MAX - 1),
        &[entry(u64::MAX), entry(u64::MAX)],
    )
    .expect_err("two entries cannot both sit at the top of the range");
    assert!(
        refused.to_string().contains("not contiguous"),
        "the second entry repeats the first, so it is a contiguity failure: {refused}"
    );
}
