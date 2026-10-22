// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The storage apply fence describes six indexes that must agree with each
//! other, and `matrixraft_validate_storage_apply_fence` holds five relations
//! between them. Only one call existed in the suite, and it was a **positive**
//! one -- `expect("storage apply fence")` on a fence that was already valid.
//!
//! A validator with nothing but a positive test is indistinguishable from a
//! validator that returns `Ok(())` unconditionally. Measured: four of the five
//! relations could be deleted with all 180 test targets still green.
//!
//! The five, and what each one would hide:
//!
//! | relation | what an unchecked fence would claim |
//! |---|---|
//! | `applied <= committed` | the state machine has applied an entry no quorum agreed on |
//! | `durable_applied <= applied` | more has been made durable than was ever applied |
//! | `storage_flushed >= durable_applied` | an apply is durable although the storage behind it is not |
//! | `installed_snapshot <= applied` | a snapshot covers state the machine has not reached |
//! | `first_retained > installed_snapshot` | the log and the snapshot overlap, so recovery would replay what the snapshot already holds |
//!
//! Each test changes **one** index from a valid fence and asserts the message,
//! not merely that an error came back: all five return `RaftError::Storage`, so
//! a test checking only the variant could not tell which relation answered --
//! and the relations are checked in order, so a sloppy fixture can trip an
//! earlier one and pass for the wrong reason.

use matrixraft::{
    storage::{matrixraft_validate_storage_apply_fence, StorageApplyFence},
    RaftError,
};

/// A fence whose six indexes all agree: everything committed has been applied
/// and made durable, the storage behind it is flushed, and the snapshot at 8
/// sits strictly below the log's first retained entry at 9.
fn valid_fence() -> StorageApplyFence {
    StorageApplyFence {
        group_id: 404,
        node_id: 7,
        committed_index: 10,
        applied_index: 10,
        durable_applied_index: 10,
        storage_flushed_index: 10,
        installed_snapshot_index: 8,
        first_retained_log_index: 9,
    }
}

fn refusal_of(fence: &StorageApplyFence) -> String {
    match matrixraft_validate_storage_apply_fence(fence) {
        Err(RaftError::Storage(reason)) => reason,
        other => panic!("expected a storage refusal, got {other:?}"),
    }
}

#[test]
fn a_fence_whose_indexes_agree_is_accepted() {
    // The control. Without it every test below would also pass against a
    // validator that refused every fence, which would stop recovery entirely.
    matrixraft_validate_storage_apply_fence(&valid_fence())
        .expect("a fence whose indexes agree is valid");
}

#[test]
fn applying_past_the_committed_index_is_refused() {
    let mut fence = valid_fence();
    fence.applied_index = 11;
    // The durable and flushed indexes stay at 10, so they remain consistent
    // with each other and only the applied/committed relation is broken.
    assert_eq!(
        refusal_of(&fence),
        "storage apply fence is ahead of committed index",
        "applying an entry no quorum agreed on is the one defect this names"
    );
}

#[test]
fn making_more_durable_than_was_applied_is_refused() {
    let mut fence = valid_fence();
    fence.applied_index = 9;
    fence.durable_applied_index = 10;
    // applied 9 is still within committed 10, so the first relation holds and
    // this one has to be the one that answers.
    assert_eq!(
        refusal_of(&fence),
        "durable applied index is ahead of in-memory applied index"
    );
}

#[test]
fn a_durable_apply_behind_its_storage_is_refused() {
    let mut fence = valid_fence();
    fence.storage_flushed_index = 9;
    assert_eq!(
        refusal_of(&fence),
        "storage flush is behind durable applied index",
        "an apply is not durable while the storage under it is not flushed"
    );
}

#[test]
fn a_snapshot_past_the_applied_index_is_refused() {
    let mut fence = valid_fence();
    fence.installed_snapshot_index = 11;
    // first_retained moves past the snapshot as well, so the overlap relation
    // below stays satisfied and this test cannot pass on the wrong one.
    fence.first_retained_log_index = 12;
    assert_eq!(
        refusal_of(&fence),
        "installed snapshot is ahead of applied index"
    );
}

#[test]
fn a_log_overlapping_its_snapshot_is_refused() {
    let mut fence = valid_fence();
    fence.first_retained_log_index = fence.installed_snapshot_index;
    assert_eq!(
        refusal_of(&fence),
        "first retained log index overlaps installed snapshot",
        "the entry at the snapshot index must not also be in the log"
    );

    // And the boundary that makes it a rule rather than an ordering: one past
    // the snapshot is exactly enough.
    fence.first_retained_log_index = fence.installed_snapshot_index + 1;
    matrixraft_validate_storage_apply_fence(&fence)
        .expect("a log starting one past the snapshot does not overlap it");
}

#[test]
fn a_fence_with_no_snapshot_does_not_have_to_leave_room_for_one() {
    // The second half of the overlap rule, and the reason it is written as two
    // clauses: with no snapshot installed there is nothing to overlap, so a log
    // retained from index 0 is legitimate rather than an overlap at 0.
    let mut fence = valid_fence();
    fence.installed_snapshot_index = 0;
    fence.first_retained_log_index = 0;
    matrixraft_validate_storage_apply_fence(&fence)
        .expect("a node with no snapshot may retain its whole log");
}
