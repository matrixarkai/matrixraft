// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Compaction past what storage has durably taken would drop records that are
//! not yet on disk, so the fence refuses a requested index storage has not
//! reached. It reads as two clauses:
//!
//! ```text
//! if fence.durable_applied_index < log_index || fence.storage_flushed_index < log_index
//! ```
//!
//! and only the first has ever been exercised -- the one test that covers this
//! fence sets `durable_applied_index` behind the requested index while leaving
//! `storage_flushed_index` ahead of it, so the second clause has never decided
//! anything.
//!
//! **It cannot.** `matrixraft_validate_storage_apply_fence` runs immediately
//! before the fence, and already refuses any fence whose `storage_flushed_index`
//! sits below its `durable_applied_index`. For the second clause to be the one
//! that answers, `durable_applied_index` must be at or above the requested
//! index while `storage_flushed_index` is below it -- and the validator's
//! ordering makes that combination unconstructible:
//!
//! ```text
//! storage_flushed >= durable_applied   (the validator)
//! durable_applied >= log_index         (first clause already false)
//! -------------------------------------------------------------
//! storage_flushed >= log_index         (so the second clause is false too)
//! ```
//!
//! So the clause is unreachable rather than untested, and writing a test that
//! "covers" it would mean constructing a fence the validator rejects and then
//! reading the validator's refusal as the fence's -- a test that passes while
//! measuring the wrong thing.
//!
//! What is load-bearing is the **ordering** that makes it unreachable. That is
//! what this file pins. The clause stays where it is, as a backstop costing one
//! comparison; if the validator ever stops ordering those two indexes, the
//! sweep below goes red to say the backstop has become live and now needs a
//! test of its own.

use matrixraft::{matrixraft_validate_storage_apply_fence, RaftError, StorageApplyFence};

const COMMITTED: u64 = 6;

/// A fence that differs from a valid one only in the two indexes under test.
fn fence(durable_applied_index: u64, storage_flushed_index: u64) -> StorageApplyFence {
    StorageApplyFence {
        group_id: 77,
        node_id: 1,
        committed_index: COMMITTED,
        applied_index: COMMITTED,
        durable_applied_index,
        storage_flushed_index,
        installed_snapshot_index: 0,
        first_retained_log_index: 1,
    }
}

#[test]
fn a_flush_behind_the_durable_apply_is_refused_and_says_so() {
    let error = matrixraft_validate_storage_apply_fence(&fence(4, 3))
        .expect_err("a flush at 3 cannot sit behind a durable apply at 4");
    match error {
        RaftError::Storage(message) => assert!(
            message.contains("storage flush is behind durable applied index"),
            "the refusal must name the ordering it enforces, because that ordering is what makes the fence's second clause unreachable: {message}"
        ),
        other => panic!("expected a storage refusal, got {other:?}"),
    }
}

#[test]
fn a_fence_whose_flush_has_caught_up_is_valid() {
    matrixraft_validate_storage_apply_fence(&fence(COMMITTED, COMMITTED))
        .expect("a fence whose durable apply and flush agree is valid");
}

#[test]
fn the_fences_flush_clause_cannot_be_reached_through_the_validator() {
    let mut tried = 0usize;
    let mut reached = 0usize;

    // Exactly the regime in which the second clause would be the one to answer:
    // the first clause already false, the second one true.
    for log_index in 1..=COMMITTED {
        for durable_applied in log_index..=COMMITTED {
            for storage_flushed in 0..log_index {
                tried += 1;
                if matrixraft_validate_storage_apply_fence(&fence(durable_applied, storage_flushed))
                    .is_ok()
                {
                    reached += 1;
                }
            }
        }
    }

    assert!(
        tried > 0,
        "the sweep must actually enter the regime it argues about"
    );
    assert_eq!(
        reached, 0,
        "no fence can pass the validator while its flush sits behind a requested index its durable apply has already reached -- if this moves off zero, the compaction fence's flush clause has become live and owes a test of its own ({tried} combinations tried)"
    );
}
