// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The log buffer is the in-memory hot log: entries land here, get flushed to
//! storage, and are released once they are safely elsewhere. Eight conditions
//! bound what a caller may ask of it, and **none of them had a test** -- each
//! could be deleted with all 186 test targets green. The four tests it already
//! had are all happy-path: they append, flush, reset and release, and never ask
//! what happens at an edge.
//!
//! **Seven of the eight turned out to be rules; one is a short-circuit.**
//! `truncate_from_index`'s upper bound can be deleted and *nothing changes* --
//! measured at four distances past the end against eight observables, because
//! the body below it is already inert for such an index: `truncate_offset`
//! exceeds `entries.len()`, the `min()` calls can only lower the two offsets
//! and cannot, and the `while len > offset` loop never runs. So that line is an
//! optimisation, not a guarantee, and this file does not pretend otherwise.
//! What it *does* pin is the **contract** the line expresses -- that asking too
//! high is not an error -- which a retrying caller depends on and which a plant
//! turning it into an `Err` does break.
//!
//! ## The thing worth learning from this file
//!
//! Two of its methods take an index and bound it at **both** ends, and they err
//! in **opposite directions**:
//!
//! | asked | `truncate_from_index` | `drain_until` |
//! |---|---|---|
//! | at or below the floor | **error** | no-op |
//! | past what is held | no-op | **error** |
//!
//! That is not an inconsistency, it is the harm pointing different ways. The
//! two methods remove entries from opposite ends:
//!
//! - **truncate** drops the tail. Asked for an index at or below the floor it
//!   would discard the floor itself, which is the buffer's own notion of where
//!   the log begins -- so it refuses. Asked past the end there is simply
//!   nothing to drop, which is a no-op a retrying caller depends on.
//! - **drain** releases the head. Asked at or below the floor there is nothing
//!   to release -- a no-op, because the release policy calls it speculatively
//!   every tick. Asked past the end it would be claiming to release entries the
//!   buffer does not hold, so it refuses.
//!
//! A test suite that only checked "out of range is an error" would get one of
//! the two wrong, and a reader who assumed symmetry would too.

use matrixraft::{LogBuffer, LogEntry, LogId, RaftError};

const FLOOR: u64 = 5;
const LAST: u64 = 10;

fn entry(index: u64, term: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term, index },
        payload: b"write".to_vec(),
        is_command: true,
    }
}

/// A buffer whose floor is index 5 and which holds entries 6..=10.
fn buffer() -> LogBuffer {
    let mut buffer = LogBuffer::new(
        4096,
        LogId {
            term: 3,
            index: FLOOR,
        },
    );
    buffer
        .append_many((FLOOR + 1..=LAST).map(|index| entry(index, 3)).collect())
        .expect("entries 6..=10 are contiguous");
    assert_eq!(
        (buffer.range().0, buffer.last_index()),
        (FLOOR + 1, LAST),
        "the premise: the first retained entry sits one past the floor"
    );
    // Worth stating once, because it is a trap: `range()` is HALF-OPEN. It
    // reports `(floor + 1, last_index + 1)`, so the second element is not an
    // index the buffer holds. Every assertion below uses `last_index()` for
    // the last held index and keeps `range()` for the pair.
    assert_eq!(buffer.range(), (FLOOR + 1, LAST + 1));
    buffer
}

fn is_refusal(error: &RaftError, fragment: &str) -> bool {
    matches!(error, RaftError::InvalidRequest(reason) if reason.contains(fragment))
}

// ---------------------------------------------------------------- contiguity

#[test]
fn an_append_must_be_the_very_next_index() {
    let mut buffer = buffer();

    let gap = buffer
        .append(entry(LAST + 2, 3))
        .expect_err("an index past the next one would leave a hole");
    assert!(
        is_refusal(&gap, "log buffer append expects index 11"),
        "{gap:?}"
    );

    let repeat = buffer
        .append(entry(LAST, 3))
        .expect_err("an index already held would duplicate it");
    assert!(is_refusal(&repeat, "expects index 11"), "{repeat:?}");

    assert_eq!(
        buffer.last_index(),
        LAST,
        "and a refused append leaves the buffer exactly where it was"
    );

    // The control: the next index is accepted, so the rule is not simply
    // refusing every append.
    buffer
        .append(entry(LAST + 1, 3))
        .expect("the very next index is appended");
    assert_eq!(buffer.last_index(), LAST + 1);
}

// ------------------------------------------------- truncate: low errs, high no-ops

#[test]
fn truncating_at_or_below_the_floor_is_refused() {
    let mut buffer = buffer();

    let at_the_floor = buffer
        .truncate_from_index(FLOOR)
        .expect_err("truncating from the floor would discard the floor itself");
    assert!(
        is_refusal(&at_the_floor, "must be after first retained index"),
        "{at_the_floor:?}"
    );
    assert!(
        buffer.truncate_from_index(FLOOR - 1).is_err(),
        "and below it"
    );

    assert_eq!(buffer.last_index(), LAST, "nothing was removed");

    // The boundary: one past the floor is the lowest index that may go, and it
    // takes everything after it, leaving only the floor.
    buffer
        .truncate_from_index(FLOOR + 1)
        .expect("one past the floor is truncatable");
    assert_eq!(
        buffer.last_index(),
        FLOOR,
        "the buffer is back to just its floor"
    );
}

#[test]
fn truncating_past_what_is_held_is_a_no_op() {
    let mut buffer = buffer();

    buffer
        .truncate_from_index(LAST + 2)
        .expect("there is nothing out there to truncate, so this is not an error");
    assert_eq!(
        buffer.last_index(),
        LAST,
        "and nothing was removed -- a caller retrying a truncate it already \
         completed must not be handed an error for it"
    );
}

// ------------------------------------------------- drain: low no-ops, high errs

#[test]
fn draining_at_or_below_the_floor_is_a_no_op() {
    let mut buffer = buffer();

    let nothing = buffer
        .drain_until(FLOOR)
        .expect("nothing to release is not an error");
    assert!(
        nothing.is_none(),
        "and it reports that it released nothing: {nothing:?}"
    );
    assert_eq!(
        buffer.range(),
        (FLOOR + 1, LAST + 1),
        "the buffer is untouched -- the release policy calls this every tick, \
         so the common case has to be free"
    );
}

#[test]
fn draining_past_what_is_held_is_refused() {
    let mut buffer = buffer();

    let too_far = buffer
        .drain_until(LAST + 1)
        .expect_err("the buffer cannot release entries it does not hold");
    assert!(
        is_refusal(&too_far, "exceeds last retained index"),
        "{too_far:?}"
    );
    assert_eq!(
        buffer.range(),
        (FLOOR + 1, LAST + 1),
        "and nothing was released"
    );

    // The boundary: the last index it holds may be drained, and then it holds
    // nothing but the new floor.
    let released = buffer
        .drain_until(LAST)
        .expect("the last held index is drainable")
        .expect("and something came back");
    assert_eq!(released.released_until, LAST);
    assert_eq!(
        buffer.last_index(),
        LAST,
        "the drained-to index becomes the new floor, which is what is left"
    );
    assert_eq!(
        buffer.range(),
        (LAST + 1, LAST + 1),
        "and the half-open range is empty"
    );
}

// ---------------------------------------------------------------- the pair

#[test]
fn the_two_bounds_err_in_opposite_directions() {
    // The asymmetry asserted as one statement, because it is the thing a
    // reader is most likely to get wrong -- and because a plant that made
    // either method symmetric with the other would pass every test above.
    let mut truncate = buffer();
    let mut drain = buffer();

    // Below the floor.
    assert!(
        truncate.truncate_from_index(FLOOR).is_err(),
        "truncate refuses at the floor: it would discard the floor"
    );
    assert!(
        drain.drain_until(FLOOR).expect("not an error").is_none(),
        "drain absorbs at the floor: there is nothing to release"
    );

    // Past the end.
    assert!(
        truncate.truncate_from_index(LAST + 1).is_ok(),
        "truncate absorbs past the end: there is nothing to drop"
    );
    assert!(
        drain.drain_until(LAST + 1).is_err(),
        "drain refuses past the end: it does not hold those entries"
    );
}

// ---------------------------------------------------------------- the rest

#[test]
fn a_floor_may_not_be_reset_backwards() {
    let mut buffer = buffer();

    let backwards = buffer
        .reset_initial_state(LogId {
            term: 3,
            index: FLOOR,
        })
        .expect_err("a floor at the current one is not forward");
    assert!(
        is_refusal(&backwards, "must be after first retained index"),
        "{backwards:?}"
    );
    assert_eq!(
        buffer.initial_state().index,
        FLOOR,
        "and the floor does not move -- it is what `term_at` answers from"
    );

    // The boundary: one past is accepted, and resets the buffer to that floor.
    buffer
        .reset_initial_state(LogId {
            term: 3,
            index: FLOOR + 1,
        })
        .expect("a floor one past the current one is forward");
    assert_eq!(buffer.initial_state().index, FLOOR + 1);
    assert_eq!(
        buffer.last_index(),
        FLOOR + 1,
        "and the entries it held are gone, because a new floor supersedes them"
    );
}

#[test]
fn memory_cannot_be_released_past_what_was_applied() {
    let mut buffer = buffer();

    let unapplied = buffer
        .release_memory(/* replicate */ 9, /* applied */ 8)
        .expect_err("replicated is not the same as applied");
    assert!(
        is_refusal(&unapplied, "must not exceed applied index"),
        "{unapplied:?}"
    );

    // The control, and the part that documents the policy: with the two the
    // right way round it releases -- but only PART of the distance. The floor
    // is 5 and the replicate index 8, so the three releasable entries are
    // drained 90% of the way, to 7, leaving a margin rather than draining to
    // the boundary and thrashing there on the next tick.
    let released = buffer
        .release_memory(8, 9)
        .expect("applied is at or past replicated")
        .expect("something was released");
    assert_eq!(
        released.released_until, 7,
        "nine tenths of the distance from the floor, not all of it"
    );
}
