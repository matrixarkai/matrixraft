// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Three of the log buffer's methods take an index and refuse it before
//! touching a single entry: `apply_append_result`, `read_entries` and
//! `term_at`. Between them they hold **seven clauses**, and every one of the
//! seven could be deleted with all 192 test targets green. The module's
//! existing bounds file covers append, truncate, drain, floor-reset and
//! release, and never reaches these three.
//!
//! ## Why they were missed, which matters more than the tests
//!
//! Every clause compares an index against the buffer's **floor**, and in the
//! fixtures that existed the floor is zero. For an unsigned index `index <
//! floor` then cannot hold at all, so the clause is *unreachable* rather than
//! merely untested, and no amount of exercising those fixtures would ever have
//! entered its regime. Move the floor off zero -- this file puts it at 5 --
//! and five of the seven become live rules whose removal panics or serves the
//! wrong bytes.
//!
//! ## What each clause turned out to be
//!
//! | clause | with it deleted |
//! |---|---|
//! | `apply_append_result`: `first_index == 0` | still an error -- but from the retained-floor check below, naming a different bound |
//! | `apply_append_result`: `last_index < first_index` | panics: `last - first + 1` subtracts past zero |
//! | `read_entries`: `from > to` | panics: `to - from + 1` subtracts past zero |
//! | `read_entries`: `from <= floor` | **serves the sentinel as a log entry** |
//! | `read_entries`: `to > last_index` | reads past the end of what is held |
//! | `term_at`: `index < floor` | panics: `index - floor` subtracts past zero |
//! | `term_at`: `index > last_index` | **nothing changes** -- `.get()` already bounds it |
//!
//! Exactly one of the seven -- `term_at`'s upper bound -- is a short-circuit
//! rather than a rule, and this file does not pretend otherwise. For that one
//! it pins the **contract** the clause expresses, that asking too high is
//! absence rather than an error, instead of claiming a guarantee that `.get()`
//! is already providing.
//!
//! `apply_append_result`'s zero check looks like the same shape and is not.
//! Deleted, the call still fails -- so a test asserting only "this is refused"
//! would stay green and call the clause redundant. What moves is **which check
//! answers**, and with it the bound the message names. The test below therefore
//! asserts the attribution rather than the refusal, which is the only way the
//! clause is visible at all.
//!
//! ## The asymmetry worth knowing
//!
//! `read_entries` refuses `from == floor`, while `term_at` **answers** for
//! `index == floor`. Same index, opposite answers, and both are correct: the
//! floor is a sentinel entry that carries a term but no payload. A caller may
//! ask what term the log began at; it may not read that sentinel back as data.
//! A suite that assumed the two methods shared one notion of "in range" would
//! get one of them wrong.

use matrixraft::{LogBuffer, LogEntry, LogId, RaftError};

const FLOOR: u64 = 5;
const LAST: u64 = 10;
const FLOOR_TERM: u64 = 3;

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
            term: FLOOR_TERM,
            index: FLOOR,
        },
    );
    buffer
        .append_many(
            (FLOOR + 1..=LAST)
                .map(|index| entry(index, FLOOR_TERM))
                .collect(),
        )
        .expect("entries 6..=10 are contiguous");
    assert_eq!(
        (buffer.range().0, buffer.last_index()),
        (FLOOR + 1, LAST),
        "the premise every test below rests on: the floor sits at 5, off zero, so an index below it is representable at all"
    );
    buffer
}

fn is_refusal(error: &RaftError, fragment: &str) -> bool {
    matches!(error, RaftError::InvalidRequest(message) if message.contains(fragment))
}

#[test]
fn a_stabled_range_that_runs_backwards_is_refused() {
    let mut buffer = buffer();
    let error = buffer
        .apply_append_result(LAST - 2, LAST - 4)
        .expect_err("8..=6 runs backwards and is not a range");
    assert!(
        is_refusal(&error, "invalid stabled range"),
        "the refusal must come from the range check itself, not from a later bound: {error:?}"
    );
}

#[test]
fn a_stabled_range_starting_at_zero_is_refused_by_the_range_check() {
    let mut buffer = buffer();
    let error = buffer
        .apply_append_result(0, FLOOR - 2)
        .expect_err("index 0 is never a valid stabled index");
    // Deleting the `first_index == 0` clause leaves this call failing -- the
    // retained-floor check below it refuses index 0 too. So the assertion is on
    // *which* check answers, because that is the whole of the clause's visible
    // effect; `is_err()` alone would stay green with the clause gone.
    assert!(
        is_refusal(&error, "invalid stabled range"),
        "expected the stabled-range refusal rather than the retained-floor one: {error:?}"
    );
}

#[test]
fn reading_a_range_that_runs_backwards_is_refused() {
    let buffer = buffer();
    let error = buffer
        .read_entries(FLOOR + 2, FLOOR + 1, 0)
        .expect_err("7..=6 runs backwards and is not a range");
    assert!(
        is_refusal(&error, "outside retained range"),
        "expected the retained-range refusal: {error:?}"
    );
}

#[test]
fn reading_may_not_start_at_the_floor_because_the_floor_is_the_sentinel() {
    let buffer = buffer();
    let error = buffer
        .read_entries(FLOOR, LAST, 0)
        .expect_err("the floor itself holds no payload and is not readable data");
    assert!(
        is_refusal(&error, "outside retained range"),
        "expected the retained-range refusal: {error:?}"
    );

    let entries = buffer
        .read_entries(FLOOR + 1, LAST, 0)
        .expect("one past the floor is the first readable entry");
    assert_eq!(
        entries.len() as u64,
        LAST - FLOOR,
        "entries 6..=10 are five entries"
    );
    assert_eq!(
        entries.first().map(|entry| entry.log_id.index),
        Some(FLOOR + 1),
        "the first entry served must be the one after the floor, never the sentinel sitting on it"
    );
}

#[test]
fn reading_past_the_last_held_index_is_refused() {
    let buffer = buffer();
    let error = buffer
        .read_entries(FLOOR + 1, LAST + 1, 0)
        .expect_err("11 is past the last index held");
    assert!(
        is_refusal(&error, "outside retained range"),
        "expected the retained-range refusal: {error:?}"
    );
}

#[test]
fn a_term_below_the_floor_is_absent_rather_than_a_subtraction_past_zero() {
    let buffer = buffer();
    assert_eq!(
        buffer.term_at(FLOOR - 1),
        None,
        "index 4 sits below the floor, and answering it would offset by 4 - 5"
    );
}

#[test]
fn a_term_past_the_last_held_index_is_absent() {
    let buffer = buffer();
    // `.get()` already bounds this, so the clause is an optimisation rather
    // than the thing providing the guarantee. The guarantee itself -- that
    // asking too high is absence and not an error or a panic -- is what a
    // caller depends on, so that is what is asserted.
    assert_eq!(buffer.term_at(LAST + 1), None);
}

#[test]
fn the_floor_answers_a_term_but_refuses_to_be_read_as_data() {
    let buffer = buffer();
    assert_eq!(
        buffer.term_at(FLOOR),
        Some(FLOOR_TERM),
        "the floor carries the term the log began at, and term_at answers from it"
    );
    assert!(
        buffer.read_entries(FLOOR, LAST, 0).is_err(),
        "the same index that answers a term must refuse to be served as an entry"
    );
}
