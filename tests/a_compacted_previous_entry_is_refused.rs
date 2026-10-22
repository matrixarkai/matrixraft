// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! An append is refused when a snapshot has already covered its previous entry.
//!
//! `matrixraft_append_safety_decision` is a public helper a host can ask before
//! taking an append. Its first rule has three clauses:
//!
//! ```text
//! if prev_index > 0
//!     && prev_index < first_retained_log_index
//!     && prev_index <= snapshot_index
//! { ..reason: "prev_log_compacted".. }
//! ```
//!
//! Planted one at a time against the whole suite, only `prev_index > 0` was
//! held. **The other two were caught by nothing** across 1,030 tests, and the
//! function is not consulted inside the crate at all -- it is host-facing
//! surface, so its rules are the product rather than an internal detail.
//!
//! ## What the third clause means, and why it is not redundant
//!
//! `prev_index <= snapshot_index` is what makes this rule say *compacted*
//! rather than merely *missing*. MEASURED, with the log retained from 10:
//!
//! | snapshot through | prev | verdict |
//! |---|---|---|
//! | 9 | 5 | refused, `prev_log_compacted` |
//! | 9 | 10 | accepted -- at the floor, nothing was compacted away |
//! | **3** | 5 | accepted -- below the floor, but **no snapshot covers it** |
//!
//! That last row is the clause earning its place. An entry below the retained
//! floor that no snapshot covers is not compacted, it is simply gone, and this
//! rule deliberately does not claim it: the ordinary contiguity check is what
//! refuses that one. Were the clause dropped, the two cases would be answered
//! identically and the reason would stop being true of the second.

use matrixraft::{matrixraft_append_safety_decision, AppendEntriesRequest, LogEntry, LogId};

/// The log is retained from here, so anything below is no longer held.
const FIRST_RETAINED: u64 = 10;

fn append_with_prev(prev_index: u64, first_entry: u64) -> AppendEntriesRequest {
    AppendEntriesRequest {
        group_id: 1,
        term: 3,
        leader_id: 1,
        prev_log_id: Some(LogId {
            term: 1,
            index: prev_index,
        }),
        entries: vec![LogEntry {
            log_id: LogId {
                term: 3,
                index: first_entry,
            },
            payload: b"x".to_vec(),
            is_command: false,
        }],
        leader_commit: 0,
        lease_epoch: 0,
    }
}

#[test]
fn a_previous_entry_a_snapshot_has_covered_is_refused() {
    let decision =
        matrixraft_append_safety_decision(FIRST_RETAINED, 9, &append_with_prev(5, FIRST_RETAINED));

    assert!(!decision.accepted);
    assert!(decision.rejected_compacted_entry);
    assert_eq!(decision.reason, "prev_log_compacted");
}

#[test]
fn a_previous_entry_a_snapshot_covers_but_the_log_still_holds_is_not_compacted() {
    // Pins `prev_index < first_retained_log_index`, and it took a second
    // attempt to pin it. The obvious case -- `prev` at the floor with the
    // snapshot behind it -- does NOT discriminate: dropping the clause leaves
    // `prev <= snapshot_index` false, so that case is accepted either way and
    // the test passed with the clause deleted.
    //
    // What discriminates is `prev` at or above the floor while the snapshot
    // reaches PAST it, which is an ordinary state: a snapshot through 12 while
    // the log is still retained from 10. The entry is held, so however far the
    // snapshot reaches, nothing compacted it away.
    let decision = matrixraft_append_safety_decision(
        FIRST_RETAINED,
        12,
        &append_with_prev(FIRST_RETAINED + 1, FIRST_RETAINED + 2),
    );

    assert!(decision.accepted, "refused instead: {}", decision.reason);
    assert!(
        !decision.rejected_compacted_entry,
        "the entry is still retained, so it was not compacted"
    );
}

#[test]
fn a_previous_entry_at_the_retained_floor_is_not_compacted() {
    // The boundary itself: the floor is retained, so it is not compacted.
    let decision = matrixraft_append_safety_decision(
        FIRST_RETAINED,
        9,
        &append_with_prev(FIRST_RETAINED, FIRST_RETAINED + 1),
    );

    assert!(decision.accepted, "refused instead: {}", decision.reason);
    assert!(!decision.rejected_compacted_entry);
}

#[test]
fn a_previous_entry_below_the_floor_but_past_the_snapshot_is_not_compacted() {
    // Pins `prev_index <= snapshot_index`, and it is the clause that gives the
    // reason its meaning. Same `prev` as the refused case above; the only
    // difference is that no snapshot reaches it, so it is missing rather than
    // compacted and a different rule has to answer for it.
    let decision =
        matrixraft_append_safety_decision(FIRST_RETAINED, 3, &append_with_prev(5, FIRST_RETAINED));

    assert!(
        !decision.rejected_compacted_entry,
        "nothing compacted this entry -- no snapshot covers index 5"
    );
    assert_ne!(
        decision.reason, "prev_log_compacted",
        "so the rule must not give that as the reason"
    );
}

#[test]
fn an_ordinary_append_above_the_floor_is_accepted() {
    // The positive control: without it, a rule that refused everything would
    // satisfy the refusal test above.
    let decision = matrixraft_append_safety_decision(FIRST_RETAINED, 9, &append_with_prev(12, 13));

    assert!(decision.accepted, "refused instead: {}", decision.reason);
    assert_eq!(decision.reason, "accepted");
}
