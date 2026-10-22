// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `matrixraft_wal_delta_base` decides whether a record may be stored as a
//! **delta** -- carrying only what was appended since the previous record in
//! the segment -- or has to be stored whole. It answers with the offset the new
//! entries begin at, or `None` for "store it whole".
//!
//! `None` is the safe answer, so every rule here is a refusal to take the
//! cheaper path. **One of the four had no test**, and it is the one with real
//! consequences:
//!
//! | condition | answer | already tested |
//! |---|---|---|
//! | no entries at all | `None` | — |
//! | **the log now starts past where the segment does** | **`None`** | **no** |
//! | the segment's last index is not in the entries | `None` | yes, by `a_log_truncated_by_a_conflict_is_stored_whole` |
//! | same index, different term | `None` | yes, by `a_tail_rewritten_at_the_same_index_is_stored_whole` |
//! | otherwise | `Some(offset)` | — |
//!
//! Measured: deleting the compaction rule left all 183 test targets green,
//! while the other two refusals each had a test that caught them. They are
//! asserted here as well, not to claim credit for covering them, but because
//! all three answer `None` and only the inputs distinguish them -- a reader
//! changing one rule needs the three cases side by side to see which is which.
//!
//! The compaction rule is the one with consequences. A delta is folded by
//! replaying the segment's records in order, so if the log has been compacted
//! past the segment's first index, folding reconstructs entries the node
//! deliberately dropped -- and they would come back as part of a record that
//! checksums correctly, so nothing downstream would question them.
//!
//! Each test states the four covered indexes explicitly, because the function
//! takes them as bare numbers and a wrong one silently moves which rule
//! answers.

use matrixraft::{wal::matrixraft_wal_delta_base, LogEntry, LogId};

fn entry(index: u64, term: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term, index },
        payload: b"write".to_vec(),
        is_command: true,
    }
}

/// Entries 4..=8 at term 3, the shape a record holds after the log has been
/// compacted through index 3.
fn entries_4_to_8() -> Vec<LogEntry> {
    (4..=8).map(|index| entry(index, 3)).collect()
}

#[test]
fn a_record_that_extends_the_segment_may_be_a_delta() {
    // The control, and it has to come first: every other test here asserts
    // `None`, so without this one they would all pass against a function that
    // never allowed a delta at all -- which is safe but makes the whole
    // delta-encoding pointless.
    //
    // The segment covers 4..=6 ending at term 3, and the record holds 4..=8,
    // so entries 7 and 8 are new. The overlap at index 6 sits at offset 2, so
    // the new entries begin at offset 3.
    let base = matrixraft_wal_delta_base(&entries_4_to_8(), 4, 6, 3);
    assert_eq!(
        base,
        Some(3),
        "the offset is one past the segment's last covered entry"
    );
}

#[test]
fn a_log_compacted_past_the_segment_start_is_stored_whole() {
    // The record now starts at 4 while the segment it would extend starts at
    // 2. Folding the segment's records would rebuild indexes 2 and 3, which
    // this node has released -- so the record has to be stored whole.
    let base = matrixraft_wal_delta_base(&entries_4_to_8(), 2, 6, 3);
    assert_eq!(
        base, None,
        "a delta must not be able to resurrect compacted entries"
    );

    // The boundary: starting exactly where the segment starts is still sound,
    // because nothing before it would be rebuilt.
    assert_eq!(
        matrixraft_wal_delta_base(&entries_4_to_8(), 4, 6, 3),
        Some(3),
        "equal is not past"
    );
}

#[test]
fn a_segment_whose_last_entry_is_absent_is_stored_whole() {
    // The segment says it covers through index 9, but the record's entries stop
    // at 8, so there is no overlapping entry to fold onto. Note the first index
    // is 4, matching the entries, so the compaction rule above is satisfied and
    // this rule has to be the one that answers.
    let base = matrixraft_wal_delta_base(&entries_4_to_8(), 4, 9, 3);
    assert_eq!(base, None, "there is no overlap to fold onto");

    // And one the record does hold, to show the absence is what mattered.
    assert_eq!(
        matrixraft_wal_delta_base(&entries_4_to_8(), 4, 8, 3),
        Some(5),
        "the segment's last entry is the record's last, so nothing is new"
    );
}

#[test]
fn a_record_with_no_entries_is_stored_whole() {
    // First in the function and trivial, but it is the case a caller hits on a
    // heartbeat-only record, so it is worth pinning that it answers `None`
    // rather than panicking on an empty slice.
    assert_eq!(matrixraft_wal_delta_base(&[], 4, 6, 3), None);
}

#[test]
fn a_tail_rewritten_at_the_same_index_is_stored_whole_here_too() {
    // The fourth rule, already covered elsewhere by
    // `a_tail_rewritten_at_the_same_index_is_stored_whole`, asserted here as
    // well because it is what separates this function's two index rules from
    // its term rule: all three answer `None`, and only the inputs say which.
    //
    // The segment's last entry at index 6 was term 2; the record holds term 3
    // at that index, so the log diverged and was rewritten.
    let base = matrixraft_wal_delta_base(&entries_4_to_8(), 4, 6, 2);
    assert_eq!(
        base, None,
        "same index, different term: the overlap is not the same entry"
    );
}
