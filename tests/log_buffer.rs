// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{RustRaftLogBuffer, RustRaftLogEntry, RustRaftLogId};

fn entry(index: u64, term: u64) -> RustRaftLogEntry {
    RustRaftLogEntry {
        log_id: RustRaftLogId { term, index },
        payload: format!("entry-{index}").into_bytes(),
        is_command: true,
    }
}

fn range(start: u64, end: u64, term: u64) -> Vec<RustRaftLogEntry> {
    (start..end).map(|index| entry(index, term)).collect()
}

#[test]
fn log_buffer_tracks_range_terms_and_bounded_load_like_matrixraft() {
    let mut buffer = RustRaftLogBuffer::new(1000, RustRaftLogId { term: 0, index: 0 });
    assert_eq!(buffer.range(), (1, 1));
    assert_eq!(buffer.last_index(), 0);
    assert_eq!(buffer.last_term(), 0);
    assert_eq!(buffer.last_synced_index(), 0);

    buffer
        .append_many(range(1, 10, 1))
        .expect("append first term");
    buffer
        .append_many(range(10, 20, 2))
        .expect("append second term");

    assert_eq!(buffer.range(), (1, 20));
    assert_eq!(buffer.last_index(), 19);
    assert_eq!(buffer.last_term(), 2);
    assert_eq!(buffer.get_term(0), Some(0));
    assert_eq!(buffer.get_term(1), Some(1));
    assert_eq!(buffer.get_term(9), Some(1));
    assert_eq!(buffer.get_term(10), Some(2));
    assert_eq!(buffer.get_term(19), Some(2));

    let loaded = buffer.get_entries(3, 8, 0).expect("load range");
    assert_eq!(
        loaded
            .iter()
            .map(|entry| entry.log_id.index)
            .collect::<Vec<_>>(),
        vec![3, 4, 5, 6, 7, 8]
    );

    let bounded = buffer
        .get_entries(1, 19, entry(1, 1).payload.len())
        .expect("bounded load includes at least one entry");
    assert_eq!(bounded.len(), 1);
    assert_eq!(bounded[0].log_id.index, 1);
}

#[test]
fn log_buffer_flush_and_apply_append_result_handles_rollback_like_matrixraft() {
    let mut buffer = RustRaftLogBuffer::new(1000, RustRaftLogId { term: 0, index: 0 });
    buffer.append_many(range(1, 6, 1)).expect("append stable");
    buffer.mark_all_stabled();
    buffer
        .append_many(range(6, 10, 1))
        .expect("append unstable");

    let flush = buffer.flush().expect("flush").expect("unstable flush");
    assert!(buffer.is_flushing());
    assert_eq!(flush.first_index, 6);
    assert_eq!(flush.last_index, 9);

    buffer
        .append_many(range(10, 16, 1))
        .expect("append after flush");
    buffer.truncate_from_index(9).expect("truncate rollback");

    let synced = buffer
        .apply_append_result(flush.first_index, flush.last_index)
        .expect("apply append result");
    assert_eq!(synced, 8);
    assert!(!buffer.is_flushing());
    assert_eq!(buffer.last_synced_index(), 8);
    assert_eq!(buffer.range(), (1, 9));
}

#[test]
fn log_buffer_apply_append_result_skips_expired_flush_like_matrixraft() {
    let mut buffer = RustRaftLogBuffer::new(1000, RustRaftLogId { term: 0, index: 0 });
    buffer.append_many(range(1, 6, 1)).expect("append stable");
    buffer.mark_all_stabled();
    buffer
        .append_many(range(6, 10, 1))
        .expect("append unstable");

    let flush = buffer.flush().expect("flush").expect("unstable flush");
    buffer
        .truncate_from_index(5)
        .expect("truncate before flush");

    let synced = buffer
        .apply_append_result(flush.first_index, flush.last_index)
        .expect("apply expired append result");
    assert_eq!(synced, 0);
    assert!(!buffer.is_flushing());
    assert_eq!(buffer.last_synced_index(), 4);
}

#[test]
fn log_buffer_reset_truncate_and_release_memory_like_matrixraft() {
    let mut buffer = RustRaftLogBuffer::new(128, RustRaftLogId { term: 0, index: 0 });
    buffer
        .append_many((1..=10).map(|index| entry(index, 1)).collect())
        .expect("append");
    buffer.truncate_from_index(8).expect("truncate suffix");
    assert_eq!(buffer.range(), (1, 8));
    assert_eq!(buffer.last_index(), 7);

    let release = buffer
        .release_memory(6, 7)
        .expect("release memory")
        .expect("released");
    assert!(release.released_until > 0);
    assert_eq!(buffer.initial_state().index, release.released_until);
    assert_eq!(buffer.range().0, release.released_until + 1);

    buffer
        .reset_initial_state(RustRaftLogId {
            term: 3,
            index: 123,
        })
        .expect("reset initial state");
    assert_eq!(buffer.range(), (124, 124));
    assert_eq!(buffer.last_index(), 123);
    assert_eq!(buffer.last_term(), 3);
    assert_eq!(buffer.last_synced_index(), 123);
}
