// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Three rules in the log store stop a caller unwinding it, and none of them
//! had a test: each could be deleted with all 182 test targets green.
//!
//! | rule | what it stops |
//! |---|---|
//! | `compact_until` ignores an index at or below the current floor | the recorded compaction floor moving **backwards**, below what the store has already released |
//! | `load_entries` refuses a range whose end precedes its start | a read that silently returns nothing where it should be a caller's bug |
//! | `switch_segment` refuses an index at or below the last entry | a new segment opening over entries an existing one already covers |
//!
//! The first is the one with teeth, and it is a **no-op rather than an error**
//! by design: compaction is driven by whatever the group has agreed it no
//! longer needs, and that number can arrive out of order or be recomputed from
//! a stale view. Refusing would turn an ordinary race into a failure, so the
//! store absorbs it -- and then reports **where it actually stands**, not what
//! was asked for, which is the part a caller can be misled by if it is wrong.
//!
//! Each test pairs its refusal with the neighbouring case that must still
//! work: the forward compaction, the empty range, and the segment switch one
//! past the end. Two of the three rules are boundary comparisons, so the case
//! that must be *allowed* is one step away from the case that must not.

use matrixraft::{
    MatrixRaftConfState, MatrixRaftEntry, MatrixRaftEntryType, MatrixRaftInitialState,
    MatrixRaftLogRange, MatrixRaftLogStorage, MatrixRaftLogStorageOptions,
    MatrixRaftLogStoragePrepareOptions, MatrixRaftLogStorageWriteTask, MatrixRaftMemberId,
    MatrixRaftMemoryLogStorage, MatrixRaftPropose, RaftError, ReplicaRole,
};

fn member(node_id: u64) -> MatrixRaftMemberId {
    MatrixRaftMemberId {
        id: node_id,
        raft_addr: format!("127.0.0.1:{}", 50_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 51_000 + node_id),
        is_from_options: true,
        conf_state: MatrixRaftConfState::Voter,
        auto_promote: false,
    }
}

fn entry(index: u64, term: u64) -> MatrixRaftEntry {
    MatrixRaftEntry {
        entry_type: MatrixRaftEntryType::Normal,
        term,
        index,
        propose: Some(MatrixRaftPropose {
            request_id: Some(index),
            data: b"write".to_vec(),
            context: Vec::new(),
            is_command: true,
        }),
        config_change: None,
        memberships: Vec::new(),
        request_id: index,
        bytes_size: 5,
    }
}

/// A store holding entries 1..=6, all at term 3.
fn store_with_six_entries() -> MatrixRaftMemoryLogStorage {
    let mut store = MatrixRaftMemoryLogStorage::new(
        MatrixRaftLogStoragePrepareOptions {
            peer_id: 1,
            max_segment_bytes: 64 * 1024,
            initial_state: MatrixRaftInitialState { index: 0, term: 0 },
            role: ReplicaRole::Voter,
            local_id: member(1),
            members: vec![member(1), member(2), member(3)],
        },
        MatrixRaftLogStorageOptions {
            peer_id: 1,
            max_segment_bytes: 64 * 1024,
            applied_index: 0,
            local_id: member(1),
            sync: false,
        },
    );
    store
        .write(MatrixRaftLogStorageWriteTask {
            sync_meta: true,
            committed_index: 6,
            size_hint: 30,
            hard_state: None,
            members: Vec::new(),
            entries: (1..=6).map(|index| entry(index, 3)).collect(),
        })
        .expect("six entries are written");
    assert_eq!((store.first_index(), store.last_index()), (1, 6));
    store
}

#[test]
fn a_compaction_floor_does_not_move_backwards() {
    let mut store = store_with_six_entries();

    // Forward first, so there is a floor to try to unwind.
    let forward = store
        .compact_until(MatrixRaftInitialState { index: 3, term: 3 })
        .expect("a forward compaction is applied");
    assert_eq!(forward.initial_state.index, 3);
    assert_eq!(
        (store.first_index(), store.last_index()),
        (4, 6),
        "the floor moved and the entries behind it went"
    );

    // Now ask for a floor that is behind the one in place.
    let backwards = store
        .compact_until(MatrixRaftInitialState { index: 1, term: 3 })
        .expect("a backwards compaction is absorbed rather than refused");

    assert_eq!(
        (store.first_index(), store.last_index()),
        (4, 6),
        "and it changed nothing: entries 1..=3 are gone and stay gone"
    );
    assert_eq!(
        backwards.initial_state.index, 3,
        "and the floor itself did not regress: the report names where the store \
         actually stands rather than what was asked for. This is the assertion \
         with teeth -- the released entries do not come back either way, but a \
         floor recorded at 1 is a floor below what was released, and it is what \
         `term()` answers from and what `last_index()` falls back to once the \
         retained entries are gone"
    );
    assert_eq!(
        backwards.first_retained_index, 4,
        "and the retained range it reports is the real one"
    );

    // The boundary: the same index again is also a no-op, which is what makes
    // a repeated or retried compaction safe.
    let repeated = store
        .compact_until(MatrixRaftInitialState { index: 3, term: 3 })
        .expect("the same floor again is absorbed");
    assert_eq!(repeated.initial_state.index, 3);
    assert_eq!((store.first_index(), store.last_index()), (4, 6));
}

#[test]
fn a_compaction_further_forward_still_applies() {
    // The control. Without it the test above would also pass against a store
    // that ignored every compaction, which would mean the log never shrinks.
    let mut store = store_with_six_entries();
    store
        .compact_until(MatrixRaftInitialState { index: 3, term: 3 })
        .expect("first compaction");
    let further = store
        .compact_until(MatrixRaftInitialState { index: 5, term: 3 })
        .expect("a floor past the current one is applied");

    assert_eq!(further.initial_state.index, 5);
    assert_eq!(
        (store.first_index(), store.last_index()),
        (6, 6),
        "only the entry past the new floor is retained"
    );
}

#[test]
fn a_range_that_ends_before_it_starts_is_refused() {
    let store = store_with_six_entries();

    let error = store
        .load_entries(MatrixRaftLogRange {
            start_index: 5,
            end_index: 2,
        })
        .expect_err("an inverted range is a caller's bug, not an empty read");
    assert!(
        matches!(&error, RaftError::InvalidRequest(reason)
            if reason.contains("log range end is before start")),
        "the refusal must say what is wrong with it: {error:?}"
    );
}

#[test]
fn an_empty_range_is_not_an_inverted_one() {
    // The boundary, and the reason the rule is `<` and not `<=`: a range whose
    // start equals its end asks for nothing, which is a legitimate thing to ask
    // and must come back as no entries rather than as an error.
    let store = store_with_six_entries();

    let empty = store
        .load_entries(MatrixRaftLogRange {
            start_index: 3,
            end_index: 3,
        })
        .expect("an empty range is a valid request");
    assert!(empty.is_empty(), "and it returns nothing: {empty:?}");

    // While a range that does span entries returns them, so the refusal above
    // is not simply a store that reads nothing.
    let spanning = store
        .load_entries(MatrixRaftLogRange {
            start_index: 2,
            end_index: 5,
        })
        .expect("a spanning range is read");
    assert_eq!(spanning.len(), 3, "indexes 2, 3 and 4");
}

#[test]
fn a_segment_cannot_open_over_entries_that_already_exist() {
    let mut store = store_with_six_entries();

    let error = store
        .switch_segment(6)
        .expect_err("a segment starting at the last entry would cover it twice");
    assert!(
        matches!(&error, RaftError::InvalidRequest(reason)
            if reason.contains("is not after last log index")),
        "the refusal must name both indexes so the caller can see the overlap: {error:?}"
    );

    // The boundary one step on: the first index the existing segment does not
    // hold is exactly where the next one may begin.
    store
        .switch_segment(7)
        .expect("a segment starting past the last entry is allowed");
}
