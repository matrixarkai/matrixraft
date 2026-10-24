// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The state machine refuses to apply a second entry at an index it has
//! already applied **at a different term**. That is the apply-time half of
//! Raft's log matching: two entries at one index from two terms are two
//! different histories, and applying the second over the first silently
//! corrupts the machine.
//!
//! `FsmAdapter` enforces it in **two places**, because `apply_batch` dispatches
//! on the entry's kind:
//!
//! | path | entry kind | had a test |
//! |---|---|---|
//! | `apply_entry` | `Data` | yes, `fsm_adapter_rejects_conflicting_replay_at_same_index` |
//! | the `NoOp` arm of `apply_batch` | `NoOp` | **no** |
//!
//! Deleting the `NoOp` arm's check left all 189 test targets green.
//!
//! **The untested path is the one a new leader's entry takes.** A leader
//! appends a no-op on election -- `RaftCluster::start` does exactly that -- so
//! a no-op arriving at an index the machine has already applied **is** the
//! shape of a cross-term collision after a leadership change. The data path
//! catches a data entry landing on an applied index; nothing caught a no-op
//! doing it, which is the likelier direction.

use matrixraft::{
    FsmAdapter, LogId, RaftApply, RaftApplyRequest, RaftApplyResponse, RaftError, RaftLogEntry,
    RaftStateMachine,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CountingFsm {
    applied: Vec<(u64, Vec<u8>)>,
}

impl RaftApply<u64, Vec<u8>> for CountingFsm {
    type Response = Vec<u8>;

    fn apply(
        &mut self,
        request: RaftApplyRequest<u64, Vec<u8>>,
    ) -> Result<RaftApplyResponse<Self::Response>, RaftError> {
        self.applied
            .push((request.log_id.index, request.payload.clone()));
        Ok(RaftApplyResponse {
            applied_index: request.log_id.index,
            response: request.payload,
        })
    }
}

impl RaftStateMachine<u64, Vec<u8>> for CountingFsm {
    type Snapshot = Vec<(u64, Vec<u8>)>;

    fn snapshot(&self, _group_id: u64) -> Result<Self::Snapshot, RaftError> {
        Ok(self.applied.clone())
    }

    fn install_snapshot(&mut self, snapshot: Self::Snapshot) -> Result<(), RaftError> {
        self.applied = snapshot;
        Ok(())
    }
}

fn data(term: u64, index: u64, payload: &[u8]) -> RaftLogEntry<Vec<u8>> {
    RaftLogEntry {
        log_id: LogId { term, index },
        payload: payload.to_vec(),
        is_command: true,
    }
}

/// A no-op: no payload and not a command, which is what a leader appends on
/// election.
fn noop(term: u64, index: u64) -> RaftLogEntry<Vec<u8>> {
    RaftLogEntry {
        log_id: LogId { term, index },
        payload: Vec::new(),
        is_command: false,
    }
}

fn adapter() -> FsmAdapter<CountingFsm, u64, Vec<u8>> {
    FsmAdapter::new(7, CountingFsm::default())
}

fn conflict_at(error: &RaftError, index: u64, existing: u64, replay: u64) -> bool {
    matches!(error, RaftError::InvalidRequest(reason) if reason
    == &format!(
        "FSM replay conflict at index {index}: existing term {existing}, replay term {replay}"
    ))
}

#[test]
fn a_noop_colliding_with_an_applied_noop_is_refused() {
    let mut adapter = adapter();
    adapter
        .apply_batch(&[noop(3, 5)], 10)
        .expect("a no-op at index 5, term 3");

    let error = adapter
        .apply_batch(&[noop(4, 5)], 10)
        .expect_err("a no-op at the same index from a later term is a conflict");
    assert!(
        conflict_at(&error, 5, 3, 4),
        "the refusal must name the index and both terms: {error:?}"
    );
}

#[test]
fn a_noop_colliding_with_an_applied_data_entry_is_refused() {
    // The case that matters most, and the one the untested path exists for: a
    // new leader appends a no-op on election, so a no-op landing on an index
    // the machine already applied as DATA is the real shape of a cross-term
    // collision after a leadership change.
    let mut adapter = adapter();
    adapter
        .apply_batch(&[data(3, 5, b"set-a")], 10)
        .expect("a data entry at index 5, term 3");

    let error = adapter
        .apply_batch(&[noop(4, 5)], 10)
        .expect_err("a term-4 no-op must not overwrite a term-3 applied entry");
    assert!(conflict_at(&error, 5, 3, 4), "{error:?}");

    assert_eq!(
        adapter.inner().applied,
        vec![(5, b"set-a".to_vec())],
        "and the machine still holds what it applied -- the refusal is what \
         keeps the no-op from being recorded over it"
    );
}

#[test]
fn a_data_entry_colliding_with_an_applied_noop_is_refused() {
    // The other path, already covered elsewhere, asserted here because the two
    // belong together: the rule is about an INDEX and a TERM, not about what
    // kind of entry either side was.
    let mut adapter = adapter();
    adapter
        .apply_batch(&[noop(3, 5)], 10)
        .expect("a no-op at index 5, term 3");

    let error = adapter
        .apply_batch(&[data(4, 5, b"set-a")], 10)
        .expect_err("a term-4 data entry must not overwrite a term-3 no-op");
    assert!(conflict_at(&error, 5, 3, 4), "{error:?}");
}

#[test]
fn the_same_noop_at_the_same_term_replays_rather_than_conflicting() {
    // The control, and it is the reason the rule compares terms rather than
    // simply refusing a repeated index: a retried or re-delivered entry is
    // ordinary and must be absorbed, not treated as corruption.
    let mut adapter = adapter();

    let first = adapter.apply_batch(&[noop(3, 5)], 10).expect("first");
    assert_eq!(first.applied, 1, "a first no-op is applied");
    assert_eq!(
        first.skipped_noop, 1,
        "and counted as a skipped no-op as well, since it carries no payload \
         for the machine -- both counters move for one entry"
    );
    assert_eq!(first.skipped_replay, 0);

    let again = adapter
        .apply_batch(&[noop(3, 5)], 10)
        .expect("the same no-op again is absorbed, not refused");
    assert_eq!(again.applied, 0, "nothing was applied the second time");
    assert_eq!(
        again.skipped_replay, 1,
        "it is counted as a replay, which is how a caller distinguishes an \
         absorbed retry from a conflict"
    );
}
