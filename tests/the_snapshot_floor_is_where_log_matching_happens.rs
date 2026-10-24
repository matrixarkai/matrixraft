// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Raft's log matching normally compares an append's `prev_log_id` against an
//! entry the node still holds. **Once a snapshot has compacted that entry
//! away, there is nothing to compare against** -- and the snapshot's own
//! `last_log_id` takes over the job. That is what these two validators are
//! for, and they are the reason a compacted node can still verify an append
//! rather than accepting it on faith.
//!
//! `matrixraft_validate_snapshot_floor_log_matching` holds three rules and
//! `matrixraft_validate_snapshot_install` one more:
//!
//! | rule | what an unchecked node would do |
//! |---|---|
//! | the retained log must begin **strictly after** the snapshot | replay entries the snapshot already contains, applying them twice |
//! | `prev_log_id` must not be **below** the floor | accept an append it cannot verify, because the entries it names are gone |
//! | at the floor, `prev_log_id`'s **term** must match the floor's | accept an append from a different history that happens to agree on the index |
//! | the install fence must **name** the snapshot it ships with | install a snapshot while recording a different one as installed |
//!
//! **Three of them had no test**, each deletable with all 188 test targets
//! green, and measured one at a time:
//!
//! - the `prev_log_id` **below-the-floor** rule;
//! - the whole **install-fence** rule. Its only existing call was
//!   `expect("valid install")` on a fence that was already correct -- the
//!   positive-only shape that cannot distinguish a validator from a stub;
//! - the overlap rule's **`first_retained_log_index > 0` clause**. The rule as
//!   a whole *is* held, which is exactly why the clause needed planting
//!   separately: zero means "no log retained", not "a log starting at zero",
//!   and a reader collapsing the two clauses into one comparison would refuse
//!   every freshly installed snapshot -- a node that has just installed one has
//!   nothing retained yet.
//!
//! The term rule and the overlap rule's main clause were already held, and are
//! asserted here anyway because all four refusals are `RaftError::Storage` and
//! only the inputs say which rule answered.

use matrixraft::{
    snapshot::{
        matrixraft_validate_snapshot_floor_log_matching, matrixraft_validate_snapshot_install,
        RaftSnapshot, SnapshotMetadata,
    },
    ApplySnapshotFence, LogId, RaftError,
};

/// A snapshot whose floor is index 10 at term 4.
fn snapshot_meta() -> SnapshotMetadata {
    SnapshotMetadata {
        snapshot_id: "floor-10".to_string(),
        last_log_id: LogId { term: 4, index: 10 },
        membership: vec![1, 2, 3],
        members: Vec::new(),
        payload_bytes: 0,
    }
}

fn snapshot() -> RaftSnapshot {
    RaftSnapshot {
        group_id: 7,
        meta: snapshot_meta(),
        payload: b"opaque snapshot bytes".to_vec(),
    }
}

/// A fence that names the snapshot above and a log beginning one past it.
fn matching_fence() -> ApplySnapshotFence {
    ApplySnapshotFence {
        applied_index: 10,
        commit_index: 10,
        installed_snapshot_index: 10,
        first_retained_log_index: 11,
    }
}

fn refusal(result: Result<(), RaftError>) -> String {
    match result {
        Err(RaftError::Storage(reason)) => reason,
        other => panic!("expected a storage refusal, got {other:?}"),
    }
}

// ----------------------------------------------------------- the floor itself

#[test]
fn a_log_that_begins_inside_the_snapshot_is_refused() {
    let meta = snapshot_meta();

    // At the floor: index 10 is in the snapshot, so a log retaining from 10
    // would replay it.
    assert_eq!(
        refusal(matrixraft_validate_snapshot_floor_log_matching(
            &meta, 10, None
        )),
        "first retained log index overlaps snapshot floor"
    );
    // And below it.
    assert_eq!(
        refusal(matrixraft_validate_snapshot_floor_log_matching(
            &meta, 5, None
        )),
        "first retained log index overlaps snapshot floor"
    );

    // The boundary: one past the floor is exactly where a log may begin.
    matrixraft_validate_snapshot_floor_log_matching(&meta, 11, None)
        .expect("a log beginning one past the snapshot does not overlap it");
}

#[test]
fn no_retained_log_is_not_an_overlap_at_zero() {
    // The second clause of the overlap rule, and the reason it is two clauses:
    // zero means the node retains NO log, not a log starting at index 0. A
    // reader collapsing this into `first_retained <= floor` would refuse every
    // freshly installed snapshot, since a node that has just installed one has
    // nothing retained yet.
    matrixraft_validate_snapshot_floor_log_matching(&snapshot_meta(), 0, None)
        .expect("a node retaining no log has nothing to overlap the snapshot");
}

// -------------------------------------------- log matching against the floor

#[test]
fn an_append_pointing_below_the_floor_is_refused() {
    let meta = snapshot_meta();

    // The entries at 9 and below are gone, so an append claiming to follow one
    // of them cannot be verified at all -- and accepting it on faith is how a
    // compacted node ends up with a log that diverges silently.
    assert_eq!(
        refusal(matrixraft_validate_snapshot_floor_log_matching(
            &meta,
            11,
            Some(&LogId { term: 4, index: 9 }),
        )),
        "previous log id is below snapshot floor"
    );

    // The boundary: the floor itself is verifiable, because the snapshot
    // carries its index AND its term.
    matrixraft_validate_snapshot_floor_log_matching(&meta, 11, Some(&LogId { term: 4, index: 10 }))
        .expect("an append following the floor is verifiable against the snapshot");

    // And anything past the floor is the ordinary log-matching case, which is
    // not this validator's business.
    matrixraft_validate_snapshot_floor_log_matching(&meta, 11, Some(&LogId { term: 4, index: 11 }))
        .expect("past the floor, the log itself answers");
}

#[test]
fn an_append_at_the_floor_with_the_wrong_term_is_refused() {
    // Already held elsewhere, asserted here because it is the other half of
    // the same property: at the floor, the snapshot's `last_log_id` stands in
    // for a log entry, and a log id is an index AND a term. Agreeing on the
    // index alone is exactly the case Raft's log matching exists to reject.
    let meta = snapshot_meta();

    assert_eq!(
        refusal(matrixraft_validate_snapshot_floor_log_matching(
            &meta,
            11,
            Some(&LogId { term: 3, index: 10 }),
        )),
        "snapshot floor term does not match previous log id"
    );
}

// ------------------------------------------------------------ the install fence

#[test]
fn an_install_fence_must_name_the_snapshot_it_ships_with() {
    let snapshot = snapshot();
    let mut fence = matching_fence();

    fence.installed_snapshot_index = 9;
    assert_eq!(
        refusal(matrixraft_validate_snapshot_install(&snapshot, &fence)),
        "snapshot install fence does not match snapshot last log index",
        "a fence naming a different index would record one snapshot as \
         installed while a different one was applied"
    );

    // Past it as well as short of it -- the rule is an equality, not a bound.
    fence.installed_snapshot_index = 11;
    assert_eq!(
        refusal(matrixraft_validate_snapshot_install(&snapshot, &fence)),
        "snapshot install fence does not match snapshot last log index"
    );
}

#[test]
fn a_matching_install_is_accepted() {
    // The control. Without it every test above would also pass against a
    // validator that refused every install, which would stop a lagging node
    // ever being caught up.
    matrixraft_validate_snapshot_install(&snapshot(), &matching_fence())
        .expect("a fence naming this snapshot, over a log beginning past it, is valid");
}

#[test]
fn the_install_validator_also_applies_the_floor_rules() {
    // `matrixraft_validate_snapshot_install` delegates to the floor validator
    // after its own check, which is worth pinning: the delegation is the only
    // thing applying the overlap rule on the install path, and deleting it
    // would leave installs unchecked against a log that overlaps them.
    let snapshot = snapshot();
    let mut fence = matching_fence();
    fence.first_retained_log_index = 10; // overlaps the floor at 10

    assert_eq!(
        refusal(matrixraft_validate_snapshot_install(&snapshot, &fence)),
        "first retained log index overlaps snapshot floor",
        "the install path inherits the floor rules rather than restating them"
    );
}
