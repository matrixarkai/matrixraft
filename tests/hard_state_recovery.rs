// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Recovery skips a record that claims more than it carries.
//!
//! `matrixraft_validate_hard_state_persistence` checks two things about a WAL
//! record: that its committed term is not ahead of the term the record
//! persists, and that its committed index is not ahead of the log and snapshot
//! the record actually holds. Both recovery entry points use it to **skip** a
//! record rather than to fail:
//!
//! - `matrixraft_recover_from_wal_records` walks candidates by highest
//!   committed index and takes the first that validates, so a bad record means
//!   falling back to an earlier one.
//! - `matrixraft_recover_latest_wal_record` filters through it and then takes
//!   the maximum committed index, so without it a bad record wins the maximum.
//!
//! Neither behaviour was pinned. No-opping the validator left all 985 tests
//! passing: the only test that called it handed it a well-formed record and
//! expected `Ok`, so the rejection path -- the half that does the work -- had
//! no coverage, while its three siblings had two to five tests each.
//!
//! What that costs if it regresses: recovery accepts a record reporting a
//! committed index it cannot serve, which is the same harm the
//! log-continues-snapshot rule exists to prevent, reached by a different route.
//!
//! **Isolation.** Every bad record here is built to pass
//! `matrixraft_validate_apply_snapshot_fence` -- its fence commit index matches
//! its hard state -- so the hard-state rule is the only thing that can reject
//! it. A bad record that also failed the fence would be skipped either way and
//! would prove nothing about this rule.

use matrixraft::{
    matrixraft_recover_from_wal_records, matrixraft_recover_latest_wal_record,
    matrixraft_validate_apply_snapshot_fence, matrixraft_wal_checksum, ApplySnapshotFence,
    HardState, LogEntry, LogId, Membership, WalRecord,
};

const GROUP_ID: u64 = 271;
const NODE_ID: u64 = 1;

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: format!("payload-{index}").into_bytes(),
        is_command: false,
    }
}

/// A record carrying entries `1..=last_entry`, reporting `committed_index` as
/// committed in `committed_term`, with a fence that matches its hard state.
fn record(last_entry: u64, committed_index: u64, committed_term: u64) -> WalRecord {
    let entries: Vec<LogEntry> = (1..=last_entry).map(entry).collect();
    let mut record = WalRecord {
        group_id: GROUP_ID,
        node_id: NODE_ID,
        hard_state: HardState {
            current_term: 1,
            voted_for: Some(NODE_ID),
            committed: Some(LogId {
                term: committed_term,
                index: committed_index,
            }),
        },
        membership: Membership {
            group_id: GROUP_ID,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries,
        entries_are_delta: false,
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: committed_index,
            commit_index: committed_index,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

/// Well formed: entries 1..=2, committed 2 in term 1.
fn good() -> WalRecord {
    record(2, 2, 1)
}

/// Claims index 9 committed while carrying only entries 1..=2.
fn claims_too_high_an_index() -> WalRecord {
    record(2, 9, 1)
}

/// Claims a committed term ahead of the term it persists.
fn claims_too_high_a_term() -> WalRecord {
    record(2, 2, 9)
}

fn committed_index_of(record: &WalRecord) -> u64 {
    record
        .hard_state
        .committed
        .as_ref()
        .map(|log_id| log_id.index)
        .unwrap_or_default()
}

#[test]
fn the_bad_records_pass_the_fence_rule_so_only_the_hard_state_rule_can_reject_them() {
    // The premise of every test below. If a bad record also failed the fence
    // rule it would be skipped either way, and nothing here would be evidence
    // about the hard-state rule.
    for (label, candidate) in [
        ("index", claims_too_high_an_index()),
        ("term", claims_too_high_a_term()),
    ] {
        matrixraft_validate_apply_snapshot_fence(&candidate).unwrap_or_else(|error| {
            panic!("the {label} record has to pass the fence rule, but: {error}")
        });
    }
}

#[test]
fn a_record_claiming_more_than_it_carries_does_not_win_recovery() {
    let recovered = matrixraft_recover_latest_wal_record(&[good(), claims_too_high_an_index()])
        .expect("the well formed record is recoverable");
    assert_eq!(
        committed_index_of(&recovered),
        2,
        "a record reporting index 9 committed while carrying entries 1 and 2 \
         must not win the highest-committed-index contest: recovering it gives \
         a node that reports a commit index it cannot serve"
    );
}

#[test]
fn a_record_whose_committed_term_is_ahead_does_not_win_either() {
    // The validator's other branch, which the index case does not exercise.
    let recovered = matrixraft_recover_latest_wal_record(&[good(), claims_too_high_a_term()])
        .expect("the well formed record is recoverable");
    assert_eq!(
        recovered
            .hard_state
            .committed
            .as_ref()
            .map(|log_id| log_id.term),
        Some(1),
        "a committed term ahead of the persisted current term is not a term \
         this record can vouch for"
    );
}

#[test]
fn the_folding_recovery_path_skips_it_too() {
    // The other entry point, which walks candidates by highest committed index
    // and takes the first that validates rather than filtering.
    let (surviving, recovered) =
        matrixraft_recover_from_wal_records(&[good(), claims_too_high_an_index()]);
    assert_eq!(surviving, 2, "both records survive the checksum pass");
    let recovered = recovered.expect("recovery falls back to the well formed record");
    assert_eq!(
        committed_index_of(&recovered),
        2,
        "the fold path has to fall back rather than accept the higher claim"
    );
}

#[test]
fn nothing_valid_is_an_error_rather_than_a_bad_record() {
    let error = matrixraft_recover_latest_wal_record(&[claims_too_high_an_index()])
        .expect_err("a record that claims more than it carries is not a fallback");
    assert!(
        error.to_string().contains("no valid WAL record survived"),
        "{error}"
    );
}

#[test]
fn the_highest_well_formed_committed_index_still_wins() {
    // The positive control. A recovery that always preferred the lower index,
    // or that rejected everything, would satisfy the tests above.
    let recovered =
        matrixraft_recover_latest_wal_record(&[good(), record(5, 5, 1)]).expect("recoverable");
    assert_eq!(
        committed_index_of(&recovered),
        5,
        "among well formed records the highest committed index is still the one \
         to recover"
    );
}
