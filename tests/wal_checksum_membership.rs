// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The WAL checksum covers the membership, now that recovery trusts it.
//!
//! The membership used to be left out of the checksum, and while nothing read
//! the field that was harmless. It is not harmless now: recovery reconciles
//! the cluster's members from it, so a membership corrupted on disk silently
//! reconfigures the group on the way back up -- a peer resurrected into the
//! quorum, or one dropped out of it -- with every other field intact and the
//! record reporting itself as valid.
//!
//! Two things here are easy to get wrong and are pinned separately:
//!
//! 1. **The lists need their lengths mixed in.** Without that the three lists
//!    are one flat stream of ids, and moving a node from `voters` to
//!    `learners` leaves the hash unchanged -- which is exactly what a
//!    demotion is, and exactly what recovery acts on.
//! 2. **A WAL written by an earlier build still has to validate.** Records
//!    this build writes carry the covering checksum; the exception closes
//!    behind each file as it is rewritten.

use matrixraft::{
    matrixraft_wal_checksum, matrixraft_wal_checksum_covers_membership,
    matrixraft_wal_checksum_format, matrixraft_wal_checksum_valid, ApplySnapshotFence, HardState,
    LogEntry, LogId, Membership, WalRecord,
};

const GROUP_ID: u64 = 44;

fn record(membership: Membership) -> WalRecord {
    let mut record = WalRecord {
        entries_are_delta: false,
        group_id: GROUP_ID,
        node_id: 1,
        hard_state: HardState {
            current_term: 4,
            voted_for: Some(1),
            committed: Some(LogId { term: 4, index: 2 }),
        },
        membership,
        entries: vec![LogEntry {
            log_id: LogId { term: 4, index: 2 },
            payload: b"entry".to_vec(),
            is_command: true,
        }],
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: 2,
            commit_index: 2,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

fn membership(voters: Vec<u64>, learners: Vec<u64>, witnesses: Vec<u64>) -> Membership {
    Membership {
        group_id: GROUP_ID,
        voters,
        learners,
        witnesses,
        epoch: 4,
    }
}

#[test]
fn changing_the_membership_changes_the_checksum() {
    let three = record(membership(vec![1, 2, 3], Vec::new(), Vec::new()));

    let mut resurrected = three.clone();
    resurrected.membership = membership(vec![1, 2, 3, 4], Vec::new(), Vec::new());
    assert!(
        !matrixraft_wal_checksum_valid(&resurrected),
        "a voter appearing in a record must not validate against its checksum"
    );

    let mut dropped = three.clone();
    dropped.membership = membership(vec![1, 2], Vec::new(), Vec::new());
    assert!(
        !matrixraft_wal_checksum_valid(&dropped),
        "nor a voter disappearing from one"
    );

    let mut other_epoch = three.clone();
    other_epoch.membership.epoch = 9;
    assert!(
        !matrixraft_wal_checksum_valid(&other_epoch),
        "nor the epoch changing"
    );

    assert!(
        matrixraft_wal_checksum_valid(&three),
        "and the record itself still validates"
    );
}

#[test]
fn moving_a_node_between_the_lists_changes_the_checksum() {
    // The same three ids in the same order, one of them demoted. If the lists
    // were hashed as one flat stream of ids this would be the same hash.
    let voters = record(membership(vec![1, 2, 3], Vec::new(), Vec::new()));
    let demoted = record(membership(vec![1, 2], vec![3], Vec::new()));
    assert_ne!(
        voters.checksum, demoted.checksum,
        "demoting node 3 to a learner has to change the checksum"
    );

    let witness = record(membership(vec![1, 2], Vec::new(), vec![3]));
    assert_ne!(
        demoted.checksum, witness.checksum,
        "and a learner is not a witness"
    );

    // And the empty lists are not interchangeable with each other either.
    let learner_only = record(membership(Vec::new(), vec![1], Vec::new()));
    let witness_only = record(membership(Vec::new(), Vec::new(), vec![1]));
    assert_ne!(
        learner_only.checksum, witness_only.checksum,
        "one node, two different roles, two different checksums"
    );
}

#[test]
fn a_record_checksummed_before_the_membership_was_covered_still_validates() {
    // What an earlier build wrote: every other field mixed, the membership
    // left out. Reproduced here by hand rather than by calling the old
    // function, so this test fails if the old shape stops being accepted for
    // any reason -- including the old function being quietly changed.
    let mut earlier = record(membership(vec![1, 2, 3], Vec::new(), Vec::new()));
    earlier.checksum = legacy_checksum(&earlier);
    assert_ne!(
        earlier.checksum,
        matrixraft_wal_checksum(&earlier),
        "the two checksums have to differ, or this test proves nothing"
    );
    assert!(
        matrixraft_wal_checksum_valid(&earlier),
        "a WAL written by an earlier build has to keep validating"
    );
    assert!(
        !matrixraft_wal_checksum_covers_membership(&earlier),
        "but it has to be possible to tell that its membership is unverified"
    );
}

#[test]
fn a_record_this_build_wrote_has_a_verified_membership() {
    let current = record(membership(vec![1, 2, 3], vec![4], vec![5]));
    assert!(matrixraft_wal_checksum_valid(&current));
    assert!(
        matrixraft_wal_checksum_covers_membership(&current),
        "a record this build checksummed vouches for its own member set"
    );

    // And the reporter is about this record's checksum, not about the build:
    // corrupt the membership and the old checksum is what it validates
    // against, so the membership stops being vouched for.
    let mut tampered = current.clone();
    tampered.checksum = legacy_checksum(&tampered);
    tampered.membership = membership(vec![1, 2, 3, 99], vec![4], vec![5]);
    assert!(
        matrixraft_wal_checksum_valid(&tampered),
        "the old checksum still validates it, which is the cost of accepting one"
    );
    assert!(
        !matrixraft_wal_checksum_covers_membership(&tampered),
        "and that is exactly the case the reporter exists to expose"
    );
}

#[test]
fn the_reported_format_says_the_membership_is_covered() {
    let format = matrixraft_wal_checksum_format();
    assert!(
        format.covered_fields.contains(&"membership".to_string()),
        "the published list of covered fields is the contract a reader checks: {format:?}"
    );
    assert_eq!(
        format.algorithm, "fnv1a64-rustraft-v2",
        "covering another field is a new algorithm, and the name has to say so"
    );
}

/// The pre-membership checksum, written out field by field.
fn legacy_checksum(record: &WalRecord) -> String {
    let mut hash = 14_695_981_039_346_656_037_u64;
    let mut mix = |value: u64| {
        for byte in value.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(1_099_511_628_211);
        }
    };
    mix(record.group_id);
    mix(record.node_id);
    mix(record.hard_state.current_term);
    mix(record.hard_state.voted_for.unwrap_or_default());
    if let Some(committed) = &record.hard_state.committed {
        mix(committed.term);
        mix(committed.index);
    }
    for entry in &record.entries {
        mix(entry.log_id.term);
        mix(entry.log_id.index);
        mix(entry.payload.len() as u64);
    }
    if record.entries_are_delta {
        mix(1);
    }
    if let Some(snapshot) = &record.installed_snapshot {
        mix(snapshot.last_log_id.term);
        mix(snapshot.last_log_id.index);
    }
    mix(record.apply_snapshot_fence.applied_index);
    mix(record.apply_snapshot_fence.commit_index);
    mix(record.apply_snapshot_fence.installed_snapshot_index);
    mix(record.apply_snapshot_fence.first_retained_log_index);
    format!("{hash:016x}")
}
