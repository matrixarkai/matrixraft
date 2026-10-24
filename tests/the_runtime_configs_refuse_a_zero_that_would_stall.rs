// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Three runtime configurations are validated before anything uses them, and
//! between them they hold seven rules. All seven refuse a zero, and none of
//! them is a tidiness check: **each zero would leave a subsystem unable to make
//! progress at all**, which is worse than a configuration that fails loudly.
//!
//! | config | field | what zero would mean |
//! |---|---|---|
//! | `SnapshotLifecycleConfig` | `chunk_size` | a snapshot send that never finishes -- zero-byte chunks, so the transfer makes no progress however many it sends |
//! | | `max_chunks_per_tick` | no chunk is ever sent |
//! | | `max_bytes_per_tick` | no byte is ever sent |
//! | `PersistentRaftWalOptions` | `max_records_per_segment` | a segment roll on every record |
//! | | `max_segment_bytes` | the same, by the other bound |
//! | | **`min_keep_segments`** | **compaction may release every segment, including the one holding the newest records** |
//! | `LocalRaftWal::new` | `max_records_per_segment` | as above, at the other constructor |
//!
//! `min_keep_segments` is the one with teeth: the others stall a subsystem,
//! that one can discard the only copy of recent records. And the last row is
//! the same rule as the fourth, at a second entry point -- a pattern this suite
//! keeps finding, so both are asserted rather than one standing for the other.
//!
//! Note what is **not** refused: `max_retry_attempts` has no zero rule, because
//! zero retries is a legitimate policy -- "do not retry" -- rather than a
//! stall. A test that assumed every numeric field rejects zero would be wrong
//! about that one, so the last test pins it.

use matrixraft::{LocalRaftWal, PersistentRaftWalOptions, RaftError, SnapshotLifecycleConfig};

fn refusal(result: Result<(), RaftError>) -> String {
    match result {
        Err(RaftError::InvalidRequest(reason)) => reason,
        other => panic!("expected an invalid-request refusal, got {other:?}"),
    }
}

#[test]
fn the_default_runtime_configs_are_valid() {
    // The control for everything below: without it, each test would also pass
    // against a validator that refused every configuration.
    SnapshotLifecycleConfig::default()
        .validate()
        .expect("the default snapshot lifecycle config is valid");
    PersistentRaftWalOptions::new("/tmp/matrixraft-config-test")
        .validate()
        .expect("the default persistent WAL options are valid");
    LocalRaftWal::new(1_000).expect("a thousand records a segment is valid");
}

#[test]
fn a_snapshot_send_that_could_not_progress_is_refused() {
    let zero_chunk_size = SnapshotLifecycleConfig {
        chunk_size: 0,
        ..SnapshotLifecycleConfig::default()
    };
    assert_eq!(
        refusal(zero_chunk_size.validate()),
        "snapshot chunk_size must be greater than zero",
        "zero-byte chunks would never finish a transfer, however many were sent"
    );

    let no_chunks_a_tick = SnapshotLifecycleConfig {
        max_chunks_per_tick: 0,
        ..SnapshotLifecycleConfig::default()
    };
    assert_eq!(
        refusal(no_chunks_a_tick.validate()),
        "snapshot max_chunks_per_tick must be greater than zero"
    );

    let no_bytes_a_tick = SnapshotLifecycleConfig {
        max_bytes_per_tick: 0,
        ..SnapshotLifecycleConfig::default()
    };
    assert_eq!(
        refusal(no_bytes_a_tick.validate()),
        "snapshot max_bytes_per_tick must be greater than zero"
    );

    // One is enough for each of the two budgets, which is what makes them
    // budgets rather than switches.
    let config = SnapshotLifecycleConfig {
        chunk_size: 1,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 1,
        ..SnapshotLifecycleConfig::default()
    };
    config
        .validate()
        .expect("one of each is a slow send, not an invalid one");
}

#[test]
fn a_wal_that_could_not_hold_a_segment_is_refused() {
    let base = PersistentRaftWalOptions::new("/tmp/matrixraft-config-test");

    let mut options = base.clone();
    options.max_records_per_segment = 0;
    assert_eq!(
        refusal(options.validate()),
        "max_records_per_segment must be greater than zero",
        "a segment that may hold no records rolls on every append"
    );

    let mut options = base.clone();
    options.max_segment_bytes = 0;
    assert_eq!(
        refusal(options.validate()),
        "max_segment_bytes must be greater than zero"
    );

    let options = PersistentRaftWalOptions {
        max_records_per_segment: 1,
        max_segment_bytes: 1,
        ..base
    };
    options
        .validate()
        .expect("one record and one byte is a pathological WAL, not an invalid one");
}

#[test]
fn a_wal_that_may_keep_no_segments_is_refused() {
    // The rule with teeth. The others stall a subsystem; this one lets
    // compaction release EVERY segment -- including the one holding the newest
    // records -- so a node could discard its only copy of what it just wrote.
    let mut options = PersistentRaftWalOptions::new("/tmp/matrixraft-config-test");
    options.min_keep_segments = 0;

    assert_eq!(
        refusal(options.validate()),
        "min_keep_segments must be greater than zero"
    );

    // One is the boundary, and the default is two: keeping exactly one is
    // valid, because the segment being written is the one that must survive.
    options.min_keep_segments = 1;
    options
        .validate()
        .expect("keeping one segment is the minimum that keeps the live one");
}

#[test]
fn the_other_wal_constructor_holds_the_same_rule() {
    // The same rule at a second entry point. `LocalRaftWal::new` takes the
    // record bound directly rather than through an options struct, and this
    // suite has repeatedly found a rule held at one site and not another, so
    // it is asserted here rather than assumed from the options validator.
    let error = LocalRaftWal::new(0).expect_err("a segment may not hold no records");
    assert!(
        matches!(&error, RaftError::InvalidRequest(reason)
            if reason == "max_records_per_segment must be greater than zero"),
        "{error:?}"
    );

    LocalRaftWal::new(1).expect("one record a segment is valid");
}

#[test]
fn zero_retries_is_a_policy_rather_than_a_stall() {
    // What is deliberately NOT refused, and the reason this file does not
    // simply assert "every numeric field rejects zero": `max_retry_attempts`
    // of zero means "do not retry", which is a choice an operator may make.
    // A validator that refused it would be imposing a retry policy.
    let config = SnapshotLifecycleConfig {
        max_retry_attempts: 0,
        ..SnapshotLifecycleConfig::default()
    };
    config
        .validate()
        .expect("zero retries is a policy, not a stalled subsystem");
}
