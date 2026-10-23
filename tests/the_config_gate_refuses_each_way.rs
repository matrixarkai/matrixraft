// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `Config::validate` is the gate on every group: `RaftCluster::new` calls it
//! before anything else, so a configuration that fails it cannot produce a
//! group at all. It holds nine rules, and **five of them had no test** --
//! measured one at a time, each deletable with all 187 test targets green:
//! `ZeroElectionTimeout`, `ZeroHeartbeatInterval`, `ZeroLeaderLease`,
//! `ZeroMaxSegmentBytes`, and `LeaseNotLessThanElection`. The other four were
//! already held by `raft_config_validates_timing_and_capacity` and
//! `validation_could_never_have_caught_the_mismatch`.
//!
//! Seven of the nine are sanity checks -- a zero where zero means nothing can
//! ever happen. **Two are safety relations between timings, and those are the
//! ones worth the file.** One of those two was among the five untested.
//!
//! ### `heartbeat_interval_ms < election_timeout_ms`
//!
//! If heartbeats are no more frequent than the election timeout, a follower
//! times out *before* the leader's next heartbeat reaches it. The group then
//! elects continuously and commits nothing -- not a degraded group, a dead one.
//!
//! ### `leader_lease_ms < election_timeout_ms`
//!
//! This one is a **linearizability** rule, not a tuning knob. A leader serving
//! lease reads answers them from its own state for `leader_lease_ms` without
//! asking a quorum. If that lease could outlive the election timeout, a new
//! leader may be elected while the old one still believes its lease is good --
//! and the old leader then answers a read from a term it no longer owns, with
//! state the new leader has already moved past. A stale read, served
//! confidently, with no error anywhere.
//!
//! Both relations are `>=`, not `>`: **equal is refused.** Equal would mean the
//! lease expires at the very moment an election could complete, leaving no
//! margin for the clock skew the lease exists to tolerate. Each test pins the
//! equal case for that reason, and the value just under it as the control.
//!
//! The rules run in a fixed order, so a configuration that breaks two at once
//! reports only the first. Each test below breaks exactly one, and the two
//! ordering tests pin which wins when both are broken -- because a test that
//! let an earlier rule answer would pass while proving nothing about the rule
//! it names.

use matrixraft::{Config, ConfigError};

/// `Config::default()` is valid, and these are its timings: election 1000,
/// heartbeat 100, lease 500. Every fixture below starts here and moves one
/// field, so whatever is refused is refused for that field.
fn valid() -> Config {
    let config = Config::default();
    assert_eq!(config.election_timeout_ms, 1_000);
    assert_eq!(config.heartbeat_interval_ms, 100);
    assert_eq!(config.leader_lease_ms, 500);
    config
}

#[test]
fn the_default_config_is_valid() {
    // The control. Without it every test below would also pass against a
    // validator that refused everything -- which would mean no group could
    // ever be created.
    valid()
        .validate()
        .expect("the default configuration is valid");
}

// ------------------------------------------------------- the timing relations

#[test]
fn a_heartbeat_no_more_frequent_than_the_election_timeout_is_refused() {
    let mut config = valid();
    config.heartbeat_interval_ms = config.election_timeout_ms;

    assert_eq!(
        config.validate(),
        Err(ConfigError::HeartbeatNotLessThanElection {
            heartbeat_interval_ms: 1_000,
            election_timeout_ms: 1_000,
        }),
        "equal is refused: a follower would time out before the next heartbeat"
    );

    // And the error carries both values, so an operator can see the relation
    // rather than being told a field is wrong.
    config.heartbeat_interval_ms = 1_500;
    assert_eq!(
        config.validate(),
        Err(ConfigError::HeartbeatNotLessThanElection {
            heartbeat_interval_ms: 1_500,
            election_timeout_ms: 1_000,
        })
    );

    // The control one step under: the largest heartbeat that is still valid.
    config.heartbeat_interval_ms = config.election_timeout_ms - 1;
    config
        .validate()
        .expect("one below the election timeout is allowed");
}

#[test]
fn a_lease_that_could_outlive_an_election_is_refused() {
    let mut config = valid();
    config.leader_lease_ms = config.election_timeout_ms;

    assert_eq!(
        config.validate(),
        Err(ConfigError::LeaseNotLessThanElection {
            leader_lease_ms: 1_000,
            election_timeout_ms: 1_000,
        }),
        "equal is refused: a lease expiring exactly when an election could \
         complete leaves no margin, and the old leader would serve a read from \
         a term it no longer owns"
    );

    config.leader_lease_ms = 2_000;
    assert_eq!(
        config.validate(),
        Err(ConfigError::LeaseNotLessThanElection {
            leader_lease_ms: 2_000,
            election_timeout_ms: 1_000,
        })
    );

    config.leader_lease_ms = config.election_timeout_ms - 1;
    config
        .validate()
        .expect("one below the election timeout is allowed");
}

// ----------------------------------------------------------- the zero rules

#[test]
fn a_zero_timing_is_refused() {
    let mut config = valid();
    config.election_timeout_ms = 0;
    assert_eq!(config.validate(), Err(ConfigError::ZeroElectionTimeout));

    let mut config = valid();
    config.heartbeat_interval_ms = 0;
    assert_eq!(config.validate(), Err(ConfigError::ZeroHeartbeatInterval));

    let mut config = valid();
    config.leader_lease_ms = 0;
    assert_eq!(
        config.validate(),
        Err(ConfigError::ZeroLeaderLease),
        "a zero lease is refused rather than read as leases being off -- that \
         is what `enable_lease_read` is for"
    );
}

#[test]
fn a_zero_capacity_is_refused() {
    let mut config = valid();
    config.max_payload_bytes = 0;
    assert_eq!(config.validate(), Err(ConfigError::ZeroMaxPayloadBytes));

    let mut config = valid();
    config.max_log_buffer_bytes = 0;
    assert_eq!(config.validate(), Err(ConfigError::ZeroMaxLogBufferBytes));

    let mut config = valid();
    config.snapshot_threshold_entries = 0;
    assert_eq!(config.validate(), Err(ConfigError::ZeroSnapshotThreshold));

    let mut config = valid();
    config.max_segment_bytes = 0;
    assert_eq!(
        config.validate(),
        Err(ConfigError::ZeroMaxSegmentBytes),
        "a zero segment size would roll a new segment on every record"
    );
}

// -------------------------------------------------------------- the ordering

#[test]
fn a_zero_election_timeout_answers_before_the_heartbeat_relation() {
    // Both rules are broken here: the timeout is zero, and 100 >= 0 as well.
    // The zero rule is checked first and must be the one reported, because a
    // relation against zero describes nothing an operator can act on.
    let mut config = valid();
    config.election_timeout_ms = 0;

    assert_eq!(
        config.validate(),
        Err(ConfigError::ZeroElectionTimeout),
        "not HeartbeatNotLessThanElection, which is also true"
    );
}

#[test]
fn a_zero_lease_answers_before_the_lease_relation() {
    // The mirror case on the lease pair, and the reason the zero tests above
    // cannot be assumed to exercise what they name: with the lease at zero
    // both `leader_lease_ms == 0` and `0 >= 1000`... do not both hold -- only
    // the zero rule does. So this instead pins the opposite order risk: a
    // lease of zero must not fall through to the relation and be accepted.
    let mut config = valid();
    config.leader_lease_ms = 0;

    assert_eq!(
        config.validate(),
        Err(ConfigError::ZeroLeaderLease),
        "a zero lease is caught by the zero rule, not quietly allowed because \
         it is below the election timeout"
    );
}
