// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The two tick-valued timeouts a host sets actually time things out.
//!
//! `MatrixRaftOptions::transfer_timeout_tick` and `offline_timeout_tick` were
//! declared, defaulted, filled in by every example in the tree, and read
//! nowhere. The behaviours they name both existed and both ran off
//! `election_timeout_ms`: a leader transfer was abandoned after
//! `election_timeout_ms / heartbeat_interval_ms` ticks, and a peer was marked
//! offline after `election_timeout_ms`. So a host could set either to anything
//! and get the election interval.
//!
//! Every test here picks numbers that differ from each other **and** from the
//! value the engine used to derive, because a timeout that happens to equal
//! the old one proves nothing.

use matrixraft::{Config, MatrixRaftOptions, Peer, RaftCluster, ReplicaRole};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

/// 1,000ms election over a 100ms heartbeat, so the value the engine used to
/// derive for the transfer timeout is 10 ticks and the offline timeout was
/// 1,000ms. Nothing below uses either number as its configured value.
fn config() -> Config {
    Config {
        election_timeout_ms: 1_000,
        heartbeat_interval_ms: 100,
        ..Config::default()
    }
}

fn cluster_with(config: Config) -> RaftCluster {
    let mut cluster = RaftCluster::new(
        7,
        config,
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
}

#[test]
fn an_unset_transfer_timeout_still_derives_from_the_election_timeout() {
    // The old behaviour has to survive, or every host that never set this gets
    // a different timeout than it had.
    let mut cluster = cluster_with(config());
    assert_eq!(cluster.config.leader_transfer_timeout_ticks, 0);
    cluster.set_node_healthy(2, false).expect("hold back");
    let transfer = cluster
        .begin_leader_transfer(2)
        .expect("begin")
        .expect("in flight");
    assert_eq!(transfer.timeout_ticks, 10, "1,000ms over a 100ms heartbeat");
}

#[test]
fn a_configured_transfer_timeout_is_the_one_used() {
    let mut cluster = cluster_with(Config {
        leader_transfer_timeout_ticks: 3,
        ..config()
    });
    cluster.set_node_healthy(2, false).expect("hold back");
    let transfer = cluster
        .begin_leader_transfer(2)
        .expect("begin")
        .expect("in flight");
    assert_eq!(
        transfer.timeout_ticks, 3,
        "the configured count, not the derived 10"
    );

    // And it is the count that actually abandons the transfer, measured rather
    // than assumed: the record says how many ticks it ran.
    let mut ticks = 0;
    while cluster.leader_transfer_state().is_some() {
        cluster.tick_leader_transfer();
        ticks += 1;
        assert!(ticks <= 11, "a 3-tick timeout must not take 10 ticks");
    }
    assert_eq!(ticks, 3);
    assert_eq!(
        cluster
            .last_leader_transfer()
            .expect("the record survives")
            .reason,
        "transfer_timeout"
    );
}

#[test]
fn an_unset_offline_timeout_is_still_the_election_timeout() {
    let cluster = cluster_with(config());
    assert_eq!(cluster.config.peer_offline_timeout_ticks, 0);
    assert_eq!(cluster.peer_offline_timeout_ms(), 1_000);
}

#[test]
fn a_configured_offline_timeout_is_the_one_used() {
    let cluster = cluster_with(Config {
        peer_offline_timeout_ticks: 4,
        ..config()
    });
    assert_eq!(
        cluster.peer_offline_timeout_ms(),
        400,
        "4 ticks of a 100ms heartbeat, not the 1,000ms election timeout"
    );
}

#[test]
fn a_configured_offline_timeout_is_when_a_peer_is_marked_offline() {
    let mut cluster = cluster_with(Config {
        peer_offline_timeout_ticks: 4,
        ..config()
    });
    cluster.campaign(1, false).ok();

    // Four ticks of 100ms is 400ms, and the check is `>` not `>=`, so the
    // fifth tick is the one that fires. Ticking to 400 exactly must NOT fire,
    // which is what pins the boundary rather than stepping over it.
    for _ in 0..4 {
        assert!(
            cluster.tick_peer_liveness(100).is_empty(),
            "nothing is offline at or below the configured timeout"
        );
    }
    let timed_out = cluster.tick_peer_liveness(100);
    assert!(
        !timed_out.is_empty(),
        "past 400ms the peers are offline, so the configured 4 ticks decided it"
    );

    // The derived timeout would have been 1,000ms: ten ticks. Reaching the
    // decision in five is the whole claim.
    assert!(timed_out.contains(&2) || timed_out.contains(&3));
}

#[test]
fn an_unset_offline_timeout_takes_the_full_election_timeout() {
    // The other side of the previous test, and the one that would catch a
    // change that simply shortened the timeout for everybody.
    let mut cluster = cluster_with(config());
    cluster.campaign(1, false).ok();
    for _ in 0..10 {
        assert!(
            cluster.tick_peer_liveness(100).is_empty(),
            "an unset timeout must not fire before the election timeout"
        );
    }
    assert!(
        !cluster.tick_peer_liveness(100).is_empty(),
        "and must fire once past it"
    );
}

/// The options struct has no `Default`, so the whole literal lives here and the
/// two tick counts are the only things a caller varies.
fn options(transfer_timeout_tick: u64, offline_timeout_tick: u64) -> MatrixRaftOptions {
    MatrixRaftOptions {
        group_id: 7,
        peer_id: 1,
        raft_addr: "127.0.0.1:9001".to_string(),
        snapshot_addr: "127.0.0.1:10001".to_string(),
        wal_dir: "/tmp/does-not-need-to-exist".to_string(),
        snapshot_dir: "/tmp/does-not-need-to-exist".to_string(),
        peers: vec![peer(1, ReplicaRole::Voter), peer(2, ReplicaRole::Voter)],
        role: ReplicaRole::Voter,
        wal_sync: true,
        election_cycle_tick: 5,
        transfer_timeout_tick,
        offline_timeout_tick,
        tick_interval_ms: 100,
        lease_duration_ms: 250,
        last_lease_duration_ms: 200,
        assume_lease_when_start: false,
        max_memory_replicate_log_bytes: 64 * 1024,
        max_disk_replicate_log_num: 64,
        max_cache_memory_bytes: 16 * 1024 * 1024,
        max_apply_batch_bytes: 64 * 1024,
        enable_reorder_queue: true,
        reorder_timeout_us: 3_000,
        reorder_window_size: 128,
        max_inflights_apply_task: 5,
        max_inflights_replicate: 128,
        enable_pre_vote: true,
        max_segment_bytes: 64 * 1024 * 1024,
        min_keep_segment_num: 2,
        can_trigger_snapshot: true,
        max_applied_log_bytes: u64::MAX,
        send_snapshot_timeout_ms: 60_000,
    }
}

#[test]
fn the_options_struct_carries_both_timeouts_into_the_config() {
    let config = options(3, 7).to_raft_config();
    // 3, 7, and the derived 5 are all different, so a conversion that crosses
    // the two settings or falls back to deriving is visible here.
    assert_eq!(config.leader_transfer_timeout_ticks, 3);
    assert_eq!(config.peer_offline_timeout_ticks, 7);
    assert_eq!(
        config.election_timeout_ms / config.heartbeat_interval_ms,
        5,
        "the derived value, which neither setting may be confused with"
    );
}

#[test]
fn a_tick_count_too_wide_for_the_field_saturates_rather_than_wrapping() {
    // A wrapped timeout is a short timeout, and a short one abandons work that
    // was fine. `u64::MAX as u32` is 0, which here means "derive" -- so
    // wrapping would silently hand back the election interval.
    let config = options(u64::MAX, u64::from(u32::MAX) + 1).to_raft_config();
    assert_eq!(config.leader_transfer_timeout_ticks, u32::MAX);
    assert_eq!(config.peer_offline_timeout_ticks, u32::MAX);
    assert_ne!(
        config.leader_transfer_timeout_ticks, 0,
        "zero would mean derive, which is the opposite of a very long timeout"
    );
}

#[test]
fn a_config_document_without_either_timeout_still_loads() {
    // A document written before these fields existed described a cluster that
    // derived both, and zero is what derives.
    let config: Config = serde_json::from_str(
        r#"{"election_timeout_ms":1000,"heartbeat_interval_ms":100,"leader_lease_ms":500,
            "max_payload_bytes":1024,"snapshot_threshold_entries":10,
            "max_segment_bytes":1024,"min_keep_segment_num":2,
            "enable_pre_vote":true,"enable_lease_read":true}"#,
    )
    .expect("an older document loads");
    assert_eq!(config.leader_transfer_timeout_ticks, 0);
    assert_eq!(config.peer_offline_timeout_ticks, 0);

    // And the serde fallback has to agree with the `Default` impl, because one
    // field with two defaults that nobody compares is how a deployment
    // upgrading with an older file silently gets different behaviour.
    assert_eq!(
        config.leader_transfer_timeout_ticks,
        Config::default().leader_transfer_timeout_ticks
    );
    assert_eq!(
        config.peer_offline_timeout_ticks,
        Config::default().peer_offline_timeout_ticks
    );
}
