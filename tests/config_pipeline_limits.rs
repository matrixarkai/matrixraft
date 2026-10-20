// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A cluster takes its pipeline limits from its own configuration.
//!
//! It used to build every peer's pipeline with `PipelineLimits::default()` at
//! four sites, and `Config` carried none of those fields -- so the reorder
//! window, the reorder timeout, the in-flight bounds and the apply batch size
//! were unreachable from a cluster's configuration. Only the defaults existed.
//! The compat layer had the same options and converted them for its own node
//! type, which is what made them look configurable.
//!
//! **The four sites matter separately.** Two build the map up front, and two
//! fill holes in it later -- after a campaign and whenever the membership
//! moves -- so a cluster can get configured limits at construction and
//! defaults for a peer added afterwards. Each one has a test.

use matrixraft::{
    Config, LogEntry, LogId, MatrixRaftOptions, Peer, PipelineLimits, RaftCluster, ReplicaRole,
};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 83_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 84_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: b"payload".to_vec(),
        is_command: true,
    }
}

/// A config whose reorder window holds exactly one out-of-order append.
fn narrow_window() -> Config {
    Config::default().with_pipeline_limits(PipelineLimits {
        reorder_window_size: 1,
        ..PipelineLimits::default()
    })
}

fn started(config: Config, peers: Vec<Peer>) -> RaftCluster {
    let mut cluster = RaftCluster::new(97, config, peers).expect("cluster");
    cluster.start().expect("start");
    cluster
}

#[test]
fn the_defaults_are_exactly_what_the_cluster_used_to_hardcode() {
    // This is what makes the change behaviour-preserving: a host that sets
    // nothing gets the limits the four sites used to build directly.
    assert_eq!(
        Config::default().pipeline_limits(),
        PipelineLimits::default(),
        "a default config has to produce the default limits"
    );
    assert_eq!(
        *Config::default().pipeline,
        PipelineLimits::production_default()
    );
}

#[test]
fn an_initial_member_gets_the_configured_window() {
    // The site in `RaftCluster::new`.
    let mut cluster = started(narrow_window(), (1..=3u64).map(peer).collect());
    cluster
        .receive_out_of_order_append_for(2, entry(500))
        .expect("the first out-of-order append fits a window of one");
    assert!(
        cluster
            .receive_out_of_order_append_for(2, entry(501))
            .is_err(),
        "the second does not -- with the default window of 1,024 it would"
    );
    assert_eq!(
        cluster
            .peer_pipeline_status(2)
            .expect("status")
            .reorder_entries_rejected,
        1
    );
}

#[test]
fn a_peer_added_after_the_start_gets_them_too() {
    // The site in `add_peer`, which is the one a host would hit last and
    // notice least: a cluster configured narrow would have handed a new peer
    // the default window.
    let mut cluster = started(narrow_window(), (1..=3u64).map(peer).collect());
    cluster.add_peer(peer(4)).expect("add a peer");

    cluster
        .receive_out_of_order_append_for(4, entry(500))
        .expect("the first fits");
    assert!(
        cluster
            .receive_out_of_order_append_for(4, entry(501))
            .is_err(),
        "the new peer's window is the configured one, not the default"
    );
}

#[test]
fn a_peer_the_membership_recovery_adds_gets_them() {
    // The site in `refresh_replication_pipelines`, which fills holes in the
    // pipeline map after the membership moves -- the path recovering a WAL
    // record's membership goes down when it adds a node the cluster did not
    // have.
    //
    // **The fourth site is not covered, and this says so rather than
    // pretending.** `reset_replication_pipelines_for_leader`'s `or_insert_with`
    // closure only runs for a node that has no pipeline at campaign time, and
    // a `panic!()` placed in it leaves the whole suite passing -- 816 tests --
    // so nothing here reaches it. Its limits are belt-and-braces. A test that
    // campaigned and then measured a window would be measuring the pipeline
    // built in `RaftCluster::new`, which the first test above already covers;
    // the earlier version of this test did exactly that and a plant on the
    // closure survived it.
    let donor_members = (1..=4u64).map(peer).collect::<Vec<_>>();
    let mut donor = started(Config::default(), donor_members);
    donor.propose(b"write".to_vec()).expect("propose");
    let record = donor
        .wal_record_for(1)
        .expect("a record naming four voters");
    assert!(
        record.membership.voters.contains(&4),
        "the record has to name the node this cluster lacks: {:?}",
        record.membership
    );

    let mut cluster = started(narrow_window(), (1..=3u64).map(peer).collect());
    cluster
        .restore_wal_record(record)
        .expect("restore adds node 4");

    cluster
        .receive_out_of_order_append_for(4, entry(500))
        .expect("the first fits");
    assert!(
        cluster
            .receive_out_of_order_append_for(4, entry(501))
            .is_err(),
        "the pipeline the recovery built is the configured one, not the default"
    );
}

#[test]
fn a_configured_reorder_timeout_expires_on_a_cluster_tick() {
    let config = Config::default().with_pipeline_limits(PipelineLimits {
        reorder_timeout_us: 1_000,
        ..PipelineLimits::default()
    });
    let mut cluster = started(config, (1..=3u64).map(peer).collect());
    cluster
        .receive_out_of_order_append_for(2, entry(500))
        .expect("queued");

    assert_eq!(cluster.tick_reorder_queues(999), 0, "one short");
    assert_eq!(
        cluster.tick_reorder_queues(1),
        1,
        "1,000us is the configured timeout, not the 5,000,000us default"
    );
    assert_eq!(
        cluster
            .peer_pipeline_status(2)
            .expect("status")
            .reorder_entry_timeouts,
        1
    );
}

#[test]
fn a_zero_capacity_becomes_one_and_a_zero_timeout_stays_off() {
    // Zero means two different things in the same struct, which is why
    // `pipeline_limits` sanitises five fields and leaves the sixth alone:
    // raising a timeout of zero to one would turn "never expire" into "expire
    // on the first tick".
    let config = Config::default().with_pipeline_limits(PipelineLimits {
        max_inflights_replicate: 0,
        max_memory_replicate_log_bytes: 0,
        max_inflights_apply_task: 0,
        max_apply_batch_bytes: 0,
        reorder_window_size: 0,
        reorder_timeout_us: 0,
        enable_reorder_queue: true,
    });
    let limits = config.pipeline_limits();
    assert_eq!(limits.max_inflights_replicate, 1);
    assert_eq!(limits.max_memory_replicate_log_bytes, 1);
    assert_eq!(limits.max_inflights_apply_task, 1);
    assert_eq!(limits.max_apply_batch_bytes, 1);
    assert_eq!(limits.reorder_window_size, 1);
    assert_eq!(
        limits.reorder_timeout_us, 0,
        "a timeout of zero is off and must not be raised to one"
    );

    // And a cluster built from it starts rather than refusing: sanitising, not
    // rejecting, is what the compat layer already does.
    let mut cluster = started(config, (1..=3u64).map(peer).collect());
    cluster
        .receive_out_of_order_append_for(2, entry(500))
        .expect("a window of one holds one");
    assert_eq!(
        cluster.tick_reorder_queues(u64::MAX),
        0,
        "and the timeout being off means no tick expires it"
    );
}

#[test]
fn an_older_configuration_without_the_key_gets_the_defaults() {
    // `#[serde(default)]` is what keeps this backward compatible: a document
    // written before `pipeline` existed must deserialise to the limits the
    // cluster used to hardcode.
    let json = r#"{
        "election_timeout_ms": 1000,
        "heartbeat_interval_ms": 100,
        "leader_lease_ms": 500,
        "max_payload_bytes": 8388608,
        "snapshot_threshold_entries": 10000,
        "max_segment_bytes": 67108864,
        "min_keep_segment_num": 2,
        "enable_pre_vote": true,
        "enable_lease_read": true
    }"#;
    let config: Config = serde_json::from_str(json).expect("an older document deserialises");
    assert_eq!(*config.pipeline, PipelineLimits::default());
    assert_eq!(config.pipeline_limits(), PipelineLimits::default());
    config.validate().expect("and it is still valid");
}

#[test]
fn the_compat_options_reach_the_cluster_pipelines() {
    // `MatrixRaftOptions` always had these fields and converted them for its
    // own node type. A cluster built from the same options got the defaults.
    let options = MatrixRaftOptions {
        group_id: 97,
        peer_id: 1,
        raft_addr: "127.0.0.1:83001".to_string(),
        snapshot_addr: "127.0.0.1:84001".to_string(),
        wal_dir: "/tmp/matrixraft-config-pipeline-wal".to_string(),
        snapshot_dir: "/tmp/matrixraft-config-pipeline-snapshot".to_string(),
        peers: (1..=3u64).map(peer).collect(),
        role: ReplicaRole::Voter,
        wal_sync: true,
        election_cycle_tick: 5,
        transfer_timeout_tick: 3,
        offline_timeout_tick: 10,
        tick_interval_ms: 100,
        lease_duration_ms: 250,
        last_lease_duration_ms: 0,
        assume_lease_when_start: true,
        max_memory_replicate_log_bytes: 64 * 1024,
        max_disk_replicate_log_num: 64,
        max_cache_memory_bytes: 16 * 1024 * 1024,
        max_apply_batch_bytes: 64 * 1024,
        enable_reorder_queue: true,
        // The two this test is about.
        reorder_timeout_us: 2_000,
        reorder_window_size: 1,
        max_inflights_apply_task: 5,
        max_inflights_replicate: 128,
        enable_pre_vote: true,
        max_segment_bytes: 64 * 1024 * 1024,
        min_keep_segment_num: 2,
        can_trigger_snapshot: true,
        max_applied_log_bytes: u64::MAX,
        send_snapshot_timeout_ms: 60_000,
    };
    let config = options.to_raft_config();
    assert_eq!(config.pipeline.reorder_window_size, 1);
    assert_eq!(config.pipeline.reorder_timeout_us, 2_000);
    assert_eq!(
        config.pipeline_limits(),
        options.to_pipeline_limits(),
        "the config's limits are the options' own conversion, not a second one"
    );
}
