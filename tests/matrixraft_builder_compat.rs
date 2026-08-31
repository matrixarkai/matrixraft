// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    Config, MatrixRaftGroupContextBuilder, MatrixRaftNodeCreatorBuilder, MatrixRaftOptions,
    MatrixRaftRateLimiterConfig, MatrixRaftTransportBuilder, Peer, ReplicaRole,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-matrixraft-builder-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 61_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 62_000 + node_id),
        role,
        auto_promote: false,
    }
}

#[test]
fn matrixraft_builders_capture_node_group_transport_and_option_shape() {
    let send_limiter = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 16 * 1024 * 1024,
        check_cycle_sec: 1,
    };
    let download_limiter = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 8 * 1024 * 1024,
        check_cycle_sec: 2,
    };

    let creator = MatrixRaftNodeCreatorBuilder::new()
        .store_id(99)
        .applier_num(2)
        .apply_max_batch_count(128)
        .snapshot_loader_num(3)
        .snapshot_downloader_num(4)
        .snapshot_creator_num(5)
        .snapshot_sender_num(6)
        .snapshot_send_rate_limiter(send_limiter.clone())
        .snapshot_download_rate_limiter(download_limiter.clone())
        .enable_flexible_apply()
        .enable_heartbeat_merge()
        .merge_heartbeat_interval_milli(25)
        .fsm()
        .group_storage()
        .build();
    assert_eq!(creator.store_id, 99);
    assert_eq!(creator.applier_num, 2);
    assert_eq!(creator.apply_max_batch_count, 128);
    assert_eq!(creator.snapshot_loader_num, 3);
    assert_eq!(creator.snapshot_downloader_num, 4);
    assert_eq!(creator.snapshot_creator_num, 5);
    assert_eq!(creator.snapshot_sender_num, 6);
    assert_eq!(
        creator.snapshot_send_rate_limiter,
        Some(send_limiter.clone())
    );
    assert_eq!(
        creator.snapshot_download_rate_limiter,
        Some(download_limiter.clone())
    );
    assert!(creator.flexible_apply);
    assert!(creator.heartbeat_merge);
    assert!(creator.has_store_fsm);
    assert!(creator.has_group_storage);

    let missing_transport = MatrixRaftGroupContextBuilder::new()
        .add_raft_node_creator(creator.clone())
        .build()
        .expect_err("transport is required");
    assert!(missing_transport.to_string().contains("requires transport"));

    let missing_resolver = MatrixRaftTransportBuilder::new()
        .set_cluster_id(42)
        .set_timeout_ms(500)
        .set_num_connection_group(3)
        .set_dynamic_address_map()
        .set_get_user_payload_callback()
        .build()
        .expect_err("resolver is required");
    assert!(missing_resolver.to_string().contains("address resolver"));

    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(42)
        .set_timeout_ms(500)
        .set_num_connection_group(3)
        .set_dynamic_address_map()
        .bind_address_resolver()
        .set_get_user_payload_callback()
        .build()
        .expect("transport");
    assert_eq!(transport.cluster_id, 42);
    assert_eq!(transport.timeout_ms, 500);
    assert_eq!(transport.num_connection_group, 3);
    assert!(transport.dynamic_address_map);
    assert!(transport.address_resolver_bound);
    assert!(transport.user_payload_callback_bound);

    let context = MatrixRaftGroupContextBuilder::new()
        .tick_interval(50)
        .transport(transport.clone())
        .max_messages_each_poll(256)
        .max_queue_depth(4096)
        .add_raft_node_creator(creator)
        .worker_num(7)
        .reader_num(8)
        .executor_num(9)
        .applier_num(10)
        .snapshot_loader_num(11)
        .snapshot_downloader_num(12)
        .snapshot_sender_num(13)
        .snapshot_creator_num(14)
        .apply_max_batch_count(512)
        .driver_batch_bytes(2 * 1024 * 1024)
        .watch_address_resolver()
        .snapshot_send_rate_limiter(send_limiter)
        .snapshot_download_rate_limiter(download_limiter)
        .enable_flexible_apply()
        .enable_heartbeat_merge()
        .build()
        .expect("group context");
    assert_eq!(context.tick_interval_ms, 50);
    assert_eq!(context.transport, Some(transport));
    assert_eq!(context.node_creators.len(), 1);
    assert_eq!(context.worker_num, 7);
    assert_eq!(context.reader_num, 8);
    assert_eq!(context.executor_num, 9);
    assert_eq!(context.applier_num, 10);
    assert_eq!(context.driver_batch_bytes, 2 * 1024 * 1024);
    assert!(context.watched_address_resolver);
    assert!(context.flexible_apply);
    assert!(context.heartbeat_merge);

    let wal_dir = temp_dir("wal");
    let snapshot_dir = temp_dir("snapshot");
    let options = MatrixRaftOptions {
        group_id: 707,
        peer_id: 1,
        raft_addr: "127.0.0.1:61001".to_string(),
        snapshot_addr: "127.0.0.1:62001".to_string(),
        wal_dir: wal_dir.display().to_string(),
        snapshot_dir: snapshot_dir.display().to_string(),
        peers: vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
        role: ReplicaRole::Voter,
        wal_sync: true,
        election_cycle_tick: 5,
        transfer_timeout_tick: 3,
        offline_timeout_tick: 10,
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
    };
    let config = options.to_raft_config();
    assert_eq!(
        config,
        Config {
            election_timeout_ms: 500,
            heartbeat_interval_ms: 100,
            leader_lease_ms: 250,
            last_follower_lease_ms: 200,
            max_payload_bytes: 64 * 1024,
            max_log_buffer_bytes: 16 * 1024 * 1024,
            snapshot_threshold_entries: 64 * 1024,
            max_segment_bytes: 64 * 1024 * 1024,
            min_keep_segment_num: 2,
            enable_pre_vote: true,
            enable_lease_read: true,
            assume_lease_when_start: false,
        }
    );
    let node_options = options.to_node_options();
    assert_eq!(node_options.group_id, 707);
    assert_eq!(node_options.node_id, 1);
    assert_eq!(node_options.peers.len(), 3);
    assert_eq!(options.max_disk_replicate_log_num, 64);
    assert_eq!(options.reorder_queue_options().reorder_timeout_us, 3_000);
    assert!(options.reorder_queue_options().enable_reorder_queue);
    assert_eq!(options.inflight_options().max_inflights_replicate, 128);
    assert_eq!(
        options.snapshot_recycle_options().send_snapshot_timeout_ms,
        60_000
    );
    let pipeline_limits = options.to_pipeline_limits();
    assert_eq!(pipeline_limits.max_inflights_replicate, 128);
    assert_eq!(pipeline_limits.max_inflights_apply_task, 5);
    assert_eq!(pipeline_limits.reorder_window_size, 128);
    assert_eq!(pipeline_limits.max_memory_replicate_log_bytes, 64 * 1024);

    let mut node = options
        .create_node(1)
        .expect("node from matrixraft options");
    node.start(1).expect("start node");
    assert_eq!(node.leader().expect("leader"), Some(1));
    node.shutdown().expect("shutdown");

    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);
}
