// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{Config, MatrixRaftOptions, Peer, ReplicaRole};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

// Byte-valued knobs, all distinct, so a mapping that picks the wrong one is
// visible in the result rather than hidden behind equal numbers.
const REPLICATE_BYTES: u64 = 3 * 1024;
const CACHE_BYTES: u64 = 5 * 1024;
const APPLY_BATCH_BYTES: u64 = 7 * 1024;
const SEGMENT_BYTES: u64 = 11 * 1024;

fn options() -> MatrixRaftOptions {
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
        transfer_timeout_tick: 3,
        offline_timeout_tick: 10,
        tick_interval_ms: 100,
        lease_duration_ms: 250,
        last_lease_duration_ms: 200,
        assume_lease_when_start: false,
        max_memory_replicate_log_bytes: REPLICATE_BYTES,
        max_disk_replicate_log_num: 64,
        max_cache_memory_bytes: CACHE_BYTES,
        max_apply_batch_bytes: APPLY_BATCH_BYTES,
        enable_reorder_queue: true,
        reorder_timeout_us: 3_000,
        reorder_window_size: 128,
        max_inflights_apply_task: 5,
        max_inflights_replicate: 128,
        enable_pre_vote: true,
        max_segment_bytes: SEGMENT_BYTES,
        min_keep_segment_num: 2,
        can_trigger_snapshot: true,
        max_applied_log_bytes: u64::MAX,
        send_snapshot_timeout_ms: 60_000,
    }
}

#[test]
fn the_snapshot_threshold_is_a_count_so_no_byte_budget_may_reach_it() {
    let config = options().to_raft_config();

    // Control: the byte-valued knobs do carry through, so this test can see the
    // conversion at all and the assertion below is not passing by accident.
    assert_eq!(config.max_payload_bytes, REPLICATE_BYTES);
    assert_eq!(config.max_log_buffer_bytes, CACHE_BYTES);
    assert_eq!(config.max_segment_bytes, SEGMENT_BYTES);

    assert_ne!(
        config.snapshot_threshold_entries, APPLY_BATCH_BYTES,
        "a byte budget must not arrive as a count of entries"
    );
    assert_eq!(
        config.snapshot_threshold_entries,
        Config::default().snapshot_threshold_entries,
        "MatrixRaftOptions has no threshold in entries, so the default stands"
    );

    // And it does not follow the byte budget around.
    let mut bigger = options();
    bigger.max_apply_batch_bytes = APPLY_BATCH_BYTES * 1000;
    assert_eq!(
        bigger.to_raft_config().snapshot_threshold_entries,
        config.snapshot_threshold_entries,
        "the threshold does not move when the apply batch budget moves"
    );
}

#[test]
fn validation_could_never_have_caught_the_mismatch() {
    // The only thing validate() asks of the threshold is that it is non-zero, so
    // 8 MiB-as-entries passed just as cleanly as 10,000 does. A unit error is not
    // something this validator can see, which is why the mapping needed a test.
    let as_bytes = Config {
        snapshot_threshold_entries: 8 * 1024 * 1024,
        ..Config::default()
    };
    assert!(as_bytes.validate().is_ok(), "the old value validated fine");
    assert!(
        Config::default().validate().is_ok(),
        "and so does the value the mapping now produces"
    );

    let zero = Config {
        snapshot_threshold_entries: 0,
        ..Config::default()
    };
    assert!(
        zero.validate().is_err(),
        "zero is the only threshold it rejects"
    );
}
