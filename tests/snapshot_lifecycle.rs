// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    rustraft_snapshot_lifecycle_evidence, PersistentRaftSnapshotStore,
    PersistentRaftSnapshotStoreOptions, RaftAdminCommand, RaftCluster, RaftSnapshot,
    RaftSnapshotLifecycle, RaftSnapshotLifecycleConfig, RustRaftApplySnapshotFence,
    RustRaftByteQuotaLimiter, RustRaftInstallSnapshotResponse, RustRaftLogEntry, RustRaftLogId,
    RustRaftMessage, RustRaftPeer, RustRaftRateLimiter, RustRaftReplicaRole, RustRaftSnapshotMeta,
    RustRaftStepResult,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_snapshot_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-snapshot-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 16_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 17_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn snapshot(index: u64, payload: &[u8]) -> RaftSnapshot {
    RaftSnapshot {
        group_id: 55,
        meta: RustRaftSnapshotMeta {
            snapshot_id: format!("snap-{index}"),
            last_log_id: RustRaftLogId { term: 2, index },
            membership: vec![1, 2, 3],
            members: Vec::new(),
        },
        payload: payload.to_vec(),
    }
}

fn tail_entry(index: u64) -> RustRaftLogEntry {
    RustRaftLogEntry {
        log_id: RustRaftLogId { term: 2, index },
        payload: format!("tail-{index}").into_bytes(),
        is_command: true,
    }
}

#[test]
fn snapshot_lifecycle_throttles_retries_and_rolls_back_install() {
    let snap = snapshot(10, b"abcdefghijklmnopqrstuvwxyz");
    let mut lifecycle = RaftSnapshotLifecycle::new(RaftSnapshotLifecycleConfig {
        chunk_size: 3,
        max_chunks_per_tick: 4,
        max_bytes_per_tick: 4,
        max_retry_attempts: 2,
    })
    .expect("lifecycle");

    lifecycle.begin_send(&snap, 2, 1).expect("begin send");
    let first = lifecycle.poll_send_requests().expect("first tick");
    assert_eq!(first.len(), 1);
    assert!(lifecycle.status().throttled_ticks > 0);

    lifecycle
        .record_send_response(&RustRaftInstallSnapshotResponse {
            term: 2,
            accepted: false,
            next_offset: 0,
            committed_index: 0,
            reason: "retry".to_string(),
        })
        .expect("retry response");
    assert_eq!(lifecycle.status().retry_count, 1);
    let resent = lifecycle.poll_send_requests().expect("retry tick");
    assert_eq!(resent[0].chunk.offset, 0);

    let mut installer = RaftSnapshotLifecycle::new(Default::default()).expect("installer");
    assert!(installer
        .install_request(first[0].clone())
        .expect("partial")
        .is_none());
    assert!(installer.status().installing);
    installer.rollback_install();
    assert!(!installer.status().installing);
    assert_eq!(installer.status().rolled_back, 1);
}

#[test]
fn snapshot_lifecycle_uses_baseline_raft_style_transfer_quota_without_advancing_on_rejection() {
    let snap = snapshot(11, b"abcdefghijkl");
    let mut lifecycle = RaftSnapshotLifecycle::new(RaftSnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 64,
        max_retry_attempts: 2,
    })
    .expect("lifecycle");
    let mut limiter = RustRaftByteQuotaLimiter::with_available(8, 4);

    lifecycle.begin_send(&snap, 2, 1).expect("begin send");
    let first = lifecycle
        .poll_send_requests_with_limiter(&mut limiter)
        .expect("first quota poll");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].chunk.offset, 0);
    assert_eq!(limiter.available_bytes(), 0);

    let blocked = lifecycle
        .poll_send_requests_with_limiter(&mut limiter)
        .expect("blocked quota poll");
    assert!(blocked.is_empty());
    assert_eq!(lifecycle.status().sent_chunks, 1);
    assert_eq!(lifecycle.status().rate_limited_ticks, 1);

    limiter.refill_bytes(8);
    let resumed = lifecycle
        .poll_send_requests_with_limiter(&mut limiter)
        .expect("resumed quota poll");
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].chunk.offset, 4);
}

#[test]
fn snapshot_lifecycle_splits_chunks_on_partial_rate_quota_like_matrixraft() {
    let snap = snapshot(12, b"abcdefghijkl");
    let mut lifecycle = RaftSnapshotLifecycle::new(RaftSnapshotLifecycleConfig {
        chunk_size: 6,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 64,
        max_retry_attempts: 2,
    })
    .expect("lifecycle");
    let mut limiter = RustRaftByteQuotaLimiter::with_available(10, 4);

    lifecycle.begin_send(&snap, 2, 1).expect("begin send");
    let partial = lifecycle
        .poll_send_requests_with_limiter(&mut limiter)
        .expect("partial quota poll");
    assert_eq!(partial.len(), 1);
    assert_eq!(partial[0].chunk.offset, 0);
    assert_eq!(partial[0].chunk.data, b"abcd".to_vec());
    assert!(!partial[0].chunk.done);
    assert_eq!(lifecycle.status().sent_chunks, 1);
    assert_eq!(lifecycle.status().total_chunks, 3);

    limiter.refill_bytes(10);
    let remainder = lifecycle
        .poll_send_requests_with_limiter(&mut limiter)
        .expect("remainder quota poll");
    assert_eq!(remainder.len(), 1);
    assert_eq!(remainder[0].chunk.offset, 4);
    assert_eq!(remainder[0].chunk.data, b"ef".to_vec());
    assert!(!remainder[0].chunk.done);

    let next = lifecycle
        .poll_send_requests_with_limiter(&mut limiter)
        .expect("next full chunk");
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].chunk.offset, 6);
    assert_eq!(next[0].chunk.data, b"ghijkl".to_vec());
    assert!(next[0].chunk.done);
}

#[test]
fn snapshot_lifecycle_sustains_sender_and_downloader_under_quota_pressure() {
    let snap = snapshot(13, b"abcdefghijklmnopqrstuvwxyz012345");
    let mut sender = RaftSnapshotLifecycle::new(RaftSnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 2,
        max_bytes_per_tick: 8,
        max_retry_attempts: 3,
    })
    .expect("sender lifecycle");
    let mut downloader = RaftSnapshotLifecycle::new(Default::default()).expect("downloader");
    let mut limiter = RustRaftByteQuotaLimiter::with_available(4, 4);

    sender
        .begin_send(&snap, 2, 1)
        .expect("begin sustained send");
    let mut ticks = 0;
    while sender.status().sending {
        let requests = sender
            .poll_send_requests_with_limiter(&mut limiter)
            .expect("quota poll");
        if requests.is_empty() {
            limiter.refill_bytes(4);
            ticks += 1;
            continue;
        }
        for request in requests {
            let installed = downloader
                .install_request(request)
                .expect("downloader accepts sustained chunk");
            sender
                .record_send_response(&RustRaftInstallSnapshotResponse {
                    term: 2,
                    accepted: true,
                    next_offset: downloader.status().received_chunks * 4,
                    committed_index: 0,
                    reason: "accepted".to_string(),
                })
                .expect("record accepted chunk");
            if let Some(installed) = installed {
                assert_eq!(installed, snap);
            }
        }
        if sender.status().sending {
            let blocked = sender
                .poll_send_requests_with_limiter(&mut limiter)
                .expect("quota exhausted poll");
            assert!(blocked.is_empty());
        }
        limiter.refill_bytes(4);
        ticks += 1;
    }

    assert!(ticks >= 4);
    assert!(sender.status().completed);
    assert!(sender.status().total_chunks >= 8);
    assert_eq!(sender.status().sent_chunks, sender.status().total_chunks);
    assert!(sender.status().throttled_ticks > 0);
    assert!(sender.status().rate_limited_ticks > 0);
    assert!(downloader.status().completed);
    assert_eq!(downloader.status().installed_index, 13);
    assert_eq!(
        downloader.status().received_chunks,
        downloader.status().total_chunks
    );
}

#[test]
fn snapshot_checkpoint_store_saves_loads_and_rechunks() {
    let dir = temp_snapshot_dir("checkpoint");
    let store = PersistentRaftSnapshotStore::open(PersistentRaftSnapshotStoreOptions {
        dir: dir.clone(),
        chunk_size: 4,
    })
    .expect("store");
    let snap = snapshot(12, b"hello snapshot store");
    let path = store.save_checkpoint(&snap).expect("save");
    assert!(path.exists());

    let loaded = store.load_checkpoint("snap-12").expect("load");
    assert_eq!(loaded, snap);
    let chunks = store.checkpoint_chunks("snap-12").expect("chunks");
    assert!(chunks.len() > 1);
    assert_eq!(chunks.first().expect("first").offset, 0);
    assert!(chunks.last().expect("last").done);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn cluster_installs_snapshot_with_lifecycle_then_catches_up_tail_after_compaction() {
    let mut cluster =
        RaftCluster::new(55, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.set_node_healthy(3, false).expect("isolate peer");
    for index in 1..=6 {
        cluster
            .propose(format!("write-{index}").into_bytes())
            .expect("propose");
    }
    let removed = cluster.compact_logs_through(4);
    assert!(removed > 0);
    cluster.set_node_healthy(3, true).expect("restore peer");

    let snap = snapshot(4, b"checkpoint-through-four");
    let mut sender = RaftSnapshotLifecycle::new(RaftSnapshotLifecycleConfig {
        chunk_size: 5,
        max_chunks_per_tick: 2,
        max_bytes_per_tick: 10,
        max_retry_attempts: 3,
    })
    .expect("sender");
    let mut receiver = RaftSnapshotLifecycle::new(Default::default()).expect("receiver");
    sender.begin_send(&snap, 2, 1).expect("begin send");

    while sender.status().sending {
        let requests = sender.poll_send_requests().expect("poll");
        for request in requests {
            let response = cluster
                .install_snapshot_lifecycle_request_to(3, &mut receiver, request)
                .expect("install lifecycle request");
            sender
                .record_send_response(&response)
                .expect("record response");
        }
    }
    assert_eq!(cluster.status(3).expect("status").last_snapshot_index, 4);

    cluster
        .install_snapshot_with_tail_to(
            3,
            snap,
            RustRaftApplySnapshotFence {
                applied_index: 4,
                commit_index: 4,
                installed_snapshot_index: 4,
                first_retained_log_index: 5,
            },
            vec![tail_entry(5), tail_entry(6)],
        )
        .expect("tail catch-up");
    assert_eq!(cluster.status(3).expect("status").last_log_index, 6);

    let peer_three = cluster.peer_pipeline_status(3).expect("pipeline");
    let evidence = rustraft_snapshot_lifecycle_evidence(&[peer_three], 1_000, 1);
    assert!(evidence.sender_lifecycle_present || evidence.downloader_lifecycle_present);
    assert!(evidence.install_progress_present);
    assert!(evidence.rejoin_after_compacted_log_present);
}

#[test]
fn snapshot_finish_catches_up_tail_like_matrixraft() {
    let mut cluster =
        RaftCluster::new(55, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.set_node_healthy(3, false).expect("isolate peer");
    for index in 1..=6 {
        cluster
            .propose(format!("write-{index}").into_bytes())
            .expect("propose");
    }
    cluster.compact_logs_through(4);
    cluster.set_node_healthy(3, true).expect("restore peer");

    let snap = snapshot(4, b"checkpoint-through-four");
    cluster
        .install_snapshot_to(
            3,
            snap,
            RustRaftApplySnapshotFence {
                applied_index: 4,
                commit_index: 4,
                installed_snapshot_index: 4,
                first_retained_log_index: 5,
            },
        )
        .expect("install snapshot");
    assert_eq!(
        cluster
            .wal_record_for(3)
            .expect("follower wal")
            .hard_state
            .committed,
        Some(RustRaftLogId { term: 2, index: 4 })
    );
    cluster
        .begin_snapshot_send_to(3, "snap-4", 4, 1)
        .expect("begin snapshot send");
    cluster
        .handle_snapshot_finish_from(3, true, 4)
        .expect("accepted snapshot finish");

    let follower = cluster.status(3).expect("follower status");
    assert_eq!(follower.last_snapshot_index, 4);
    assert_eq!(follower.last_log_index, 7);
    assert_eq!(
        cluster
            .peer_pipeline_status(3)
            .expect("pipeline")
            .match_index,
        7
    );
}

#[test]
fn rejected_snapshot_finish_triggers_fresh_snapshot_like_matrixraft() {
    let mut cluster =
        RaftCluster::new(56, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster
        .propose(b"write-before-snapshot".to_vec())
        .expect("propose");
    cluster
        .begin_snapshot_send_to(3, "stale-snap-1", 1, 1)
        .expect("begin snapshot send");

    cluster
        .handle_snapshot_finish_from(3, false, 0)
        .expect("rejected snapshot finish");

    let pipeline = cluster.peer_pipeline_status(3).expect("pipeline");
    assert!(!pipeline.snapshot_sending);
    assert_eq!(pipeline.snapshot_chunk_retry_count, 1);

    cluster
        .handle_snapshot_finish_from(3, true, 1)
        .expect("stray snapshot finish is ignored");

    let trigger = cluster.snapshot_trigger_status();
    assert!(trigger.in_progress);
    assert_eq!(
        trigger.last_log_id.expect("snapshot trigger log id").index,
        cluster.status(1).expect("leader status").commit_index
    );
}

#[test]
fn failed_replication_task_sends_snapshot_or_triggers_one_like_matrixraft() {
    let mut direct =
        RaftCluster::new(57, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    direct.start().expect("start");
    direct
        .install_snapshot_to(
            1,
            RaftSnapshot {
                group_id: 57,
                meta: RustRaftSnapshotMeta {
                    snapshot_id: "leader-snap-4".to_string(),
                    last_log_id: RustRaftLogId { term: 1, index: 4 },
                    membership: vec![1, 2, 3],
                    members: Vec::new(),
                },
                payload: b"leader snapshot".to_vec(),
            },
            RustRaftApplySnapshotFence {
                applied_index: 4,
                commit_index: 4,
                installed_snapshot_index: 4,
                first_retained_log_index: 5,
            },
        )
        .expect("install leader snapshot");
    assert!(direct
        .record_replication_task_result_for(2, false)
        .expect("broken replication sends snapshot"));
    let pipeline = direct.peer_pipeline_status(2).expect("pipeline");
    assert!(pipeline.snapshot_sending);
    assert_eq!(pipeline.snapshot_send_attempts, 1);
    assert_eq!(pipeline.snapshot_install_total_chunks, 1);
    assert!(!direct.snapshot_trigger_status().in_progress);

    let mut trigger =
        RaftCluster::new(58, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    trigger.start().expect("start");
    let triggered = trigger
        .step(RustRaftMessage::Admin {
            command: RaftAdminCommand::Replicated {
                peer_id: 2,
                success: false,
            },
        })
        .expect("broken replication triggers snapshot through step");
    assert_eq!(triggered, RustRaftStepResult::Handled);
    assert!(trigger.snapshot_trigger_status().in_progress);
}
