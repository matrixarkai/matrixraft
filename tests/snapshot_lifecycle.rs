// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_snapshot_lifecycle_evidence, AdminCommand, ApplySnapshotFence, ByteQuotaLimiter,
    Driver, DriverGroupKey, DriverOptions, DriverTickReceiver, InstallSnapshotResponse, LogEntry,
    LogId, MatrixRaftRateLimiterConfig, Message, Peer, PersistentRaftSnapshotStore,
    PersistentRaftSnapshotStoreOptions, RaftCluster, RaftSnapshot, RateLimiter,
    RateLimiterRefiller, ReplicaRole, SnapshotLifecycle, SnapshotLifecycleConfig, SnapshotMetadata,
    StepResult,
};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Polls to a deadline instead of sleeping a fixed time, so an assertion is
/// about behaviour and not about how busy the machine was.
fn wait_until(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    let settled = done();
    if !settled {
        eprintln!("wait_until({what}) gave up after {timeout:?}");
    }
    settled
}

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

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 16_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 17_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn snapshot(index: u64, payload: &[u8]) -> RaftSnapshot {
    RaftSnapshot {
        group_id: 55,
        meta: SnapshotMetadata {
            snapshot_id: format!("snap-{index}"),
            last_log_id: LogId { term: 2, index },
            membership: vec![1, 2, 3],
            members: Vec::new(),
        },
        payload: payload.to_vec(),
    }
}

fn tail_entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 2, index },
        payload: format!("tail-{index}").into_bytes(),
        is_command: true,
    }
}

#[test]
fn snapshot_lifecycle_throttles_retries_and_rolls_back_install() {
    let snap = snapshot(10, b"abcdefghijklmnopqrstuvwxyz");
    let mut lifecycle = SnapshotLifecycle::new(SnapshotLifecycleConfig {
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
        .record_send_response(&InstallSnapshotResponse {
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

    let mut installer = SnapshotLifecycle::new(Default::default()).expect("installer");
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
    let mut lifecycle = SnapshotLifecycle::new(SnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 64,
        max_retry_attempts: 2,
    })
    .expect("lifecycle");
    let mut limiter = ByteQuotaLimiter::with_available(8, 4);

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
fn snapshot_lifecycle_splits_chunks_on_partial_rate_quota() {
    let snap = snapshot(12, b"abcdefghijkl");
    let mut lifecycle = SnapshotLifecycle::new(SnapshotLifecycleConfig {
        chunk_size: 6,
        max_chunks_per_tick: 1,
        max_bytes_per_tick: 64,
        max_retry_attempts: 2,
    })
    .expect("lifecycle");
    let mut limiter = ByteQuotaLimiter::with_available(10, 4);

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
    let mut sender = SnapshotLifecycle::new(SnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 2,
        max_bytes_per_tick: 8,
        max_retry_attempts: 3,
    })
    .expect("sender lifecycle");
    let mut downloader = SnapshotLifecycle::new(Default::default()).expect("downloader");
    let mut limiter = ByteQuotaLimiter::with_available(4, 4);

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
                .record_send_response(&InstallSnapshotResponse {
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
    let mut sender = SnapshotLifecycle::new(SnapshotLifecycleConfig {
        chunk_size: 5,
        max_chunks_per_tick: 2,
        max_bytes_per_tick: 10,
        max_retry_attempts: 3,
    })
    .expect("sender");
    let mut receiver = SnapshotLifecycle::new(Default::default()).expect("receiver");
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
            ApplySnapshotFence {
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
    let evidence = matrixraft_snapshot_lifecycle_evidence(&[peer_three], 1_000, 1);
    assert!(evidence.sender_lifecycle_present || evidence.downloader_lifecycle_present);
    assert!(evidence.install_progress_present);
    assert!(evidence.rejoin_after_compacted_log_present);
}

#[test]
fn snapshot_finish_catches_up_tail() {
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
            ApplySnapshotFence {
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
        Some(LogId { term: 2, index: 4 })
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
fn rejected_snapshot_finish_triggers_fresh_snapshot() {
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
fn failed_replication_task_sends_snapshot_or_triggers_one() {
    let mut direct =
        RaftCluster::new(57, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    direct.start().expect("start");
    direct
        .install_snapshot_to(
            1,
            RaftSnapshot {
                group_id: 57,
                meta: SnapshotMetadata {
                    snapshot_id: "leader-snap-4".to_string(),
                    last_log_id: LogId { term: 1, index: 4 },
                    membership: vec![1, 2, 3],
                    members: Vec::new(),
                },
                payload: b"leader snapshot".to_vec(),
            },
            ApplySnapshotFence {
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
        .step(Message::Admin {
            command: AdminCommand::Replicated {
                peer_id: 2,
                success: false,
            },
        })
        .expect("broken replication triggers snapshot through step");
    assert_eq!(triggered, StepResult::Handled);
    assert!(trigger.snapshot_trigger_status().in_progress);
}

// ---------------------------------------------------------------------------
// The recorded limiter config becomes the limiter the send path takes, and the
// driver's tick is what refills it.
// ---------------------------------------------------------------------------

#[test]
fn a_config_becomes_a_limiter_holding_one_cycle_of_bytes() {
    let config = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 1024,
        check_cycle_sec: 3,
    };
    assert_eq!(config.cycle_capacity_bytes(), 3072);
    assert_eq!(config.check_cycle(), Duration::from_secs(3));

    let limiter = config.to_byte_quota_limiter().expect("limiter");
    assert_eq!(limiter.capacity_bytes(), 3072);
    assert_eq!(limiter.available_bytes(), 3072);
}

#[test]
fn a_zero_in_the_config_is_refused_rather_than_silently_stopping_everything() {
    // A limiter built from a zero grants nothing, so every transfer it gates
    // stops. That must be an error, not a configuration someone can reach by
    // leaving a field at its default.
    let no_bytes = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 0,
        check_cycle_sec: 1,
    };
    let error = no_bytes.to_byte_quota_limiter().expect_err("must refuse");
    assert!(
        format!("{error:?}").contains("bytes_limit_per_sec"),
        "the error should name the field: {error:?}"
    );

    let no_cycle = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 1,
        check_cycle_sec: 0,
    };
    assert!(no_cycle.to_byte_quota_limiter().is_err());
}

#[test]
fn a_configured_limiter_actually_throttles_a_snapshot_send() {
    // The point of the chain: a number on the config changes what the send path
    // does. Without `to_byte_quota_limiter` a host built the limiter itself and
    // the configured number reached nothing.
    let config = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 4,
        check_cycle_sec: 1,
    };
    let mut limiter = config.to_byte_quota_limiter().expect("limiter");
    assert_eq!(limiter.capacity_bytes(), 4);

    let snap = snapshot(21, b"abcdefghijkl");
    let mut lifecycle = SnapshotLifecycle::new(SnapshotLifecycleConfig {
        chunk_size: 4,
        max_chunks_per_tick: 8,
        max_bytes_per_tick: 1024,
        max_retry_attempts: 2,
    })
    .expect("lifecycle");

    lifecycle.begin_send(&snap, 2, 1).expect("begin send");
    let first = lifecycle
        .poll_send_requests_with_limiter(&mut limiter)
        .expect("first tick");
    let first_bytes: usize = first.iter().map(|r| r.chunk.data.len()).sum();
    assert!(
        first_bytes <= 4,
        "the configured 4 bytes per cycle must bound a tick; it sent {first_bytes}"
    );
    assert_eq!(limiter.available_bytes(), 0, "the quota should be spent");

    // Spent: the next tick carries nothing until a refill.
    let second = lifecycle
        .poll_send_requests_with_limiter(&mut limiter)
        .expect("second tick");
    assert!(
        second.is_empty(),
        "with the quota spent the send should stall rather than continue: {} requests",
        second.len()
    );

    // And a refill lets it move again -- which is what the driver tick does.
    let refiller = RateLimiterRefiller::new(Arc::new(Mutex::new(limiter)));
    assert_eq!(refiller.refill_now(), 4, "a spent 4-byte quota restores 4");
    let mut limiter = refiller.limiter().lock().expect("limiter").clone();
    let third = lifecycle
        .poll_send_requests_with_limiter(&mut limiter)
        .expect("third tick");
    assert!(!third.is_empty(), "after a refill the send should continue");
}

#[test]
fn the_drivers_tick_refills_the_limiter() {
    // A limiter hands bytes out and never gets them back on its own, so a
    // configured limit without a refill throttles once and stays empty -- which
    // reads as a snapshot that mysteriously stopped.
    let config = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 16,
        check_cycle_sec: 1,
    };
    let limiter = Arc::new(Mutex::new(config.to_byte_quota_limiter().expect("limiter")));

    // Spend it.
    {
        let mut guard = limiter.lock().expect("limiter");
        let decision = guard.reserve_bytes(16);
        assert!(decision.allowed);
        assert_eq!(guard.available_bytes(), 0);
    }

    let refiller = RateLimiterRefiller::new(Arc::clone(&limiter));
    let driver = Driver::start(DriverOptions {
        worker_num: 1,
        tick_interval_ms: 2,
        ..DriverOptions::default()
    })
    .expect("driver");
    driver
        .register_group_every(
            DriverGroupKey::new(1, 1),
            Arc::clone(&refiller) as Arc<dyn DriverTickReceiver>,
            2,
        )
        .expect("register");

    assert!(
        wait_until("the tick refills", Duration::from_secs(20), || {
            limiter.lock().expect("limiter").available_bytes() == 16
        }),
        "the driver's tick never refilled the limiter; available is {}",
        limiter.lock().expect("limiter").available_bytes()
    );
    assert!(refiller.refills() >= 1);
    assert!(refiller.bytes_restored() >= 16);
}

#[test]
fn a_refill_on_a_full_limiter_restores_nothing() {
    let limiter = Arc::new(Mutex::new(ByteQuotaLimiter::new(100)));
    let refiller = RateLimiterRefiller::new(Arc::clone(&limiter));
    assert_eq!(refiller.refill_now(), 0, "nothing to restore when full");
    assert_eq!(refiller.refills(), 1, "the attempt still counts");
    assert_eq!(refiller.bytes_restored(), 0);
    assert_eq!(limiter.lock().expect("limiter").available_bytes(), 100);
}
