// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_apply_batch_outcome, matrixraft_peer_pipeline_status_from_observed, RaftCluster,
    RaftReplicationPipeline, RustRaftAppendEntriesResponse, RustRaftApplyBatchStatus,
    RustRaftLogEntry, RustRaftLogId, RustRaftObservedPeerPipeline, RustRaftPeer,
    RustRaftPeerProgressState, RustRaftPipelineLimits, RustRaftReplicaRole, RustRaftRole,
    RustRaftSnapshotState,
};

fn entry(index: u64, payload: &[u8]) -> RustRaftLogEntry {
    RustRaftLogEntry {
        log_id: RustRaftLogId { term: 1, index },
        payload: payload.to_vec(),
        is_command: true,
    }
}

fn peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 14_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 15_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn small_limits() -> RustRaftPipelineLimits {
    RustRaftPipelineLimits {
        max_inflights_replicate: 1,
        max_memory_replicate_log_bytes: 16,
        max_inflights_apply_task: 2,
        max_apply_batch_bytes: 8,
        enable_reorder_queue: true,
        reorder_window_size: 2,
        reorder_timeout_us: 10,
    }
}

#[test]
fn observed_peer_pipeline_converts_into_full_status_surface() {
    let observed = RustRaftObservedPeerPipeline {
        peer_id: 7,
        match_index: 8,
        next_index: 11,
        append_requests: 13,
        append_accepted: 9,
        append_rejected: 2,
        inflight_entries: 3,
        inflight_bytes: 1024,
        append_queue_depth: 4,
        append_queue_limit: 4,
        append_queue_max_depth: 7,
        inflight_bytes_limit: 1024,
        apply_inflight_tasks: 2,
        apply_inflight_limit: 2,
        apply_queue_depth: 5,
        apply_queue_max_depth: 8,
        apply_batch_bytes_limit: 4096,
        apply_backpressure_rejections: 1,
        memory_backpressure_rejections: 2,
        oversized_log_rejections: 3,
        reorder_queue_depth: 4,
        out_of_order_append_rejections: 2,
        reorder_entries_rejected: 3,
        reorder_entry_timeouts: 4,
        reorder_dropped_packages: 5,
        stale_term_rejections: 6,
        packet_loss_events: 7,
        network_error_probe_transitions: 2,
        snapshot_sending: true,
        snapshot_installing: false,
        snapshot_installed_index: 40,
        snapshot_send_attempts: 2,
        snapshot_install_total_chunks: 10,
        snapshot_install_progress_per_mille: 1000,
        snapshot_backpressure_rejections: 1,
        snapshot_rate_limit_rejections: 2,
        snapshot_install_rolled_back: 3,
        snapshot_chunk_retry_count: 4,
        snapshot_send_timeouts: 5,
        snapshot_during_membership_change: true,
        snapshot_rejoin_after_compacted_log: true,
        transfer_leader_target: true,
        transfer_leader_timeouts: 1,
        pre_vote_rejections: 2,
        election_rejections: 3,
        offline_timeout_reached: true,
        offline_timeout_rejections: 4,
        auto_promoted_from_learner: true,
        witness_quorum_required: 3,
        witness_quorum_acked: 3,
    };

    let status = matrixraft_peer_pipeline_status_from_observed(&observed);

    assert_eq!(status.peer_id, 7);
    assert_eq!(status.progress_state, RustRaftPeerProgressState::Replicate);
    assert!(status.paused);
    assert!(status.old_paused);
    assert_eq!(status.follower_lag, 2);
    assert_eq!(status.required_snapshot_index, 40);
    assert_eq!(status.acked_snapshot_index, 40);
    assert!(status.learner_caught_up);
    assert!(status.witness_quorum_reached);
    assert_eq!(status.reorder_dropped_packages, 5);
    assert_eq!(status.stale_term_rejections, 6);
    assert_eq!(status.packet_loss_events, 7);
    assert_eq!(status.network_error_probe_transitions, 2);
}

#[test]
fn apply_batch_outcome_splits_pending_suffix() {
    let entries = vec![
        entry(10, b"ten"),
        entry(11, b"eleven"),
        entry(12, b"twelve"),
    ];

    let full =
        matrixraft_apply_batch_outcome(&entries, entries.len(), RustRaftApplyBatchStatus::Applied);
    assert_eq!(full.status, RustRaftApplyBatchStatus::Applied);
    assert_eq!(full.first_log_id.as_ref().map(|id| id.index), Some(10));
    assert_eq!(full.last_log_id.as_ref().map(|id| id.index), Some(12));
    assert_eq!(full.applied_through, 12);
    assert_eq!(full.next_index, 13);
    assert_eq!(full.applied_entries.len(), 3);
    assert!(full.pending_entries.is_empty());

    let partial = matrixraft_apply_batch_outcome(&entries, 2, RustRaftApplyBatchStatus::NotReady);
    assert_eq!(partial.status, RustRaftApplyBatchStatus::NotReady);
    assert_eq!(partial.applied_through, 11);
    assert_eq!(partial.next_index, 12);
    assert_eq!(
        partial
            .applied_entries
            .iter()
            .map(|entry| entry.log_id.index)
            .collect::<Vec<_>>(),
        vec![10, 11]
    );
    assert_eq!(
        partial
            .pending_entries
            .iter()
            .map(|entry| entry.log_id.index)
            .collect::<Vec<_>>(),
        vec![12]
    );

    let rejected = matrixraft_apply_batch_outcome(&entries, 0, RustRaftApplyBatchStatus::Rejected);
    assert_eq!(rejected.status, RustRaftApplyBatchStatus::Rejected);
    assert_eq!(rejected.applied_through, 0);
    assert_eq!(rejected.next_index, 10);
    assert!(rejected.applied_entries.is_empty());
    assert_eq!(rejected.pending_entries, entries);

    let empty = matrixraft_apply_batch_outcome(&[], 4, RustRaftApplyBatchStatus::Applied);
    assert_eq!(empty.status, RustRaftApplyBatchStatus::Applied);
    assert_eq!(empty.first_log_id, None);
    assert_eq!(empty.last_log_id, None);
    assert_eq!(empty.applied_through, 0);
    assert_eq!(empty.next_index, 0);
    assert!(empty.applied_entries.is_empty());
    assert!(empty.pending_entries.is_empty());
}

#[test]
fn replication_pipeline_batches_retries_backoff_and_lag() {
    let mut pipeline = RaftReplicationPipeline::new(
        2,
        1,
        RustRaftPipelineLimits {
            max_inflights_replicate: 8,
            max_memory_replicate_log_bytes: 1024,
            max_inflights_apply_task: 2,
            max_apply_batch_bytes: 256,
            enable_reorder_queue: true,
            reorder_window_size: 8,
            reorder_timeout_us: 10,
        },
    );
    pipeline.set_progress_state(RustRaftPeerProgressState::Replicate);

    pipeline.queue_append(&entry(1, b"one")).expect("queue one");
    pipeline.queue_append(&entry(2, b"two")).expect("queue two");
    pipeline
        .queue_append(&entry(3, b"three"))
        .expect("queue three");

    let flushed = pipeline.flush_append_batch(3, 1024);
    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].entry_count, 3);
    assert_eq!(flushed[0].first_log_id.index, 1);
    assert_eq!(flushed[0].last_log_id.index, 3);
    assert_eq!(pipeline.status().append_batches, 1);
    assert_eq!(pipeline.status().max_append_batch_entries, 3);

    assert!(pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 0,
            rejection_hint: Some(0),
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .is_err());
    assert_eq!(pipeline.status().retry_attempts, 1);
    assert!(pipeline.status().backoff_ms > 0);
    assert_eq!(pipeline.status().inflight_entries, 0);
    assert_eq!(pipeline.status().inflight_bytes, 0);
    assert!(!pipeline.record_retry_backoff_tick(1));
    let remaining = pipeline.status().next_retry_after_ms;
    assert!(pipeline.record_retry_backoff_tick(remaining));

    pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 3,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .expect("ack batch");
    assert_eq!(pipeline.status().retry_attempts, 0);
    assert_eq!(pipeline.status().inflight_entries, 0);
    assert_eq!(pipeline.update_follower_lag(5), 2);

    pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 1,
            rejection_hint: Some(1),
            rejected_index: Some(2),
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .expect("stale rejection is ignored");
    assert_eq!(pipeline.status().match_index, 3);
    assert_eq!(pipeline.status().next_index, 4);
    assert_eq!(pipeline.status().retry_attempts, 0);

    pipeline
        .queue_append(&entry(4, b"four"))
        .expect("queue four");
    assert_eq!(pipeline.flush_append_window().len(), 1);
    assert_eq!(pipeline.status().inflight_entries, 1);
    assert!(pipeline.record_network_error());
    assert_eq!(pipeline.status().inflight_entries, 0);
    assert_eq!(pipeline.status().inflight_bytes, 0);
    assert_eq!(pipeline.status().next_index, 4);
    assert_eq!(pipeline.status().packet_loss_events, 1);
    assert_eq!(pipeline.status().network_error_probe_transitions, 1);

    let mut probing = RaftReplicationPipeline::new(4, 7, RustRaftPipelineLimits::default());
    probing
        .queue_append(&entry(7, b"seven"))
        .expect("queue probe");
    probing
        .queue_append(&entry(8, b"eight"))
        .expect("queue second probe");
    let probe_flush = probing.flush_append_batch(4, 1024);
    assert_eq!(probe_flush.len(), 1);
    assert_eq!(probe_flush[0].entry_count, 1);
    assert_eq!(probe_flush[0].first_log_id.index, 7);
    assert_eq!(probe_flush[0].last_log_id.index, 7);
    assert_eq!(probing.progress_state(), RustRaftPeerProgressState::Probe);
    assert!(probing.is_paused());
    let before = probing.status();
    assert!(!probing.record_network_error());
    assert_eq!(
        probing.status().packet_loss_events,
        before.packet_loss_events + 1
    );
    assert_eq!(
        probing.status().network_error_probe_transitions,
        before.network_error_probe_transitions
    );
}

#[test]
fn replication_pipeline_pauses_and_resumes_progress() {
    let mut probe = RaftReplicationPipeline::new(
        2,
        1,
        RustRaftPipelineLimits {
            max_inflights_replicate: 4,
            max_memory_replicate_log_bytes: 1024,
            max_inflights_apply_task: 2,
            max_apply_batch_bytes: 256,
            enable_reorder_queue: true,
            reorder_window_size: 4,
            reorder_timeout_us: 10,
        },
    );
    probe.queue_append(&entry(1, b"a")).expect("queue 1");
    probe.queue_append(&entry(2, b"b")).expect("queue 2");
    assert_eq!(probe.progress_state(), RustRaftPeerProgressState::Probe);
    assert_eq!(probe.flush_append_batch(1, 256).len(), 1);
    assert!(probe.is_paused());
    assert_eq!(probe.flush_append_batch(1, 256).len(), 0);
    probe.resume();
    assert!(!probe.is_paused());
    assert_eq!(probe.flush_append_batch(1, 256).len(), 1);
    probe
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 2,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .expect("probe success promotes replicate");
    assert_eq!(probe.progress_state(), RustRaftPeerProgressState::Replicate);
    assert!(probe.no_inflights());

    probe.queue_append(&entry(3, b"c")).expect("queue 3");
    assert_eq!(probe.flush_append_window().len(), 1);
    assert!(probe
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 2,
            rejection_hint: Some(2),
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .is_err());
    assert_eq!(probe.progress_state(), RustRaftPeerProgressState::Probe);

    let mut replicate = RaftReplicationPipeline::new(3, 1, small_limits());
    replicate.set_progress_state(RustRaftPeerProgressState::Replicate);
    replicate.queue_append(&entry(1, b"a")).expect("queue");
    assert_eq!(replicate.flush_append_window().len(), 1);
    assert!(replicate.is_paused());
    assert!(!replicate.no_inflights());
    replicate.resume();
    assert!(!replicate.is_paused());
    assert!(replicate.no_inflights());
    assert!(replicate.status().old_paused);
    assert!(replicate.take_empty_append_due_to_old_pause());
    assert!(!replicate.status().old_paused);
    assert!(!replicate.take_empty_append_due_to_old_pause());
}

#[test]
fn heartbeat_response_resumes_paused_peer() {
    let mut probe = RaftReplicationPipeline::new(2, 1, small_limits());
    probe.queue_append(&entry(1, b"a")).expect("queue append");
    assert_eq!(probe.flush_append_batch(1, 1024).len(), 1);
    assert!(probe.is_paused());

    probe.record_heartbeat_response();

    assert!(!probe.is_paused());
    assert_eq!(probe.progress_state(), RustRaftPeerProgressState::Probe);
    assert_eq!(probe.status().next_index, 1);
}

#[test]
fn stale_success_does_not_unpause_or_free_inflight() {
    let mut probe = RaftReplicationPipeline::new(2, 5, RustRaftPipelineLimits::default());
    probe.queue_append(&entry(5, b"five")).expect("queue probe");
    assert_eq!(probe.flush_append_batch(1, 1024).len(), 1);
    assert!(probe.is_paused());

    probe
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 4,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .expect("stale probe success is accepted but not advanced");
    assert_eq!(probe.progress_state(), RustRaftPeerProgressState::Probe);
    assert!(probe.is_paused());
    assert_eq!(probe.status().next_index, 5);

    let mut replicate = RaftReplicationPipeline::new(3, 5, small_limits());
    replicate.set_progress_state(RustRaftPeerProgressState::Replicate);
    replicate
        .queue_append(&entry(5, b"five"))
        .expect("queue replicate");
    assert_eq!(replicate.flush_append_window().len(), 1);
    assert!(replicate.is_paused());

    replicate
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 4,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .expect("stale replicate success is accepted but not advanced");
    assert_eq!(
        replicate.progress_state(),
        RustRaftPeerProgressState::Replicate
    );
    assert_eq!(replicate.status().inflight_entries, 1);
    assert!(replicate.is_paused());
}

#[test]
fn append_rejection_can_require_snapshot_transfer() {
    let mut pipeline = RaftReplicationPipeline::new(2, 10, RustRaftPipelineLimits::default());

    assert!(pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 4,
            rejection_hint: Some(4),
            rejected_index: None,
            require_snapshot: Some(8),
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .is_err());

    let status = pipeline.status();
    assert!(status.snapshot_sending);
    assert_eq!(status.snapshot_send_attempts, 1);
    assert_eq!(status.snapshot_installed_index, 0);
    assert_eq!(status.next_index, 4);

    let mut heartbeat_pipeline =
        RaftReplicationPipeline::new(3, 10, RustRaftPipelineLimits::default());
    heartbeat_pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 9,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot: Some(12),
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .expect("successful heartbeat can require snapshot");
    assert!(heartbeat_pipeline.status().snapshot_sending);
    assert_eq!(heartbeat_pipeline.status().snapshot_send_attempts, 1);
}

#[test]
fn replicate_rejection_falls_back_to_matched_boundary() {
    let mut pipeline = RaftReplicationPipeline::new(2, 8, RustRaftPipelineLimits::default());
    pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 8,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .expect("promote to replicate");
    assert_eq!(
        pipeline.progress_state(),
        RustRaftPeerProgressState::Replicate
    );
    assert_eq!(pipeline.status().next_index, 9);

    pipeline
        .queue_append(&entry(9, b"next"))
        .expect("queue next");
    assert_eq!(pipeline.flush_append_window().len(), 1);
    assert!(pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 8,
            rejection_hint: Some(1),
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .is_err());

    assert_eq!(pipeline.progress_state(), RustRaftPeerProgressState::Probe);
    assert_eq!(pipeline.status().next_index, 9);
}

#[test]
fn probe_rejection_ignores_stale_index_and_pauses_same_index() {
    let mut pipeline = RaftReplicationPipeline::new(2, 5, RustRaftPipelineLimits::default());

    pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 0,
            rejection_hint: Some(3),
            rejected_index: Some(4),
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .expect("stale probe rejection is ignored");
    assert_eq!(pipeline.status().append_rejected, 0);
    assert_eq!(pipeline.status().retry_attempts, 0);
    assert_eq!(pipeline.status().next_index, 5);

    assert!(pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 0,
            rejection_hint: Some(5),
            rejected_index: Some(5),
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .is_err());
    assert_eq!(pipeline.status().append_rejected, 1);
    assert_eq!(pipeline.status().retry_attempts, 1);
    assert_eq!(pipeline.status().next_index, 5);
    assert!(pipeline.is_paused());
}

#[test]
fn replication_pipeline_enforces_windows_and_memory_backpressure() {
    let mut pipeline = RaftReplicationPipeline::new(2, 1, small_limits());

    pipeline
        .queue_append(&entry(1, b"12345678"))
        .expect("queue first");
    let flushed = pipeline.flush_append_window();
    assert_eq!(flushed.len(), 1);
    assert_eq!(pipeline.status().inflight_entries, 1);
    assert_eq!(pipeline.status().inflight_bytes, 8);

    pipeline
        .queue_append(&entry(2, b"abcd"))
        .expect("queue second");
    assert!(pipeline.queue_append(&entry(3, b"efgh")).is_err());
    assert_eq!(pipeline.status().append_queue_depth, 1);
    assert_eq!(pipeline.status().apply_backpressure_rejections, 1);

    pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 1,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: RustRaftSnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
        .expect("ack first");
    assert_eq!(pipeline.status().inflight_entries, 0);
    assert_eq!(pipeline.flush_append_window().len(), 1);

    let mut memory_limited = RaftReplicationPipeline::new(
        3,
        1,
        RustRaftPipelineLimits {
            max_inflights_replicate: 8,
            max_memory_replicate_log_bytes: 10,
            max_inflights_apply_task: 2,
            max_apply_batch_bytes: 8,
            enable_reorder_queue: true,
            reorder_window_size: 2,
            reorder_timeout_us: 10,
        },
    );
    memory_limited
        .queue_append(&entry(1, b"12345678"))
        .expect("queue memory baseline");
    assert!(memory_limited.queue_append(&entry(2, b"abcd")).is_err());
    assert_eq!(memory_limited.status().memory_backpressure_rejections, 1);
}

#[test]
fn replication_pipeline_drains_and_expires_reorder_queue() {
    let mut pipeline = RaftReplicationPipeline::new(2, 1, small_limits());

    pipeline
        .receive_out_of_order(&entry(3, b"three"))
        .expect("queue out of order");
    assert_eq!(pipeline.status().reorder_queue_depth, 1);

    pipeline
        .receive_out_of_order(&entry(1, b"one"))
        .expect("accept next index");
    assert_eq!(pipeline.status().match_index, 1);
    assert_eq!(pipeline.status().reorder_queue_depth, 1);

    pipeline
        .receive_out_of_order(&entry(2, b"two"))
        .expect("drain through queued three");
    assert_eq!(pipeline.status().match_index, 3);
    assert_eq!(pipeline.status().reorder_queue_depth, 0);

    pipeline
        .receive_out_of_order(&entry(5, b"five"))
        .expect("queue gap");
    assert_eq!(pipeline.expire_reorder_queue(), 1);
    assert_eq!(pipeline.status().reorder_entry_timeouts, 1);
    assert_eq!(pipeline.status().reorder_dropped_packages, 1);
}

#[test]
fn replication_pipeline_tracks_snapshot_sender_and_receiver_state() {
    let mut sender = RaftReplicationPipeline::new(2, 10, RustRaftPipelineLimits::default());
    sender
        .begin_snapshot_send("snap-20", 20, 2)
        .expect("begin send");
    assert!(sender.status().snapshot_sending);
    assert_eq!(sender.status().snapshot_send_attempts, 1);
    sender
        .record_snapshot_chunk_sent(128)
        .expect("record sent bytes");
    sender
        .acknowledge_snapshot_chunk()
        .expect("ack first chunk");
    assert_eq!(sender.status().snapshot_install_progress_per_mille, 500);
    sender
        .acknowledge_snapshot_chunk()
        .expect("ack final chunk");
    assert!(!sender.status().snapshot_sending);
    assert_eq!(sender.status().snapshot_installed_index, 20);

    let mut receiver = RaftReplicationPipeline::new(1, 1, RustRaftPipelineLimits::default());
    receiver
        .begin_snapshot_install("snap-40", 40, 2)
        .expect("begin install");
    assert!(receiver.status().snapshot_installing);
    receiver
        .receive_snapshot_chunk(64, false)
        .expect("receive first");
    receiver
        .receive_snapshot_chunk(64, true)
        .expect("receive done");
    assert!(!receiver.status().snapshot_installing);
    assert_eq!(receiver.status().snapshot_installed_index, 40);
}

#[test]
fn snapshot_finish_advances_or_retries() {
    let mut accepted = RaftReplicationPipeline::new(2, 10, RustRaftPipelineLimits::default());
    accepted
        .begin_snapshot_send("snap-20", 20, 2)
        .expect("begin send");
    accepted
        .handle_snapshot_finish(true, 18)
        .expect("accepted snapshot finish");
    assert!(!accepted.status().snapshot_sending);
    assert_eq!(accepted.status().snapshot_installed_index, 20);
    assert_eq!(accepted.status().match_index, 18);
    assert_eq!(accepted.status().next_index, 19);

    let mut rejected = RaftReplicationPipeline::new(3, 10, RustRaftPipelineLimits::default());
    rejected
        .begin_snapshot_send("snap-30", 30, 4)
        .expect("begin send");
    rejected
        .handle_snapshot_finish(false, 0)
        .expect("rejected snapshot finish stops stale transfer");
    assert!(!rejected.status().snapshot_sending);
    assert_eq!(rejected.status().snapshot_send_attempts, 1);
    assert_eq!(rejected.status().snapshot_chunk_retry_count, 1);
}

#[test]
fn snapshot_require_is_tracked_and_acked() {
    let mut pipeline = RaftReplicationPipeline::new(2, 10, RustRaftPipelineLimits::default());

    assert!(pipeline.maybe_require_snapshot(8));
    assert!(pipeline.is_snapshot_required());
    assert_eq!(pipeline.status().required_snapshot_index, 8);
    assert_eq!(pipeline.status().acked_snapshot_index, 0);
    assert_eq!(pipeline.status().snapshot_send_attempts, 1);

    assert!(!pipeline.maybe_require_snapshot(8));
    assert!(!pipeline.maybe_require_snapshot(7));
    assert_eq!(pipeline.status().snapshot_send_attempts, 1);

    pipeline
        .handle_snapshot_finish(true, 8)
        .expect("accepted snapshot finish");
    assert!(!pipeline.is_snapshot_required());
    assert_eq!(pipeline.status().acked_snapshot_index, 8);

    assert!(pipeline.maybe_require_snapshot(12));
    assert!(pipeline.is_snapshot_required());
    assert_eq!(pipeline.status().required_snapshot_index, 12);
    assert_eq!(pipeline.status().acked_snapshot_index, 8);
}

#[test]
fn snapshot_progress_times_out_non_receiving_peer() {
    let mut pipeline = RaftReplicationPipeline::new(2, 10, RustRaftPipelineLimits::default());
    pipeline
        .begin_snapshot_send("snap-20", 20, 2)
        .expect("begin send");

    assert!(!pipeline.update_snapshot_progress(false, 99, 100));
    assert!(pipeline.status().snapshot_sending);
    assert_eq!(pipeline.status().snapshot_send_timeouts, 0);

    assert!(!pipeline.update_snapshot_progress(true, 500, 100));
    assert!(pipeline.status().snapshot_sending);

    assert!(pipeline.update_snapshot_progress(false, 101, 100));
    assert!(!pipeline.status().snapshot_sending);
    assert_eq!(pipeline.status().snapshot_send_timeouts, 1);
}

#[test]
fn raft_cluster_updates_live_peer_pipelines_during_replication() {
    let mut cluster =
        RaftCluster::new(88, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"x".to_vec()).expect("propose");

    let peer_two = cluster.peer_pipeline_status(2).expect("pipeline");
    assert_eq!(peer_two.append_requests, 1);
    assert_eq!(peer_two.append_accepted, 1);
    assert_eq!(peer_two.match_index, 2);
    assert_eq!(peer_two.next_index, 3);

    cluster
        .receive_out_of_order_append_for(2, entry(4, b"future"))
        .expect("track out of order");
    assert_eq!(
        cluster
            .peer_pipeline_status(2)
            .expect("pipeline")
            .reorder_queue_depth,
        1
    );

    cluster
        .begin_snapshot_send_to(2, "snap-5", 5, 1)
        .expect("begin snapshot send");
    cluster
        .record_snapshot_chunk_sent_to(2, 32)
        .expect("sent chunk");
    cluster.acknowledge_snapshot_chunk_to(2).expect("ack chunk");
    assert_eq!(
        cluster
            .peer_pipeline_status(2)
            .expect("pipeline")
            .snapshot_installed_index,
        5
    );
}

#[test]
fn raft_cluster_network_error_immediately_probes_replicating_peer() {
    let mut cluster = RaftCluster::new(188, Default::default(), vec![peer(1), peer(2), peer(3)])
        .expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"warmup".to_vec()).expect("warmup");

    cluster.set_node_healthy(2, false).expect("isolate peer");
    cluster
        .propose(b"missed".to_vec())
        .expect("write while isolated");
    let before = cluster.peer_pipeline_status(2).expect("pipeline before");
    assert_eq!(before.match_index, 2);
    assert!(before.inflight_entries > 0);

    cluster.set_node_healthy(2, true).expect("heal peer");
    cluster
        .record_network_error_for(2)
        .expect("network error probes peer");

    let after = cluster.peer_pipeline_status(2).expect("pipeline after");
    assert_eq!(
        after.match_index,
        cluster.status(1).expect("leader").last_log_index
    );
    assert_eq!(after.inflight_entries, 0);
}

#[test]
fn raft_cluster_runs_learner_catchup_and_witness_quorum_accounting() {
    let mut cluster =
        RaftCluster::new(99, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"a".to_vec()).expect("first write");
    cluster.propose(b"b".to_vec()).expect("second write");

    let mut learner = peer(4);
    learner.role = RustRaftReplicaRole::Learner;
    cluster.add_learner(learner).expect("add learner");
    let catchup = cluster.learner_catch_up_loop(4).expect("catch up");
    assert!(catchup.caught_up);
    assert_eq!(
        catchup.learner_match_index_after,
        catchup.leader_commit_index
    );
    assert!(
        cluster
            .catchup_report(4)
            .expect("catchup report")
            .promotable
    );
    let learner_pipeline = cluster.peer_pipeline_status(4).expect("learner pipeline");
    assert_eq!(learner_pipeline.learner_catchup_rounds, 2);
    assert!(learner_pipeline.learner_caught_up);
    assert_eq!(learner_pipeline.follower_lag, 0);

    let mut witness = peer(5);
    witness.role = RustRaftReplicaRole::Witness;
    cluster.add_witness(witness).expect("add witness");
    let quorum = cluster.witness_quorum_report([1, 2, 5]);
    assert_eq!(quorum.required, 3);
    assert_eq!(quorum.acknowledged, 3);
    assert!(quorum.reached);
    assert_eq!(quorum.witnesses, vec![5]);
    let witness_pipeline = cluster.peer_pipeline_status(5).expect("witness pipeline");
    assert_eq!(witness_pipeline.witness_quorum_required, 3);
    assert_eq!(witness_pipeline.witness_quorum_acked, 3);
    assert!(witness_pipeline.witness_quorum_reached);
}

#[test]
fn raft_cluster_peer_catchup_records_snapshot_rejoin_lifecycle() {
    let mut cluster =
        RaftCluster::new(88, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.set_node_healthy(2, false).expect("isolate peer");
    cluster.propose(b"a".to_vec()).expect("first write");
    cluster.propose(b"b".to_vec()).expect("second write");
    cluster.propose(b"c".to_vec()).expect("third write");

    let snapshot = cluster
        .checkpoint_snapshot(1, "leader-snapshot-4")
        .expect("checkpoint");
    cluster.set_node_healthy(2, true).expect("restore peer");
    cluster
        .install_snapshot_with_tail_to(
            2,
            snapshot,
            matrixraft::RustRaftApplySnapshotFence {
                applied_index: 4,
                commit_index: 4,
                installed_snapshot_index: 4,
                first_retained_log_index: 5,
            },
            vec![],
        )
        .expect("install follower snapshot");

    let catchup = cluster.catch_up_peer(2).expect("catch up peer");
    assert!(catchup.caught_up);
    assert_eq!(catchup.learner_match_index_after, 4);
    let pipeline = cluster.peer_pipeline_status(2).expect("pipeline");
    assert_eq!(pipeline.snapshot_installed_index, 4);
    assert!(pipeline.snapshot_rejoin_after_compacted_log);
    assert_eq!(pipeline.follower_lag, 0);
}

#[test]
fn raft_cluster_auto_promotes_marked_learner_after_catchup() {
    let mut cluster =
        RaftCluster::new(99, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"a".to_vec()).expect("first write");
    cluster.propose(b"b".to_vec()).expect("second write");

    let mut learner = peer(4);
    learner.role = RustRaftReplicaRole::Learner;
    learner.auto_promote = true;
    cluster.add_learner(learner).expect("add learner");

    let catchup = cluster.catch_up_peer(4).expect("catch up learner");
    assert!(catchup.caught_up);

    let membership = cluster.membership();
    assert!(membership.voters.contains(&4));
    assert!(!membership.learners.contains(&4));
    assert_eq!(
        cluster.status(4).expect("promoted learner status").role,
        RustRaftRole::Follower
    );
}
