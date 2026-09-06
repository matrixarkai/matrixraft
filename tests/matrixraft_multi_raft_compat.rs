// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    AppendEntriesRequest, AppendEntriesResponse, ApplySnapshotFence, InstallSnapshotRequest,
    InstallSnapshotResponse, LearnerAutoPromoteState, LogId, LogRetainedRange, MailPriority,
    MatrixRaftAdminCommand, MatrixRaftAdminCommandType, MatrixRaftAppendEntriesResponse,
    MatrixRaftApplyResultReport, MatrixRaftAsyncGroupSummary, MatrixRaftAsyncOperation,
    MatrixRaftAsyncResult, MatrixRaftAsyncResultStatus, MatrixRaftBatchRouteGroupSummary,
    MatrixRaftBatchRouteResult, MatrixRaftBatchRouteResultStatus,
    MatrixRaftBoundedStaleReadOptions, MatrixRaftCheckpoint, MatrixRaftConfState,
    MatrixRaftConfigChange, MatrixRaftConfigChangeType, MatrixRaftConfigurationApplied,
    MatrixRaftEntry, MatrixRaftEntryType, MatrixRaftFsm, MatrixRaftFsmRuntimeBinding,
    MatrixRaftGroupContextBuilder, MatrixRaftLeaseRequest, MatrixRaftLeaseResponse,
    MatrixRaftMessage, MatrixRaftMessageType, MatrixRaftMultiRaftServer,
    MatrixRaftNodeCreatorBuilder, MatrixRaftOldSnapshotFinish, MatrixRaftOldSnapshotFinishState,
    MatrixRaftOptions, MatrixRaftPriorityRoutedAdminCommand, MatrixRaftPriorityRoutedMessage,
    MatrixRaftPropose, MatrixRaftProposeOptions, MatrixRaftRateLimiterConfig,
    MatrixRaftReadIndexMode, MatrixRaftReadIndexOptions, MatrixRaftReplicatedReport,
    MatrixRaftResignReport, MatrixRaftRouteGroupSummary, MatrixRaftRouteKey, MatrixRaftRouteResult,
    MatrixRaftRouteResultKind, MatrixRaftRouteResultStatus, MatrixRaftRoutedAdminCommand,
    MatrixRaftRoutedMessage, MatrixRaftSnapshotDesc, MatrixRaftSnapshotProgress,
    MatrixRaftStepDownReport, MatrixRaftSyncedReport, MatrixRaftTransferLeaderReport,
    MatrixRaftTransportBuilder, MembershipOperation, Peer, RaftError, RaftSnapshot,
    ReadIndexRequest, ReadIndexResponse, ReplicaRole, SnapshotChunk, SnapshotMetadata,
    SnapshotState, StateRole, StorageApplyFence, TimeoutNowResponse, VoteRequest, VoteResponse,
    WalCompactionReport, WitnessQuorumReport,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-matrixraft-multi-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn peer(group_id: u64, node_id: u64) -> Peer {
    peer_with_role(group_id, node_id, ReplicaRole::Voter)
}

fn peer_with_role(group_id: u64, node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 30_000 + group_id + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 31_000 + group_id + node_id),
        role,
        auto_promote: false,
    }
}

fn options(
    group_id: u64,
    wal_dir: &std::path::Path,
    snapshot_dir: &std::path::Path,
) -> MatrixRaftOptions {
    options_with_role(group_id, wal_dir, snapshot_dir, ReplicaRole::Voter)
}

fn options_for_peer(
    group_id: u64,
    peer_id: u64,
    wal_dir: &std::path::Path,
    snapshot_dir: &std::path::Path,
) -> MatrixRaftOptions {
    let mut options = options(group_id, wal_dir, snapshot_dir);
    options.peer_id = peer_id;
    options.raft_addr = peer(group_id, peer_id).raft_addr;
    options.snapshot_addr = peer(group_id, peer_id).snapshot_addr;
    options
}

fn options_with_role(
    group_id: u64,
    wal_dir: &std::path::Path,
    snapshot_dir: &std::path::Path,
    local_role: ReplicaRole,
) -> MatrixRaftOptions {
    MatrixRaftOptions {
        group_id,
        peer_id: 1,
        raft_addr: peer(group_id, 1).raft_addr,
        snapshot_addr: peer(group_id, 1).snapshot_addr,
        wal_dir: wal_dir.display().to_string(),
        snapshot_dir: snapshot_dir.display().to_string(),
        peers: vec![
            peer_with_role(group_id, 1, local_role),
            peer(group_id, 2),
            peer(group_id, 3),
        ],
        role: local_role,
        wal_sync: true,
        election_cycle_tick: 4,
        transfer_timeout_tick: 3,
        offline_timeout_tick: 10,
        // A tick interval far longer than any test here suppresses the
        // runtime's automatic tick, which fires from the timeout arm of its
        // command loop whenever the command channel sits idle. That tick is
        // what made this binary load-sensitive: it advances leases and election
        // timers between two statements of a test, at a rate set by the
        // scheduler rather than by the test.
        //
        // The lease durations stay small on purpose, so tests that drive a tick
        // explicitly -- `tick_follower_lease(20)` and friends -- still cross
        // the expiry boundary exactly as they did.
        tick_interval_ms: 10_000,
        lease_duration_ms: 20,
        last_lease_duration_ms: 10,
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

fn assert_invalid_request_contains<T: Debug>(result: Result<T, RaftError>, expected: &str) {
    match result {
        Err(RaftError::InvalidRequest(message)) => assert!(
            message.contains(expected),
            "expected invalid request containing {expected:?}, got {message:?}"
        ),
        other => panic!("expected invalid request containing {expected:?}, got {other:?}"),
    }
}

#[test]
fn matrixraft_async_group_summary_exposes_read_and_timeout_payloads_by_route() {
    let read_key = MatrixRaftRouteKey::new(701, 1);
    let timeout_key = MatrixRaftRouteKey::new(701, 2);
    let error_read_key = MatrixRaftRouteKey::new(701, 3);
    let timed_out_timeout_key = MatrixRaftRouteKey::new(701, 4);
    let read_response = ReadIndexResponse {
        safe: true,
        read_index: 42,
        lease_read: true,
        reason: "lease read accepted".to_string(),
    };
    let error_read_response = ReadIndexResponse {
        safe: false,
        read_index: 43,
        lease_read: false,
        reason: "lease read rejected".to_string(),
    };
    let timeout_response = TimeoutNowResponse {
        node_id: 2,
        from: 1,
        campaigned: true,
        term: 7,
        leader_id: Some(2),
        reason: "timeout-now campaign accepted".to_string(),
    };
    let timed_out_timeout_response = TimeoutNowResponse {
        node_id: 4,
        from: 1,
        campaigned: false,
        term: 8,
        leader_id: Some(1),
        reason: "timeout-now callback expired".to_string(),
    };
    let mut read_result = MatrixRaftAsyncResult::ok(MatrixRaftAsyncOperation::ReadIndex, 50);
    read_result.read_index = Some(read_response.clone());
    let mut error_read_result =
        MatrixRaftAsyncResult::error(MatrixRaftAsyncOperation::ReadIndex, 50, "read failed");
    error_read_result.read_index = Some(error_read_response.clone());
    let mut timeout_result = MatrixRaftAsyncResult::ok(MatrixRaftAsyncOperation::TimeoutNow, 50);
    timeout_result.timeout_now = Some(timeout_response.clone());
    let mut timed_out_timeout_result =
        MatrixRaftAsyncResult::timeout(MatrixRaftAsyncOperation::TimeoutNow, 50);
    timed_out_timeout_result.timeout_now = Some(timed_out_timeout_response.clone());

    let summary = MatrixRaftAsyncGroupSummary::from_results(
        701,
        &[
            (read_key, read_result),
            (timeout_key, timeout_result),
            (error_read_key, error_read_result),
            (timed_out_timeout_key, timed_out_timeout_result),
        ],
    );

    assert_eq!(
        summary.read_index_responses_by_route_key(),
        vec![
            (read_key, Some(read_response.clone())),
            (timeout_key, None),
            (error_read_key, Some(error_read_response.clone())),
            (timed_out_timeout_key, None)
        ]
    );
    assert_eq!(
        summary.ok_read_index_responses_by_route_key(),
        vec![(read_key, Some(read_response.clone())), (timeout_key, None)]
    );
    assert_eq!(
        summary.error_read_index_responses_by_route_key(),
        vec![(error_read_key, Some(error_read_response.clone()))]
    );
    assert_eq!(
        summary.timed_out_read_index_responses_by_route_key(),
        vec![(timed_out_timeout_key, None)]
    );
    assert_eq!(
        summary.read_indices_by_route_key(),
        vec![
            (read_key, Some(42)),
            (timeout_key, None),
            (error_read_key, Some(43)),
            (timed_out_timeout_key, None)
        ]
    );
    assert_eq!(
        summary.ok_read_indices_by_route_key(),
        vec![(read_key, Some(42)), (timeout_key, None)]
    );
    assert_eq!(
        summary.error_read_indices_by_route_key(),
        vec![(error_read_key, Some(43))]
    );
    assert_eq!(
        summary.timed_out_read_indices_by_route_key(),
        vec![(timed_out_timeout_key, None)]
    );
    assert_eq!(
        summary.read_index_safe_by_route_key(),
        vec![
            (read_key, Some(true)),
            (timeout_key, None),
            (error_read_key, Some(false)),
            (timed_out_timeout_key, None)
        ]
    );
    assert_eq!(
        summary.ok_read_index_safe_by_route_key(),
        vec![(read_key, Some(true)), (timeout_key, None)]
    );
    assert_eq!(
        summary.error_read_index_safe_by_route_key(),
        vec![(error_read_key, Some(false))]
    );
    assert_eq!(
        summary.timed_out_read_index_safe_by_route_key(),
        vec![(timed_out_timeout_key, None)]
    );
    assert_eq!(
        summary.read_index_lease_read_by_route_key(),
        vec![
            (read_key, Some(true)),
            (timeout_key, None),
            (error_read_key, Some(false)),
            (timed_out_timeout_key, None)
        ]
    );
    assert_eq!(
        summary.ok_read_index_lease_read_by_route_key(),
        vec![(read_key, Some(true)), (timeout_key, None)]
    );
    assert_eq!(
        summary.error_read_index_lease_read_by_route_key(),
        vec![(error_read_key, Some(false))]
    );
    assert_eq!(
        summary.timed_out_read_index_lease_read_by_route_key(),
        vec![(timed_out_timeout_key, None)]
    );
    assert_eq!(
        summary.read_index_reasons_by_route_key(),
        vec![
            (read_key, Some("lease read accepted".to_string())),
            (timeout_key, None),
            (error_read_key, Some("lease read rejected".to_string())),
            (timed_out_timeout_key, None)
        ]
    );
    assert_eq!(
        summary.ok_read_index_reasons_by_route_key(),
        vec![
            (read_key, Some("lease read accepted".to_string())),
            (timeout_key, None)
        ]
    );
    assert_eq!(
        summary.error_read_index_reasons_by_route_key(),
        vec![(error_read_key, Some("lease read rejected".to_string()))]
    );
    assert_eq!(
        summary.timed_out_read_index_reasons_by_route_key(),
        vec![(timed_out_timeout_key, None)]
    );
    assert_eq!(
        summary.timeout_now_responses_by_route_key(),
        vec![
            (read_key, None),
            (timeout_key, Some(timeout_response.clone())),
            (error_read_key, None),
            (
                timed_out_timeout_key,
                Some(timed_out_timeout_response.clone())
            )
        ]
    );
    assert_eq!(
        summary.ok_timeout_now_responses_by_route_key(),
        vec![
            (read_key, None),
            (timeout_key, Some(timeout_response.clone()))
        ]
    );
    assert_eq!(
        summary.error_timeout_now_responses_by_route_key(),
        vec![(error_read_key, None)]
    );
    assert_eq!(
        summary.timed_out_timeout_now_responses_by_route_key(),
        vec![(
            timed_out_timeout_key,
            Some(timed_out_timeout_response.clone())
        )]
    );
    assert_eq!(
        summary.timeout_now_node_ids_by_route_key(),
        vec![
            (read_key, None),
            (timeout_key, Some(2)),
            (error_read_key, None),
            (timed_out_timeout_key, Some(4))
        ]
    );
    assert_eq!(
        summary.ok_timeout_now_node_ids_by_route_key(),
        vec![(read_key, None), (timeout_key, Some(2))]
    );
    assert_eq!(
        summary.error_timeout_now_node_ids_by_route_key(),
        vec![(error_read_key, None)]
    );
    assert_eq!(
        summary.timed_out_timeout_now_node_ids_by_route_key(),
        vec![(timed_out_timeout_key, Some(4))]
    );
    assert_eq!(
        summary.timeout_now_from_ids_by_route_key(),
        vec![
            (read_key, None),
            (timeout_key, Some(1)),
            (error_read_key, None),
            (timed_out_timeout_key, Some(1))
        ]
    );
    assert_eq!(
        summary.ok_timeout_now_from_ids_by_route_key(),
        vec![(read_key, None), (timeout_key, Some(1))]
    );
    assert_eq!(
        summary.error_timeout_now_from_ids_by_route_key(),
        vec![(error_read_key, None)]
    );
    assert_eq!(
        summary.timed_out_timeout_now_from_ids_by_route_key(),
        vec![(timed_out_timeout_key, Some(1))]
    );
    assert_eq!(
        summary.timeout_now_campaigned_by_route_key(),
        vec![
            (read_key, None),
            (timeout_key, Some(true)),
            (error_read_key, None),
            (timed_out_timeout_key, Some(false))
        ]
    );
    assert_eq!(
        summary.ok_timeout_now_campaigned_by_route_key(),
        vec![(read_key, None), (timeout_key, Some(true))]
    );
    assert_eq!(
        summary.error_timeout_now_campaigned_by_route_key(),
        vec![(error_read_key, None)]
    );
    assert_eq!(
        summary.timed_out_timeout_now_campaigned_by_route_key(),
        vec![(timed_out_timeout_key, Some(false))]
    );
    assert_eq!(
        summary.timeout_now_terms_by_route_key(),
        vec![
            (read_key, None),
            (timeout_key, Some(7)),
            (error_read_key, None),
            (timed_out_timeout_key, Some(8))
        ]
    );
    assert_eq!(
        summary.ok_timeout_now_terms_by_route_key(),
        vec![(read_key, None), (timeout_key, Some(7))]
    );
    assert_eq!(
        summary.error_timeout_now_terms_by_route_key(),
        vec![(error_read_key, None)]
    );
    assert_eq!(
        summary.timed_out_timeout_now_terms_by_route_key(),
        vec![(timed_out_timeout_key, Some(8))]
    );
    assert_eq!(
        summary.timeout_now_leader_ids_by_route_key(),
        vec![
            (read_key, None),
            (timeout_key, Some(2)),
            (error_read_key, None),
            (timed_out_timeout_key, Some(1))
        ]
    );
    assert_eq!(
        summary.ok_timeout_now_leader_ids_by_route_key(),
        vec![(read_key, None), (timeout_key, Some(2))]
    );
    assert_eq!(
        summary.error_timeout_now_leader_ids_by_route_key(),
        vec![(error_read_key, None)]
    );
    assert_eq!(
        summary.timed_out_timeout_now_leader_ids_by_route_key(),
        vec![(timed_out_timeout_key, Some(1))]
    );
    assert_eq!(
        summary.timeout_now_reasons_by_route_key(),
        vec![
            (read_key, None),
            (
                timeout_key,
                Some("timeout-now campaign accepted".to_string())
            ),
            (error_read_key, None),
            (
                timed_out_timeout_key,
                Some("timeout-now callback expired".to_string())
            )
        ]
    );
    assert_eq!(
        summary.ok_timeout_now_reasons_by_route_key(),
        vec![
            (read_key, None),
            (
                timeout_key,
                Some("timeout-now campaign accepted".to_string())
            )
        ]
    );
    assert_eq!(
        summary.error_timeout_now_reasons_by_route_key(),
        vec![(error_read_key, None)]
    );
    assert_eq!(
        summary.timed_out_timeout_now_reasons_by_route_key(),
        vec![(
            timed_out_timeout_key,
            Some("timeout-now callback expired".to_string())
        )]
    );
}

#[test]
fn matrixraft_async_group_summary_exposes_leadership_payloads_by_status() {
    let transfer_key = MatrixRaftRouteKey::new(701, 11);
    let step_key = MatrixRaftRouteKey::new(701, 12);
    let resign_key = MatrixRaftRouteKey::new(701, 13);

    let mut transfer_result =
        MatrixRaftAsyncResult::ok(MatrixRaftAsyncOperation::TransferLeader, 50);
    transfer_result.transfer_leader = Some(MatrixRaftTransferLeaderReport {
        transferee_id: 4,
        transferee_node: None,
        state: None,
        transferred: true,
    });

    let mut step_result =
        MatrixRaftAsyncResult::error(MatrixRaftAsyncOperation::StepDown, 50, "step-down rejected");
    step_result.step_down = Some(MatrixRaftStepDownReport {
        requested_transferee_id: Some(5),
        transferee_id: Some(5),
        transferee_node: None,
        state: None,
        stepped_down: false,
    });

    let mut resign_result =
        MatrixRaftAsyncResult::timeout(MatrixRaftAsyncOperation::ResignLeader, 50);
    resign_result.resign = Some(MatrixRaftResignReport {
        reason: "leader lease expired".to_string(),
        leader_before: Some(1),
        leader_after: None,
        resigned: false,
    });

    let summary = MatrixRaftAsyncGroupSummary::from_results(
        701,
        &[
            (transfer_key, transfer_result),
            (step_key, step_result),
            (resign_key, resign_result),
        ],
    );

    assert_eq!(
        summary.transfer_leader_presence_by_route_key(),
        vec![(transfer_key, true), (step_key, false), (resign_key, false)]
    );
    assert_eq!(
        summary.ok_transfer_leader_presence_by_route_key(),
        vec![(transfer_key, true)]
    );
    assert_eq!(
        summary.error_transfer_leader_presence_by_route_key(),
        vec![(step_key, false)]
    );
    assert_eq!(
        summary.timed_out_transfer_leader_presence_by_route_key(),
        vec![(resign_key, false)]
    );
    assert_eq!(
        summary.transfer_leader_transferee_ids_by_route_key(),
        vec![
            (transfer_key, Some(4)),
            (step_key, None),
            (resign_key, None)
        ]
    );
    assert_eq!(
        summary.ok_transfer_leader_transferee_ids_by_route_key(),
        vec![(transfer_key, Some(4))]
    );
    assert_eq!(
        summary.error_transfer_leader_transferee_ids_by_route_key(),
        vec![(step_key, None)]
    );
    assert_eq!(
        summary.timed_out_transfer_leader_transferee_ids_by_route_key(),
        vec![(resign_key, None)]
    );
    assert_eq!(
        summary.ok_transfer_leader_transferred_by_route_key(),
        vec![(transfer_key, Some(true))]
    );
    assert_eq!(
        summary.error_transfer_leader_transferred_by_route_key(),
        vec![(step_key, None)]
    );
    assert_eq!(
        summary.timed_out_transfer_leader_transferred_by_route_key(),
        vec![(resign_key, None)]
    );

    assert_eq!(
        summary.step_down_presence_by_route_key(),
        vec![(transfer_key, false), (step_key, true), (resign_key, false)]
    );
    assert_eq!(
        summary.ok_step_down_presence_by_route_key(),
        vec![(transfer_key, false)]
    );
    assert_eq!(
        summary.error_step_down_presence_by_route_key(),
        vec![(step_key, true)]
    );
    assert_eq!(
        summary.timed_out_step_down_presence_by_route_key(),
        vec![(resign_key, false)]
    );
    assert_eq!(
        summary.ok_step_down_requested_transferee_ids_by_route_key(),
        vec![(transfer_key, None)]
    );
    assert_eq!(
        summary.error_step_down_requested_transferee_ids_by_route_key(),
        vec![(step_key, Some(5))]
    );
    assert_eq!(
        summary.timed_out_step_down_requested_transferee_ids_by_route_key(),
        vec![(resign_key, None)]
    );
    assert_eq!(
        summary.ok_step_down_transferee_ids_by_route_key(),
        vec![(transfer_key, None)]
    );
    assert_eq!(
        summary.error_step_down_transferee_ids_by_route_key(),
        vec![(step_key, Some(5))]
    );
    assert_eq!(
        summary.timed_out_step_down_transferee_ids_by_route_key(),
        vec![(resign_key, None)]
    );
    assert_eq!(
        summary.ok_step_down_stepped_down_by_route_key(),
        vec![(transfer_key, None)]
    );
    assert_eq!(
        summary.error_step_down_stepped_down_by_route_key(),
        vec![(step_key, Some(false))]
    );
    assert_eq!(
        summary.timed_out_step_down_stepped_down_by_route_key(),
        vec![(resign_key, None)]
    );

    assert_eq!(
        summary.resign_presence_by_route_key(),
        vec![(transfer_key, false), (step_key, false), (resign_key, true)]
    );
    assert_eq!(
        summary.ok_resign_presence_by_route_key(),
        vec![(transfer_key, false)]
    );
    assert_eq!(
        summary.error_resign_presence_by_route_key(),
        vec![(step_key, false)]
    );
    assert_eq!(
        summary.timed_out_resign_presence_by_route_key(),
        vec![(resign_key, true)]
    );
    assert_eq!(
        summary.ok_resign_reasons_by_route_key(),
        vec![(transfer_key, None)]
    );
    assert_eq!(
        summary.error_resign_reasons_by_route_key(),
        vec![(step_key, None)]
    );
    assert_eq!(
        summary.timed_out_resign_reasons_by_route_key(),
        vec![(resign_key, Some("leader lease expired".to_string()))]
    );
    assert_eq!(
        summary.ok_resign_resigned_by_route_key(),
        vec![(transfer_key, None)]
    );
    assert_eq!(
        summary.error_resign_resigned_by_route_key(),
        vec![(step_key, None)]
    );
    assert_eq!(
        summary.timed_out_resign_resigned_by_route_key(),
        vec![(resign_key, Some(false))]
    );
}

#[test]
fn matrixraft_route_summaries_expose_response_payloads_by_route() {
    let key = MatrixRaftRouteKey::new(702, 3);
    let proposed_log_id = LogId { term: 4, index: 9 };
    let read_response = ReadIndexResponse {
        safe: true,
        read_index: 9,
        lease_read: false,
        reason: "quorum read accepted".to_string(),
    };
    let append_response = MatrixRaftAppendEntriesResponse {
        received: true,
        matched_index: Some(9),
        rejected_hint: None,
        rejected_index: None,
    };
    let install_response = InstallSnapshotResponse {
        term: 4,
        accepted: true,
        next_offset: 128,
        committed_index: 9,
        reason: "snapshot chunk accepted".to_string(),
    };
    let vote_response = VoteResponse {
        term: 4,
        vote_granted: true,
        reason: "campaign freshness accepted".to_string(),
    };
    let timeout_response = TimeoutNowResponse {
        node_id: 3,
        from: 1,
        campaigned: true,
        term: 5,
        leader_id: Some(3),
        reason: "leadership transfer campaign accepted".to_string(),
    };
    let snapshot = MatrixRaftSnapshotDesc {
        snapshot_id: Some("snapshot-702-9".to_string()),
        index: 9,
        term: 4,
        members: Vec::new(),
        checksum_type: Some("crc32".to_string()),
        checksum: Some(17),
        url: Some("local://snapshot-702-9".to_string()),
        local: true,
        version: 1,
    };
    let apply_result = MatrixRaftApplyResultReport {
        node_id: 3,
        applied_index: 9,
        rejected: false,
    };
    let synced = MatrixRaftSyncedReport {
        first_index: Some(1),
        last_index: Some(9),
        stabled_config_change_index: 8,
    };
    let replicated = MatrixRaftReplicatedReport {
        peer_id: 4,
        success: true,
    };
    let retained_range = LogRetainedRange {
        first_log_index: 5,
        last_log_index: 9,
        first_segment_id: 1,
        last_segment_id: 1,
        record_count: 5,
    };
    let compaction = WalCompactionReport {
        requested_log_index: 5,
        released_segments: 1,
        retained_range: retained_range.clone(),
        fence_valid: true,
        blocker: None,
    };
    let checkpoint = RaftSnapshot {
        group_id: 702,
        meta: SnapshotMetadata {
            snapshot_id: "checkpoint-702-9".to_string(),
            last_log_id: proposed_log_id.clone(),
            membership: vec![1, 3, 4],
            members: Vec::new(),
        },
        payload: b"checkpoint payload".to_vec(),
    };
    let witness = WitnessQuorumReport {
        required: 2,
        acknowledged: 2,
        reached: true,
        voters: vec![1, 3],
        witnesses: vec![4],
    };
    let transfer_leader = MatrixRaftTransferLeaderReport {
        transferee_id: 4,
        transferee_node: None,
        state: None,
        transferred: true,
    };
    let step_down = MatrixRaftStepDownReport {
        requested_transferee_id: Some(4),
        transferee_id: Some(4),
        transferee_node: None,
        state: None,
        stepped_down: true,
    };
    let resign = MatrixRaftResignReport {
        reason: "manual leadership release".to_string(),
        leader_before: Some(3),
        leader_after: Some(4),
        resigned: true,
    };
    let route = MatrixRaftRouteResult {
        key,
        message_type: MatrixRaftMessageType::AppendEntriesResponse,
        kind: MatrixRaftRouteResultKind::Delivered,
        handled: true,
        detail: "response payload projection".to_string(),
        proposed_log_id: Some(proposed_log_id.clone()),
        membership: None,
        append_entries_response: Some(append_response.clone()),
        install_snapshot_response: Some(install_response.clone()),
        read_index_response: Some(read_response.clone()),
        catch_up: None,
        promote: None,
        auto_promote: None,
        vote_response: Some(vote_response.clone()),
        campaign_candidate_id: Some(3),
        campaign_forced: Some(true),
        transfer_leader: Some(transfer_leader.clone()),
        leader_transfer_completed: Some(true),
        leader_transfer_aborted: Some(false),
        step_down: Some(step_down.clone()),
        resign: Some(resign.clone()),
        timeout_now_response: Some(timeout_response.clone()),
        snapshot: Some(snapshot.clone()),
        snapshot_peer_report: None,
        apply_result: Some(apply_result.clone()),
        synced: Some(synced.clone()),
        replicated: Some(replicated.clone()),
        compacted_logs: Some(5),
        fenced_compaction: Some(compaction.clone()),
        checkpoint: Some(checkpoint.clone()),
        witness_quorum: Some(witness.clone()),
        released_memory: Some(true),
        leader_lease_valid: Some(true),
        leader_lease_confirmed: Some(true),
        leader_lease_expired: Some(false),
        follower_lease_received: Some(true),
        follower_lease_expired: Some(false),
        node_healthy: Some(true),
        reorder_queue_dropped: Some(2),
        fatal_event_transfer_target: Some(5),
    };

    let route_summary =
        MatrixRaftRouteGroupSummary::from_results(702, std::slice::from_ref(&route));
    assert_eq!(
        route_summary.proposed_log_ids_by_route_key(),
        vec![(key, Some(proposed_log_id.clone()))]
    );
    assert_eq!(
        route_summary.read_index_responses_by_route_key(),
        vec![(key, Some(read_response.clone()))]
    );
    assert_eq!(
        route_summary.append_entries_responses_by_route_key(),
        vec![(key, Some(append_response.clone()))]
    );
    assert_eq!(
        route_summary.install_snapshot_responses_by_route_key(),
        vec![(key, Some(install_response.clone()))]
    );
    assert_eq!(
        route_summary.vote_responses_by_route_key(),
        vec![(key, Some(vote_response.clone()))]
    );
    assert_eq!(
        route_summary.timeout_now_responses_by_route_key(),
        vec![(key, Some(timeout_response.clone()))]
    );
    assert_eq!(
        route_summary.handled_proposed_log_ids_by_route_key(),
        vec![(key, Some(proposed_log_id.clone()))]
    );
    assert_eq!(
        route_summary.handled_proposed_log_id_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_read_index_responses_by_route_key(),
        vec![(key, Some(read_response.clone()))]
    );
    assert_eq!(
        route_summary.handled_read_index_response_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_append_entries_responses_by_route_key(),
        vec![(key, Some(append_response.clone()))]
    );
    assert_eq!(
        route_summary.handled_append_entries_response_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_install_snapshot_responses_by_route_key(),
        vec![(key, Some(install_response.clone()))]
    );
    assert_eq!(
        route_summary.handled_install_snapshot_response_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_vote_responses_by_route_key(),
        vec![(key, Some(vote_response.clone()))]
    );
    assert_eq!(
        route_summary.handled_vote_response_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_timeout_now_responses_by_route_key(),
        vec![(key, Some(timeout_response.clone()))]
    );
    assert_eq!(
        route_summary.handled_timeout_now_response_presence_by_route_key(),
        vec![(key, true)]
    );
    assert!(route_summary
        .unhandled_read_index_responses_by_route_key()
        .is_empty());
    assert!(route_summary
        .unhandled_read_index_response_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.campaign_candidate_ids_by_route_key(),
        vec![(key, Some(3))]
    );
    assert_eq!(
        route_summary.handled_campaign_candidate_ids_by_route_key(),
        vec![(key, Some(3))]
    );
    assert!(route_summary
        .unhandled_campaign_candidate_ids_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.handled_campaign_forced_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_transfer_leader_transferee_ids_by_route_key(),
        vec![(key, Some(4))]
    );
    assert_eq!(
        route_summary.handled_transfer_leader_transferred_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_leader_transfer_completed_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_leader_transfer_aborted_by_route_key(),
        vec![(key, Some(false))]
    );
    assert_eq!(
        route_summary.handled_step_down_requested_transferee_ids_by_route_key(),
        vec![(key, Some(4))]
    );
    assert_eq!(
        route_summary.handled_step_down_transferee_ids_by_route_key(),
        vec![(key, Some(4))]
    );
    assert_eq!(
        route_summary.handled_step_down_stepped_down_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_resign_reasons_by_route_key(),
        vec![(key, Some("manual leadership release".to_string()))]
    );
    assert_eq!(
        route_summary.handled_resign_resigned_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.snapshots_by_route_key(),
        vec![(key, Some(snapshot.clone()))]
    );
    assert_eq!(
        route_summary.handled_snapshots_by_route_key(),
        vec![(key, Some(snapshot.clone()))]
    );
    assert_eq!(
        route_summary.handled_snapshot_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_snapshot_ids_by_route_key(),
        vec![(key, Some("snapshot-702-9".to_string()))]
    );
    assert_eq!(
        route_summary.handled_snapshot_indices_by_route_key(),
        vec![(key, Some(9))]
    );
    assert!(route_summary.unhandled_snapshots_by_route_key().is_empty());
    assert!(route_summary
        .unhandled_snapshot_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.handled_snapshot_peer_reports_by_route_key(),
        vec![(key, None)]
    );
    assert_eq!(
        route_summary.handled_snapshot_peer_report_presence_by_route_key(),
        vec![(key, false)]
    );
    assert_eq!(
        route_summary.handled_snapshot_peer_ids_by_route_key(),
        vec![(key, None)]
    );
    assert!(route_summary
        .unhandled_snapshot_peer_reports_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.apply_results_by_route_key(),
        vec![(key, Some(apply_result.clone()))]
    );
    assert_eq!(
        route_summary.handled_apply_results_by_route_key(),
        vec![(key, Some(apply_result.clone()))]
    );
    assert_eq!(
        route_summary.handled_apply_result_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_apply_result_node_ids_by_route_key(),
        vec![(key, Some(3))]
    );
    assert_eq!(
        route_summary.handled_applied_indices_by_route_key(),
        vec![(key, Some(9))]
    );
    assert_eq!(
        route_summary.handled_apply_rejected_by_route_key(),
        vec![(key, Some(false))]
    );
    assert!(route_summary
        .unhandled_apply_results_by_route_key()
        .is_empty());
    assert!(route_summary
        .unhandled_apply_result_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.synced_reports_by_route_key(),
        vec![(key, Some(synced.clone()))]
    );
    assert_eq!(
        route_summary.handled_synced_reports_by_route_key(),
        vec![(key, Some(synced.clone()))]
    );
    assert_eq!(
        route_summary.handled_synced_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_synced_first_indices_by_route_key(),
        vec![(key, Some(1))]
    );
    assert_eq!(
        route_summary.handled_synced_last_indices_by_route_key(),
        vec![(key, Some(9))]
    );
    assert_eq!(
        route_summary.handled_synced_stabled_config_change_indices_by_route_key(),
        vec![(key, Some(8))]
    );
    assert!(route_summary
        .unhandled_synced_reports_by_route_key()
        .is_empty());
    assert!(route_summary
        .unhandled_synced_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.replicated_reports_by_route_key(),
        vec![(key, Some(replicated.clone()))]
    );
    assert_eq!(
        route_summary.handled_replicated_reports_by_route_key(),
        vec![(key, Some(replicated.clone()))]
    );
    assert_eq!(
        route_summary.handled_replicated_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_replicated_peer_ids_by_route_key(),
        vec![(key, Some(4))]
    );
    assert_eq!(
        route_summary.handled_replicated_success_by_route_key(),
        vec![(key, Some(true))]
    );
    assert!(route_summary
        .unhandled_replicated_reports_by_route_key()
        .is_empty());
    assert!(route_summary
        .unhandled_replicated_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.compacted_logs_by_route_key(),
        vec![(key, Some(5))]
    );
    assert_eq!(
        route_summary.handled_compacted_logs_by_route_key(),
        vec![(key, Some(5))]
    );
    assert_eq!(
        route_summary.handled_compacted_logs_presence_by_route_key(),
        vec![(key, true)]
    );
    assert!(route_summary
        .unhandled_compacted_logs_by_route_key()
        .is_empty());
    assert!(route_summary
        .unhandled_compacted_logs_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.fenced_compactions_by_route_key(),
        vec![(key, Some(compaction.clone()))]
    );
    assert_eq!(
        route_summary.handled_fenced_compactions_by_route_key(),
        vec![(key, Some(compaction.clone()))]
    );
    assert_eq!(
        route_summary.handled_fenced_compaction_presence_by_route_key(),
        vec![(key, true)]
    );
    assert!(route_summary
        .unhandled_fenced_compactions_by_route_key()
        .is_empty());
    assert!(route_summary
        .unhandled_fenced_compaction_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.checkpoints_by_route_key(),
        vec![(key, Some(checkpoint.clone()))]
    );
    assert_eq!(
        route_summary.handled_checkpoints_by_route_key(),
        vec![(key, Some(checkpoint.clone()))]
    );
    assert_eq!(
        route_summary.handled_checkpoint_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_checkpoint_snapshot_ids_by_route_key(),
        vec![(key, Some("checkpoint-702-9".to_string()))]
    );
    assert_eq!(
        route_summary.handled_checkpoint_last_log_indices_by_route_key(),
        vec![(key, Some(9))]
    );
    assert!(route_summary
        .unhandled_checkpoints_by_route_key()
        .is_empty());
    assert!(route_summary
        .unhandled_checkpoint_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.witness_quorums_by_route_key(),
        vec![(key, Some(witness.clone()))]
    );
    assert_eq!(
        route_summary.handled_witness_quorums_by_route_key(),
        vec![(key, Some(witness.clone()))]
    );
    assert_eq!(
        route_summary.handled_witness_quorum_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_witness_quorum_required_by_route_key(),
        vec![(key, Some(2))]
    );
    assert_eq!(
        route_summary.handled_witness_quorum_acknowledged_by_route_key(),
        vec![(key, Some(2))]
    );
    assert_eq!(
        route_summary.handled_witness_quorum_reached_by_route_key(),
        vec![(key, Some(true))]
    );
    assert!(route_summary
        .unhandled_witness_quorums_by_route_key()
        .is_empty());
    assert!(route_summary
        .unhandled_witness_quorum_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.released_memory_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_released_memory_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_released_memory_presence_by_route_key(),
        vec![(key, true)]
    );
    assert!(route_summary
        .unhandled_released_memory_values_by_route_key()
        .is_empty());
    assert!(route_summary
        .unhandled_released_memory_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.leader_lease_valid_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_leader_lease_valid_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_leader_lease_valid_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_leader_lease_confirmed_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_leader_lease_confirmed_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_leader_lease_expired_values_by_route_key(),
        vec![(key, Some(false))]
    );
    assert_eq!(
        route_summary.handled_leader_lease_expired_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_follower_lease_received_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_follower_lease_received_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_follower_lease_expired_values_by_route_key(),
        vec![(key, Some(false))]
    );
    assert_eq!(
        route_summary.handled_follower_lease_expired_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_node_healthy_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        route_summary.handled_node_healthy_presence_by_route_key(),
        vec![(key, true)]
    );
    assert!(route_summary
        .unhandled_node_healthy_values_by_route_key()
        .is_empty());
    assert!(route_summary
        .unhandled_node_healthy_presence_by_route_key()
        .is_empty());
    assert_eq!(
        route_summary.handled_reorder_queue_dropped_values_by_route_key(),
        vec![(key, Some(2))]
    );
    assert_eq!(
        route_summary.handled_reorder_queue_dropped_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        route_summary.handled_fatal_event_transfer_targets_by_route_key(),
        vec![(key, Some(5))]
    );
    assert_eq!(
        route_summary.handled_fatal_event_transfer_target_presence_by_route_key(),
        vec![(key, true)]
    );

    let batch = MatrixRaftBatchRouteResult {
        group_id: key.group_id,
        runtime_node_id: key.node_id,
        message_type: MatrixRaftMessageType::AppendEntriesResponse,
        result: Some(route),
        error: None,
    };
    let batch_summary = MatrixRaftBatchRouteGroupSummary::from_results(702, &[batch]);
    assert_eq!(
        batch_summary.proposed_log_ids_by_route_key(),
        vec![(key, Some(proposed_log_id.clone()))]
    );
    assert_eq!(
        batch_summary.read_index_responses_by_route_key(),
        vec![(key, Some(read_response.clone()))]
    );
    assert_eq!(
        batch_summary.append_entries_responses_by_route_key(),
        vec![(key, Some(append_response.clone()))]
    );
    assert_eq!(
        batch_summary.install_snapshot_responses_by_route_key(),
        vec![(key, Some(install_response.clone()))]
    );
    assert_eq!(
        batch_summary.vote_responses_by_route_key(),
        vec![(key, Some(vote_response.clone()))]
    );
    assert_eq!(
        batch_summary.timeout_now_responses_by_route_key(),
        vec![(key, Some(timeout_response.clone()))]
    );
    assert_eq!(
        batch_summary.ok_proposed_log_ids_by_route_key(),
        vec![(key, Some(proposed_log_id.clone()))]
    );
    assert_eq!(
        batch_summary.ok_proposed_log_id_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_read_index_responses_by_route_key(),
        vec![(key, Some(read_response.clone()))]
    );
    assert_eq!(
        batch_summary.ok_read_index_response_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_append_entries_responses_by_route_key(),
        vec![(key, Some(append_response.clone()))]
    );
    assert_eq!(
        batch_summary.ok_append_entries_response_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_install_snapshot_responses_by_route_key(),
        vec![(key, Some(install_response.clone()))]
    );
    assert_eq!(
        batch_summary.ok_install_snapshot_response_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_vote_responses_by_route_key(),
        vec![(key, Some(vote_response.clone()))]
    );
    assert_eq!(
        batch_summary.ok_vote_response_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_timeout_now_responses_by_route_key(),
        vec![(key, Some(timeout_response.clone()))]
    );
    assert_eq!(
        batch_summary.ok_timeout_now_response_presence_by_route_key(),
        vec![(key, true)]
    );
    assert!(batch_summary
        .error_read_index_responses_by_route_key()
        .is_empty());
    assert!(batch_summary
        .error_read_index_response_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.campaign_candidate_ids_by_route_key(),
        vec![(key, Some(3))]
    );
    assert_eq!(
        batch_summary.ok_campaign_candidate_ids_by_route_key(),
        vec![(key, Some(3))]
    );
    assert!(batch_summary
        .error_campaign_candidate_ids_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.ok_campaign_forced_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_transfer_leader_transferee_ids_by_route_key(),
        vec![(key, Some(4))]
    );
    assert_eq!(
        batch_summary.ok_transfer_leader_transferred_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_leader_transfer_completed_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_leader_transfer_aborted_by_route_key(),
        vec![(key, Some(false))]
    );
    assert_eq!(
        batch_summary.ok_step_down_requested_transferee_ids_by_route_key(),
        vec![(key, Some(4))]
    );
    assert_eq!(
        batch_summary.ok_step_down_transferee_ids_by_route_key(),
        vec![(key, Some(4))]
    );
    assert_eq!(
        batch_summary.ok_step_down_stepped_down_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_resign_reasons_by_route_key(),
        vec![(key, Some("manual leadership release".to_string()))]
    );
    assert_eq!(
        batch_summary.ok_resign_resigned_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.snapshots_by_route_key(),
        vec![(key, Some(snapshot.clone()))]
    );
    assert_eq!(
        batch_summary.ok_snapshots_by_route_key(),
        vec![(key, Some(snapshot))]
    );
    assert_eq!(
        batch_summary.ok_snapshot_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_snapshot_ids_by_route_key(),
        vec![(key, Some("snapshot-702-9".to_string()))]
    );
    assert_eq!(
        batch_summary.ok_snapshot_indices_by_route_key(),
        vec![(key, Some(9))]
    );
    assert!(batch_summary.error_snapshots_by_route_key().is_empty());
    assert!(batch_summary
        .error_snapshot_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.ok_snapshot_peer_reports_by_route_key(),
        vec![(key, None)]
    );
    assert_eq!(
        batch_summary.ok_snapshot_peer_report_presence_by_route_key(),
        vec![(key, false)]
    );
    assert_eq!(
        batch_summary.ok_snapshot_peer_ids_by_route_key(),
        vec![(key, None)]
    );
    assert!(batch_summary
        .error_snapshot_peer_reports_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.snapshot_peer_reports_by_route_key(),
        vec![(key, None)]
    );
    assert_eq!(
        batch_summary.apply_results_by_route_key(),
        vec![(key, Some(apply_result.clone()))]
    );
    assert_eq!(
        batch_summary.ok_apply_results_by_route_key(),
        vec![(key, Some(apply_result))]
    );
    assert_eq!(
        batch_summary.ok_apply_result_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_apply_result_node_ids_by_route_key(),
        vec![(key, Some(3))]
    );
    assert_eq!(
        batch_summary.ok_applied_indices_by_route_key(),
        vec![(key, Some(9))]
    );
    assert_eq!(
        batch_summary.ok_apply_rejected_by_route_key(),
        vec![(key, Some(false))]
    );
    assert!(batch_summary.error_apply_results_by_route_key().is_empty());
    assert!(batch_summary
        .error_apply_result_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.synced_reports_by_route_key(),
        vec![(key, Some(synced.clone()))]
    );
    assert_eq!(
        batch_summary.ok_synced_reports_by_route_key(),
        vec![(key, Some(synced))]
    );
    assert_eq!(
        batch_summary.ok_synced_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_synced_first_indices_by_route_key(),
        vec![(key, Some(1))]
    );
    assert_eq!(
        batch_summary.ok_synced_last_indices_by_route_key(),
        vec![(key, Some(9))]
    );
    assert_eq!(
        batch_summary.ok_synced_stabled_config_change_indices_by_route_key(),
        vec![(key, Some(8))]
    );
    assert!(batch_summary.error_synced_reports_by_route_key().is_empty());
    assert!(batch_summary
        .error_synced_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.replicated_reports_by_route_key(),
        vec![(key, Some(replicated.clone()))]
    );
    assert_eq!(
        batch_summary.ok_replicated_reports_by_route_key(),
        vec![(key, Some(replicated))]
    );
    assert_eq!(
        batch_summary.ok_replicated_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_replicated_peer_ids_by_route_key(),
        vec![(key, Some(4))]
    );
    assert_eq!(
        batch_summary.ok_replicated_success_by_route_key(),
        vec![(key, Some(true))]
    );
    assert!(batch_summary
        .error_replicated_reports_by_route_key()
        .is_empty());
    assert!(batch_summary
        .error_replicated_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.compacted_logs_by_route_key(),
        vec![(key, Some(5))]
    );
    assert_eq!(
        batch_summary.ok_compacted_logs_by_route_key(),
        vec![(key, Some(5))]
    );
    assert_eq!(
        batch_summary.ok_compacted_logs_presence_by_route_key(),
        vec![(key, true)]
    );
    assert!(batch_summary.error_compacted_logs_by_route_key().is_empty());
    assert!(batch_summary
        .error_compacted_logs_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.fenced_compactions_by_route_key(),
        vec![(key, Some(compaction.clone()))]
    );
    assert_eq!(
        batch_summary.ok_fenced_compactions_by_route_key(),
        vec![(key, Some(compaction))]
    );
    assert_eq!(
        batch_summary.ok_fenced_compaction_presence_by_route_key(),
        vec![(key, true)]
    );
    assert!(batch_summary
        .error_fenced_compactions_by_route_key()
        .is_empty());
    assert!(batch_summary
        .error_fenced_compaction_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.checkpoints_by_route_key(),
        vec![(key, Some(checkpoint.clone()))]
    );
    assert_eq!(
        batch_summary.ok_checkpoints_by_route_key(),
        vec![(key, Some(checkpoint))]
    );
    assert_eq!(
        batch_summary.ok_checkpoint_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_checkpoint_snapshot_ids_by_route_key(),
        vec![(key, Some("checkpoint-702-9".to_string()))]
    );
    assert_eq!(
        batch_summary.ok_checkpoint_last_log_indices_by_route_key(),
        vec![(key, Some(9))]
    );
    assert!(batch_summary.error_checkpoints_by_route_key().is_empty());
    assert!(batch_summary
        .error_checkpoint_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.witness_quorums_by_route_key(),
        vec![(key, Some(witness.clone()))]
    );
    assert_eq!(
        batch_summary.ok_witness_quorums_by_route_key(),
        vec![(key, Some(witness))]
    );
    assert_eq!(
        batch_summary.ok_witness_quorum_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_witness_quorum_required_by_route_key(),
        vec![(key, Some(2))]
    );
    assert_eq!(
        batch_summary.ok_witness_quorum_acknowledged_by_route_key(),
        vec![(key, Some(2))]
    );
    assert_eq!(
        batch_summary.ok_witness_quorum_reached_by_route_key(),
        vec![(key, Some(true))]
    );
    assert!(batch_summary
        .error_witness_quorums_by_route_key()
        .is_empty());
    assert!(batch_summary
        .error_witness_quorum_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.released_memory_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_released_memory_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_released_memory_presence_by_route_key(),
        vec![(key, true)]
    );
    assert!(batch_summary
        .error_released_memory_values_by_route_key()
        .is_empty());
    assert!(batch_summary
        .error_released_memory_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.leader_lease_valid_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_leader_lease_valid_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_leader_lease_valid_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_leader_lease_confirmed_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_leader_lease_confirmed_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_leader_lease_expired_values_by_route_key(),
        vec![(key, Some(false))]
    );
    assert_eq!(
        batch_summary.ok_leader_lease_expired_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_follower_lease_received_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_follower_lease_received_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_follower_lease_expired_values_by_route_key(),
        vec![(key, Some(false))]
    );
    assert_eq!(
        batch_summary.ok_follower_lease_expired_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_node_healthy_values_by_route_key(),
        vec![(key, Some(true))]
    );
    assert_eq!(
        batch_summary.ok_node_healthy_presence_by_route_key(),
        vec![(key, true)]
    );
    assert!(batch_summary
        .error_node_healthy_values_by_route_key()
        .is_empty());
    assert!(batch_summary
        .error_node_healthy_presence_by_route_key()
        .is_empty());
    assert_eq!(
        batch_summary.ok_reorder_queue_dropped_values_by_route_key(),
        vec![(key, Some(2))]
    );
    assert_eq!(
        batch_summary.ok_reorder_queue_dropped_presence_by_route_key(),
        vec![(key, true)]
    );
    assert_eq!(
        batch_summary.ok_fatal_event_transfer_targets_by_route_key(),
        vec![(key, Some(5))]
    );
    assert_eq!(
        batch_summary.ok_fatal_event_transfer_target_presence_by_route_key(),
        vec![(key, true)]
    );
}

#[test]
fn matrixraft_route_summaries_expose_status_filtered_details_by_route() {
    let handled_key = MatrixRaftRouteKey::new(703, 1);
    let unhandled_key = MatrixRaftRouteKey::new(703, 2);
    let route = |key: MatrixRaftRouteKey, handled: bool, detail: &str| MatrixRaftRouteResult {
        key,
        message_type: MatrixRaftMessageType::AppendEntriesRequest,
        kind: MatrixRaftRouteResultKind::Delivered,
        handled,
        detail: detail.to_string(),
        proposed_log_id: None,
        membership: None,
        append_entries_response: None,
        install_snapshot_response: None,
        read_index_response: None,
        catch_up: None,
        promote: None,
        auto_promote: None,
        vote_response: None,
        campaign_candidate_id: None,
        campaign_forced: None,
        transfer_leader: None,
        leader_transfer_completed: None,
        leader_transfer_aborted: None,
        step_down: None,
        resign: None,
        timeout_now_response: None,
        snapshot: None,
        snapshot_peer_report: None,
        apply_result: None,
        synced: None,
        replicated: None,
        compacted_logs: None,
        fenced_compaction: None,
        checkpoint: None,
        witness_quorum: None,
        released_memory: None,
        leader_lease_valid: None,
        leader_lease_confirmed: None,
        leader_lease_expired: None,
        follower_lease_received: None,
        follower_lease_expired: None,
        node_healthy: None,
        reorder_queue_dropped: None,
        fatal_event_transfer_target: None,
    };

    let strict_summary = MatrixRaftRouteGroupSummary::from_results(
        703,
        &[
            route(handled_key, true, "strict route delivered"),
            route(unhandled_key, false, "strict route ignored"),
        ],
    );
    assert_eq!(
        strict_summary.details_by_route_key(),
        vec![
            (handled_key, "strict route delivered".to_string()),
            (unhandled_key, "strict route ignored".to_string())
        ]
    );
    assert_eq!(
        strict_summary.handled_details_by_route_key(),
        vec![(handled_key, "strict route delivered".to_string())]
    );
    assert_eq!(
        strict_summary.unhandled_details_by_route_key(),
        vec![(unhandled_key, "strict route ignored".to_string())]
    );

    let ok_batch = MatrixRaftBatchRouteResult {
        group_id: handled_key.group_id,
        runtime_node_id: handled_key.node_id,
        message_type: MatrixRaftMessageType::AppendEntriesRequest,
        result: Some(route(handled_key, true, "best-effort route delivered")),
        error: None,
    };
    let error_batch = MatrixRaftBatchRouteResult {
        group_id: unhandled_key.group_id,
        runtime_node_id: unhandled_key.node_id,
        message_type: MatrixRaftMessageType::AppendEntriesRequest,
        result: None,
        error: Some("best-effort route failed".to_string()),
    };
    let batch_summary =
        MatrixRaftBatchRouteGroupSummary::from_results(703, &[ok_batch, error_batch]);
    assert_eq!(
        batch_summary.details_by_route_key(),
        vec![
            (handled_key, Some("best-effort route delivered".to_string())),
            (unhandled_key, None)
        ]
    );
    assert_eq!(
        batch_summary.ok_details_by_route_key(),
        vec![(handled_key, Some("best-effort route delivered".to_string()))]
    );
    assert_eq!(
        batch_summary.error_details_by_route_key(),
        vec![(unhandled_key, Some("best-effort route failed".to_string()))]
    );
}

#[derive(Debug, Default)]
struct RuntimeSyncFsm {
    events: Vec<String>,
}

impl MatrixRaftFsm for RuntimeSyncFsm {
    fn open(&mut self) -> Result<(), RaftError> {
        self.events.push("open".to_string());
        Ok(())
    }

    fn close(&mut self) -> Result<(), RaftError> {
        self.events.push("close".to_string());
        Ok(())
    }

    fn on_start_following(&mut self, term: u64, leader_id: u64) -> Result<(), RaftError> {
        self.events.push(format!("follow:{term}:{leader_id}"));
        Ok(())
    }

    fn on_stop_following(&mut self, term: u64, leader_id: u64) -> Result<(), RaftError> {
        self.events.push(format!("stop-follow:{term}:{leader_id}"));
        Ok(())
    }

    fn on_leader_start(&mut self, term: u64) -> Result<(), RaftError> {
        self.events.push(format!("lead:{term}"));
        Ok(())
    }

    fn on_leader_stop(&mut self, term: u64) -> Result<(), RaftError> {
        self.events.push(format!("stop-lead:{term}"));
        Ok(())
    }

    fn checkpoint(&mut self, path: &str) -> Result<MatrixRaftCheckpoint, RaftError> {
        self.events.push(format!("checkpoint:{path}"));
        Ok(MatrixRaftCheckpoint {
            path: path.to_string(),
            applied_index: 0,
        })
    }

    fn on_snapshot_load(&mut self, snapshot_path: &str) -> Result<(), RaftError> {
        self.events.push(format!("snapshot:{snapshot_path}"));
        Ok(())
    }

    fn on_configuration_applied(&mut self, config: MatrixRaftConfigurationApplied) {
        self.events.push(format!(
            "config:{}->{}",
            config.old_config.len(),
            config.new_config.len()
        ));
    }
}

#[test]
fn matrixraft_multi_raft_server_hosts_groups_and_routes_messages() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(99)
        .set_num_connection_group(3)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let send_limiter = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 128 * 1024 * 1024,
        check_cycle_sec: 1,
    };
    let download_limiter = MatrixRaftRateLimiterConfig {
        bytes_limit_per_sec: 64 * 1024 * 1024,
        check_cycle_sec: 2,
    };
    let creator = MatrixRaftNodeCreatorBuilder::new()
        .store_id(7001)
        .applier_num(4)
        .apply_max_batch_count(512)
        .snapshot_loader_num(5)
        .snapshot_downloader_num(6)
        .snapshot_creator_num(7)
        .snapshot_sender_num(8)
        .snapshot_send_rate_limiter(send_limiter.clone())
        .snapshot_download_rate_limiter(download_limiter.clone())
        .enable_flexible_apply()
        .enable_heartbeat_merge()
        .merge_heartbeat_interval_milli(25)
        .fsm()
        .group_storage()
        .build();
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport.clone())
        .worker_num(2)
        .reader_num(3)
        .executor_num(4)
        .applier_num(9)
        .apply_max_batch_count(256)
        .add_raft_node_creator(creator)
        .snapshot_sender_num(2)
        .snapshot_loader_num(2)
        .snapshot_downloader_num(2)
        .snapshot_creator_num(2)
        .watch_address_resolver()
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let wal_810 = temp_dir("wal-810");
    let snap_810 = temp_dir("snap-810");
    let wal_811 = temp_dir("wal-811");
    let snap_811 = temp_dir("snap-811");

    server
        .create_node(options(810, &wal_810, &snap_810), 1)
        .expect("group 810 node");
    server
        .create_node(options(811, &wal_811, &snap_811), 1)
        .expect("group 811 node");

    assert_eq!(server.node_count(), 2);
    assert_eq!(server.group_count(), 2);
    assert_eq!(server.group_ids(), vec![810, 811]);
    assert!(server.has_node(810, 1));
    assert!(server.has_node(811, 1));
    assert_eq!(server.runtime_wiring_count(), 2);

    let wiring = server.runtime_wiring(810, 1).expect("runtime wiring");
    assert_eq!(wiring.group_id, 810);
    assert_eq!(wiring.node_id, 1);
    assert_eq!(wiring.creator_index, Some(0));
    assert_eq!(wiring.store_id, 7001);
    assert_eq!(wiring.worker_num, 2);
    assert_eq!(wiring.reader_num, 3);
    assert_eq!(wiring.executor_num, 4);
    assert_eq!(wiring.applier_num, 4);
    assert_eq!(wiring.apply_max_batch_count, 512);
    assert_eq!(wiring.snapshot_loader_num, 5);
    assert_eq!(wiring.snapshot_downloader_num, 6);
    assert_eq!(wiring.snapshot_creator_num, 7);
    assert_eq!(wiring.snapshot_sender_num, 8);
    assert!(wiring.flexible_apply);
    assert!(wiring.heartbeat_merge);
    assert_eq!(wiring.merge_heartbeat_interval_milli, 25);
    assert!(wiring.watched_address_resolver);
    assert_eq!(wiring.transport, transport);
    assert_eq!(wiring.snapshot_send_rate_limiter, Some(send_limiter));
    assert_eq!(
        wiring.snapshot_download_rate_limiter,
        Some(download_limiter)
    );
    assert!(wiring.has_store_fsm);
    assert!(wiring.has_group_storage);

    server.start_all(1).expect("start all");
    assert_eq!(server.node(810, 1).expect("node").group_id(), 810);
    assert_eq!(server.node(811, 1).expect("node").node_id(), 1);

    let meta_data_batch = server
        .route_message_batch(vec![
            MatrixRaftRoutedMessage::new(
                810,
                1,
                MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(40),
                        data: b"meta-server-membership-update".to_vec(),
                        context: b"meta".to_vec(),
                        is_command: true,
                    },
                ),
            ),
            MatrixRaftRoutedMessage::new(
                811,
                1,
                MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(41),
                        data: b"data-node-shard-write".to_vec(),
                        context: b"data".to_vec(),
                        is_command: true,
                    },
                ),
            ),
        ])
        .expect("meta/data route batch");
    assert_eq!(meta_data_batch.len(), 2);
    assert!(meta_data_batch.iter().all(|result| result.handled));
    assert_eq!(meta_data_batch[0].key.group_id, 810);
    assert_eq!(meta_data_batch[1].key.group_id, 811);
    let meta_log_id = meta_data_batch[0]
        .proposed_log_id
        .as_ref()
        .expect("meta log id");
    let data_log_id = meta_data_batch[1]
        .proposed_log_id
        .as_ref()
        .expect("data log id");
    assert_eq!(
        server
            .node(810, 1)
            .expect("meta group node")
            .get_status()
            .expect("meta status after batch")
            .last_log_index,
        meta_log_id.index
    );
    assert_eq!(
        server
            .node(811, 1)
            .expect("data group node")
            .get_status()
            .expect("data status after batch")
            .last_log_index,
        data_log_id.index
    );

    let status = server
        .node(810, 1)
        .expect("node")
        .get_status()
        .expect("status");
    let append = AppendEntriesRequest {
        group_id: 810,
        term: status.term,
        leader_id: 1,
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: 0,
        lease_epoch: 0,
    };
    let append_result = server
        .route_message(810, 1, MatrixRaftMessage::append_entries(1, 2, &append))
        .expect("append route");
    assert!(append_result.handled);
    assert_eq!(append_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(append_result.append_entries_response_presence());
    assert!(!append_result.vote_response_presence());
    assert!(append_result.append_entries_response.is_some());

    let append_response_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::append_entries_response(
                2,
                1,
                &AppendEntriesResponse {
                    term: status.term + 1,
                    success: false,
                    match_index: 0,
                    rejection_hint: Some(1),
                    rejected_index: Some(1),
                    require_snapshot: Some(1),
                    snapshot_state: SnapshotState::NotReady,
                    lease_confirmation_epoch: 7,
                    lease_duration_ms: 11,
                },
            ),
        )
        .expect("append response route");
    assert!(append_response_result.handled);
    assert_eq!(
        append_response_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert!(append_response_result.append_entries_response_presence());
    assert!(append_response_result
        .append_entries_response
        .as_ref()
        .expect("append response payload")
        .rejected_hint
        .is_some());
    assert_eq!(
        server
            .node(810, 1)
            .expect("node")
            .get_status()
            .expect("status after high-term append response")
            .leader_id,
        None
    );
    server
        .node(810, 1)
        .expect("node")
        .forced_campaign()
        .expect("restore local leadership after high-term append response");

    let pre_vote_result = server
        .route_message(810, 1, MatrixRaftMessage::pre_vote(2, 1))
        .expect("pre-vote route");
    assert!(pre_vote_result.handled);
    assert_eq!(pre_vote_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert_eq!(pre_vote_result.message_type, MatrixRaftMessageType::PreVote);
    assert!(pre_vote_result.vote_response_presence());
    let pre_vote_response = pre_vote_result
        .vote_response
        .as_ref()
        .expect("pre-vote response payload");
    assert!(pre_vote_response.vote_granted);
    assert_eq!(pre_vote_response.reason, "pre_vote_granted");

    let status_after_append_response = server
        .node(810, 1)
        .expect("node")
        .get_status()
        .expect("status before vote response");
    let vote_response_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::vote_response(
                2,
                1,
                VoteResponse {
                    term: status_after_append_response.term + 1,
                    vote_granted: false,
                    reason: "higher_term_rejection".to_string(),
                },
                false,
            ),
        )
        .expect("vote response route");
    assert!(vote_response_result.handled);
    assert_eq!(
        vote_response_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert_eq!(
        vote_response_result
            .vote_response
            .as_ref()
            .expect("vote response payload")
            .reason,
        "higher_term_rejection"
    );
    assert_eq!(
        server
            .node(810, 1)
            .expect("node")
            .get_status()
            .expect("status after high-term vote response")
            .leader_id,
        None
    );
    server
        .node(810, 1)
        .expect("node")
        .forced_campaign()
        .expect("restore local leadership after high-term vote response");

    let status_after_vote_response = server
        .node(810, 1)
        .expect("node")
        .get_status()
        .expect("status before install-snapshot response");
    let install_response_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::install_snapshot_response(
                2,
                1,
                InstallSnapshotResponse {
                    term: status_after_vote_response.term + 1,
                    accepted: false,
                    next_offset: 0,
                    committed_index: 0,
                    reason: "higher_term_snapshot_rejection".to_string(),
                },
            ),
        )
        .expect("install-snapshot response route");
    assert!(install_response_result.handled);
    assert_eq!(
        install_response_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert_eq!(
        install_response_result
            .install_snapshot_response
            .as_ref()
            .expect("install-snapshot response payload")
            .reason,
        "higher_term_snapshot_rejection"
    );
    assert_eq!(
        server
            .node(810, 1)
            .expect("node")
            .get_status()
            .expect("status after high-term install-snapshot response")
            .leader_id,
        None
    );
    server
        .node(810, 1)
        .expect("node")
        .forced_campaign()
        .expect("restore local leadership after high-term install-snapshot response");

    let snapshot_request_term = server
        .node(810, 1)
        .expect("node")
        .get_status()
        .expect("status before install-snapshot request")
        .term;
    let install_request_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::install_snapshot(
                2,
                1,
                InstallSnapshotRequest {
                    group_id: 0,
                    term: snapshot_request_term,
                    leader_id: 0,
                    chunk: SnapshotChunk {
                        meta: SnapshotMetadata {
                            snapshot_id: "multi-route-snapshot-12".to_string(),
                            last_log_id: LogId { term: 1, index: 12 },
                            membership: vec![1, 2, 3],
                            members: Vec::new(),
                        },
                        offset: 0,
                        data: b"snapshot-route-state".to_vec(),
                        done: true,
                    },
                },
            ),
        )
        .expect("install-snapshot request route");
    assert!(install_request_result.handled);
    assert_eq!(
        install_request_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    let install_request_response = install_request_result
        .install_snapshot_response
        .as_ref()
        .expect("install-snapshot request response");
    assert!(install_request_response.accepted);
    assert_eq!(install_request_response.committed_index, 12);
    assert_eq!(install_request_response.next_offset, 20);
    assert_eq!(install_request_response.reason, "snapshot_installed");
    assert_eq!(
        server
            .node(810, 1)
            .expect("node")
            .get_status()
            .expect("status after install-snapshot request")
            .leader_id,
        Some(2)
    );
    server
        .node(810, 1)
        .expect("node")
        .forced_campaign()
        .expect("restore local leadership after install-snapshot request");

    let propose_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::propose(
                1,
                1,
                MatrixRaftPropose {
                    request_id: Some(42),
                    data: b"proxy-propose".to_vec(),
                    context: b"ctx".to_vec(),
                    is_command: true,
                },
            ),
        )
        .expect("propose route");
    assert!(propose_result.handled);
    assert_eq!(propose_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(propose_result.proposed_log_id_presence());
    assert!(!propose_result.apply_result_presence());
    assert!(!propose_result.read_index_response_presence());
    let proposed_log_id = propose_result.proposed_log_id.expect("proposed log id");
    assert_eq!(
        server
            .node(810, 1)
            .expect("node")
            .get_status()
            .expect("status after propose")
            .last_log_index,
        proposed_log_id.index
    );

    let applied_index = server
        .node(810, 1)
        .expect("node")
        .get_status()
        .expect("status before scheduler reports")
        .applied_index;
    assert!(applied_index > 0);
    let applied_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::applied(1, applied_index, false),
            ),
        )
        .expect("applied admin route");
    assert!(applied_result.handled);
    assert_eq!(applied_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(applied_result.apply_result_presence());
    assert!(!applied_result.synced_presence());
    let apply_report = applied_result
        .apply_result
        .as_ref()
        .expect("apply result report");
    assert_eq!(apply_report.node_id, 1);
    assert_eq!(apply_report.applied_index, applied_index);
    assert!(!apply_report.rejected);

    let inflight_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::apply_task_inflight(1, applied_index),
            ),
        )
        .expect("apply-task-inflight admin route");
    assert!(inflight_result.handled);
    assert_eq!(inflight_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(inflight_result.apply_result_presence());
    let inflight_report = inflight_result
        .apply_result
        .as_ref()
        .expect("apply inflight report");
    assert_eq!(inflight_report.node_id, 1);
    assert_eq!(inflight_report.applied_index, applied_index);
    assert!(!inflight_report.rejected);

    let synced_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::synced(Some(1), Some(proposed_log_id.index), 0),
            ),
        )
        .expect("synced admin route");
    assert!(synced_result.handled);
    assert_eq!(synced_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(synced_result.synced_presence());
    assert!(!synced_result.apply_result_presence());
    let synced_report = synced_result.synced.as_ref().expect("synced report");
    assert_eq!(synced_report.first_index, Some(1));
    assert_eq!(synced_report.last_index, Some(proposed_log_id.index));
    assert_eq!(synced_report.stabled_config_change_index, 0);

    let replicated_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::replicated(2, true)),
        )
        .expect("replicated admin route");
    assert!(replicated_result.handled);
    assert_eq!(replicated_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(replicated_result.replicated_presence());
    assert!(replicated_result.snapshot_peer_report_presence());
    let replicated_report = replicated_result
        .replicated
        .as_ref()
        .expect("replicated report");
    assert_eq!(replicated_report.peer_id, 2);
    assert!(replicated_report.success);
    assert_eq!(
        replicated_result
            .snapshot_peer_report
            .as_ref()
            .expect("replicated peer report")
            .status
            .peer_id,
        2
    );

    let unhealthy_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::set_node_healthy(2, false)),
        )
        .expect("set node unhealthy admin route");
    assert!(unhealthy_result.handled);
    assert_eq!(unhealthy_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(unhealthy_result.node_healthy_presence());
    assert!(unhealthy_result.snapshot_peer_report_presence());
    assert_eq!(unhealthy_result.node_healthy, Some(false));
    assert_eq!(
        unhealthy_result
            .snapshot_peer_report
            .as_ref()
            .expect("unhealthy peer report")
            .peer_healthy,
        Some(false)
    );

    let healthy_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::set_node_healthy(2, true)),
        )
        .expect("set node healthy admin route");
    assert!(healthy_result.handled);
    assert_eq!(healthy_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(healthy_result.node_healthy_presence());
    assert_eq!(healthy_result.node_healthy, Some(true));

    let reorder_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::receive_out_of_order_append(
                    2,
                    MatrixRaftEntry {
                        term: proposed_log_id.term,
                        index: proposed_log_id.index + 2,
                        entry_type: MatrixRaftEntryType::Normal,
                        propose: Some(MatrixRaftPropose {
                            request_id: Some(144),
                            data: b"future-route-entry".to_vec(),
                            context: Vec::new(),
                            is_command: true,
                        }),
                        config_change: None,
                        memberships: Vec::new(),
                        request_id: 144,
                        bytes_size: 18,
                    },
                ),
            ),
        )
        .expect("receive out-of-order append admin route");
    assert!(reorder_result.handled);
    assert_eq!(reorder_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert_eq!(
        reorder_result
            .snapshot_peer_report
            .as_ref()
            .expect("queued reorder peer report")
            .status
            .reorder_queue_depth,
        1
    );

    let expire_reorder_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::expire_peer_reorder_queue(2)),
        )
        .expect("expire reorder queue admin route");
    assert!(expire_reorder_result.handled);
    assert_eq!(
        expire_reorder_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert!(expire_reorder_result.reorder_queue_dropped_presence());
    assert_eq!(expire_reorder_result.reorder_queue_dropped, Some(1));
    assert_eq!(
        expire_reorder_result
            .snapshot_peer_report
            .as_ref()
            .expect("expired reorder peer report")
            .status
            .reorder_queue_depth,
        0
    );

    let fatal_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::fire_fatal_event(3, "test fatal"),
            ),
        )
        .expect("fatal event admin route");
    assert!(fatal_result.handled);
    assert_eq!(fatal_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(!fatal_result.fatal_event_transfer_target_presence());
    assert_eq!(fatal_result.fatal_event_transfer_target, None);
    server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::set_node_healthy(3, true)),
        )
        .expect("restore fatal peer health");

    let set_leader_lease_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::set_leader_lease_valid(false)),
        )
        .expect("set leader lease admin route");
    assert!(set_leader_lease_result.handled);
    assert_eq!(
        set_leader_lease_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert_eq!(set_leader_lease_result.leader_lease_valid, Some(false));

    let leader_confirmation_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::receive_leader_lease_confirmation(2, 77, Some(5)),
            ),
        )
        .expect("leader lease confirmation admin route");
    assert!(leader_confirmation_result.handled);
    assert_eq!(
        leader_confirmation_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert_eq!(
        leader_confirmation_result.leader_lease_confirmed,
        Some(true)
    );

    let leader_tick_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::tick_leader_lease(5)),
        )
        .expect("tick leader lease admin route");
    assert!(leader_tick_result.handled);
    assert_eq!(
        leader_tick_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert_eq!(leader_tick_result.leader_lease_expired, Some(true));

    let follower_lease_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::receive_follower_lease(88)),
        )
        .expect("receive follower lease admin route");
    assert!(follower_lease_result.handled);
    assert_eq!(
        follower_lease_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert_eq!(follower_lease_result.follower_lease_received, Some(true));

    let follower_tick_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::tick_follower_lease(20)),
        )
        .expect("tick follower lease admin route");
    assert!(follower_tick_result.handled);
    assert_eq!(
        follower_tick_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert_eq!(follower_tick_result.follower_lease_expired, Some(true));

    let witness_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::witness_quorum([1, 2])),
        )
        .expect("witness-quorum admin route");
    assert!(witness_result.handled);
    assert_eq!(witness_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(witness_result.witness_quorum_presence());
    let witness_report = witness_result
        .witness_quorum
        .as_ref()
        .expect("witness quorum report");
    assert_eq!(witness_report.required, 2);
    assert_eq!(witness_report.acknowledged, 2);
    assert!(witness_report.reached);

    let release_memory_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        )
        .expect("release-memory admin route");
    assert!(release_memory_result.handled);
    assert_eq!(
        release_memory_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert!(release_memory_result.released_memory_presence());
    assert!(release_memory_result.released_memory.is_some());

    let compact_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::compact_logs_through(2)),
        )
        .expect("compact-logs admin route");
    assert!(compact_result.handled);
    assert_eq!(compact_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(compact_result.compacted_logs_presence());
    assert!(compact_result.compacted_logs.is_some());

    let fenced_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::compact_logs_with_storage_fence(
                    2,
                    StorageApplyFence {
                        group_id: 810,
                        node_id: 1,
                        committed_index: proposed_log_id.index,
                        applied_index,
                        durable_applied_index: applied_index,
                        storage_flushed_index: applied_index,
                        installed_snapshot_index: 0,
                        first_retained_log_index: 1,
                    },
                ),
            ),
        )
        .expect("fenced compact-logs admin route");
    assert!(fenced_result.handled);
    assert_eq!(fenced_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(fenced_result.fenced_compaction_presence());
    let fenced_report = fenced_result
        .fenced_compaction
        .as_ref()
        .expect("fenced compaction report");
    assert_eq!(fenced_report.requested_log_index, 2);
    assert!(fenced_report.fence_valid);

    let checkpoint_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::checkpoint_snapshot(1, "route-checkpoint-810"),
            ),
        )
        .expect("checkpoint-snapshot admin route");
    assert!(checkpoint_result.handled);
    assert_eq!(checkpoint_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(checkpoint_result.checkpoint_presence());
    assert!(checkpoint_result.snapshot_presence());
    let checkpoint = checkpoint_result
        .checkpoint
        .as_ref()
        .expect("checkpoint snapshot payload");
    assert_eq!(checkpoint.group_id, 810);
    assert_eq!(checkpoint.meta.snapshot_id, "route-checkpoint-810");
    assert_eq!(
        checkpoint_result
            .snapshot
            .as_ref()
            .expect("checkpoint snapshot descriptor")
            .index,
        checkpoint.meta.last_log_id.index
    );

    let network_error_result = server
        .route_message(810, 1, MatrixRaftMessage::network_error(1, 2))
        .expect("network-error route");
    assert!(network_error_result.handled);
    assert_eq!(
        network_error_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert_eq!(
        network_error_result.message_type,
        MatrixRaftMessageType::NetworkError
    );
    assert!(network_error_result.snapshot_peer_report_presence());
    let pipeline_after_network_error = &network_error_result
        .snapshot_peer_report
        .as_ref()
        .expect("network-error peer report")
        .status;
    assert_eq!(pipeline_after_network_error.peer_id, 2);
    assert_eq!(pipeline_after_network_error.packet_loss_events, 1);

    let read_index_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::read_index(
                2,
                1,
                ReadIndexRequest {
                    group_id: 0,
                    requester_id: 0,
                    min_commit_index: 0,
                    allow_lease_read: false,
                },
            ),
        )
        .expect("read-index route");
    assert!(read_index_result.handled);
    assert_eq!(read_index_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(read_index_result.read_index_response_presence());
    let read_index_response = read_index_result
        .read_index_response
        .as_ref()
        .expect("read-index response payload");
    assert!(!read_index_response.safe);
    assert!(!read_index_response.lease_read);
    assert!(!read_index_response.reason.is_empty());

    let add_config_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::config_change(
                1,
                1,
                MatrixRaftConfigChange {
                    request_id: Some(43),
                    change_type: MatrixRaftConfigChangeType::AddNode,
                    member_id: 5,
                    raft_addr: "127.0.0.1:30005".to_string(),
                    snapshot_addr: "127.0.0.1:31005".to_string(),
                    old_members: Vec::new(),
                    conf_state: MatrixRaftConfState::Learner,
                    auto_promote: false,
                },
            ),
        )
        .expect("add learner config route");
    assert!(add_config_result.handled);
    assert_eq!(add_config_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(add_config_result.membership_presence());
    assert!(
        add_config_result
            .membership
            .as_ref()
            .expect("membership report")
            .success
    );
    assert!(server
        .node(810, 1)
        .expect("node")
        .get_status()
        .expect("status after config add")
        .membership
        .learners
        .contains(&5));

    let catch_up_result = server
        .route_message(810, 1, MatrixRaftMessage::catch_up_peer(1, 5))
        .expect("catch-up route");
    assert!(catch_up_result.handled);
    assert_eq!(catch_up_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(catch_up_result.catch_up_presence());
    let catch_up = catch_up_result.catch_up.as_ref().expect("catch-up report");
    assert_eq!(catch_up.learner_id, 5);
    assert!(catch_up.caught_up);

    let promote_result = server
        .route_message(810, 1, MatrixRaftMessage::promote_peer(1, 5))
        .expect("promote route");
    assert!(promote_result.handled);
    assert_eq!(promote_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(promote_result.promote_presence());
    let promote = promote_result.promote.as_ref().expect("promote report");
    assert_eq!(promote.learner_id, 5);
    assert!(promote.promoted);
    assert!(promote.catch_up.caught_up);
    assert!(promote.membership.success);
    assert!(!server
        .node(810, 1)
        .expect("node")
        .get_status()
        .expect("status after promote")
        .membership
        .learners
        .contains(&5));

    let add_auto_config_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::config_change(
                1,
                1,
                MatrixRaftConfigChange {
                    request_id: Some(45),
                    change_type: MatrixRaftConfigChangeType::AddNode,
                    member_id: 6,
                    raft_addr: "127.0.0.1:30006".to_string(),
                    snapshot_addr: "127.0.0.1:31006".to_string(),
                    old_members: Vec::new(),
                    conf_state: MatrixRaftConfState::Learner,
                    auto_promote: true,
                },
            ),
        )
        .expect("add auto learner config route");
    assert!(add_auto_config_result.membership_presence());
    assert!(
        add_auto_config_result
            .membership
            .as_ref()
            .expect("add auto learner report")
            .success
    );

    let auto_promote_result = server
        .route_message(810, 1, MatrixRaftMessage::auto_promote_learner(1, 6))
        .expect("auto-promote route");
    assert!(auto_promote_result.handled);
    assert_eq!(
        auto_promote_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert!(auto_promote_result.auto_promote_presence());
    let auto_promote = auto_promote_result
        .auto_promote
        .as_ref()
        .expect("auto-promote report");
    assert_eq!(auto_promote.learner_id, 6);
    assert!(auto_promote.auto_promote);
    assert!(auto_promote.promoted);
    assert_eq!(auto_promote.state_after, LearnerAutoPromoteState::Promoted);

    let add_voter_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::membership_operation(
                1,
                1,
                MembershipOperation::AddVoter(peer_with_role(810, 7, ReplicaRole::Learner)),
            ),
        )
        .expect("membership add-voter route");
    assert!(add_voter_result.handled);
    assert_eq!(add_voter_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(add_voter_result.membership_presence());
    assert!(
        add_voter_result
            .membership
            .as_ref()
            .expect("add-voter report")
            .success
    );
    assert!(server
        .node(810, 1)
        .expect("node")
        .get_status()
        .expect("status after membership add-voter")
        .membership
        .voters
        .contains(&7));
    assert_eq!(
        server
            .node(810, 1)
            .expect("node")
            .resolve_address(7)
            .expect("resolve voter")
            .peer_id,
        7
    );

    let remove_voter_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::membership_operation(1, 1, MembershipOperation::Remove(7)),
        )
        .expect("membership remove route");
    assert!(
        remove_voter_result
            .membership
            .as_ref()
            .expect("remove-voter report")
            .success
    );
    assert!(!server
        .node(810, 1)
        .expect("node")
        .get_status()
        .expect("status after membership remove")
        .membership
        .voters
        .contains(&7));
    assert!(server
        .node(810, 1)
        .expect("node")
        .resolve_address(7)
        .is_err());

    let remove_config_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::config_change(
                1,
                1,
                MatrixRaftConfigChange {
                    request_id: Some(44),
                    change_type: MatrixRaftConfigChangeType::RemoveNode,
                    member_id: 5,
                    raft_addr: String::new(),
                    snapshot_addr: String::new(),
                    old_members: Vec::new(),
                    conf_state: MatrixRaftConfState::Voter,
                    auto_promote: false,
                },
            ),
        )
        .expect("remove learner config route");
    assert!(
        remove_config_result
            .membership
            .as_ref()
            .expect("remove report")
            .success
    );
    assert!(!server
        .node(810, 1)
        .expect("node")
        .get_status()
        .expect("status after config remove")
        .membership
        .learners
        .contains(&5));

    let admin_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::ignore_witness(true)),
        )
        .expect("admin route");
    assert!(admin_result.handled);
    server
        .node_mut(810, 1)
        .expect("node")
        .add_learner(
            matrixraft::MatrixRaftNodeId {
                peer_id: 4,
                raft_addr: "127.0.0.1:30004".to_string(),
                snapshot_addr: "127.0.0.1:31004".to_string(),
            },
            false,
        )
        .expect("add learner for transfer route");

    assert_invalid_request_contains(
        server.route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::transfer_leader(4)),
        ),
        "leader transfer target must be voter",
    );

    let step_down_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::step_down(Some(2))),
        )
        .expect("step-down admin route");
    assert!(step_down_result.handled);
    assert!(step_down_result.step_down_presence());
    let step_down_report = step_down_result
        .step_down
        .as_ref()
        .expect("step-down route report");
    assert!(step_down_report.stepped_down);
    assert_eq!(step_down_report.requested_transferee_id, Some(2));
    assert_eq!(step_down_report.transferee_id, Some(2));
    server
        .node(810, 1)
        .expect("node")
        .forced_campaign()
        .expect("restore local leadership after step-down route");

    let resign_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::resign()),
        )
        .expect("resign admin route");
    assert!(resign_result.handled);
    assert!(resign_result.resign_presence());
    let resign_report = resign_result.resign.as_ref().expect("resign route report");
    assert!(resign_report.resigned);
    assert_eq!(resign_report.leader_before, Some(1));
    assert_eq!(resign_report.leader_after, None);
    server
        .node(810, 1)
        .expect("node")
        .forced_campaign()
        .expect("restore local leadership after resign route");

    let transfer_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::transfer_leader(2)),
        )
        .expect("transfer-leader admin route");
    assert!(transfer_result.handled);
    assert!(transfer_result.transfer_leader_presence());
    let transfer_report = transfer_result
        .transfer_leader
        .as_ref()
        .expect("transfer-leader route report");
    assert!(transfer_report.transferred);
    assert_eq!(transfer_report.transferee_id, 2);
    assert_eq!(
        transfer_report
            .transferee_node
            .as_ref()
            .expect("transferee node")
            .peer_id,
        2
    );
    assert_eq!(
        transfer_report
            .state
            .as_ref()
            .expect("leader transfer state")
            .transferee_id,
        2
    );

    let abort_transfer_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::abort_leader_transfer("route abort"),
            ),
        )
        .expect("abort leader transfer admin route");
    assert!(abort_transfer_result.handled);
    assert_eq!(
        abort_transfer_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert!(abort_transfer_result.leader_transfer_aborted_presence());
    assert_eq!(abort_transfer_result.leader_transfer_aborted, Some(true));

    let complete_transfer_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::complete_leader_transfer()),
        )
        .expect("complete leader transfer admin route");
    assert!(complete_transfer_result.handled);
    assert_eq!(
        complete_transfer_result.kind,
        MatrixRaftRouteResultKind::Delivered
    );
    assert!(complete_transfer_result.leader_transfer_completed_presence());
    assert_eq!(
        complete_transfer_result.leader_transfer_completed,
        Some(false)
    );

    server
        .node(810, 1)
        .expect("node")
        .forced_campaign()
        .expect("restore local leadership after transfer route");

    let partition_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::partition_peer(2)),
        )
        .expect("partition-peer admin route");
    assert!(partition_result.handled);
    assert_eq!(partition_result.kind, MatrixRaftRouteResultKind::Delivered);
    let partition_report = partition_result
        .snapshot_peer_report
        .as_ref()
        .expect("partition peer report");
    assert_eq!(partition_report.peer_id, 2);
    assert_eq!(partition_report.peer_healthy, Some(false));
    server
        .route_message(
            810,
            1,
            MatrixRaftMessage::propose(
                1,
                1,
                MatrixRaftPropose {
                    request_id: Some(46),
                    data: b"partitioned-route-write".to_vec(),
                    context: Vec::new(),
                    is_command: true,
                },
            ),
        )
        .expect("propose while peer is partitioned");

    let heal_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::heal_peer(2)),
        )
        .expect("heal-peer admin route");
    assert!(heal_result.handled);
    let heal_report = heal_result.catch_up.as_ref().expect("heal catch-up report");
    assert_eq!(heal_report.learner_id, 2);
    assert!(heal_report.caught_up);
    let healed_report = heal_result
        .snapshot_peer_report
        .as_ref()
        .expect("heal peer report");
    assert_eq!(healed_report.peer_healthy, Some(true));
    assert_eq!(healed_report.peer_lag, Some(0));

    let trigger_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::trigger_snapshot()),
        )
        .expect("trigger snapshot route");
    assert!(trigger_result.handled);
    let ready_snapshot = trigger_result.snapshot.expect("trigger snapshot");
    let ready_snapshot_id = format!("810-{}", ready_snapshot.index);
    let ready_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::snapshot_ready(&ready_snapshot_id, true),
            ),
        )
        .expect("snapshot ready route");
    assert_eq!(ready_result.kind, MatrixRaftRouteResultKind::Delivered);

    let begin_send = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::begin_snapshot_send(2, "route-send-12", 12, 2),
            ),
        )
        .expect("begin routed snapshot send");
    let begin_send_report = begin_send
        .snapshot_peer_report
        .as_ref()
        .expect("begin send report");
    assert_eq!(begin_send_report.peer_id, 2);
    assert!(begin_send_report.status.snapshot_sending);
    assert_eq!(begin_send_report.status.snapshot_install_total_chunks, 2);

    let progress = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::snapshot_progress(
                2,
                1,
                MatrixRaftSnapshotProgress {
                    remote_receiving: true,
                    elapsed_since_last_receiving_ms: 25,
                    send_timeout_ms: 100,
                },
            ),
        )
        .expect("snapshot progress route");
    assert!(
        progress
            .snapshot_peer_report
            .as_ref()
            .expect("progress report")
            .status
            .snapshot_sending
    );

    let sent = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::record_snapshot_chunk_sent(2, 8),
            ),
        )
        .expect("record routed sent chunk");
    assert!(
        sent.snapshot_peer_report
            .as_ref()
            .expect("sent report")
            .status
            .snapshot_sending
    );

    let retry = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::retry_snapshot_chunk(2)),
        )
        .expect("retry routed snapshot chunk");
    assert_eq!(
        retry
            .snapshot_peer_report
            .as_ref()
            .expect("retry report")
            .status
            .snapshot_chunk_retry_count,
        1
    );

    let canceled = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::cancel_snapshot_send(2)),
        )
        .expect("cancel routed snapshot send");
    assert!(
        !canceled
            .snapshot_peer_report
            .as_ref()
            .expect("cancel report")
            .status
            .snapshot_sending
    );

    let begin_install = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::begin_snapshot_install(2, "route-install-13", 13, 2),
            ),
        )
        .expect("begin routed snapshot install");
    assert!(
        begin_install
            .snapshot_peer_report
            .as_ref()
            .expect("begin install report")
            .status
            .snapshot_installing
    );

    let received = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::receive_snapshot_chunk(2, 8, false),
            ),
        )
        .expect("receive routed snapshot chunk");
    assert_eq!(
        received
            .snapshot_peer_report
            .as_ref()
            .expect("receive report")
            .status
            .snapshot_install_progress_per_mille,
        500
    );

    let rolled_back = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::rollback_snapshot_install(2)),
        )
        .expect("rollback routed snapshot install");
    let rollback_report = rolled_back
        .snapshot_peer_report
        .as_ref()
        .expect("rollback report");
    assert!(!rollback_report.status.snapshot_installing);
    assert_eq!(rollback_report.status.snapshot_install_rolled_back, 1);

    let applied_trigger = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::trigger_snapshot()),
        )
        .expect("second trigger snapshot route");
    let applied_snapshot = applied_trigger.snapshot.expect("second trigger snapshot");
    let applied_snapshot_id = format!("810-{}", applied_snapshot.index);
    let applied_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage::admin(
                1,
                1,
                MatrixRaftAdminCommand::snapshot_applied(&applied_snapshot_id),
            ),
        )
        .expect("snapshot applied route");
    assert_eq!(applied_result.kind, MatrixRaftRouteResultKind::Delivered);

    let snapshot_meta = SnapshotMetadata {
        snapshot_id: "snap-810".to_string(),
        last_log_id: LogId { term: 1, index: 1 },
        membership: vec![1, 2, 3],
        members: vec![peer(810, 1), peer(810, 2), peer(810, 3)],
    };
    let snapshot_desc = MatrixRaftSnapshotDesc::from_snapshot_meta(&snapshot_meta);
    let snapshot_result = server
        .publish_snapshot_route(810, 1, snapshot_desc.clone())
        .expect("snapshot route");
    assert_eq!(
        snapshot_result.kind,
        MatrixRaftRouteResultKind::SnapshotRegistered
    );
    assert_eq!(server.snapshot_route_count(), 1);
    assert_eq!(server.snapshot_route(810, 1), Some(&snapshot_desc));
    let finish_result = server
        .route_message(
            810,
            1,
            MatrixRaftMessage {
                message_type: MatrixRaftMessageType::SnapshotFinish,
                from: Some(1),
                to: Some(1),
                old_snapshot_finish: Some(MatrixRaftOldSnapshotFinish::received(1)),
                ..MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::snapshot_applied("unused"))
            },
        )
        .expect("snapshot finish route");
    assert_eq!(
        finish_result.kind,
        MatrixRaftRouteResultKind::SnapshotFinished
    );
    assert!(finish_result.handled);
    assert_eq!(finish_result.snapshot, Some(snapshot_desc));
    assert_eq!(server.snapshot_route_count(), 0);
    assert!(server.snapshot_route(810, 1).is_none());

    let removed = server.unregister_node(811, 1).expect("unregister node");
    assert_eq!(removed.group_id(), 811);
    assert!(!server.has_node(811, 1));
    assert!(server.runtime_wiring(811, 1).is_none());
    assert_eq!(server.runtime_wiring_count(), 1);

    server.stop_all().expect("stop all");
    server.shutdown_all().expect("shutdown all");

    let _ = fs::remove_dir_all(wal_810);
    let _ = fs::remove_dir_all(snap_810);
    let _ = fs::remove_dir_all(wal_811);
    let _ = fs::remove_dir_all(snap_811);
}

#[test]
fn matrixraft_multi_raft_server_creates_meta_and_data_nodes_in_batch() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let creator = MatrixRaftNodeCreatorBuilder::new()
        .store_id(7002)
        .applier_num(5)
        .enable_flexible_apply()
        .build();
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .add_raft_node_creator(creator)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("batch-create-meta-1-wal");
    let meta_snap_1 = temp_dir("batch-create-meta-1-snapshot");
    let meta_wal_2 = temp_dir("batch-create-meta-2-wal");
    let meta_snap_2 = temp_dir("batch-create-meta-2-snapshot");
    let data_wal = temp_dir("batch-create-data-wal");
    let data_snap = temp_dir("batch-create-data-snapshot");
    let duplicate_wal = temp_dir("batch-create-duplicate-wal");
    let duplicate_snap = temp_dir("batch-create-duplicate-snapshot");
    let existing_wal = temp_dir("batch-create-existing-wal");
    let existing_snap = temp_dir("batch-create-existing-snapshot");
    let best_effort_meta_wal = temp_dir("batch-create-best-effort-meta-wal");
    let best_effort_meta_snap = temp_dir("batch-create-best-effort-meta-snapshot");
    let best_effort_data_wal = temp_dir("batch-create-best-effort-data-wal");
    let best_effort_data_snap = temp_dir("batch-create-best-effort-data-snapshot");

    let batch_nodes = vec![
        (options_for_peer(848, 1, &meta_wal_1, &meta_snap_1), 1),
        (options_for_peer(848, 2, &meta_wal_2, &meta_snap_2), 2),
        (options(849, &data_wal, &data_snap), 1),
    ];
    let plan = server
        .plan_create_nodes_with_creator_index(batch_nodes.clone(), 0)
        .expect("plan batch create meta and data nodes");
    assert_eq!(plan.creator_index, Some(0));
    assert_eq!(plan.node_count, 3);
    assert_eq!(plan.group_count, 2);
    assert_eq!(plan.group_ids, vec![848, 849]);
    assert_eq!(
        plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(848, 1),
            MatrixRaftRouteKey::new(848, 2),
            MatrixRaftRouteKey::new(849, 1)
        ]
    );
    assert_eq!(
        plan.groups
            .iter()
            .map(|group| (
                group.group_id,
                group.node_ids.clone(),
                group.start_indices.clone()
            ))
            .collect::<Vec<_>>(),
        vec![(848, vec![1, 2], vec![1, 2]), (849, vec![1], vec![1])]
    );
    assert_eq!(
        plan.route_keys_by_group(),
        vec![
            (
                848,
                vec![
                    MatrixRaftRouteKey::new(848, 1),
                    MatrixRaftRouteKey::new(848, 2),
                ],
            ),
            (849, vec![MatrixRaftRouteKey::new(849, 1)]),
        ]
    );
    assert_eq!(
        plan.node_ids_by_group(),
        vec![(848, vec![1, 2]), (849, vec![1])]
    );
    assert_eq!(
        plan.start_indices_by_group(),
        vec![(848, vec![1, 2]), (849, vec![1])]
    );
    assert_eq!(plan.node_counts_by_group(), vec![(848, 2), (849, 1)]);
    assert_eq!(plan.route_key_counts_by_group(), vec![(848, 2), (849, 1)]);
    assert_eq!(
        plan.fanout_counts_by_group(),
        vec![(848, 2, 2), (849, 1, 1)]
    );
    assert_eq!(
        plan.creator_indices_by_group(),
        vec![(848, vec![Some(0), Some(0)]), (849, vec![Some(0)])]
    );
    assert_eq!(
        plan.creator_index_presence_by_group(),
        vec![(848, vec![true, true]), (849, vec![true])]
    );
    assert_eq!(
        plan.store_ids_by_group(),
        vec![(848, vec![7002, 7002]), (849, vec![7002])]
    );
    assert_eq!(
        plan.flexible_apply_by_group(),
        vec![(848, vec![true, true]), (849, vec![true])]
    );
    assert_eq!(
        plan.heartbeat_merge_by_group(),
        vec![(848, vec![false, false]), (849, vec![false])]
    );
    assert_eq!(
        plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(848, 1), 1),
            (MatrixRaftRouteKey::new(848, 2), 2),
            (MatrixRaftRouteKey::new(849, 1), 1),
        ]
    );
    assert_eq!(
        plan.runtime_wiring_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(848, 1), true),
            (MatrixRaftRouteKey::new(848, 2), true),
            (MatrixRaftRouteKey::new(849, 1), true),
        ]
    );
    assert_eq!(
        plan.creator_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(848, 1), Some(0)),
            (MatrixRaftRouteKey::new(848, 2), Some(0)),
            (MatrixRaftRouteKey::new(849, 1), Some(0)),
        ]
    );
    assert_eq!(
        plan.creator_index_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(848, 1), true),
            (MatrixRaftRouteKey::new(848, 2), true),
            (MatrixRaftRouteKey::new(849, 1), true),
        ]
    );
    assert_eq!(
        plan.store_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(848, 1), 7002),
            (MatrixRaftRouteKey::new(848, 2), 7002),
            (MatrixRaftRouteKey::new(849, 1), 7002),
        ]
    );
    assert_eq!(
        plan.flexible_apply_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(848, 1), true),
            (MatrixRaftRouteKey::new(848, 2), true),
            (MatrixRaftRouteKey::new(849, 1), true),
        ]
    );
    assert_eq!(
        plan.heartbeat_merge_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(848, 1), false),
            (MatrixRaftRouteKey::new(848, 2), false),
            (MatrixRaftRouteKey::new(849, 1), false),
        ]
    );
    assert!(plan
        .nodes
        .iter()
        .all(|node| node.runtime_wiring.creator_index == Some(0)));
    assert!(plan
        .nodes
        .iter()
        .all(|node| node.runtime_wiring.flexible_apply));
    assert_eq!(plan.nodes[0].runtime_wiring.store_id, 7002);

    let created = server
        .create_nodes_with_creator_index(batch_nodes, 0)
        .expect("batch create meta and data nodes");
    assert_eq!(
        created,
        vec![
            MatrixRaftRouteKey::new(848, 1),
            MatrixRaftRouteKey::new(848, 2),
            MatrixRaftRouteKey::new(849, 1)
        ]
    );
    assert_eq!(server.node_count(), 3);
    assert_eq!(server.group_ids(), vec![848, 849]);
    assert_eq!(server.runtime_wiring_count(), 3);
    assert_eq!(
        server
            .runtime_wiring(848, 1)
            .expect("meta wiring")
            .creator_index,
        Some(0)
    );
    assert!(
        server
            .runtime_wiring(848, 1)
            .expect("meta wiring")
            .flexible_apply
    );
    assert_eq!(server.node(848, 2).expect("meta node 2").start_index(), 2);

    assert_invalid_request_contains(
        server.plan_create_nodes([
            (options_for_peer(850, 1, &duplicate_wal, &duplicate_snap), 1),
            (options_for_peer(850, 1, &duplicate_wal, &duplicate_snap), 1),
        ]),
        "appears more than once in create batch",
    );
    assert!(!server.has_node(850, 1));

    assert_invalid_request_contains(
        server.plan_create_nodes([(options_for_peer(848, 1, &existing_wal, &existing_snap), 1)]),
        "node 1 in group 848 is already registered",
    );
    assert_invalid_request_contains(
        server.plan_create_nodes_with_creator_index(
            [(options_for_peer(851, 1, &existing_wal, &existing_snap), 1)],
            7,
        ),
        "node creator index 7 is not registered",
    );

    let best_effort_create = server
        .create_nodes_with_creator_index_best_effort(
            [
                (options_for_peer(848, 1, &existing_wal, &existing_snap), 1),
                (
                    options_for_peer(850, 1, &best_effort_meta_wal, &best_effort_meta_snap),
                    3,
                ),
                (options_for_peer(850, 1, &duplicate_wal, &duplicate_snap), 4),
                (
                    options(851, &best_effort_data_wal, &best_effort_data_snap),
                    5,
                ),
            ],
            0,
        )
        .expect("best-effort batch create meta and data nodes");
    assert_eq!(
        best_effort_create
            .iter()
            .map(|group| (
                group.group_id,
                group.node_count,
                group.ok_count,
                group.error_count
            ))
            .collect::<Vec<_>>(),
        vec![(848, 1, 0, 1), (850, 2, 1, 1), (851, 1, 1, 0)]
    );
    assert!(best_effort_create
        .iter()
        .find(|group| group.group_id == 848)
        .expect("existing group result")
        .results
        .iter()
        .all(|result| result
            .error
            .as_ref()
            .is_some_and(|error| error.contains("already registered"))));
    assert!(best_effort_create
        .iter()
        .find(|group| group.group_id == 850)
        .expect("meta best-effort group")
        .results
        .iter()
        .any(|result| {
            result.key == MatrixRaftRouteKey::new(850, 1)
                && result.is_ok()
                && result.start_index == 3
                && result
                    .runtime_wiring
                    .as_ref()
                    .is_some_and(|wiring| wiring.creator_index == Some(0))
        }));
    assert!(best_effort_create
        .iter()
        .find(|group| group.group_id == 850)
        .expect("meta best-effort group")
        .results
        .iter()
        .any(|result| result
            .error
            .as_ref()
            .is_some_and(|error| error.contains("appears more than once in create batch"))));
    let best_effort_meta_create = best_effort_create
        .iter()
        .find(|group| group.group_id == 850)
        .expect("meta best-effort create metadata");
    assert_eq!(
        best_effort_meta_create.start_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), 3),
            (MatrixRaftRouteKey::new(850, 1), 4),
        ]
    );
    assert_eq!(
        best_effort_meta_create.runtime_wiring_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), true),
            (MatrixRaftRouteKey::new(850, 1), false),
        ]
    );
    assert_eq!(
        best_effort_meta_create.creator_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), Some(0)),
            (MatrixRaftRouteKey::new(850, 1), None),
        ]
    );
    assert_eq!(
        best_effort_meta_create.creator_index_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), true),
            (MatrixRaftRouteKey::new(850, 1), false),
        ]
    );
    assert_eq!(
        best_effort_meta_create.store_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), Some(7002)),
            (MatrixRaftRouteKey::new(850, 1), None),
        ]
    );
    assert_eq!(
        best_effort_meta_create.store_id_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), true),
            (MatrixRaftRouteKey::new(850, 1), false),
        ]
    );
    assert_eq!(
        best_effort_meta_create.flexible_apply_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), Some(true)),
            (MatrixRaftRouteKey::new(850, 1), None),
        ]
    );
    assert_eq!(
        best_effort_meta_create.flexible_apply_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), true),
            (MatrixRaftRouteKey::new(850, 1), false),
        ]
    );
    assert_eq!(
        best_effort_meta_create.heartbeat_merge_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), Some(false)),
            (MatrixRaftRouteKey::new(850, 1), None),
        ]
    );
    assert_eq!(
        best_effort_meta_create.heartbeat_merge_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), true),
            (MatrixRaftRouteKey::new(850, 1), false),
        ]
    );
    assert_eq!(
        best_effort_meta_create
            .results_by_route_key()
            .iter()
            .map(|(key, result)| (*key, result.start_index, result.is_ok()))
            .collect::<Vec<_>>(),
        vec![
            (MatrixRaftRouteKey::new(850, 1), 3, true),
            (MatrixRaftRouteKey::new(850, 1), 4, false),
        ]
    );
    assert_eq!(
        best_effort_meta_create
            .ok_results_by_route_key()
            .iter()
            .map(|(key, result)| (*key, result.start_index, result.runtime_wiring.is_some()))
            .collect::<Vec<_>>(),
        vec![(MatrixRaftRouteKey::new(850, 1), 3, true)]
    );
    assert_eq!(
        best_effort_meta_create
            .error_results_by_route_key()
            .iter()
            .map(|(key, result)| (
                *key,
                result.start_index,
                result
                    .error
                    .as_ref()
                    .is_some_and(|error| error.contains("appears more than once in create batch"))
            ))
            .collect::<Vec<_>>(),
        vec![(MatrixRaftRouteKey::new(850, 1), 4, true)]
    );
    assert!(best_effort_create
        .iter()
        .find(|group| group.group_id == 851)
        .expect("data best-effort group")
        .is_ok());
    assert!(server.has_node(850, 1));
    assert!(server.has_node(851, 1));
    assert_eq!(server.node_count(), 5);
    assert_invalid_request_contains(
        server.create_nodes_with_creator_index_best_effort(
            [(options_for_peer(852, 1, &existing_wal, &existing_snap), 1)],
            7,
        ),
        "node creator index 7 is not registered",
    );
    assert!(!server.has_node(852, 1));

    server.shutdown_all().expect("shutdown batch create server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
    let _ = fs::remove_dir_all(duplicate_wal);
    let _ = fs::remove_dir_all(duplicate_snap);
    let _ = fs::remove_dir_all(existing_wal);
    let _ = fs::remove_dir_all(existing_snap);
    let _ = fs::remove_dir_all(best_effort_meta_wal);
    let _ = fs::remove_dir_all(best_effort_meta_snap);
    let _ = fs::remove_dir_all(best_effort_data_wal);
    let _ = fs::remove_dir_all(best_effort_data_snap);
}

#[test]
fn matrixraft_multi_raft_server_routes_best_effort_batches() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_dir = temp_dir("best-effort-meta-wal");
    let meta_snapshot_dir = temp_dir("best-effort-meta-snapshot");
    let data_wal_dir = temp_dir("best-effort-data-wal");
    let data_snapshot_dir = temp_dir("best-effort-data-snapshot");
    server
        .create_node(options(814, &meta_wal_dir, &meta_snapshot_dir), 1)
        .expect("meta node");
    server
        .create_node(options(815, &data_wal_dir, &data_snapshot_dir), 1)
        .expect("data node");
    server.start_all(1).expect("start best-effort server");

    let route_plan = server
        .plan_route_message_batch(vec![
            MatrixRaftRoutedMessage::new(
                814,
                1,
                MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
            ),
            MatrixRaftRoutedMessage::new(
                815,
                1,
                MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(41),
                        data: b"data-node-planned-batch-write".to_vec(),
                        context: b"route-plan".to_vec(),
                        is_command: true,
                    },
                ),
            ),
            MatrixRaftRoutedMessage::new(
                815,
                1,
                MatrixRaftMessage::install_snapshot(
                    1,
                    1,
                    InstallSnapshotRequest {
                        group_id: 0,
                        term: 1,
                        leader_id: 0,
                        chunk: SnapshotChunk {
                            meta: SnapshotMetadata {
                                snapshot_id: "batch-route-snapshot-17".to_string(),
                                last_log_id: LogId { term: 1, index: 17 },
                                membership: vec![1],
                                members: vec![peer(815, 1)],
                            },
                            offset: 0,
                            data: b"batch route snapshot chunk".to_vec(),
                            done: false,
                        },
                    },
                ),
            ),
        ])
        .expect("plan mixed route batch");
    assert_eq!(route_plan.message_count, 3);
    assert_eq!(route_plan.group_count, 2);
    assert_eq!(route_plan.group_ids, vec![814, 815]);
    assert_eq!(route_plan.node_count, 1);
    assert_eq!(route_plan.node_ids, vec![1]);
    assert_eq!(
        route_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(814, 1),
            MatrixRaftRouteKey::new(815, 1),
            MatrixRaftRouteKey::new(815, 1),
        ]
    );
    assert_eq!(
        route_plan
            .messages
            .iter()
            .map(MatrixRaftRoutedMessage::route_key)
            .collect::<Vec<_>>(),
        route_plan.route_keys
    );
    assert_eq!(
        route_plan.message_types,
        vec![
            MatrixRaftMessageType::AdminCommand,
            MatrixRaftMessageType::Propose,
            MatrixRaftMessageType::InstallSnapshotRequest,
        ]
    );
    assert_eq!(
        route_plan
            .groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group.message_count,
                    group.node_ids.clone(),
                    group.message_types.clone(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (814, 1, vec![1], vec![MatrixRaftMessageType::AdminCommand],),
            (
                815,
                2,
                vec![1],
                vec![
                    MatrixRaftMessageType::Propose,
                    MatrixRaftMessageType::InstallSnapshotRequest,
                ],
            ),
        ]
    );
    assert_eq!(
        route_plan.route_keys_by_group(),
        vec![
            (814, vec![MatrixRaftRouteKey::new(814, 1)]),
            (
                815,
                vec![
                    MatrixRaftRouteKey::new(815, 1),
                    MatrixRaftRouteKey::new(815, 1),
                ],
            ),
        ]
    );
    assert_eq!(
        route_plan.node_ids_by_group(),
        vec![(814, vec![1]), (815, vec![1])]
    );
    assert_eq!(
        route_plan.message_counts_by_group(),
        vec![(814, 1), (815, 2)]
    );
    assert_eq!(
        route_plan.route_key_counts_by_group(),
        vec![(814, 1), (815, 2)]
    );
    assert_eq!(
        route_plan.fanout_counts_by_group(),
        vec![(814, 1, 1), (815, 2, 2)]
    );
    assert_eq!(
        route_plan.message_types_by_group(),
        vec![
            (814, vec![MatrixRaftMessageType::AdminCommand]),
            (
                815,
                vec![
                    MatrixRaftMessageType::Propose,
                    MatrixRaftMessageType::InstallSnapshotRequest,
                ],
            ),
        ]
    );
    assert_eq!(
        route_plan.messages_by_group(),
        vec![
            (
                814,
                vec![MatrixRaftMessage::admin(
                    1,
                    1,
                    MatrixRaftAdminCommand::release_memory()
                )],
            ),
            (
                815,
                vec![
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(41),
                            data: b"data-node-planned-batch-write".to_vec(),
                            context: b"route-plan".to_vec(),
                            is_command: true,
                        },
                    ),
                    MatrixRaftMessage::install_snapshot(
                        1,
                        1,
                        InstallSnapshotRequest {
                            group_id: 0,
                            term: 1,
                            leader_id: 0,
                            chunk: SnapshotChunk {
                                meta: SnapshotMetadata {
                                    snapshot_id: "batch-route-snapshot-17".to_string(),
                                    last_log_id: LogId { term: 1, index: 17 },
                                    membership: vec![1],
                                    members: vec![peer(815, 1)],
                                },
                                offset: 0,
                                data: b"batch route snapshot chunk".to_vec(),
                                done: false,
                            },
                        },
                    ),
                ],
            ),
        ]
    );
    assert_eq!(
        route_plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(814, 1), 1),
            (MatrixRaftRouteKey::new(815, 1), 1),
            (MatrixRaftRouteKey::new(815, 1), 1),
        ]
    );
    assert_eq!(
        route_plan.messages_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(814, 1),
                MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
            ),
            (
                MatrixRaftRouteKey::new(815, 1),
                MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(41),
                        data: b"data-node-planned-batch-write".to_vec(),
                        context: b"route-plan".to_vec(),
                        is_command: true,
                    },
                ),
            ),
            (
                MatrixRaftRouteKey::new(815, 1),
                MatrixRaftMessage::install_snapshot(
                    1,
                    1,
                    InstallSnapshotRequest {
                        group_id: 0,
                        term: 1,
                        leader_id: 0,
                        chunk: SnapshotChunk {
                            meta: SnapshotMetadata {
                                snapshot_id: "batch-route-snapshot-17".to_string(),
                                last_log_id: LogId { term: 1, index: 17 },
                                membership: vec![1],
                                members: vec![peer(815, 1)],
                            },
                            offset: 0,
                            data: b"batch route snapshot chunk".to_vec(),
                            done: false,
                        },
                    },
                ),
            ),
        ]
    );
    assert_eq!(
        route_plan.message_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(814, 1),
                MatrixRaftMessageType::AdminCommand,
            ),
            (
                MatrixRaftRouteKey::new(815, 1),
                MatrixRaftMessageType::Propose
            ),
            (
                MatrixRaftRouteKey::new(815, 1),
                MatrixRaftMessageType::InstallSnapshotRequest,
            ),
        ]
    );
    assert_eq!(
        route_plan.propose_request_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(814, 1), None),
            (MatrixRaftRouteKey::new(815, 1), Some(41)),
            (MatrixRaftRouteKey::new(815, 1), None),
        ]
    );
    assert_eq!(
        route_plan.propose_request_id_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(814, 1), false),
            (MatrixRaftRouteKey::new(815, 1), true),
            (MatrixRaftRouteKey::new(815, 1), false),
        ]
    );
    assert_eq!(
        route_plan.snapshot_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(814, 1), None),
            (MatrixRaftRouteKey::new(815, 1), None),
            (
                MatrixRaftRouteKey::new(815, 1),
                Some("batch-route-snapshot-17".to_string()),
            ),
        ]
    );
    assert_eq!(
        route_plan.snapshot_id_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(814, 1), false),
            (MatrixRaftRouteKey::new(815, 1), false),
            (MatrixRaftRouteKey::new(815, 1), true),
        ]
    );
    assert_eq!(
        route_plan.snapshot_chunk_payload_bytes_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(814, 1), None),
            (MatrixRaftRouteKey::new(815, 1), None),
            (
                MatrixRaftRouteKey::new(815, 1),
                Some(b"batch route snapshot chunk".len()),
            ),
        ]
    );
    assert_eq!(
        route_plan.snapshot_chunk_offset_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(814, 1), false),
            (MatrixRaftRouteKey::new(815, 1), false),
            (MatrixRaftRouteKey::new(815, 1), true),
        ]
    );
    assert_eq!(
        route_plan.snapshot_chunk_done_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(814, 1), false),
            (MatrixRaftRouteKey::new(815, 1), false),
            (MatrixRaftRouteKey::new(815, 1), true),
        ]
    );
    assert_eq!(
        route_plan.snapshot_chunk_payload_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(814, 1), false),
            (MatrixRaftRouteKey::new(815, 1), false),
            (MatrixRaftRouteKey::new(815, 1), true),
        ]
    );
    assert_eq!(
        route_plan.sender_receiver_by_group(),
        vec![
            (814, vec![(Some(1), Some(1))]),
            (815, vec![(Some(1), Some(1)), (Some(1), Some(1))]),
        ]
    );
    assert_eq!(
        route_plan.terms_by_group(),
        vec![(814, vec![None]), (815, vec![None, Some(1)])]
    );
    assert_eq!(
        route_plan.committed_indices_by_group(),
        vec![(814, vec![None]), (815, vec![None, Some(17)])]
    );
    assert_eq!(
        route_plan.message_bytes_by_group(),
        vec![(814, vec![0]), (815, vec![29, 0])]
    );
    assert_eq!(
        route_plan.propose_request_ids_by_group(),
        vec![(814, vec![None]), (815, vec![Some(41), None])]
    );
    assert_eq!(
        route_plan.propose_request_id_presence_by_group(),
        vec![(814, vec![false]), (815, vec![true, false])]
    );
    assert_eq!(
        route_plan.snapshot_ids_by_group(),
        vec![
            (814, vec![None]),
            (815, vec![None, Some("batch-route-snapshot-17".to_string())]),
        ]
    );
    assert_eq!(
        route_plan.snapshot_id_presence_by_group(),
        vec![(814, vec![false]), (815, vec![false, true])]
    );
    assert_eq!(
        route_plan.snapshot_chunk_offsets_by_group(),
        vec![(814, vec![None]), (815, vec![None, Some(0)])]
    );
    assert_eq!(
        route_plan.snapshot_chunk_offset_presence_by_group(),
        vec![(814, vec![false]), (815, vec![false, true])]
    );
    assert_eq!(
        route_plan.snapshot_chunk_done_by_group(),
        vec![(814, vec![None]), (815, vec![None, Some(false)])]
    );
    assert_eq!(
        route_plan.snapshot_chunk_done_presence_by_group(),
        vec![(814, vec![false]), (815, vec![false, true])]
    );
    assert_eq!(
        route_plan.snapshot_chunk_payload_bytes_by_group(),
        vec![
            (814, vec![None]),
            (815, vec![None, Some(b"batch route snapshot chunk".len())]),
        ]
    );
    assert_eq!(
        route_plan.snapshot_chunk_payload_presence_by_group(),
        vec![(814, vec![false]), (815, vec![false, true])]
    );
    let route_results = server
        .route_message_batch(route_plan.messages.clone())
        .expect("route planned mixed batch");
    assert_eq!(route_results.len(), 3);
    assert_eq!(route_results[0].key, MatrixRaftRouteKey::new(814, 1));
    assert_eq!(route_results[0].released_memory, Some(false));
    assert_eq!(route_results[1].key, MatrixRaftRouteKey::new(815, 1));
    assert!(route_results[1].proposed_log_id.is_some());
    assert_eq!(route_results[2].key, MatrixRaftRouteKey::new(815, 1));
    assert_eq!(
        route_results[2].message_type,
        MatrixRaftMessageType::InstallSnapshotRequest
    );
    assert!(route_results[2].install_snapshot_response.is_some());
    let grouped_route_results = server
        .route_message_batch_grouped(route_plan.messages.clone())
        .expect("route grouped mixed batch");
    assert_eq!(
        grouped_route_results
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(814, 1), (815, 2)]
    );
    assert_eq!(
        grouped_route_results[0].1[0].key,
        MatrixRaftRouteKey::new(814, 1)
    );
    assert_eq!(
        grouped_route_results[1].1[0].key,
        MatrixRaftRouteKey::new(815, 1)
    );
    assert_eq!(
        grouped_route_results[1].1[1].message_type,
        MatrixRaftMessageType::InstallSnapshotRequest
    );
    assert_eq!(
        server.plan_route_message_batch(vec![MatrixRaftRoutedMessage::new(
            899,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        )]),
        Err(RaftError::NodeNotFound(1))
    );

    let best_effort_batch = server.route_message_batch_best_effort(vec![
        MatrixRaftRoutedMessage::new(
            814,
            1,
            MatrixRaftMessage::propose(
                1,
                1,
                MatrixRaftPropose {
                    request_id: Some(42),
                    data: b"meta-server-best-effort-update".to_vec(),
                    context: b"meta-best-effort".to_vec(),
                    is_command: true,
                },
            ),
        ),
        MatrixRaftRoutedMessage::new(
            899,
            1,
            MatrixRaftMessage::propose(
                1,
                1,
                MatrixRaftPropose {
                    request_id: Some(43),
                    data: b"missing-group-write".to_vec(),
                    context: b"missing".to_vec(),
                    is_command: true,
                },
            ),
        ),
        MatrixRaftRoutedMessage::new(
            815,
            1,
            MatrixRaftMessage::propose(
                1,
                1,
                MatrixRaftPropose {
                    request_id: Some(44),
                    data: b"data-node-best-effort-write".to_vec(),
                    context: b"data-best-effort".to_vec(),
                    is_command: true,
                },
            ),
        ),
    ]);
    assert_eq!(best_effort_batch.len(), 3);
    assert_eq!(best_effort_batch[0].group_id, 814);
    assert!(best_effort_batch[0].is_ok());
    assert!(best_effort_batch[0]
        .result
        .as_ref()
        .expect("meta best-effort result")
        .proposed_log_id
        .is_some());
    assert_eq!(best_effort_batch[1].group_id, 899);
    assert!(best_effort_batch[1].result.is_none());
    assert!(best_effort_batch[1]
        .error
        .as_deref()
        .expect("best-effort error")
        .contains("node 1 not found"));
    assert_eq!(best_effort_batch[2].group_id, 815);
    assert!(best_effort_batch[2].is_ok());
    assert_eq!(
        server
            .node(814, 1)
            .expect("meta group node")
            .get_status()
            .expect("meta status after best-effort batch")
            .last_log_index,
        best_effort_batch[0]
            .result
            .as_ref()
            .expect("meta best-effort result")
            .proposed_log_id
            .as_ref()
            .expect("meta best-effort log id")
            .index
    );
    assert_eq!(
        server
            .node(815, 1)
            .expect("data group node")
            .get_status()
            .expect("data status after best-effort batch")
            .last_log_index,
        best_effort_batch[2]
            .result
            .as_ref()
            .expect("data best-effort result")
            .proposed_log_id
            .as_ref()
            .expect("data best-effort log id")
            .index
    );

    let grouped_best_effort_batch = server.route_message_batch_grouped_best_effort(vec![
        MatrixRaftRoutedMessage::new(
            814,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        ),
        MatrixRaftRoutedMessage::new(
            899,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        ),
        MatrixRaftRoutedMessage::new(
            815,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        ),
        MatrixRaftRoutedMessage::new(
            814,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        ),
    ]);
    assert_eq!(
        grouped_best_effort_batch
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(814, 2), (899, 1), (815, 1)]
    );
    assert!(grouped_best_effort_batch[0]
        .1
        .iter()
        .all(|result| result.is_ok()));
    assert!(grouped_best_effort_batch[1].1[0].error.is_some());
    assert!(grouped_best_effort_batch[2].1[0].is_ok());
    let grouped_best_effort_batch_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&grouped_best_effort_batch);
    assert_eq!(
        grouped_best_effort_batch_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.result_count,
                summary.ok_count,
                summary.error_count,
                summary.node_ids.clone(),
                summary.ok_node_ids.clone(),
                summary.error_node_ids.clone(),
                summary.message_types.clone(),
                summary.ok_message_types.clone(),
                summary.error_message_types.clone(),
                summary.counts_by_message_type.clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                814,
                2,
                2,
                0,
                vec![1, 1],
                vec![1, 1],
                Vec::new(),
                vec![MatrixRaftMessageType::AdminCommand],
                vec![MatrixRaftMessageType::AdminCommand],
                Vec::new(),
                vec![(MatrixRaftMessageType::AdminCommand, 2, 2, 0)],
            ),
            (
                899,
                1,
                0,
                1,
                vec![1],
                Vec::new(),
                vec![1],
                vec![MatrixRaftMessageType::AdminCommand],
                Vec::new(),
                vec![MatrixRaftMessageType::AdminCommand],
                vec![(MatrixRaftMessageType::AdminCommand, 1, 0, 1)],
            ),
            (
                815,
                1,
                1,
                0,
                vec![1],
                vec![1],
                Vec::new(),
                vec![MatrixRaftMessageType::AdminCommand],
                vec![MatrixRaftMessageType::AdminCommand],
                Vec::new(),
                vec![(MatrixRaftMessageType::AdminCommand, 1, 1, 0)],
            ),
        ]
    );
    assert_eq!(
        grouped_best_effort_batch_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.route_keys.clone(),
                summary.ok_route_keys.clone(),
                summary.error_route_keys.clone(),
                summary.statuses_by_route_key.clone(),
                summary.errors_by_route_key.clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                814,
                vec![
                    MatrixRaftRouteKey::new(814, 1),
                    MatrixRaftRouteKey::new(814, 1),
                ],
                vec![
                    MatrixRaftRouteKey::new(814, 1),
                    MatrixRaftRouteKey::new(814, 1),
                ],
                Vec::new(),
                vec![
                    (MatrixRaftRouteKey::new(814, 1), true),
                    (MatrixRaftRouteKey::new(814, 1), true),
                ],
                vec![
                    (MatrixRaftRouteKey::new(814, 1), None),
                    (MatrixRaftRouteKey::new(814, 1), None),
                ],
            ),
            (
                899,
                vec![MatrixRaftRouteKey::new(899, 1)],
                Vec::new(),
                vec![MatrixRaftRouteKey::new(899, 1)],
                vec![(MatrixRaftRouteKey::new(899, 1), false)],
                vec![(
                    MatrixRaftRouteKey::new(899, 1),
                    grouped_best_effort_batch[1].1[0].error.clone(),
                )],
            ),
            (
                815,
                vec![MatrixRaftRouteKey::new(815, 1)],
                vec![MatrixRaftRouteKey::new(815, 1)],
                Vec::new(),
                vec![(MatrixRaftRouteKey::new(815, 1), true)],
                vec![(MatrixRaftRouteKey::new(815, 1), None)],
            ),
        ]
    );
    assert_eq!(
        grouped_best_effort_batch_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.node_ids_by_route_key(),
                summary.ok_node_ids_by_route_key(),
                summary.error_node_ids_by_route_key(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                814,
                vec![
                    (MatrixRaftRouteKey::new(814, 1), 1),
                    (MatrixRaftRouteKey::new(814, 1), 1),
                ],
                vec![
                    (MatrixRaftRouteKey::new(814, 1), 1),
                    (MatrixRaftRouteKey::new(814, 1), 1),
                ],
                Vec::<(MatrixRaftRouteKey, u64)>::new(),
            ),
            (
                899,
                vec![(MatrixRaftRouteKey::new(899, 1), 1)],
                Vec::new(),
                vec![(MatrixRaftRouteKey::new(899, 1), 1)],
            ),
            (
                815,
                vec![(MatrixRaftRouteKey::new(815, 1), 1)],
                vec![(MatrixRaftRouteKey::new(815, 1), 1)],
                Vec::new(),
            ),
        ]
    );
    assert!(grouped_best_effort_batch_summaries
        .iter()
        .all(|summary| summary
            .proposed_log_ids_by_route_key
            .iter()
            .all(|(_, proposed_log_id)| proposed_log_id.is_none())));
    assert!(grouped_best_effort_batch_summaries
        .iter()
        .all(|summary| summary
            .read_index_responses_by_route_key
            .iter()
            .all(|(_, read_index_response)| read_index_response.is_none())));
    assert!(grouped_best_effort_batch_summaries
        .iter()
        .find(|summary| summary.group_id == 814)
        .is_some_and(|summary| summary
            .released_memory_by_route_key
            .iter()
            .all(|(_, released_memory)| released_memory.is_some())));
    assert!(grouped_best_effort_batch_summaries
        .iter()
        .find(|summary| summary.group_id == 814)
        .is_some_and(|summary| summary
            .released_memory_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));
    assert!(grouped_best_effort_batch_summaries
        .iter()
        .find(|summary| summary.group_id == 899)
        .is_some_and(|summary| summary
            .released_memory_by_route_key
            .iter()
            .all(|(_, released_memory)| released_memory.is_none())));
    assert!(grouped_best_effort_batch_summaries
        .iter()
        .find(|summary| summary.group_id == 899)
        .is_some_and(|summary| summary
            .released_memory_presence_by_route_key()
            .iter()
            .all(|(_, present)| !present)));
    assert_eq!(
        grouped_best_effort_batch_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary
                    .results_by_route_key
                    .iter()
                    .map(|(key, result)| (*key, result.message_type, result.is_ok()))
                    .collect::<Vec<_>>(),
                summary.ok_results_by_route_key.len(),
                summary.error_results_by_route_key.len()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                814,
                vec![
                    (
                        MatrixRaftRouteKey::new(814, 1),
                        MatrixRaftMessageType::AdminCommand,
                        true
                    ),
                    (
                        MatrixRaftRouteKey::new(814, 1),
                        MatrixRaftMessageType::AdminCommand,
                        true
                    ),
                ],
                2,
                0
            ),
            (
                899,
                vec![(
                    MatrixRaftRouteKey::new(899, 1),
                    MatrixRaftMessageType::AdminCommand,
                    false
                )],
                0,
                1
            ),
            (
                815,
                vec![(
                    MatrixRaftRouteKey::new(815, 1),
                    MatrixRaftMessageType::AdminCommand,
                    true
                )],
                1,
                0
            ),
        ]
    );

    let priority_plan = server
        .plan_priority_route_message_batch(vec![
            MatrixRaftPriorityRoutedMessage::new(
                MailPriority::Normal,
                MatrixRaftRoutedMessage::new(
                    814,
                    1,
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(45),
                            data: b"normal-meta-priority-write".to_vec(),
                            context: b"priority-normal".to_vec(),
                            is_command: true,
                        },
                    ),
                ),
            ),
            MatrixRaftPriorityRoutedMessage::new(
                MailPriority::Slowly,
                MatrixRaftRoutedMessage::new(
                    815,
                    1,
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(46),
                            data: b"slow-data-priority-write".to_vec(),
                            context: b"priority-slow".to_vec(),
                            is_command: true,
                        },
                    ),
                ),
            ),
            MatrixRaftPriorityRoutedMessage::new(
                MailPriority::Urgent,
                MatrixRaftRoutedMessage::new(
                    815,
                    1,
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(47),
                            data: b"urgent-data-priority-write".to_vec(),
                            context: b"priority-urgent".to_vec(),
                            is_command: true,
                        },
                    ),
                ),
            ),
            MatrixRaftPriorityRoutedMessage::new(
                MailPriority::Urgent,
                MatrixRaftRoutedMessage::new(
                    814,
                    1,
                    MatrixRaftMessage::install_snapshot(
                        1,
                        1,
                        InstallSnapshotRequest {
                            group_id: 0,
                            term: 1,
                            leader_id: 0,
                            chunk: SnapshotChunk {
                                meta: SnapshotMetadata {
                                    snapshot_id: "priority-route-snapshot-23".to_string(),
                                    last_log_id: LogId { term: 1, index: 23 },
                                    membership: vec![1],
                                    members: vec![peer(814, 1)],
                                },
                                offset: 0,
                                data: b"priority route snapshot chunk".to_vec(),
                                done: true,
                            },
                        },
                    ),
                ),
            ),
        ])
        .expect("plan priority route batch");
    assert_eq!(priority_plan.message_count, 4);
    assert_eq!(priority_plan.group_count, 2);
    assert_eq!(priority_plan.group_ids, vec![814, 815]);
    assert_eq!(priority_plan.node_count, 1);
    assert_eq!(priority_plan.node_ids, vec![1]);
    assert_eq!(
        priority_plan.message_types,
        vec![
            MatrixRaftMessageType::Propose,
            MatrixRaftMessageType::InstallSnapshotRequest,
        ]
    );
    assert_eq!(
        priority_plan
            .priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    group.message_count,
                    group.group_count,
                    group.group_ids.clone(),
                    group.route_keys.clone(),
                    group.node_ids.clone(),
                    group.message_types.clone(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                MailPriority::Urgent,
                2,
                2,
                vec![815, 814],
                vec![
                    MatrixRaftRouteKey::new(815, 1),
                    MatrixRaftRouteKey::new(814, 1),
                ],
                vec![1],
                vec![
                    MatrixRaftMessageType::Propose,
                    MatrixRaftMessageType::InstallSnapshotRequest,
                ],
            ),
            (
                MailPriority::Normal,
                1,
                1,
                vec![814],
                vec![MatrixRaftRouteKey::new(814, 1)],
                vec![1],
                vec![MatrixRaftMessageType::Propose],
            ),
            (
                MailPriority::Slowly,
                1,
                1,
                vec![815],
                vec![MatrixRaftRouteKey::new(815, 1)],
                vec![1],
                vec![MatrixRaftMessageType::Propose],
            ),
        ]
    );
    assert_eq!(
        priority_plan
            .groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group.message_count,
                    group.node_ids.clone(),
                    group.message_types.clone(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                814,
                2,
                vec![1],
                vec![
                    MatrixRaftMessageType::InstallSnapshotRequest,
                    MatrixRaftMessageType::Propose,
                ],
            ),
            (815, 2, vec![1], vec![MatrixRaftMessageType::Propose]),
        ]
    );
    assert_eq!(
        priority_plan.route_keys_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![
                    MatrixRaftRouteKey::new(815, 1),
                    MatrixRaftRouteKey::new(814, 1),
                ]
            ),
            (MailPriority::Normal, vec![MatrixRaftRouteKey::new(814, 1)]),
            (MailPriority::Slowly, vec![MatrixRaftRouteKey::new(815, 1)]),
        ]
    );
    assert_eq!(
        priority_plan.group_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![815, 814]),
            (MailPriority::Normal, vec![814]),
            (MailPriority::Slowly, vec![815]),
        ]
    );
    assert_eq!(
        priority_plan.node_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![1]),
            (MailPriority::Normal, vec![1]),
            (MailPriority::Slowly, vec![1]),
        ]
    );
    assert_eq!(
        priority_plan.message_types_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![
                    MatrixRaftMessageType::Propose,
                    MatrixRaftMessageType::InstallSnapshotRequest,
                ]
            ),
            (MailPriority::Normal, vec![MatrixRaftMessageType::Propose]),
            (MailPriority::Slowly, vec![MatrixRaftMessageType::Propose]),
        ]
    );
    assert_eq!(
        priority_plan.messages_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(47),
                            data: b"urgent-data-priority-write".to_vec(),
                            context: b"priority-urgent".to_vec(),
                            is_command: true,
                        },
                    ),
                    MatrixRaftMessage::install_snapshot(
                        1,
                        1,
                        InstallSnapshotRequest {
                            group_id: 0,
                            term: 1,
                            leader_id: 0,
                            chunk: SnapshotChunk {
                                meta: SnapshotMetadata {
                                    snapshot_id: "priority-route-snapshot-23".to_string(),
                                    last_log_id: LogId { term: 1, index: 23 },
                                    membership: vec![1],
                                    members: vec![peer(814, 1)],
                                },
                                offset: 0,
                                data: b"priority route snapshot chunk".to_vec(),
                                done: true,
                            },
                        },
                    ),
                ],
            ),
            (
                MailPriority::Normal,
                vec![MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(45),
                        data: b"normal-meta-priority-write".to_vec(),
                        context: b"priority-normal".to_vec(),
                        is_command: true,
                    },
                )],
            ),
            (
                MailPriority::Slowly,
                vec![MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(46),
                        data: b"slow-data-priority-write".to_vec(),
                        context: b"priority-slow".to_vec(),
                        is_command: true,
                    },
                )],
            ),
        ]
    );
    assert_eq!(
        priority_plan.sender_receiver_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![(Some(1), Some(1)), (Some(1), Some(1))],
            ),
            (MailPriority::Normal, vec![(Some(1), Some(1))]),
            (MailPriority::Slowly, vec![(Some(1), Some(1))]),
        ]
    );
    assert_eq!(
        priority_plan.terms_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, Some(1)]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_plan.committed_indices_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, Some(23)]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_plan.message_counts_by_priority(),
        vec![
            (MailPriority::Urgent, 2),
            (MailPriority::Normal, 1),
            (MailPriority::Slowly, 1),
        ]
    );
    assert_eq!(
        priority_plan.route_key_counts_by_priority(),
        vec![
            (MailPriority::Urgent, 2),
            (MailPriority::Normal, 1),
            (MailPriority::Slowly, 1),
        ]
    );
    assert_eq!(
        priority_plan.fanout_counts_by_priority(),
        vec![
            (MailPriority::Urgent, 2, 2),
            (MailPriority::Normal, 1, 1),
            (MailPriority::Slowly, 1, 1),
        ]
    );
    assert_eq!(
        priority_plan.message_bytes_by_priority(),
        vec![
            (MailPriority::Urgent, vec![26, 0]),
            (MailPriority::Normal, vec![26]),
            (MailPriority::Slowly, vec![24]),
        ]
    );
    assert_eq!(
        priority_plan.propose_request_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![Some(47), None]),
            (MailPriority::Normal, vec![Some(45)]),
            (MailPriority::Slowly, vec![Some(46)]),
        ]
    );
    assert_eq!(
        priority_plan.propose_request_id_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![true, false]),
            (MailPriority::Normal, vec![true]),
            (MailPriority::Slowly, vec![true]),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_ids_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![None, Some("priority-route-snapshot-23".to_string())],
            ),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_id_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false, true]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_offsets_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, Some(0)]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_offset_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false, true]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_done_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, Some(true)]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_done_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false, true]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_payload_bytes_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![None, Some(b"priority route snapshot chunk".len())],
            ),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_payload_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false, true]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        priority_plan.groups[1].route_keys,
        vec![
            MatrixRaftRouteKey::new(815, 1),
            MatrixRaftRouteKey::new(815, 1),
        ]
    );
    assert_eq!(
        priority_plan.route_keys_by_group(),
        vec![
            (
                814,
                vec![
                    MatrixRaftRouteKey::new(814, 1),
                    MatrixRaftRouteKey::new(814, 1),
                ],
            ),
            (
                815,
                vec![
                    MatrixRaftRouteKey::new(815, 1),
                    MatrixRaftRouteKey::new(815, 1),
                ],
            ),
        ]
    );
    assert_eq!(
        priority_plan.node_ids_by_group(),
        vec![(814, vec![1]), (815, vec![1])]
    );
    assert_eq!(
        priority_plan.message_counts_by_group(),
        vec![(814, 2), (815, 2)]
    );
    assert_eq!(
        priority_plan.route_key_counts_by_group(),
        vec![(814, 2), (815, 2)]
    );
    assert_eq!(
        priority_plan.fanout_counts_by_group(),
        vec![(814, 2, 2), (815, 2, 2)]
    );
    assert_eq!(
        priority_plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(815, 1), 1),
            (MatrixRaftRouteKey::new(814, 1), 1),
            (MatrixRaftRouteKey::new(814, 1), 1),
            (MatrixRaftRouteKey::new(815, 1), 1),
        ]
    );
    assert_eq!(
        priority_plan.message_types_by_group(),
        vec![
            (
                814,
                vec![
                    MatrixRaftMessageType::InstallSnapshotRequest,
                    MatrixRaftMessageType::Propose,
                ],
            ),
            (815, vec![MatrixRaftMessageType::Propose]),
        ]
    );
    assert_eq!(
        priority_plan.priorities_by_group(),
        vec![
            (814, vec![MailPriority::Urgent, MailPriority::Normal]),
            (815, vec![MailPriority::Urgent, MailPriority::Slowly]),
        ]
    );
    assert_eq!(
        priority_plan.messages_by_group(),
        vec![
            (
                814,
                vec![
                    MatrixRaftMessage::install_snapshot(
                        1,
                        1,
                        InstallSnapshotRequest {
                            group_id: 0,
                            term: 1,
                            leader_id: 0,
                            chunk: SnapshotChunk {
                                meta: SnapshotMetadata {
                                    snapshot_id: "priority-route-snapshot-23".to_string(),
                                    last_log_id: LogId { term: 1, index: 23 },
                                    membership: vec![1],
                                    members: vec![peer(814, 1)],
                                },
                                offset: 0,
                                data: b"priority route snapshot chunk".to_vec(),
                                done: true,
                            },
                        },
                    ),
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(45),
                            data: b"normal-meta-priority-write".to_vec(),
                            context: b"priority-normal".to_vec(),
                            is_command: true,
                        },
                    ),
                ],
            ),
            (
                815,
                vec![
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(47),
                            data: b"urgent-data-priority-write".to_vec(),
                            context: b"priority-urgent".to_vec(),
                            is_command: true,
                        },
                    ),
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(46),
                            data: b"slow-data-priority-write".to_vec(),
                            context: b"priority-slow".to_vec(),
                            is_command: true,
                        },
                    ),
                ],
            ),
        ]
    );
    assert_eq!(
        priority_plan.sender_receiver_by_group(),
        vec![
            (814, vec![(Some(1), Some(1)), (Some(1), Some(1))]),
            (815, vec![(Some(1), Some(1)), (Some(1), Some(1))]),
        ]
    );
    assert_eq!(
        priority_plan.terms_by_group(),
        vec![(814, vec![Some(1), None]), (815, vec![None, None])]
    );
    assert_eq!(
        priority_plan.committed_indices_by_group(),
        vec![(814, vec![Some(23), None]), (815, vec![None, None])]
    );
    assert_eq!(
        priority_plan.message_bytes_by_group(),
        vec![(814, vec![0, 26]), (815, vec![26, 24])]
    );
    assert_eq!(
        priority_plan.propose_request_ids_by_group(),
        vec![(814, vec![None, Some(45)]), (815, vec![Some(47), Some(46)]),]
    );
    assert_eq!(
        priority_plan.propose_request_id_presence_by_group(),
        vec![(814, vec![false, true]), (815, vec![true, true]),]
    );
    assert_eq!(
        priority_plan.snapshot_ids_by_group(),
        vec![
            (
                814,
                vec![Some("priority-route-snapshot-23".to_string()), None]
            ),
            (815, vec![None, None]),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_id_presence_by_group(),
        vec![(814, vec![true, false]), (815, vec![false, false])]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_offsets_by_group(),
        vec![(814, vec![Some(0), None]), (815, vec![None, None])]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_offset_presence_by_group(),
        vec![(814, vec![true, false]), (815, vec![false, false])]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_done_by_group(),
        vec![(814, vec![Some(true), None]), (815, vec![None, None])]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_done_presence_by_group(),
        vec![(814, vec![true, false]), (815, vec![false, false])]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_payload_bytes_by_group(),
        vec![
            (
                814,
                vec![Some(b"priority route snapshot chunk".len()), None]
            ),
            (815, vec![None, None]),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_payload_presence_by_group(),
        vec![(814, vec![true, false]), (815, vec![false, false])]
    );
    assert_eq!(
        priority_plan
            .messages
            .iter()
            .map(|message| message.priority)
            .collect::<Vec<_>>(),
        vec![
            MailPriority::Urgent,
            MailPriority::Urgent,
            MailPriority::Normal,
            MailPriority::Slowly,
        ]
    );
    assert_eq!(
        priority_plan.priorities_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(815, 1), MailPriority::Urgent),
            (MatrixRaftRouteKey::new(814, 1), MailPriority::Urgent),
            (MatrixRaftRouteKey::new(814, 1), MailPriority::Normal),
            (MatrixRaftRouteKey::new(815, 1), MailPriority::Slowly),
        ]
    );
    assert_eq!(
        priority_plan.messages_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(815, 1),
                MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(47),
                        data: b"urgent-data-priority-write".to_vec(),
                        context: b"priority-urgent".to_vec(),
                        is_command: true,
                    },
                ),
            ),
            (
                MatrixRaftRouteKey::new(814, 1),
                MatrixRaftMessage::install_snapshot(
                    1,
                    1,
                    InstallSnapshotRequest {
                        group_id: 0,
                        term: 1,
                        leader_id: 0,
                        chunk: SnapshotChunk {
                            meta: SnapshotMetadata {
                                snapshot_id: "priority-route-snapshot-23".to_string(),
                                last_log_id: LogId { term: 1, index: 23 },
                                membership: vec![1],
                                members: vec![peer(814, 1)],
                            },
                            offset: 0,
                            data: b"priority route snapshot chunk".to_vec(),
                            done: true,
                        },
                    },
                ),
            ),
            (
                MatrixRaftRouteKey::new(814, 1),
                MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(45),
                        data: b"normal-meta-priority-write".to_vec(),
                        context: b"priority-normal".to_vec(),
                        is_command: true,
                    },
                ),
            ),
            (
                MatrixRaftRouteKey::new(815, 1),
                MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(46),
                        data: b"slow-data-priority-write".to_vec(),
                        context: b"priority-slow".to_vec(),
                        is_command: true,
                    },
                ),
            ),
        ]
    );
    assert_eq!(
        priority_plan.message_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(815, 1),
                MatrixRaftMessageType::Propose
            ),
            (
                MatrixRaftRouteKey::new(814, 1),
                MatrixRaftMessageType::InstallSnapshotRequest,
            ),
            (
                MatrixRaftRouteKey::new(814, 1),
                MatrixRaftMessageType::Propose
            ),
            (
                MatrixRaftRouteKey::new(815, 1),
                MatrixRaftMessageType::Propose
            ),
        ]
    );
    assert_eq!(
        priority_plan.propose_request_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(815, 1), Some(47)),
            (MatrixRaftRouteKey::new(814, 1), None),
            (MatrixRaftRouteKey::new(814, 1), Some(45)),
            (MatrixRaftRouteKey::new(815, 1), Some(46)),
        ]
    );
    assert_eq!(
        priority_plan.propose_request_id_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(815, 1), true),
            (MatrixRaftRouteKey::new(814, 1), false),
            (MatrixRaftRouteKey::new(814, 1), true),
            (MatrixRaftRouteKey::new(815, 1), true),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(815, 1), None),
            (
                MatrixRaftRouteKey::new(814, 1),
                Some("priority-route-snapshot-23".to_string()),
            ),
            (MatrixRaftRouteKey::new(814, 1), None),
            (MatrixRaftRouteKey::new(815, 1), None),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_id_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(815, 1), false),
            (MatrixRaftRouteKey::new(814, 1), true),
            (MatrixRaftRouteKey::new(814, 1), false),
            (MatrixRaftRouteKey::new(815, 1), false),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_done_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(815, 1), None),
            (MatrixRaftRouteKey::new(814, 1), Some(true)),
            (MatrixRaftRouteKey::new(814, 1), None),
            (MatrixRaftRouteKey::new(815, 1), None),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_offset_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(815, 1), false),
            (MatrixRaftRouteKey::new(814, 1), true),
            (MatrixRaftRouteKey::new(814, 1), false),
            (MatrixRaftRouteKey::new(815, 1), false),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_done_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(815, 1), false),
            (MatrixRaftRouteKey::new(814, 1), true),
            (MatrixRaftRouteKey::new(814, 1), false),
            (MatrixRaftRouteKey::new(815, 1), false),
        ]
    );
    assert_eq!(
        priority_plan.snapshot_chunk_payload_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(815, 1), false),
            (MatrixRaftRouteKey::new(814, 1), true),
            (MatrixRaftRouteKey::new(814, 1), false),
            (MatrixRaftRouteKey::new(815, 1), false),
        ]
    );
    assert_eq!(
        priority_plan
            .messages
            .iter()
            .map(MatrixRaftPriorityRoutedMessage::route_key)
            .collect::<Vec<_>>(),
        priority_plan.route_keys
    );
    let priority_results = server
        .route_priority_message_batch(priority_plan.messages.clone())
        .expect("route priority batch");
    assert_eq!(
        priority_results
            .iter()
            .map(|result| result.key)
            .collect::<Vec<_>>(),
        vec![
            MatrixRaftRouteKey::new(815, 1),
            MatrixRaftRouteKey::new(814, 1),
            MatrixRaftRouteKey::new(814, 1),
            MatrixRaftRouteKey::new(815, 1),
        ]
    );
    assert!(priority_results
        .iter()
        .all(|result| result.kind == MatrixRaftRouteResultKind::Delivered));
    assert!(priority_results[0].proposed_log_id.is_some());
    assert_eq!(
        priority_results[1].message_type,
        MatrixRaftMessageType::InstallSnapshotRequest
    );
    assert!(priority_results[1].install_snapshot_response.is_some());
    assert!(priority_results[2].proposed_log_id.is_some());
    assert!(priority_results[3].proposed_log_id.is_some());
    let priority_grouped = server
        .route_priority_message_batch_grouped(vec![
            MatrixRaftPriorityRoutedMessage::new(
                MailPriority::Slowly,
                MatrixRaftRoutedMessage::new(
                    815,
                    1,
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(50),
                            data: b"grouped-slow-data-priority-write".to_vec(),
                            context: b"priority-grouped-slow".to_vec(),
                            is_command: true,
                        },
                    ),
                ),
            ),
            MatrixRaftPriorityRoutedMessage::new(
                MailPriority::Urgent,
                MatrixRaftRoutedMessage::new(
                    814,
                    1,
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(51),
                            data: b"grouped-urgent-meta-priority-write".to_vec(),
                            context: b"priority-grouped-urgent".to_vec(),
                            is_command: true,
                        },
                    ),
                ),
            ),
            MatrixRaftPriorityRoutedMessage::new(
                MailPriority::Normal,
                MatrixRaftRoutedMessage::new(
                    815,
                    1,
                    MatrixRaftMessage::propose(
                        1,
                        1,
                        MatrixRaftPropose {
                            request_id: Some(52),
                            data: b"grouped-normal-data-priority-write".to_vec(),
                            context: b"priority-grouped-normal".to_vec(),
                            is_command: true,
                        },
                    ),
                ),
            ),
        ])
        .expect("route grouped priority batch");
    assert_eq!(
        priority_grouped
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(814, 1), (815, 2)]
    );
    assert_eq!(
        priority_grouped[0].1[0].key,
        MatrixRaftRouteKey::new(814, 1)
    );
    assert_eq!(
        priority_grouped[1]
            .1
            .iter()
            .map(|result| result.key)
            .collect::<Vec<_>>(),
        vec![
            MatrixRaftRouteKey::new(815, 1),
            MatrixRaftRouteKey::new(815, 1)
        ]
    );
    let priority_best_effort = server.route_priority_message_batch_best_effort(vec![
        MatrixRaftPriorityRoutedMessage::new(
            MailPriority::Slowly,
            MatrixRaftRoutedMessage::new(
                899,
                1,
                MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(48),
                        data: b"missing-slow-priority-write".to_vec(),
                        context: b"priority-missing".to_vec(),
                        is_command: true,
                    },
                ),
            ),
        ),
        MatrixRaftPriorityRoutedMessage::new(
            MailPriority::Urgent,
            MatrixRaftRoutedMessage::new(
                814,
                1,
                MatrixRaftMessage::propose(
                    1,
                    1,
                    MatrixRaftPropose {
                        request_id: Some(49),
                        data: b"urgent-meta-priority-best-effort".to_vec(),
                        context: b"priority-best-effort".to_vec(),
                        is_command: true,
                    },
                ),
            ),
        ),
    ]);
    assert_eq!(priority_best_effort.len(), 2);
    assert_eq!(priority_best_effort[0].group_id, 814);
    assert!(priority_best_effort[0].is_ok());
    assert_eq!(priority_best_effort[1].group_id, 899);
    assert!(priority_best_effort[1].error.is_some());
    let priority_grouped_best_effort =
        server.route_priority_message_batch_grouped_best_effort(vec![
            MatrixRaftPriorityRoutedMessage::new(
                MailPriority::Slowly,
                MatrixRaftRoutedMessage::new(
                    899,
                    1,
                    MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
                ),
            ),
            MatrixRaftPriorityRoutedMessage::new(
                MailPriority::Urgent,
                MatrixRaftRoutedMessage::new(
                    815,
                    1,
                    MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
                ),
            ),
            MatrixRaftPriorityRoutedMessage::new(
                MailPriority::Normal,
                MatrixRaftRoutedMessage::new(
                    814,
                    1,
                    MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
                ),
            ),
        ]);
    assert_eq!(
        priority_grouped_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(815, 1), (814, 1), (899, 1)]
    );
    assert!(priority_grouped_best_effort[0].1[0].is_ok());
    assert!(priority_grouped_best_effort[1].1[0].is_ok());
    assert!(priority_grouped_best_effort[2].1[0].error.is_some());

    server.shutdown_all().expect("shutdown best-effort server");

    let _ = fs::remove_dir_all(meta_wal_dir);
    let _ = fs::remove_dir_all(meta_snapshot_dir);
    let _ = fs::remove_dir_all(data_wal_dir);
    let _ = fs::remove_dir_all(data_snapshot_dir);
}

#[test]
fn matrixraft_multi_raft_server_controls_group_lifecycle_independently() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_dir = temp_dir("lifecycle-meta-wal");
    let meta_snapshot_dir = temp_dir("lifecycle-meta-snapshot");
    let data_wal_dir = temp_dir("lifecycle-data-wal");
    let data_snapshot_dir = temp_dir("lifecycle-data-snapshot");
    let aux_wal_dir = temp_dir("lifecycle-aux-wal");
    let aux_snapshot_dir = temp_dir("lifecycle-aux-snapshot");
    server
        .create_node(options(816, &meta_wal_dir, &meta_snapshot_dir), 1)
        .expect("meta node");
    server
        .create_node(options(817, &data_wal_dir, &data_snapshot_dir), 1)
        .expect("data node");
    server
        .create_node(options(846, &aux_wal_dir, &aux_snapshot_dir), 1)
        .expect("aux node");

    assert_eq!(
        server
            .start_index_on_node(816, 1)
            .expect("start index on meta node"),
        1
    );
    assert_eq!(
        server
            .start_indices_on_group(816)
            .expect("start indices on meta group"),
        vec![1]
    );
    assert_eq!(
        server
            .start_indices_for_groups([816, 817])
            .expect("start indices on selected groups"),
        vec![(816, vec![1]), (817, vec![1])]
    );
    let start_indices_plan = server
        .plan_start_indices_for_groups([816, 817])
        .expect("plan start indices on selected groups");
    assert_eq!(start_indices_plan.operation, "start_indices");
    assert_eq!(start_indices_plan.group_count, 2);
    assert_eq!(start_indices_plan.node_count, 2);
    assert_eq!(
        server
            .recover_fsm_from_snapshot_on_group(816)
            .expect("initial recover flags on meta group"),
        vec![false]
    );
    assert_eq!(
        server
            .recover_fsm_from_snapshot_for_groups([816, 817])
            .expect("initial recover flags on selected groups"),
        vec![(816, vec![false]), (817, vec![false])]
    );
    let recover_flags_plan = server
        .plan_recover_fsm_from_snapshot_for_groups([816, 817])
        .expect("plan recover flags on selected groups");
    assert_eq!(recover_flags_plan.operation, "recover_fsm_from_snapshot");
    assert_eq!(recover_flags_plan.node_count, 2);

    let start_plan = server
        .plan_start_groups([816, 817], 1)
        .expect("plan start meta and data groups");
    assert_eq!(
        start_plan.action,
        matrixraft::MatrixRaftLifecycleAction::Start
    );
    assert_eq!(start_plan.group_count, 2);
    assert_eq!(start_plan.group_ids, vec![816, 817]);
    assert_eq!(start_plan.node_count, 2);
    assert_eq!(start_plan.start_index, Some(1));
    assert_eq!(start_plan.recover_fsm_from_snapshot, None);
    assert_eq!(
        start_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone(), group.start_index))
            .collect::<Vec<_>>(),
        vec![(816, vec![1], Some(1)), (817, vec![1], Some(1))]
    );
    assert_eq!(
        start_plan.actions_by_group(),
        vec![
            (816, matrixraft::MatrixRaftLifecycleAction::Start),
            (817, matrixraft::MatrixRaftLifecycleAction::Start),
        ]
    );
    assert_eq!(
        start_plan.actions_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(816, 1),
                matrixraft::MatrixRaftLifecycleAction::Start
            ),
            (
                MatrixRaftRouteKey::new(817, 1),
                matrixraft::MatrixRaftLifecycleAction::Start
            ),
        ]
    );
    assert_eq!(start_plan.node_counts_by_group(), vec![(816, 1), (817, 1)]);
    assert_eq!(
        start_plan.route_key_counts_by_group(),
        vec![(816, 1), (817, 1)]
    );
    assert_eq!(
        start_plan.fanout_counts_by_group(),
        vec![(816, 1, 1), (817, 1, 1)]
    );
    assert_eq!(
        start_plan.start_indices_by_group(),
        vec![(816, Some(1)), (817, Some(1))]
    );
    assert_eq!(
        start_plan.start_index_presence_by_group(),
        vec![(816, true), (817, true)]
    );
    assert_eq!(
        start_plan.start_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), Some(1)),
            (MatrixRaftRouteKey::new(817, 1), Some(1)),
        ]
    );
    assert_eq!(
        start_plan.start_index_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), true),
            (MatrixRaftRouteKey::new(817, 1), true),
        ]
    );
    assert_eq!(
        start_plan.recover_fsm_from_snapshot_by_group(),
        vec![(816, None), (817, None)]
    );
    assert_eq!(
        start_plan.recover_fsm_from_snapshot_presence_by_group(),
        vec![(816, false), (817, false)]
    );
    assert_eq!(
        start_plan.recover_fsm_from_snapshot_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), None),
            (MatrixRaftRouteKey::new(817, 1), None),
        ]
    );
    assert_eq!(
        start_plan.recover_fsm_from_snapshot_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), false),
            (MatrixRaftRouteKey::new(817, 1), false),
        ]
    );
    assert_eq!(
        server
            .start_groups([816, 817], 1)
            .expect("start meta and data groups"),
        vec![(816, 1), (817, 1)]
    );
    let start_best_effort = server
        .start_groups_best_effort([816, 817], 1)
        .expect("best-effort start meta and data groups");
    assert_eq!(
        start_best_effort
            .iter()
            .map(|group| (
                group.group_id,
                group.action,
                group.node_count,
                group.ok_count
            ))
            .collect::<Vec<_>>(),
        vec![
            (816, matrixraft::MatrixRaftLifecycleAction::Start, 1, 1),
            (817, matrixraft::MatrixRaftLifecycleAction::Start, 1, 1),
        ]
    );
    assert!(start_best_effort.iter().all(|group| group.is_ok()));
    assert_eq!(
        start_best_effort
            .iter()
            .map(|group| (group.group_id, group.route_keys(), group.ok_route_keys()))
            .collect::<Vec<_>>(),
        vec![
            (
                816,
                vec![MatrixRaftRouteKey::new(816, 1)],
                vec![MatrixRaftRouteKey::new(816, 1)]
            ),
            (
                817,
                vec![MatrixRaftRouteKey::new(817, 1)],
                vec![MatrixRaftRouteKey::new(817, 1)]
            ),
        ]
    );
    assert!(start_best_effort
        .iter()
        .all(|group| group.error_route_keys().is_empty()));
    assert_eq!(
        start_best_effort
            .iter()
            .map(|group| (
                group.group_id,
                group.node_ids_by_route_key(),
                group.ok_node_ids_by_route_key()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                816,
                vec![(MatrixRaftRouteKey::new(816, 1), 1)],
                vec![(MatrixRaftRouteKey::new(816, 1), 1)]
            ),
            (
                817,
                vec![(MatrixRaftRouteKey::new(817, 1), 1)],
                vec![(MatrixRaftRouteKey::new(817, 1), 1)]
            ),
        ]
    );
    assert!(start_best_effort
        .iter()
        .all(|group| group.error_node_ids_by_route_key().is_empty()));
    assert_eq!(
        start_best_effort
            .iter()
            .map(|group| (group.group_id, group.actions_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                816,
                vec![(
                    MatrixRaftRouteKey::new(816, 1),
                    matrixraft::MatrixRaftLifecycleAction::Start
                )],
            ),
            (
                817,
                vec![(
                    MatrixRaftRouteKey::new(817, 1),
                    matrixraft::MatrixRaftLifecycleAction::Start
                )],
            ),
        ]
    );
    assert_eq!(
        start_best_effort
            .iter()
            .map(|group| (
                group.group_id,
                group
                    .results_by_route_key()
                    .iter()
                    .map(|(key, result)| (*key, result.action, result.is_ok()))
                    .collect::<Vec<_>>(),
                group.ok_results_by_route_key().len(),
                group.error_results_by_route_key().len()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                816,
                vec![(
                    MatrixRaftRouteKey::new(816, 1),
                    matrixraft::MatrixRaftLifecycleAction::Start,
                    true
                )],
                1,
                0
            ),
            (
                817,
                vec![(
                    MatrixRaftRouteKey::new(817, 1),
                    matrixraft::MatrixRaftLifecycleAction::Start,
                    true
                )],
                1,
                0
            ),
        ]
    );
    assert_eq!(
        server
            .start_indices_for_groups([816, 817])
            .expect("start indices after selected start"),
        vec![(816, vec![1]), (817, vec![1])]
    );
    assert!(
        server
            .node(846, 1)
            .expect("aux node")
            .get_local_status()
            .expect("aux local status")
            .worker_running
    );
    assert_invalid_request_contains(server.start_group(899, 1), "group 899 is not registered");

    let stop_plan = server
        .plan_stop_groups([816, 817])
        .expect("plan stop meta and data groups");
    assert_eq!(
        stop_plan.action,
        matrixraft::MatrixRaftLifecycleAction::Stop
    );
    assert_eq!(stop_plan.group_count, 2);
    assert_eq!(stop_plan.group_ids, vec![816, 817]);
    assert_eq!(stop_plan.node_count, 2);
    assert_eq!(stop_plan.start_index, None);
    assert_eq!(
        stop_plan
            .route_keys
            .iter()
            .map(|key| (key.group_id, key.node_id))
            .collect::<Vec<_>>(),
        vec![(816, 1), (817, 1)]
    );
    assert_eq!(
        stop_plan.route_keys_by_group(),
        vec![
            (816, vec![MatrixRaftRouteKey::new(816, 1)]),
            (817, vec![MatrixRaftRouteKey::new(817, 1)]),
        ]
    );
    assert_eq!(
        stop_plan.node_ids_by_group(),
        vec![(816, vec![1]), (817, vec![1])]
    );
    assert_eq!(
        stop_plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), 1),
            (MatrixRaftRouteKey::new(817, 1), 1),
        ]
    );
    assert_eq!(
        stop_plan.actions_by_group(),
        vec![
            (816, matrixraft::MatrixRaftLifecycleAction::Stop),
            (817, matrixraft::MatrixRaftLifecycleAction::Stop),
        ]
    );
    assert_eq!(
        stop_plan.actions_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(816, 1),
                matrixraft::MatrixRaftLifecycleAction::Stop
            ),
            (
                MatrixRaftRouteKey::new(817, 1),
                matrixraft::MatrixRaftLifecycleAction::Stop
            ),
        ]
    );
    assert_eq!(
        stop_plan.start_indices_by_group(),
        vec![(816, None), (817, None)]
    );
    assert_eq!(
        stop_plan.start_index_presence_by_group(),
        vec![(816, false), (817, false)]
    );
    assert_eq!(
        stop_plan.start_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), None),
            (MatrixRaftRouteKey::new(817, 1), None),
        ]
    );
    assert_eq!(
        stop_plan.start_index_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), false),
            (MatrixRaftRouteKey::new(817, 1), false),
        ]
    );
    assert_eq!(
        server
            .stop_groups([816, 817])
            .expect("stop meta and data groups"),
        vec![(816, 1), (817, 1)]
    );
    let stop_best_effort = server
        .stop_groups_best_effort([816, 817])
        .expect("best-effort stop meta and data groups");
    assert_eq!(
        stop_best_effort
            .iter()
            .map(|group| (
                group.group_id,
                group.action,
                group.node_count,
                group.ok_count
            ))
            .collect::<Vec<_>>(),
        vec![
            (816, matrixraft::MatrixRaftLifecycleAction::Stop, 1, 1),
            (817, matrixraft::MatrixRaftLifecycleAction::Stop, 1, 1),
        ]
    );
    assert!(stop_best_effort.iter().all(|group| group.is_ok()));
    assert_eq!(
        stop_best_effort
            .iter()
            .map(|group| (group.group_id, group.ok_actions_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                816,
                vec![(
                    MatrixRaftRouteKey::new(816, 1),
                    matrixraft::MatrixRaftLifecycleAction::Stop
                )],
            ),
            (
                817,
                vec![(
                    MatrixRaftRouteKey::new(817, 1),
                    matrixraft::MatrixRaftLifecycleAction::Stop
                )],
            ),
        ]
    );

    let restart_plan = server
        .plan_restart_groups([816, 817], true)
        .expect("plan restart meta and data groups");
    assert_eq!(
        restart_plan.action,
        matrixraft::MatrixRaftLifecycleAction::Restart
    );
    assert_eq!(restart_plan.group_count, 2);
    assert_eq!(restart_plan.group_ids, vec![816, 817]);
    assert_eq!(restart_plan.node_count, 2);
    assert_eq!(restart_plan.recover_fsm_from_snapshot, Some(true));
    assert!(restart_plan
        .groups
        .iter()
        .all(|group| group.recover_fsm_from_snapshot == Some(true)));
    assert_eq!(
        restart_plan.actions_by_group(),
        vec![
            (816, matrixraft::MatrixRaftLifecycleAction::Restart),
            (817, matrixraft::MatrixRaftLifecycleAction::Restart),
        ]
    );
    assert_eq!(
        restart_plan.actions_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(816, 1),
                matrixraft::MatrixRaftLifecycleAction::Restart
            ),
            (
                MatrixRaftRouteKey::new(817, 1),
                matrixraft::MatrixRaftLifecycleAction::Restart
            ),
        ]
    );
    assert_eq!(
        restart_plan.recover_fsm_from_snapshot_by_group(),
        vec![(816, Some(true)), (817, Some(true))]
    );
    assert_eq!(
        restart_plan.recover_fsm_from_snapshot_presence_by_group(),
        vec![(816, true), (817, true)]
    );
    assert_eq!(
        restart_plan.recover_fsm_from_snapshot_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), Some(true)),
            (MatrixRaftRouteKey::new(817, 1), Some(true)),
        ]
    );
    assert_eq!(
        restart_plan.recover_fsm_from_snapshot_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), true),
            (MatrixRaftRouteKey::new(817, 1), true),
        ]
    );
    assert_eq!(
        server
            .restart_groups([816, 817], true)
            .expect("restart meta and data groups"),
        vec![(816, 1), (817, 1)]
    );
    assert!(server
        .node(816, 1)
        .expect("meta node")
        .recover_fsm_from_snapshot());
    assert!(server
        .node(817, 1)
        .expect("data node")
        .recover_fsm_from_snapshot());
    assert!(server
        .recover_fsm_from_snapshot_on_node(816, 1)
        .expect("recover flag on meta node"));
    assert_eq!(
        server
            .recover_fsm_from_snapshot_on_group(816)
            .expect("recover flags on meta group"),
        vec![true]
    );
    assert_eq!(
        server
            .recover_fsm_from_snapshot_for_groups([816, 817])
            .expect("recover flags on selected groups"),
        vec![(816, vec![true]), (817, vec![true])]
    );
    assert_eq!(
        server
            .node(816, 1)
            .expect("meta node")
            .get_local_status()
            .expect("meta local status")
            .restart_count,
        1
    );
    assert_eq!(
        server
            .node(817, 1)
            .expect("data node")
            .get_local_status()
            .expect("data local status")
            .restart_count,
        1
    );
    let restart_best_effort = server
        .restart_groups_best_effort([816, 817], false)
        .expect("best-effort restart meta and data groups");
    assert_eq!(
        restart_best_effort
            .iter()
            .map(|group| (
                group.group_id,
                group.action,
                group.node_count,
                group.ok_count
            ))
            .collect::<Vec<_>>(),
        vec![
            (816, matrixraft::MatrixRaftLifecycleAction::Restart, 1, 1),
            (817, matrixraft::MatrixRaftLifecycleAction::Restart, 1, 1),
        ]
    );
    assert!(restart_best_effort.iter().all(|group| group.is_ok()));
    assert_eq!(
        server
            .recover_fsm_from_snapshot_for_groups([816, 817])
            .expect("recover flags after best-effort restart"),
        vec![(816, vec![false]), (817, vec![false])]
    );

    let shutdown_plan = server
        .plan_shutdown_groups([816, 817])
        .expect("plan shutdown meta and data groups");
    assert_eq!(
        shutdown_plan.action,
        matrixraft::MatrixRaftLifecycleAction::Shutdown
    );
    assert_eq!(shutdown_plan.group_count, 2);
    assert_eq!(shutdown_plan.group_ids, vec![816, 817]);
    assert_eq!(shutdown_plan.node_count, 2);
    assert_eq!(shutdown_plan.recover_fsm_from_snapshot, None);
    assert_eq!(
        shutdown_plan.actions_by_group(),
        vec![
            (816, matrixraft::MatrixRaftLifecycleAction::Shutdown),
            (817, matrixraft::MatrixRaftLifecycleAction::Shutdown),
        ]
    );
    assert_eq!(
        shutdown_plan.actions_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(816, 1),
                matrixraft::MatrixRaftLifecycleAction::Shutdown
            ),
            (
                MatrixRaftRouteKey::new(817, 1),
                matrixraft::MatrixRaftLifecycleAction::Shutdown
            ),
        ]
    );
    assert_eq!(
        shutdown_plan.recover_fsm_from_snapshot_by_group(),
        vec![(816, None), (817, None)]
    );
    assert_eq!(
        shutdown_plan.recover_fsm_from_snapshot_presence_by_group(),
        vec![(816, false), (817, false)]
    );
    assert_eq!(
        shutdown_plan.recover_fsm_from_snapshot_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), None),
            (MatrixRaftRouteKey::new(817, 1), None),
        ]
    );
    assert_eq!(
        shutdown_plan.recover_fsm_from_snapshot_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(816, 1), false),
            (MatrixRaftRouteKey::new(817, 1), false),
        ]
    );
    assert_eq!(
        server
            .shutdown_groups([816, 817])
            .expect("shutdown meta and data groups"),
        vec![(816, 1), (817, 1)]
    );
    let shutdown_aux_best_effort = server
        .shutdown_group_best_effort(846)
        .expect("best-effort shutdown aux group");
    assert_eq!(shutdown_aux_best_effort.group_id, 846);
    assert_eq!(
        shutdown_aux_best_effort.action,
        matrixraft::MatrixRaftLifecycleAction::Shutdown
    );
    assert!(shutdown_aux_best_effort.is_ok());
    assert_eq!(
        shutdown_aux_best_effort
            .results
            .iter()
            .map(|result| (result.key, result.action, result.ok))
            .collect::<Vec<_>>(),
        vec![(
            MatrixRaftRouteKey::new(846, 1),
            matrixraft::MatrixRaftLifecycleAction::Shutdown,
            true
        )]
    );
    let shutdown_best_effort = server
        .shutdown_groups_best_effort([816, 817])
        .expect("best-effort shutdown meta and data groups");
    assert_eq!(
        shutdown_best_effort
            .iter()
            .map(|group| (
                group.group_id,
                group.action,
                group.node_count,
                group.ok_count
            ))
            .collect::<Vec<_>>(),
        vec![
            (816, matrixraft::MatrixRaftLifecycleAction::Shutdown, 1, 1),
            (817, matrixraft::MatrixRaftLifecycleAction::Shutdown, 1, 1),
        ]
    );
    assert!(shutdown_best_effort.iter().all(|group| group.is_ok()));
    assert_eq!(
        server.start_index_on_node(816, 99),
        Err(RaftError::NodeNotFound(99))
    );
    assert_eq!(
        server.recover_fsm_from_snapshot_on_node(816, 99),
        Err(RaftError::NodeNotFound(99))
    );
    assert_invalid_request_contains(
        server.plan_start_indices_for_groups([899, 816]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_recover_fsm_from_snapshot_for_groups([899, 816]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(server.stop_groups([899]), "group 899 is not registered");
    assert_invalid_request_contains(
        server.start_groups_best_effort([899], 1),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_shutdown_groups([899]),
        "group 899 is not registered",
    );

    let _ = fs::remove_dir_all(meta_wal_dir);
    let _ = fs::remove_dir_all(meta_snapshot_dir);
    let _ = fs::remove_dir_all(data_wal_dir);
    let _ = fs::remove_dir_all(data_snapshot_dir);
    let _ = fs::remove_dir_all(aux_wal_dir);
    let _ = fs::remove_dir_all(aux_snapshot_dir);
}

#[test]
fn matrixraft_multi_raft_server_unregisters_groups_and_cleans_routes() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("unregister-meta-1-wal");
    let meta_snap_1 = temp_dir("unregister-meta-1-snapshot");
    let meta_wal_2 = temp_dir("unregister-meta-2-wal");
    let meta_snap_2 = temp_dir("unregister-meta-2-snapshot");
    let data_wal = temp_dir("unregister-data-wal");
    let data_snap = temp_dir("unregister-data-snapshot");
    let aux_wal = temp_dir("unregister-aux-wal");
    let aux_snap = temp_dir("unregister-aux-snapshot");
    let best_effort_meta_wal = temp_dir("unregister-best-effort-meta-wal");
    let best_effort_meta_snap = temp_dir("unregister-best-effort-meta-snapshot");
    let best_effort_data_wal = temp_dir("unregister-best-effort-data-wal");
    let best_effort_data_snap = temp_dir("unregister-best-effort-data-snapshot");
    let best_effort_aux_wal = temp_dir("unregister-best-effort-aux-wal");
    let best_effort_aux_snap = temp_dir("unregister-best-effort-aux-snapshot");
    server
        .create_node(options_for_peer(818, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(818, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(819, &data_wal, &data_snap), 1)
        .expect("data node");
    server
        .create_node(options(852, &aux_wal, &aux_snap), 1)
        .expect("aux node");

    let snapshot_meta = SnapshotMetadata {
        snapshot_id: "snap-818".to_string(),
        last_log_id: LogId { term: 1, index: 1 },
        membership: vec![1, 2, 3],
        members: vec![peer(818, 1), peer(818, 2), peer(818, 3)],
    };
    let snapshot_desc = MatrixRaftSnapshotDesc::from_snapshot_meta(&snapshot_meta);
    server
        .publish_snapshot_route(818, 1, snapshot_desc)
        .expect("snapshot route");
    server
        .publish_snapshot_route(
            819,
            1,
            MatrixRaftSnapshotDesc::from_snapshot_meta(&SnapshotMetadata {
                snapshot_id: "snap-819".to_string(),
                last_log_id: LogId { term: 1, index: 1 },
                membership: vec![1, 2, 3],
                members: vec![peer(819, 1), peer(819, 2), peer(819, 3)],
            }),
        )
        .expect("data snapshot route");
    assert_eq!(server.node_count(), 4);
    assert_eq!(server.group_count(), 3);
    assert_eq!(server.runtime_wiring_count(), 4);
    assert_eq!(server.snapshot_route_count(), 2);

    let meta_group_snapshot = MatrixRaftSnapshotDesc::from_snapshot_meta(&SnapshotMetadata {
        snapshot_id: "snap-818-group".to_string(),
        last_log_id: LogId { term: 1, index: 2 },
        membership: vec![1, 2, 3],
        members: vec![peer(818, 1), peer(818, 2), peer(818, 3)],
    });
    let meta_group_publish_plan = server
        .plan_publish_snapshot_route_on_group(818, meta_group_snapshot.clone())
        .expect("plan publish meta group snapshot routes");
    assert_eq!(meta_group_publish_plan.group_id, 818);
    assert_eq!(meta_group_publish_plan.node_ids, vec![1, 2]);
    assert_eq!(
        meta_group_publish_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(818, 1),
            MatrixRaftRouteKey::new(818, 2)
        ]
    );
    assert_eq!(meta_group_publish_plan.node_count, 2);
    assert_eq!(meta_group_publish_plan.existing_route_count, 1);
    assert_eq!(meta_group_publish_plan.snapshot, meta_group_snapshot);
    let meta_group_routes = server
        .publish_snapshot_route_on_group(818, meta_group_snapshot.clone())
        .expect("publish meta group snapshot routes");
    assert_eq!(meta_group_routes.len(), 2);
    assert!(meta_group_routes.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::SnapshotRegistered
            && result.snapshot.as_ref() == Some(&meta_group_snapshot)
    }));
    let meta_group_routes_best_effort = server
        .publish_snapshot_route_on_group_best_effort(818, meta_group_snapshot.clone())
        .expect("best-effort publish meta group snapshot routes");
    assert_eq!(meta_group_routes_best_effort.len(), 2);
    assert!(meta_group_routes_best_effort.iter().all(|result| {
        result.result.as_ref().is_some_and(|route| {
            route.kind == MatrixRaftRouteResultKind::SnapshotRegistered
                && route.snapshot.as_ref() == Some(&meta_group_snapshot)
        })
    }));
    assert_eq!(server.snapshot_route_count(), 3);
    assert_eq!(server.snapshot_route(818, 1), Some(&meta_group_snapshot));
    assert_eq!(server.snapshot_route(818, 2), Some(&meta_group_snapshot));

    let selected_meta_snapshot = MatrixRaftSnapshotDesc::from_snapshot_meta(&SnapshotMetadata {
        snapshot_id: "snap-818-selected".to_string(),
        last_log_id: LogId { term: 1, index: 3 },
        membership: vec![1, 2, 3],
        members: vec![peer(818, 1), peer(818, 2), peer(818, 3)],
    });
    let selected_data_snapshot = MatrixRaftSnapshotDesc::from_snapshot_meta(&SnapshotMetadata {
        snapshot_id: "snap-819-selected".to_string(),
        last_log_id: LogId { term: 1, index: 3 },
        membership: vec![1, 2, 3],
        members: vec![peer(819, 1), peer(819, 2), peer(819, 3)],
    });
    let selected_routes = server
        .publish_snapshot_routes_for_groups([
            (818, selected_meta_snapshot.clone()),
            (819, selected_data_snapshot.clone()),
        ])
        .expect("publish selected snapshot routes");
    let selected_publish_plan = server
        .plan_publish_snapshot_routes_for_groups([
            (818, selected_meta_snapshot.clone()),
            (819, selected_data_snapshot.clone()),
        ])
        .expect("plan publish selected snapshot routes");
    assert_eq!(selected_publish_plan.group_count, 2);
    assert_eq!(selected_publish_plan.group_ids, vec![818, 819]);
    assert_eq!(selected_publish_plan.node_count, 3);
    assert_eq!(selected_publish_plan.existing_route_count, 3);
    assert_eq!(
        selected_publish_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(818, 1),
            MatrixRaftRouteKey::new(818, 2),
            MatrixRaftRouteKey::new(819, 1),
        ]
    );
    assert_eq!(
        selected_publish_plan
            .groups
            .iter()
            .map(|group| (
                group.group_id,
                group.node_ids.clone(),
                group.existing_route_count,
                group.snapshot.index
            ))
            .collect::<Vec<_>>(),
        vec![(818, vec![1, 2], 2, 3), (819, vec![1], 1, 3)]
    );
    assert_eq!(
        selected_publish_plan.existing_route_counts_by_group(),
        vec![(818, 2), (819, 1)]
    );
    assert_eq!(
        selected_publish_plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), 1),
            (MatrixRaftRouteKey::new(818, 2), 2),
            (MatrixRaftRouteKey::new(819, 1), 1),
        ]
    );
    assert_eq!(
        selected_publish_plan.existing_route_keys_by_group(),
        vec![
            (
                818,
                vec![
                    MatrixRaftRouteKey::new(818, 1),
                    MatrixRaftRouteKey::new(818, 2),
                ],
            ),
            (819, vec![MatrixRaftRouteKey::new(819, 1)]),
        ]
    );
    assert_eq!(
        selected_publish_plan.existing_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), true),
            (MatrixRaftRouteKey::new(818, 2), true),
            (MatrixRaftRouteKey::new(819, 1), true),
        ]
    );
    assert_eq!(
        selected_publish_plan.snapshots_by_group(),
        vec![
            (818, selected_meta_snapshot.clone()),
            (819, selected_data_snapshot.clone()),
        ]
    );
    assert_eq!(
        selected_publish_plan.snapshots_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(818, 1),
                selected_meta_snapshot.clone()
            ),
            (
                MatrixRaftRouteKey::new(818, 2),
                selected_meta_snapshot.clone()
            ),
            (
                MatrixRaftRouteKey::new(819, 1),
                selected_data_snapshot.clone()
            ),
        ]
    );
    assert_eq!(
        selected_publish_plan.snapshot_ids_by_group(),
        vec![
            (818, Some("snap-818-selected".to_string())),
            (819, Some("snap-819-selected".to_string())),
        ]
    );
    assert_eq!(
        selected_publish_plan.snapshot_ids_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(818, 1),
                Some("snap-818-selected".to_string())
            ),
            (
                MatrixRaftRouteKey::new(818, 2),
                Some("snap-818-selected".to_string())
            ),
            (
                MatrixRaftRouteKey::new(819, 1),
                Some("snap-819-selected".to_string())
            ),
        ]
    );
    assert_eq!(
        selected_publish_plan.snapshot_indices_by_group(),
        vec![(818, 3), (819, 3)]
    );
    assert_eq!(
        selected_publish_plan.snapshot_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), 3),
            (MatrixRaftRouteKey::new(818, 2), 3),
            (MatrixRaftRouteKey::new(819, 1), 3),
        ]
    );
    assert_eq!(
        selected_publish_plan.snapshot_terms_by_group(),
        vec![(818, 1), (819, 1)]
    );
    assert_eq!(
        selected_publish_plan.snapshot_terms_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), 1),
            (MatrixRaftRouteKey::new(818, 2), 1),
            (MatrixRaftRouteKey::new(819, 1), 1),
        ]
    );
    assert_eq!(
        selected_publish_plan.snapshot_member_counts_by_group(),
        vec![(818, 3), (819, 3)]
    );
    assert_eq!(
        selected_publish_plan.snapshot_member_counts_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), 3),
            (MatrixRaftRouteKey::new(818, 2), 3),
            (MatrixRaftRouteKey::new(819, 1), 3),
        ]
    );
    assert_eq!(
        selected_routes
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(818, 2), (819, 1)]
    );
    assert_eq!(server.snapshot_route_count(), 3);
    assert_eq!(server.snapshot_route(818, 1), Some(&selected_meta_snapshot));
    assert_eq!(server.snapshot_route(818, 2), Some(&selected_meta_snapshot));
    assert_eq!(server.snapshot_route(819, 1), Some(&selected_data_snapshot));
    let selected_routes_best_effort = server
        .publish_snapshot_routes_for_groups_best_effort([
            (818, selected_meta_snapshot.clone()),
            (819, selected_data_snapshot.clone()),
        ])
        .expect("best-effort publish selected snapshot routes");
    assert_eq!(
        selected_routes_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(818, 2), (819, 1)]
    );
    assert!(selected_routes_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::SnapshotRegistered
                    && route.snapshot.is_some()
            })
        })
    }));
    let meta_snapshot_routes = server
        .snapshot_routes_on_group(818)
        .expect("snapshot routes on meta group");
    assert_eq!(
        meta_snapshot_routes
            .iter()
            .map(|(key, route)| (*key, route.clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                MatrixRaftRouteKey::new(818, 1),
                Some(selected_meta_snapshot.clone())
            ),
            (
                MatrixRaftRouteKey::new(818, 2),
                Some(selected_meta_snapshot.clone())
            ),
        ]
    );
    let selected_snapshot_routes = server
        .snapshot_routes_for_groups([818, 819])
        .expect("snapshot routes on selected groups");
    assert_eq!(
        selected_snapshot_routes
            .iter()
            .map(|(group_id, routes)| (*group_id, routes.len()))
            .collect::<Vec<_>>(),
        vec![(818, 2), (819, 1)]
    );
    assert!(selected_snapshot_routes
        .iter()
        .all(|(_, routes)| { routes.iter().all(|(_, route)| route.is_some()) }));
    let selected_snapshot_route_plan = server
        .plan_snapshot_routes_for_groups([818, 819])
        .expect("plan selected snapshot route reads");
    assert_eq!(selected_snapshot_route_plan.operation, "snapshot_routes");
    assert_eq!(selected_snapshot_route_plan.group_count, 2);
    assert_eq!(selected_snapshot_route_plan.node_count, 3);
    assert_eq!(
        selected_snapshot_route_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(818, 1),
            MatrixRaftRouteKey::new(818, 2),
            MatrixRaftRouteKey::new(819, 1),
        ]
    );
    assert_eq!(
        selected_snapshot_route_plan.operations_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(818, 1),
                "snapshot_routes".to_string(),
            ),
            (
                MatrixRaftRouteKey::new(818, 2),
                "snapshot_routes".to_string(),
            ),
            (
                MatrixRaftRouteKey::new(819, 1),
                "snapshot_routes".to_string(),
            ),
        ]
    );
    assert_eq!(
        selected_snapshot_route_plan.operation_argument_counts_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), 0),
            (MatrixRaftRouteKey::new(818, 2), 0),
            (MatrixRaftRouteKey::new(819, 1), 0),
        ]
    );

    let finish_meta_plan = server
        .plan_finish_snapshot_route_on_group(818, MatrixRaftOldSnapshotFinish::received(3))
        .expect("plan finish meta group snapshot routes");
    assert_eq!(finish_meta_plan.group_id, 818);
    assert_eq!(finish_meta_plan.node_ids, vec![1, 2]);
    assert_eq!(finish_meta_plan.node_count, 2);
    assert_eq!(finish_meta_plan.active_route_count, 2);
    assert_eq!(
        finish_meta_plan.finish,
        MatrixRaftOldSnapshotFinish::received(3)
    );
    let finished_meta = server
        .finish_snapshot_route_on_group(818, MatrixRaftOldSnapshotFinish::received(3))
        .expect("finish meta group snapshot routes");
    assert_eq!(finished_meta.len(), 2);
    assert!(finished_meta.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::SnapshotFinished && result.snapshot.is_some()
    }));
    assert_eq!(server.snapshot_route_count(), 1);
    assert!(server.snapshot_route(818, 1).is_none());
    assert!(server.snapshot_route(818, 2).is_none());

    server
        .publish_snapshot_route_on_group(818, selected_meta_snapshot.clone())
        .expect("republish meta snapshot routes before best-effort finish");
    let finished_meta_best_effort = server
        .finish_snapshot_route_on_group_best_effort(818, MatrixRaftOldSnapshotFinish::received(3))
        .expect("best-effort finish meta group snapshot routes");
    assert_eq!(finished_meta_best_effort.len(), 2);
    assert!(finished_meta_best_effort.iter().all(|result| {
        result.result.as_ref().is_some_and(|route| {
            route.kind == MatrixRaftRouteResultKind::SnapshotFinished && route.snapshot.is_some()
        })
    }));
    assert_eq!(server.snapshot_route_count(), 1);

    server
        .publish_snapshot_routes_for_groups([
            (818, selected_meta_snapshot.clone()),
            (819, selected_data_snapshot.clone()),
        ])
        .expect("republish selected snapshot routes");
    let finish_selected_plan = server
        .plan_finish_snapshot_routes_for_groups(
            [818, 819],
            MatrixRaftOldSnapshotFinish::received(3),
        )
        .expect("plan finish selected snapshot routes");
    assert_eq!(finish_selected_plan.group_count, 2);
    assert_eq!(finish_selected_plan.group_ids, vec![818, 819]);
    assert_eq!(finish_selected_plan.node_count, 3);
    assert_eq!(finish_selected_plan.active_route_count, 3);
    assert_eq!(
        finish_selected_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(818, 1),
            MatrixRaftRouteKey::new(818, 2),
            MatrixRaftRouteKey::new(819, 1),
        ]
    );
    assert_eq!(
        finish_selected_plan
            .groups
            .iter()
            .map(|group| (
                group.group_id,
                group.node_ids.clone(),
                group.active_route_count
            ))
            .collect::<Vec<_>>(),
        vec![(818, vec![1, 2], 2), (819, vec![1], 1)]
    );
    assert_eq!(
        finish_selected_plan.active_route_counts_by_group(),
        vec![(818, 2), (819, 1)]
    );
    assert_eq!(
        finish_selected_plan.active_route_keys_by_group(),
        vec![
            (
                818,
                vec![
                    MatrixRaftRouteKey::new(818, 1),
                    MatrixRaftRouteKey::new(818, 2),
                ],
            ),
            (819, vec![MatrixRaftRouteKey::new(819, 1)]),
        ]
    );
    assert_eq!(
        finish_selected_plan.active_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), true),
            (MatrixRaftRouteKey::new(818, 2), true),
            (MatrixRaftRouteKey::new(819, 1), true),
        ]
    );
    assert_eq!(
        finish_selected_plan.finishes_by_group(),
        vec![
            (818, MatrixRaftOldSnapshotFinish::received(3)),
            (819, MatrixRaftOldSnapshotFinish::received(3)),
        ]
    );
    assert_eq!(
        finish_selected_plan.finishes_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(818, 1),
                MatrixRaftOldSnapshotFinish::received(3),
            ),
            (
                MatrixRaftRouteKey::new(818, 2),
                MatrixRaftOldSnapshotFinish::received(3),
            ),
            (
                MatrixRaftRouteKey::new(819, 1),
                MatrixRaftOldSnapshotFinish::received(3),
            ),
        ]
    );
    assert_eq!(
        finish_selected_plan.finish_states_by_group(),
        vec![
            (818, MatrixRaftOldSnapshotFinishState::Received),
            (819, MatrixRaftOldSnapshotFinishState::Received),
        ]
    );
    assert_eq!(
        finish_selected_plan.finish_states_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(818, 1),
                MatrixRaftOldSnapshotFinishState::Received,
            ),
            (
                MatrixRaftRouteKey::new(818, 2),
                MatrixRaftOldSnapshotFinishState::Received,
            ),
            (
                MatrixRaftRouteKey::new(819, 1),
                MatrixRaftOldSnapshotFinishState::Received,
            ),
        ]
    );
    assert_eq!(
        finish_selected_plan.snapshot_indices_by_group(),
        vec![(818, 3), (819, 3)]
    );
    assert_eq!(
        finish_selected_plan.snapshot_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), 3),
            (MatrixRaftRouteKey::new(818, 2), 3),
            (MatrixRaftRouteKey::new(819, 1), 3),
        ]
    );
    let unregister_meta_plan = server
        .plan_unregister_group(818)
        .expect("plan unregister meta group");
    assert_eq!(unregister_meta_plan.group_id, 818);
    assert_eq!(unregister_meta_plan.node_ids, vec![1, 2]);
    assert_eq!(unregister_meta_plan.node_count, 2);
    assert_eq!(unregister_meta_plan.runtime_wiring_count, 2);
    assert_eq!(unregister_meta_plan.snapshot_route_count, 2);
    let unregister_plan = server
        .plan_unregister_groups([818, 819])
        .expect("plan unregister meta and data groups");
    assert_eq!(unregister_plan.group_count, 2);
    assert_eq!(unregister_plan.group_ids, vec![818, 819]);
    assert_eq!(unregister_plan.node_count, 3);
    assert_eq!(unregister_plan.runtime_wiring_count, 3);
    assert_eq!(unregister_plan.snapshot_route_count, 3);
    assert_eq!(
        unregister_plan
            .route_keys
            .iter()
            .map(|key| (key.group_id, key.node_id))
            .collect::<Vec<_>>(),
        vec![(818, 1), (818, 2), (819, 1)]
    );
    assert_eq!(
        unregister_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect::<Vec<_>>(),
        vec![(818, vec![1, 2]), (819, vec![1])]
    );
    assert_eq!(
        unregister_plan.route_keys_by_group(),
        vec![
            (
                818,
                vec![
                    MatrixRaftRouteKey::new(818, 1),
                    MatrixRaftRouteKey::new(818, 2),
                ],
            ),
            (819, vec![MatrixRaftRouteKey::new(819, 1)]),
        ]
    );
    assert_eq!(
        unregister_plan.node_ids_by_group(),
        vec![(818, vec![1, 2]), (819, vec![1])]
    );
    assert_eq!(
        unregister_plan.counts_by_group(),
        vec![(818, 2, 2, 2), (819, 1, 1, 1)]
    );
    assert_eq!(
        unregister_plan.route_key_counts_by_group(),
        vec![(818, 2), (819, 1)]
    );
    assert_eq!(
        unregister_plan.unregister_counts_by_group(),
        vec![(818, 2, 2, 2, 2), (819, 1, 1, 1, 1)]
    );
    assert_eq!(
        unregister_plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), 1),
            (MatrixRaftRouteKey::new(818, 2), 2),
            (MatrixRaftRouteKey::new(819, 1), 1),
        ]
    );
    assert_eq!(
        unregister_plan.runtime_wiring_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), true),
            (MatrixRaftRouteKey::new(818, 2), true),
            (MatrixRaftRouteKey::new(819, 1), true),
        ]
    );
    assert_eq!(
        unregister_plan.snapshot_route_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(818, 1), true),
            (MatrixRaftRouteKey::new(818, 2), true),
            (MatrixRaftRouteKey::new(819, 1), true),
        ]
    );
    let finished_selected = server
        .finish_snapshot_routes_for_groups([818, 819], MatrixRaftOldSnapshotFinish::received(3))
        .expect("finish selected snapshot routes");
    assert_eq!(
        finished_selected
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(818, 2), (819, 1)]
    );
    assert_eq!(server.snapshot_route_count(), 0);
    server
        .publish_snapshot_routes_for_groups([
            (818, selected_meta_snapshot.clone()),
            (819, selected_data_snapshot.clone()),
        ])
        .expect("republish selected snapshot routes before best-effort finish");
    let finished_selected_best_effort = server
        .finish_snapshot_routes_for_groups_best_effort(
            [818, 819],
            MatrixRaftOldSnapshotFinish::received(3),
        )
        .expect("best-effort finish selected snapshot routes");
    assert_eq!(
        finished_selected_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(818, 2), (819, 1)]
    );
    assert!(finished_selected_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.kind == MatrixRaftRouteResultKind::SnapshotFinished)
        })
    }));
    assert_eq!(server.snapshot_route_count(), 0);

    assert_invalid_request_contains(
        server.plan_unregister_groups([818, 899]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_publish_snapshot_routes_for_groups([
            (818, selected_meta_snapshot),
            (899, selected_data_snapshot),
        ]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_snapshot_routes_for_groups([818, 899]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_finish_snapshot_routes_for_groups(
            [818, 899],
            MatrixRaftOldSnapshotFinish::received(3),
        ),
        "group 899 is not registered",
    );
    assert!(server.has_node(818, 1));
    assert!(server.has_node(819, 1));
    assert_eq!(server.snapshot_route_count(), 0);
    assert_invalid_request_contains(
        server.plan_unregister_groups([818, 818]),
        "group 818 appears more than once in unregister batch",
    );
    assert!(server.has_node(818, 1));
    assert!(server.has_node(818, 2));

    let removed_groups = server
        .unregister_groups([818, 819])
        .expect("unregister meta and data groups");
    assert_eq!(removed_groups.len(), 2);
    assert_eq!(removed_groups[0].0, 818);
    assert_eq!(
        removed_groups[0]
            .1
            .iter()
            .map(|node| node.node_id())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(removed_groups[1].0, 819);
    assert_eq!(removed_groups[1].1[0].node_id(), 1);
    assert!(!server.has_node(818, 1));
    assert!(!server.has_node(818, 2));
    assert!(!server.has_node(819, 1));
    assert!(server.has_node(852, 1));
    assert_eq!(server.node_count(), 1);
    assert_eq!(server.group_ids(), vec![852]);
    assert_eq!(server.runtime_wiring_count(), 1);
    assert_eq!(server.snapshot_route_count(), 0);
    assert!(server.runtime_wiring(818, 1).is_none());
    assert!(server.snapshot_route(818, 1).is_none());
    assert!(server.snapshot_route(819, 1).is_none());

    let removed = server.unregister_group(852).expect("unregister aux group");
    let removed_ids: Vec<_> = removed.iter().map(|node| node.node_id()).collect();
    assert_eq!(removed_ids, vec![1]);
    assert_eq!(server.node_count(), 0);
    assert_eq!(server.snapshot_route_count(), 0);
    assert_invalid_request_contains(server.unregister_group(818), "group 818 is not registered");

    server
        .create_node(
            options(853, &best_effort_meta_wal, &best_effort_meta_snap),
            1,
        )
        .expect("best-effort meta unregister node");
    server
        .create_node(
            options(854, &best_effort_data_wal, &best_effort_data_snap),
            1,
        )
        .expect("best-effort data unregister node");
    server
        .create_node(options(855, &best_effort_aux_wal, &best_effort_aux_snap), 1)
        .expect("best-effort aux unregister node");
    server
        .publish_snapshot_route(
            853,
            1,
            MatrixRaftSnapshotDesc::from_snapshot_meta(&SnapshotMetadata {
                snapshot_id: "snap-853".to_string(),
                last_log_id: LogId { term: 1, index: 1 },
                membership: vec![1],
                members: vec![peer(853, 1)],
            }),
        )
        .expect("best-effort meta snapshot route");
    server
        .publish_snapshot_route(
            854,
            1,
            MatrixRaftSnapshotDesc::from_snapshot_meta(&SnapshotMetadata {
                snapshot_id: "snap-854".to_string(),
                last_log_id: LogId { term: 1, index: 1 },
                membership: vec![1],
                members: vec![peer(854, 1)],
            }),
        )
        .expect("best-effort data snapshot route");
    let best_effort_removed = server.unregister_groups_best_effort([853, 899, 853, 854]);
    assert_eq!(
        best_effort_removed
            .iter()
            .map(|group| (
                group.group_id,
                group.ok,
                group.node_count,
                group.runtime_wiring_count,
                group.snapshot_route_count
            ))
            .collect::<Vec<_>>(),
        vec![
            (853, true, 1, 1, 1),
            (899, false, 0, 0, 0),
            (853, false, 0, 0, 0),
            (854, true, 1, 1, 1),
        ]
    );
    assert!(best_effort_removed[0].is_ok());
    assert_eq!(
        best_effort_removed[0].removed_route_keys,
        vec![MatrixRaftRouteKey::new(853, 1)]
    );
    assert_eq!(
        best_effort_removed[0].route_keys(),
        vec![MatrixRaftRouteKey::new(853, 1)]
    );
    assert_eq!(best_effort_removed[0].node_ids(), vec![1]);
    assert_eq!(best_effort_removed[0].removal_counts(), (1, 1, 1, 1));
    assert_eq!(
        best_effort_removed[0].removed_node_ids_by_route_key(),
        vec![(MatrixRaftRouteKey::new(853, 1), 1)]
    );
    assert_eq!(
        best_effort_removed[0].runtime_wiring_removed_by_route_key(),
        vec![(MatrixRaftRouteKey::new(853, 1), true)]
    );
    assert_eq!(
        best_effort_removed[0].snapshot_routes_removed_by_route_key(),
        vec![(MatrixRaftRouteKey::new(853, 1), true)]
    );
    assert_eq!(best_effort_removed[0].result_by_group_id().0, 853);
    assert_eq!(
        best_effort_removed[0]
            .ok_result_by_group_id()
            .map(|(group_id, result)| (group_id, result.node_count, result.snapshot_route_count)),
        Some((853, 1, 1))
    );
    assert!(best_effort_removed[0].error_result_by_group_id().is_none());
    assert_eq!(
        best_effort_removed[0]
            .results_by_route_key()
            .iter()
            .map(|(key, result)| (
                *key,
                result.group_id,
                result.is_ok(),
                result.removed_node_ids.clone()
            ))
            .collect::<Vec<_>>(),
        vec![(MatrixRaftRouteKey::new(853, 1), 853, true, vec![1])]
    );
    assert_eq!(
        best_effort_removed[0]
            .ok_results_by_route_key()
            .iter()
            .map(|(key, result)| (
                *key,
                result.runtime_wiring_count,
                result.snapshot_route_count
            ))
            .collect::<Vec<_>>(),
        vec![(MatrixRaftRouteKey::new(853, 1), 1, 1)]
    );
    assert!(best_effort_removed[0]
        .error_results_by_route_key()
        .is_empty());
    assert!(best_effort_removed[1].route_keys().is_empty());
    assert!(best_effort_removed[1].node_ids().is_empty());
    assert_eq!(best_effort_removed[1].removal_counts(), (0, 0, 0, 0));
    assert!(best_effort_removed[1].ok_result_by_group_id().is_none());
    assert_eq!(
        best_effort_removed[1]
            .error_result_by_group_id()
            .map(|(group_id, result)| (group_id, result.is_ok(), result.error.is_some())),
        Some((899, false, true))
    );
    assert!(best_effort_removed[1].results_by_route_key().is_empty());
    assert!(best_effort_removed[1]
        .error
        .as_ref()
        .is_some_and(|error| error.contains("group 899 is not registered")));
    assert!(best_effort_removed[2]
        .error
        .as_ref()
        .is_some_and(|error| error.contains("appears more than once in unregister batch")));
    assert_eq!(
        best_effort_removed[2]
            .error_result_by_group_id()
            .map(|(group_id, result)| (group_id, result.is_ok(), result.error.is_some())),
        Some((853, false, true))
    );
    assert!(best_effort_removed[3].ok_result_by_group_id().is_some());
    assert_eq!(
        best_effort_removed[3]
            .results_by_route_key()
            .iter()
            .map(|(key, result)| (*key, result.group_id, result.is_ok()))
            .collect::<Vec<_>>(),
        vec![(MatrixRaftRouteKey::new(854, 1), 854, true)]
    );
    assert!(!server.has_node(853, 1));
    assert!(!server.has_node(854, 1));
    assert!(server.has_node(855, 1));
    assert_eq!(server.runtime_wiring_count(), 1);
    assert_eq!(server.snapshot_route_count(), 0);

    let aux_best_effort = server.unregister_group_best_effort(855);
    assert!(aux_best_effort.is_ok());
    assert_eq!(aux_best_effort.removed_node_ids, vec![1]);
    assert_eq!(aux_best_effort.node_ids(), vec![1]);
    assert_eq!(
        aux_best_effort.route_keys(),
        vec![MatrixRaftRouteKey::new(855, 1)]
    );
    assert_eq!(server.node_count(), 0);
    let missing_best_effort = server.unregister_group_best_effort(855);
    assert!(!missing_best_effort.ok);
    assert!(missing_best_effort
        .error
        .as_ref()
        .is_some_and(|error| error.contains("group 855 is not registered")));

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
    let _ = fs::remove_dir_all(aux_wal);
    let _ = fs::remove_dir_all(aux_snap);
    let _ = fs::remove_dir_all(best_effort_meta_wal);
    let _ = fs::remove_dir_all(best_effort_meta_snap);
    let _ = fs::remove_dir_all(best_effort_data_wal);
    let _ = fs::remove_dir_all(best_effort_data_snap);
    let _ = fs::remove_dir_all(best_effort_aux_wal);
    let _ = fs::remove_dir_all(best_effort_aux_snap);
}

#[test]
fn matrixraft_multi_raft_server_broadcasts_messages_to_group_nodes() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let group_wal_1 = temp_dir("broadcast-group-1-wal");
    let group_snap_1 = temp_dir("broadcast-group-1-snapshot");
    let group_wal_2 = temp_dir("broadcast-group-2-wal");
    let group_snap_2 = temp_dir("broadcast-group-2-snapshot");
    let data_wal = temp_dir("broadcast-data-wal");
    let data_snap = temp_dir("broadcast-data-snapshot");
    server
        .create_node(options_for_peer(820, 1, &group_wal_1, &group_snap_1), 1)
        .expect("group node 1");
    server
        .create_node(options_for_peer(820, 2, &group_wal_2, &group_snap_2), 1)
        .expect("group node 2");
    server
        .create_node(options(821, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start broadcast server");

    let broadcast_results = server
        .route_message_to_group(
            820,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::set_node_healthy(3, false)),
        )
        .expect("group broadcast route");
    assert_eq!(broadcast_results.len(), 2);
    assert_eq!(broadcast_results[0].key.group_id, 820);
    assert_eq!(broadcast_results[0].key.node_id, 1);
    assert_eq!(broadcast_results[1].key.group_id, 820);
    assert_eq!(broadcast_results[1].key.node_id, 2);
    assert!(broadcast_results.iter().all(|result| result.handled));
    assert!(broadcast_results
        .iter()
        .all(|result| result.node_healthy == Some(false)));

    let best_effort_results = server
        .route_message_to_group_best_effort(
            820,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        )
        .expect("group best-effort broadcast");
    assert_eq!(best_effort_results.len(), 2);
    assert!(best_effort_results.iter().all(|result| result.is_ok()));
    assert!(best_effort_results
        .iter()
        .all(|result| result.group_id == 820));

    assert_invalid_request_contains(
        server.route_message_to_group(
            899,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        ),
        "group 899 is not registered",
    );
    assert_eq!(server.group_ids(), vec![820, 821]);

    server.shutdown_all().expect("shutdown broadcast server");

    let _ = fs::remove_dir_all(group_wal_1);
    let _ = fs::remove_dir_all(group_snap_1);
    let _ = fs::remove_dir_all(group_wal_2);
    let _ = fs::remove_dir_all(group_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_broadcasts_messages_to_meta_and_data_groups() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("cross-plane-meta-1-wal");
    let meta_snap_1 = temp_dir("cross-plane-meta-1-snapshot");
    let meta_wal_2 = temp_dir("cross-plane-meta-2-wal");
    let meta_snap_2 = temp_dir("cross-plane-meta-2-snapshot");
    let data_wal = temp_dir("cross-plane-data-wal");
    let data_snap = temp_dir("cross-plane-data-snapshot");
    server
        .create_node(options_for_peer(846, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(846, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(847, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start cross-plane server");

    let admin_plan = server
        .plan_route_admin_command_to_groups(
            [846, 847],
            MatrixRaftAdminCommand::set_node_healthy(3, false),
        )
        .expect("plan admin route to meta and data groups");
    assert_eq!(admin_plan.group_count, 2);
    assert_eq!(admin_plan.group_ids, vec![846, 847]);
    assert_eq!(admin_plan.node_count, 3);
    assert_eq!(
        admin_plan.command_type,
        MatrixRaftAdminCommandType::SetNodeHealthy
    );
    assert_eq!(
        admin_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(846, 1),
            MatrixRaftRouteKey::new(846, 2),
            MatrixRaftRouteKey::new(847, 1),
        ]
    );
    assert_eq!(
        admin_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone(), group.command_type))
            .collect::<Vec<_>>(),
        vec![
            (846, vec![1, 2], MatrixRaftAdminCommandType::SetNodeHealthy),
            (847, vec![1], MatrixRaftAdminCommandType::SetNodeHealthy),
        ]
    );
    assert_eq!(
        admin_plan.route_keys_by_group(),
        vec![
            (
                846,
                vec![
                    MatrixRaftRouteKey::new(846, 1),
                    MatrixRaftRouteKey::new(846, 2),
                ],
            ),
            (847, vec![MatrixRaftRouteKey::new(847, 1)]),
        ]
    );
    assert_eq!(
        admin_plan.node_ids_by_group(),
        vec![(846, vec![1, 2]), (847, vec![1])]
    );
    assert_eq!(admin_plan.node_counts_by_group(), vec![(846, 2), (847, 1)]);
    assert_eq!(
        admin_plan.command_types_by_group(),
        vec![
            (846, MatrixRaftAdminCommandType::SetNodeHealthy),
            (847, MatrixRaftAdminCommandType::SetNodeHealthy),
        ]
    );
    assert_eq!(
        admin_plan.commands_by_group(),
        vec![
            (846, MatrixRaftAdminCommand::set_node_healthy(3, false)),
            (847, MatrixRaftAdminCommand::set_node_healthy(3, false)),
        ]
    );
    assert_eq!(
        admin_plan.commands_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftAdminCommand::set_node_healthy(3, false),
            ),
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftAdminCommand::set_node_healthy(3, false),
            ),
            (
                MatrixRaftRouteKey::new(847, 1),
                MatrixRaftAdminCommand::set_node_healthy(3, false),
            ),
        ]
    );
    assert_eq!(
        admin_plan.command_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftAdminCommandType::SetNodeHealthy,
            ),
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftAdminCommandType::SetNodeHealthy,
            ),
            (
                MatrixRaftRouteKey::new(847, 1),
                MatrixRaftAdminCommandType::SetNodeHealthy,
            ),
        ]
    );
    assert_eq!(
        admin_plan.command_node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), Some(3)),
            (MatrixRaftRouteKey::new(846, 2), Some(3)),
            (MatrixRaftRouteKey::new(847, 1), Some(3)),
        ]
    );
    assert_eq!(
        admin_plan.request_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        admin_plan.request_id_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        admin_plan.snapshot_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        admin_plan.snapshot_id_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        admin_plan.snapshot_peer_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        admin_plan.snapshot_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        admin_plan.snapshot_index_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        admin_plan.transferee_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        admin_plan.forced_campaigns_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        admin_plan.node_healthy_values_by_group(),
        vec![(846, Some(false)), (847, Some(false))]
    );
    assert_eq!(
        admin_plan.node_healthy_presence_by_group(),
        vec![(846, true), (847, true)]
    );
    assert_eq!(
        admin_plan.node_healthy_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), Some(false)),
            (MatrixRaftRouteKey::new(846, 2), Some(false)),
            (MatrixRaftRouteKey::new(847, 1), Some(false)),
        ]
    );
    assert_eq!(
        admin_plan.node_healthy_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), true),
            (MatrixRaftRouteKey::new(846, 2), true),
            (MatrixRaftRouteKey::new(847, 1), true),
        ]
    );
    assert_eq!(
        admin_plan.lease_valid_values_by_group(),
        vec![(846, None), (847, None)]
    );
    assert_eq!(
        admin_plan.lease_valid_presence_by_group(),
        vec![(846, false), (847, false)]
    );
    assert_eq!(
        admin_plan.lease_valid_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        admin_plan.lease_valid_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        admin_plan.log_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        admin_plan.log_index_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        admin_plan.storage_fence_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        admin_plan.storage_fences_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    let admin_results = server
        .route_admin_command_to_groups(
            [846, 847],
            MatrixRaftAdminCommand::set_node_healthy(3, false),
        )
        .expect("route admin to meta and data groups");
    assert_eq!(
        admin_results
            .iter()
            .map(|result| (result.key.group_id, result.key.node_id))
            .collect::<Vec<_>>(),
        vec![(846, 1), (846, 2), (847, 1)]
    );
    assert!(admin_results.iter().all(|result| result.handled));
    assert!(admin_results
        .iter()
        .all(|result| result.node_healthy == Some(false)));

    let grouped_admin_results = server
        .route_admin_command_to_groups_grouped(
            [846, 847],
            MatrixRaftAdminCommand::set_node_healthy(3, true),
        )
        .expect("route grouped admin to meta and data groups");
    assert_eq!(
        grouped_admin_results
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 1)]
    );
    assert!(grouped_admin_results.iter().all(|(_, results)| results
        .iter()
        .all(|result| result.handled && result.node_healthy == Some(true))));

    let mixed_admin_plan = server
        .plan_route_admin_commands_for_groups([
            (846, MatrixRaftAdminCommand::set_node_healthy(3, false)),
            (847, MatrixRaftAdminCommand::set_leader_lease_valid(false)),
        ])
        .expect("plan mixed admin route to meta and data groups");
    assert_eq!(mixed_admin_plan.group_count, 2);
    assert_eq!(mixed_admin_plan.group_ids, vec![846, 847]);
    assert_eq!(mixed_admin_plan.node_count, 3);
    assert_eq!(
        mixed_admin_plan.command_types,
        vec![
            MatrixRaftAdminCommandType::SetNodeHealthy,
            MatrixRaftAdminCommandType::SetLeaderLeaseValid,
        ]
    );
    assert_eq!(
        mixed_admin_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(846, 1),
            MatrixRaftRouteKey::new(846, 2),
            MatrixRaftRouteKey::new(847, 1),
        ]
    );
    assert_eq!(
        mixed_admin_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone(), group.command_type))
            .collect::<Vec<_>>(),
        vec![
            (846, vec![1, 2], MatrixRaftAdminCommandType::SetNodeHealthy),
            (
                847,
                vec![1],
                MatrixRaftAdminCommandType::SetLeaderLeaseValid,
            ),
        ]
    );
    assert_eq!(
        mixed_admin_plan.route_keys_by_group(),
        vec![
            (
                846,
                vec![
                    MatrixRaftRouteKey::new(846, 1),
                    MatrixRaftRouteKey::new(846, 2),
                ],
            ),
            (847, vec![MatrixRaftRouteKey::new(847, 1)]),
        ]
    );
    assert_eq!(
        mixed_admin_plan.node_ids_by_group(),
        vec![(846, vec![1, 2]), (847, vec![1])]
    );
    assert_eq!(
        mixed_admin_plan.command_types_by_group(),
        vec![
            (846, MatrixRaftAdminCommandType::SetNodeHealthy),
            (847, MatrixRaftAdminCommandType::SetLeaderLeaseValid),
        ]
    );
    assert_eq!(
        mixed_admin_plan.commands_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftAdminCommand::set_node_healthy(3, false),
            ),
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftAdminCommand::set_node_healthy(3, false),
            ),
            (
                MatrixRaftRouteKey::new(847, 1),
                MatrixRaftAdminCommand::set_leader_lease_valid(false),
            ),
        ]
    );
    assert_eq!(
        mixed_admin_plan.command_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftAdminCommandType::SetNodeHealthy,
            ),
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftAdminCommandType::SetNodeHealthy,
            ),
            (
                MatrixRaftRouteKey::new(847, 1),
                MatrixRaftAdminCommandType::SetLeaderLeaseValid,
            ),
        ]
    );
    assert_eq!(
        mixed_admin_plan.command_node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), Some(3)),
            (MatrixRaftRouteKey::new(846, 2), Some(3)),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        mixed_admin_plan.node_healthy_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), Some(false)),
            (MatrixRaftRouteKey::new(846, 2), Some(false)),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        mixed_admin_plan.lease_valid_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), Some(false)),
        ]
    );
    assert_eq!(
        mixed_admin_plan.request_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        mixed_admin_plan.snapshot_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        mixed_admin_plan.storage_fence_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    let mixed_admin_results = server
        .route_admin_commands_for_groups([
            (846, MatrixRaftAdminCommand::set_node_healthy(3, true)),
            (847, MatrixRaftAdminCommand::set_leader_lease_valid(false)),
        ])
        .expect("route mixed admin commands to meta and data groups");
    assert_eq!(mixed_admin_results.len(), 3);
    assert!(mixed_admin_results
        .iter()
        .filter(|result| result.key.group_id == 846)
        .all(|result| result.node_healthy == Some(true)));
    assert!(mixed_admin_results
        .iter()
        .filter(|result| result.key.group_id == 847)
        .all(|result| result.leader_lease_valid == Some(false)));
    let mixed_admin_grouped = server
        .route_admin_commands_for_groups_grouped([
            (846, MatrixRaftAdminCommand::set_node_healthy(3, false)),
            (847, MatrixRaftAdminCommand::set_leader_lease_valid(true)),
        ])
        .expect("route grouped mixed admin commands to meta and data groups");
    assert_eq!(
        mixed_admin_grouped
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 1)]
    );
    assert!(mixed_admin_grouped.iter().all(|(group_id, results)| {
        results.iter().all(|result| {
            if *group_id == 846 {
                result.node_healthy == Some(false)
            } else {
                result.leader_lease_valid == Some(true)
            }
        })
    }));

    let routed_admin_plan = server
        .plan_route_admin_command_batch(vec![
            MatrixRaftRoutedAdminCommand::new(
                846,
                1,
                MatrixRaftAdminCommand::set_node_healthy(3, false),
            ),
            MatrixRaftRoutedAdminCommand::new(
                846,
                2,
                MatrixRaftAdminCommand::set_leader_lease_valid(false),
            ),
            MatrixRaftRoutedAdminCommand::new(
                847,
                1,
                MatrixRaftAdminCommand::set_node_healthy(3, true),
            ),
        ])
        .expect("plan routed admin command batch");
    assert_eq!(routed_admin_plan.command_count, 3);
    assert_eq!(routed_admin_plan.group_count, 2);
    assert_eq!(routed_admin_plan.group_ids, vec![846, 847]);
    assert_eq!(routed_admin_plan.node_count, 2);
    assert_eq!(routed_admin_plan.node_ids, vec![1, 2]);
    assert_eq!(
        routed_admin_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(846, 1),
            MatrixRaftRouteKey::new(846, 2),
            MatrixRaftRouteKey::new(847, 1),
        ]
    );
    assert_eq!(
        routed_admin_plan.command_types,
        vec![
            MatrixRaftAdminCommandType::SetNodeHealthy,
            MatrixRaftAdminCommandType::SetLeaderLeaseValid,
        ]
    );
    assert_eq!(
        routed_admin_plan
            .groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group.command_count,
                    group.node_ids.clone(),
                    group.command_types.clone(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                2,
                vec![1, 2],
                vec![
                    MatrixRaftAdminCommandType::SetNodeHealthy,
                    MatrixRaftAdminCommandType::SetLeaderLeaseValid,
                ],
            ),
            (
                847,
                1,
                vec![1],
                vec![MatrixRaftAdminCommandType::SetNodeHealthy]
            ),
        ]
    );
    assert_eq!(
        routed_admin_plan.route_keys_by_group(),
        vec![
            (
                846,
                vec![
                    MatrixRaftRouteKey::new(846, 1),
                    MatrixRaftRouteKey::new(846, 2),
                ],
            ),
            (847, vec![MatrixRaftRouteKey::new(847, 1)]),
        ]
    );
    assert_eq!(
        routed_admin_plan.node_ids_by_group(),
        vec![(846, vec![1, 2]), (847, vec![1])]
    );
    assert_eq!(
        routed_admin_plan.command_counts_by_group(),
        vec![(846, 2), (847, 1)]
    );
    assert_eq!(
        routed_admin_plan.route_key_counts_by_group(),
        vec![(846, 2), (847, 1)]
    );
    assert_eq!(
        routed_admin_plan.command_fanout_counts_by_group(),
        vec![(846, 2, 2), (847, 1, 1)]
    );
    assert_eq!(
        routed_admin_plan.command_types_by_group(),
        vec![
            (
                846,
                vec![
                    MatrixRaftAdminCommandType::SetNodeHealthy,
                    MatrixRaftAdminCommandType::SetLeaderLeaseValid,
                ],
            ),
            (847, vec![MatrixRaftAdminCommandType::SetNodeHealthy]),
        ]
    );
    assert_eq!(
        routed_admin_plan.commands_by_group(),
        vec![
            (
                846,
                vec![
                    MatrixRaftAdminCommand::set_node_healthy(3, false),
                    MatrixRaftAdminCommand::set_leader_lease_valid(false),
                ],
            ),
            (847, vec![MatrixRaftAdminCommand::set_node_healthy(3, true)]),
        ]
    );
    assert_eq!(
        routed_admin_plan.commands_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftAdminCommand::set_node_healthy(3, false),
            ),
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftAdminCommand::set_leader_lease_valid(false),
            ),
            (
                MatrixRaftRouteKey::new(847, 1),
                MatrixRaftAdminCommand::set_node_healthy(3, true),
            ),
        ]
    );
    assert_eq!(
        routed_admin_plan.command_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftAdminCommandType::SetNodeHealthy,
            ),
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftAdminCommandType::SetLeaderLeaseValid,
            ),
            (
                MatrixRaftRouteKey::new(847, 1),
                MatrixRaftAdminCommandType::SetNodeHealthy,
            ),
        ]
    );
    assert_eq!(
        routed_admin_plan.command_node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), Some(3)),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), Some(3)),
        ]
    );
    assert_eq!(
        routed_admin_plan.node_healthy_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), Some(false)),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), Some(true)),
        ]
    );
    assert_eq!(
        routed_admin_plan.node_healthy_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), true),
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(847, 1), true),
        ]
    );
    assert_eq!(
        routed_admin_plan.lease_valid_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), Some(false)),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        routed_admin_plan.lease_valid_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), true),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        routed_admin_plan.storage_fence_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        routed_admin_plan.command_node_ids_by_group(),
        vec![(846, vec![Some(3), None]), (847, vec![Some(3)])]
    );
    assert_eq!(
        routed_admin_plan.request_ids_by_group(),
        vec![(846, vec![None, None]), (847, vec![None])]
    );
    assert_eq!(
        routed_admin_plan.request_id_presence_by_group(),
        vec![(846, vec![false, false]), (847, vec![false])]
    );
    assert_eq!(
        routed_admin_plan.snapshot_ids_by_group(),
        vec![(846, vec![None, None]), (847, vec![None])]
    );
    assert_eq!(
        routed_admin_plan.snapshot_id_presence_by_group(),
        vec![(846, vec![false, false]), (847, vec![false])]
    );
    assert_eq!(
        routed_admin_plan.snapshot_peer_ids_by_group(),
        vec![(846, vec![None, None]), (847, vec![None])]
    );
    assert_eq!(
        routed_admin_plan.snapshot_indices_by_group(),
        vec![(846, vec![None, None]), (847, vec![None])]
    );
    assert_eq!(
        routed_admin_plan.transferee_ids_by_group(),
        vec![(846, vec![None, None]), (847, vec![None])]
    );
    assert_eq!(
        routed_admin_plan.forced_campaigns_by_group(),
        vec![(846, vec![false, false]), (847, vec![false])]
    );
    assert_eq!(
        routed_admin_plan.log_indices_by_group(),
        vec![(846, vec![None, None]), (847, vec![None])]
    );
    assert_eq!(
        routed_admin_plan.log_index_presence_by_group(),
        vec![(846, vec![false, false]), (847, vec![false])]
    );
    assert_eq!(
        routed_admin_plan.storage_fence_presence_by_group(),
        vec![(846, vec![false, false]), (847, vec![false])]
    );
    assert_eq!(
        routed_admin_plan.storage_fences_by_group(),
        vec![(846, vec![None, None]), (847, vec![None])]
    );
    assert_eq!(
        routed_admin_plan.node_healthy_values_by_group(),
        vec![(846, vec![Some(false), None]), (847, vec![Some(true)])]
    );
    assert_eq!(
        routed_admin_plan.node_healthy_presence_by_group(),
        vec![(846, vec![true, false]), (847, vec![true])]
    );
    assert_eq!(
        routed_admin_plan.lease_valid_values_by_group(),
        vec![(846, vec![None, Some(false)]), (847, vec![None])]
    );
    assert_eq!(
        routed_admin_plan.lease_valid_presence_by_group(),
        vec![(846, vec![false, true]), (847, vec![false])]
    );
    let routed_admin_results = server
        .route_admin_command_batch(routed_admin_plan.commands.clone())
        .expect("route admin command batch");
    assert_eq!(
        routed_admin_results
            .iter()
            .map(|result| result.key)
            .collect::<Vec<_>>(),
        vec![
            MatrixRaftRouteKey::new(846, 1),
            MatrixRaftRouteKey::new(846, 2),
            MatrixRaftRouteKey::new(847, 1),
        ]
    );
    assert_eq!(routed_admin_results[0].node_healthy, Some(false));
    assert_eq!(routed_admin_results[1].leader_lease_valid, Some(false));
    assert_eq!(routed_admin_results[2].node_healthy, Some(true));
    let routed_admin_grouped = server
        .route_admin_command_batch_grouped(routed_admin_plan.commands.clone())
        .expect("route grouped admin command batch");
    assert_eq!(
        routed_admin_grouped
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 1)]
    );
    let routed_admin_best_effort = server.route_admin_command_batch_best_effort(vec![
        MatrixRaftRoutedAdminCommand::new(
            846,
            1,
            MatrixRaftAdminCommand::set_node_healthy(3, true),
        ),
        MatrixRaftRoutedAdminCommand::new(899, 1, MatrixRaftAdminCommand::release_memory()),
        MatrixRaftRoutedAdminCommand::new(
            847,
            1,
            MatrixRaftAdminCommand::set_leader_lease_valid(true),
        ),
    ]);
    assert_eq!(routed_admin_best_effort.len(), 3);
    assert!(routed_admin_best_effort[0].is_ok());
    assert_eq!(routed_admin_best_effort[1].group_id, 899);
    assert!(routed_admin_best_effort[1].error.is_some());
    assert!(routed_admin_best_effort[2]
        .result
        .as_ref()
        .is_some_and(|result| result.leader_lease_valid == Some(true)));
    let routed_admin_grouped_best_effort =
        server.route_admin_command_batch_grouped_best_effort(routed_admin_plan.commands.clone());
    assert_eq!(
        routed_admin_grouped_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 1)]
    );
    assert!(routed_admin_grouped_best_effort
        .iter()
        .all(|(_, results)| results.iter().all(|result| result.is_ok())));
    assert_eq!(
        server.plan_route_admin_command_batch(vec![MatrixRaftRoutedAdminCommand::new(
            846,
            99,
            MatrixRaftAdminCommand::release_memory(),
        )]),
        Err(RaftError::NodeNotFound(99))
    );

    let priority_admin_plan = server
        .plan_priority_route_admin_command_batch(vec![
            MatrixRaftPriorityRoutedAdminCommand::new(
                MailPriority::Normal,
                MatrixRaftRoutedAdminCommand::new(
                    846,
                    1,
                    MatrixRaftAdminCommand::set_node_healthy(3, true),
                ),
            ),
            MatrixRaftPriorityRoutedAdminCommand::new(
                MailPriority::Slowly,
                MatrixRaftRoutedAdminCommand::new(
                    847,
                    1,
                    MatrixRaftAdminCommand::set_leader_lease_valid(false),
                ),
            ),
            MatrixRaftPriorityRoutedAdminCommand::new(
                MailPriority::Urgent,
                MatrixRaftRoutedAdminCommand::new(
                    846,
                    2,
                    MatrixRaftAdminCommand::set_leader_lease_valid(true),
                ),
            ),
            MatrixRaftPriorityRoutedAdminCommand::new(
                MailPriority::Urgent,
                MatrixRaftRoutedAdminCommand::new(
                    846,
                    1,
                    MatrixRaftAdminCommand::checkpoint_snapshot(1, "priority-admin-snapshot-41"),
                ),
            ),
        ])
        .expect("plan priority routed admin batch");
    assert_eq!(priority_admin_plan.command_count, 4);
    assert_eq!(priority_admin_plan.group_count, 2);
    assert_eq!(priority_admin_plan.group_ids, vec![846, 847]);
    assert_eq!(priority_admin_plan.node_count, 2);
    assert_eq!(priority_admin_plan.node_ids, vec![2, 1]);
    assert_eq!(
        priority_admin_plan
            .priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    group.command_count,
                    group.group_count,
                    group.group_ids.clone(),
                    group.route_keys.clone(),
                    group.node_ids.clone(),
                    group.command_types.clone(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                MailPriority::Urgent,
                2,
                1,
                vec![846],
                vec![
                    MatrixRaftRouteKey::new(846, 2),
                    MatrixRaftRouteKey::new(846, 1),
                ],
                vec![2, 1],
                vec![
                    MatrixRaftAdminCommandType::SetLeaderLeaseValid,
                    MatrixRaftAdminCommandType::CheckpointSnapshot,
                ],
            ),
            (
                MailPriority::Normal,
                1,
                1,
                vec![846],
                vec![MatrixRaftRouteKey::new(846, 1)],
                vec![1],
                vec![MatrixRaftAdminCommandType::SetNodeHealthy],
            ),
            (
                MailPriority::Slowly,
                1,
                1,
                vec![847],
                vec![MatrixRaftRouteKey::new(847, 1)],
                vec![1],
                vec![MatrixRaftAdminCommandType::SetLeaderLeaseValid],
            ),
        ]
    );
    assert_eq!(
        priority_admin_plan
            .commands
            .iter()
            .map(|command| command.priority)
            .collect::<Vec<_>>(),
        vec![
            MailPriority::Urgent,
            MailPriority::Urgent,
            MailPriority::Normal,
            MailPriority::Slowly,
        ]
    );
    assert_eq!(
        priority_admin_plan.priorities_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 2), MailPriority::Urgent),
            (MatrixRaftRouteKey::new(846, 1), MailPriority::Urgent),
            (MatrixRaftRouteKey::new(846, 1), MailPriority::Normal),
            (MatrixRaftRouteKey::new(847, 1), MailPriority::Slowly),
        ]
    );
    assert_eq!(
        priority_admin_plan.commands_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftAdminCommand::set_leader_lease_valid(true),
            ),
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftAdminCommand::checkpoint_snapshot(1, "priority-admin-snapshot-41"),
            ),
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftAdminCommand::set_node_healthy(3, true),
            ),
            (
                MatrixRaftRouteKey::new(847, 1),
                MatrixRaftAdminCommand::set_leader_lease_valid(false),
            ),
        ]
    );
    assert_eq!(
        priority_admin_plan.command_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftAdminCommandType::SetLeaderLeaseValid,
            ),
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftAdminCommandType::CheckpointSnapshot,
            ),
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftAdminCommandType::SetNodeHealthy,
            ),
            (
                MatrixRaftRouteKey::new(847, 1),
                MatrixRaftAdminCommandType::SetLeaderLeaseValid,
            ),
        ]
    );
    assert_eq!(
        priority_admin_plan.snapshot_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 2), None),
            (
                MatrixRaftRouteKey::new(846, 1),
                Some("priority-admin-snapshot-41".to_string()),
            ),
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        priority_admin_plan.snapshot_id_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(846, 1), true),
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        priority_admin_plan.node_healthy_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 1), Some(true)),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        priority_admin_plan.node_healthy_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 2), false),
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 1), true),
            (MatrixRaftRouteKey::new(847, 1), false),
        ]
    );
    assert_eq!(
        priority_admin_plan.lease_valid_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 2), Some(true)),
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(847, 1), Some(false)),
        ]
    );
    assert_eq!(
        priority_admin_plan.lease_valid_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 2), true),
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(847, 1), true),
        ]
    );
    assert_eq!(
        priority_admin_plan.route_keys_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![
                    MatrixRaftRouteKey::new(846, 2),
                    MatrixRaftRouteKey::new(846, 1),
                ]
            ),
            (MailPriority::Normal, vec![MatrixRaftRouteKey::new(846, 1)]),
            (MailPriority::Slowly, vec![MatrixRaftRouteKey::new(847, 1)]),
        ]
    );
    assert_eq!(
        priority_admin_plan.group_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![846]),
            (MailPriority::Normal, vec![846]),
            (MailPriority::Slowly, vec![847]),
        ]
    );
    assert_eq!(
        priority_admin_plan.node_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![2, 1]),
            (MailPriority::Normal, vec![1]),
            (MailPriority::Slowly, vec![1]),
        ]
    );
    assert_eq!(
        priority_admin_plan.command_types_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![
                    MatrixRaftAdminCommandType::SetLeaderLeaseValid,
                    MatrixRaftAdminCommandType::CheckpointSnapshot,
                ]
            ),
            (
                MailPriority::Normal,
                vec![MatrixRaftAdminCommandType::SetNodeHealthy]
            ),
            (
                MailPriority::Slowly,
                vec![MatrixRaftAdminCommandType::SetLeaderLeaseValid]
            ),
        ]
    );
    assert_eq!(
        priority_admin_plan.commands_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![
                    MatrixRaftAdminCommand::set_leader_lease_valid(true),
                    MatrixRaftAdminCommand::checkpoint_snapshot(1, "priority-admin-snapshot-41"),
                ],
            ),
            (
                MailPriority::Normal,
                vec![MatrixRaftAdminCommand::set_node_healthy(3, true)],
            ),
            (
                MailPriority::Slowly,
                vec![MatrixRaftAdminCommand::set_leader_lease_valid(false)],
            ),
        ]
    );
    assert_eq!(
        priority_admin_plan.command_counts_by_priority(),
        vec![
            (MailPriority::Urgent, 2),
            (MailPriority::Normal, 1),
            (MailPriority::Slowly, 1),
        ]
    );
    assert_eq!(
        priority_admin_plan.route_key_counts_by_priority(),
        vec![
            (MailPriority::Urgent, 2),
            (MailPriority::Normal, 1),
            (MailPriority::Slowly, 1),
        ]
    );
    assert_eq!(
        priority_admin_plan.command_fanout_counts_by_priority(),
        vec![
            (MailPriority::Urgent, 2, 2),
            (MailPriority::Normal, 1, 1),
            (MailPriority::Slowly, 1, 1),
        ]
    );
    assert_eq!(
        priority_admin_plan.command_node_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, Some(1)]),
            (MailPriority::Normal, vec![Some(3)]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_admin_plan.request_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, None]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_admin_plan.request_id_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false, false]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        priority_admin_plan.snapshot_ids_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![None, Some("priority-admin-snapshot-41".to_string())],
            ),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_admin_plan.snapshot_id_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false, true]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        priority_admin_plan.snapshot_peer_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, None]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_admin_plan.snapshot_indices_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, None]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_admin_plan.transferee_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, None]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_admin_plan.forced_campaigns_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false, false]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        priority_admin_plan.log_indices_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, None]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_admin_plan.storage_fence_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false, false]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        priority_admin_plan.storage_fences_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, None]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_admin_plan.node_healthy_values_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None, None]),
            (MailPriority::Normal, vec![Some(true)]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        priority_admin_plan.node_healthy_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false, false]),
            (MailPriority::Normal, vec![true]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        priority_admin_plan.lease_valid_values_by_priority(),
        vec![
            (MailPriority::Urgent, vec![Some(true), None]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![Some(false)]),
        ]
    );
    assert_eq!(
        priority_admin_plan.lease_valid_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![true, false]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![true]),
        ]
    );
    assert_eq!(
        priority_admin_plan
            .commands
            .iter()
            .map(MatrixRaftPriorityRoutedAdminCommand::route_key)
            .collect::<Vec<_>>(),
        priority_admin_plan.route_keys
    );
    assert_eq!(
        priority_admin_plan.route_keys_by_group(),
        vec![
            (
                846,
                vec![
                    MatrixRaftRouteKey::new(846, 2),
                    MatrixRaftRouteKey::new(846, 1),
                    MatrixRaftRouteKey::new(846, 1),
                ],
            ),
            (847, vec![MatrixRaftRouteKey::new(847, 1)]),
        ]
    );
    assert_eq!(
        priority_admin_plan.node_ids_by_group(),
        vec![(846, vec![2, 1]), (847, vec![1])]
    );
    assert_eq!(
        priority_admin_plan.command_counts_by_group(),
        vec![(846, 3), (847, 1)]
    );
    assert_eq!(
        priority_admin_plan.route_key_counts_by_group(),
        vec![(846, 3), (847, 1)]
    );
    assert_eq!(
        priority_admin_plan.command_fanout_counts_by_group(),
        vec![(846, 3, 3), (847, 1, 1)]
    );
    assert_eq!(
        priority_admin_plan.command_types_by_group(),
        vec![
            (
                846,
                vec![
                    MatrixRaftAdminCommandType::SetLeaderLeaseValid,
                    MatrixRaftAdminCommandType::CheckpointSnapshot,
                    MatrixRaftAdminCommandType::SetNodeHealthy,
                ],
            ),
            (847, vec![MatrixRaftAdminCommandType::SetLeaderLeaseValid]),
        ]
    );
    assert_eq!(
        priority_admin_plan.priorities_by_group(),
        vec![
            (
                846,
                vec![
                    MailPriority::Urgent,
                    MailPriority::Urgent,
                    MailPriority::Normal
                ]
            ),
            (847, vec![MailPriority::Slowly]),
        ]
    );
    assert_eq!(
        priority_admin_plan.commands_by_group(),
        vec![
            (
                846,
                vec![
                    MatrixRaftAdminCommand::set_leader_lease_valid(true),
                    MatrixRaftAdminCommand::checkpoint_snapshot(1, "priority-admin-snapshot-41"),
                    MatrixRaftAdminCommand::set_node_healthy(3, true),
                ],
            ),
            (
                847,
                vec![MatrixRaftAdminCommand::set_leader_lease_valid(false)]
            ),
        ]
    );
    assert_eq!(
        priority_admin_plan.command_node_ids_by_group(),
        vec![(846, vec![None, Some(1), Some(3)]), (847, vec![None])]
    );
    assert_eq!(
        priority_admin_plan.request_ids_by_group(),
        vec![(846, vec![None, None, None]), (847, vec![None])]
    );
    assert_eq!(
        priority_admin_plan.request_id_presence_by_group(),
        vec![(846, vec![false, false, false]), (847, vec![false])]
    );
    assert_eq!(
        priority_admin_plan.snapshot_ids_by_group(),
        vec![
            (
                846,
                vec![None, Some("priority-admin-snapshot-41".to_string()), None],
            ),
            (847, vec![None]),
        ]
    );
    assert_eq!(
        priority_admin_plan.snapshot_id_presence_by_group(),
        vec![(846, vec![false, true, false]), (847, vec![false])]
    );
    assert_eq!(
        priority_admin_plan.snapshot_peer_ids_by_group(),
        vec![(846, vec![None, None, None]), (847, vec![None])]
    );
    assert_eq!(
        priority_admin_plan.snapshot_indices_by_group(),
        vec![(846, vec![None, None, None]), (847, vec![None])]
    );
    assert_eq!(
        priority_admin_plan.transferee_ids_by_group(),
        vec![(846, vec![None, None, None]), (847, vec![None])]
    );
    assert_eq!(
        priority_admin_plan.forced_campaigns_by_group(),
        vec![(846, vec![false, false, false]), (847, vec![false])]
    );
    assert_eq!(
        priority_admin_plan.log_indices_by_group(),
        vec![(846, vec![None, None, None]), (847, vec![None])]
    );
    assert_eq!(
        priority_admin_plan.storage_fence_presence_by_group(),
        vec![(846, vec![false, false, false]), (847, vec![false])]
    );
    assert_eq!(
        priority_admin_plan.storage_fences_by_group(),
        vec![(846, vec![None, None, None]), (847, vec![None])]
    );
    assert_eq!(
        priority_admin_plan.node_healthy_values_by_group(),
        vec![(846, vec![None, None, Some(true)]), (847, vec![None])]
    );
    assert_eq!(
        priority_admin_plan.node_healthy_presence_by_group(),
        vec![(846, vec![false, false, true]), (847, vec![false])]
    );
    assert_eq!(
        priority_admin_plan.lease_valid_values_by_group(),
        vec![
            (846, vec![Some(true), None, None]),
            (847, vec![Some(false)]),
        ]
    );
    assert_eq!(
        priority_admin_plan.lease_valid_presence_by_group(),
        vec![(846, vec![true, false, false]), (847, vec![true])]
    );
    let priority_admin_results = server
        .route_priority_admin_command_batch(priority_admin_plan.commands.clone())
        .expect("route priority admin batch");
    assert_eq!(
        priority_admin_results
            .iter()
            .map(|result| result.key)
            .collect::<Vec<_>>(),
        vec![
            MatrixRaftRouteKey::new(846, 2),
            MatrixRaftRouteKey::new(846, 1),
            MatrixRaftRouteKey::new(846, 1),
            MatrixRaftRouteKey::new(847, 1),
        ]
    );
    assert_eq!(priority_admin_results[0].leader_lease_valid, Some(true));
    assert!(priority_admin_results[1]
        .checkpoint
        .as_ref()
        .is_some_and(|snapshot| snapshot.meta.snapshot_id == "priority-admin-snapshot-41"));
    assert_eq!(priority_admin_results[2].node_healthy, Some(true));
    assert_eq!(priority_admin_results[3].leader_lease_valid, Some(false));
    let priority_admin_grouped = server
        .route_priority_admin_command_batch_grouped(priority_admin_plan.commands.clone())
        .expect("route grouped priority admin batch");
    assert_eq!(
        priority_admin_grouped
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 3), (847, 1)]
    );
    assert_eq!(
        priority_admin_grouped[0]
            .1
            .iter()
            .map(|result| result.key)
            .collect::<Vec<_>>(),
        vec![
            MatrixRaftRouteKey::new(846, 2),
            MatrixRaftRouteKey::new(846, 1),
            MatrixRaftRouteKey::new(846, 1)
        ]
    );

    let metadata_fence = StorageApplyFence {
        group_id: 847,
        node_id: 1,
        applied_index: 18,
        committed_index: 18,
        durable_applied_index: 18,
        storage_flushed_index: 18,
        installed_snapshot_index: 0,
        first_retained_log_index: 19,
    };
    let metadata_admin_plan = server
        .plan_priority_route_admin_command_batch(vec![
            MatrixRaftPriorityRoutedAdminCommand::new(
                MailPriority::Urgent,
                MatrixRaftRoutedAdminCommand::new(
                    846,
                    1,
                    MatrixRaftAdminCommand::campaign(2, true),
                ),
            ),
            MatrixRaftPriorityRoutedAdminCommand::new(
                MailPriority::Normal,
                MatrixRaftRoutedAdminCommand::new(
                    846,
                    2,
                    MatrixRaftAdminCommand::transfer_leader(1),
                ),
            ),
            MatrixRaftPriorityRoutedAdminCommand::new(
                MailPriority::Slowly,
                MatrixRaftRoutedAdminCommand::new(
                    847,
                    1,
                    MatrixRaftAdminCommand::compact_logs_with_storage_fence(
                        18,
                        metadata_fence.clone(),
                    ),
                ),
            ),
        ])
        .expect("plan priority routed admin metadata batch");
    assert_eq!(
        metadata_admin_plan.command_types_by_priority(),
        vec![
            (
                MailPriority::Urgent,
                vec![MatrixRaftAdminCommandType::Election]
            ),
            (
                MailPriority::Normal,
                vec![MatrixRaftAdminCommandType::TransferLeader],
            ),
            (
                MailPriority::Slowly,
                vec![MatrixRaftAdminCommandType::CompactLogsWithStorageFence],
            ),
        ]
    );
    assert_eq!(
        metadata_admin_plan.command_node_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![Some(2)]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        metadata_admin_plan.transferee_ids_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None]),
            (MailPriority::Normal, vec![Some(1)]),
            (MailPriority::Slowly, vec![None]),
        ]
    );
    assert_eq!(
        metadata_admin_plan.transferee_id_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false]),
            (MailPriority::Normal, vec![true]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        metadata_admin_plan.forced_campaigns_by_priority(),
        vec![
            (MailPriority::Urgent, vec![true]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![false]),
        ]
    );
    assert_eq!(
        metadata_admin_plan.log_indices_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![Some(18)]),
        ]
    );
    assert_eq!(
        metadata_admin_plan.log_index_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![true]),
        ]
    );
    assert_eq!(
        metadata_admin_plan.storage_fence_presence_by_priority(),
        vec![
            (MailPriority::Urgent, vec![false]),
            (MailPriority::Normal, vec![false]),
            (MailPriority::Slowly, vec![true]),
        ]
    );
    assert_eq!(
        metadata_admin_plan.storage_fences_by_priority(),
        vec![
            (MailPriority::Urgent, vec![None]),
            (MailPriority::Normal, vec![None]),
            (MailPriority::Slowly, vec![Some(metadata_fence.clone())]),
        ]
    );
    assert_eq!(
        metadata_admin_plan.transferee_ids_by_group(),
        vec![(846, vec![None, Some(1)]), (847, vec![None])]
    );
    assert_eq!(
        metadata_admin_plan.transferee_id_presence_by_group(),
        vec![(846, vec![false, true]), (847, vec![false])]
    );
    assert_eq!(
        metadata_admin_plan.forced_campaigns_by_group(),
        vec![(846, vec![true, false]), (847, vec![false])]
    );
    assert_eq!(
        metadata_admin_plan.log_indices_by_group(),
        vec![(846, vec![None, None]), (847, vec![Some(18)])]
    );
    assert_eq!(
        metadata_admin_plan.log_index_presence_by_group(),
        vec![(846, vec![false, false]), (847, vec![true])]
    );
    assert_eq!(
        metadata_admin_plan.storage_fence_presence_by_group(),
        vec![(846, vec![false, false]), (847, vec![true])]
    );
    assert_eq!(
        metadata_admin_plan.storage_fences_by_group(),
        vec![(846, vec![None, None]), (847, vec![Some(metadata_fence)]),]
    );
    let priority_admin_best_effort = server.route_priority_admin_command_batch_best_effort(vec![
        MatrixRaftPriorityRoutedAdminCommand::new(
            MailPriority::Slowly,
            MatrixRaftRoutedAdminCommand::new(899, 1, MatrixRaftAdminCommand::release_memory()),
        ),
        MatrixRaftPriorityRoutedAdminCommand::new(
            MailPriority::Urgent,
            MatrixRaftRoutedAdminCommand::new(
                846,
                1,
                MatrixRaftAdminCommand::set_node_healthy(3, false),
            ),
        ),
    ]);
    assert_eq!(priority_admin_best_effort.len(), 2);
    assert_eq!(priority_admin_best_effort[0].group_id, 846);
    assert!(priority_admin_best_effort[0].is_ok());
    assert_eq!(priority_admin_best_effort[1].group_id, 899);
    assert!(priority_admin_best_effort[1].error.is_some());
    let priority_admin_grouped_best_effort = server
        .route_priority_admin_command_batch_grouped_best_effort(vec![
            MatrixRaftPriorityRoutedAdminCommand::new(
                MailPriority::Slowly,
                MatrixRaftRoutedAdminCommand::new(899, 1, MatrixRaftAdminCommand::release_memory()),
            ),
            MatrixRaftPriorityRoutedAdminCommand::new(
                MailPriority::Urgent,
                MatrixRaftRoutedAdminCommand::new(
                    847,
                    1,
                    MatrixRaftAdminCommand::set_leader_lease_valid(true),
                ),
            ),
        ]);
    assert_eq!(
        priority_admin_grouped_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(847, 1), (899, 1)]
    );
    assert!(priority_admin_grouped_best_effort[0].1[0].is_ok());
    assert!(priority_admin_grouped_best_effort[1].1[0].error.is_some());

    let fanout_message = MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory());
    let message_plan = server
        .plan_route_message_to_groups([846, 847], fanout_message.clone())
        .expect("plan message route to meta and data groups");
    assert_eq!(message_plan.group_count, 2);
    assert_eq!(message_plan.group_ids, vec![846, 847]);
    assert_eq!(message_plan.node_count, 3);
    assert_eq!(
        message_plan.message_type,
        MatrixRaftMessageType::AdminCommand
    );
    assert_eq!(
        message_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(846, 1),
            MatrixRaftRouteKey::new(846, 2),
            MatrixRaftRouteKey::new(847, 1),
        ]
    );
    assert_eq!(
        message_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone(), group.message_type))
            .collect::<Vec<_>>(),
        vec![
            (846, vec![1, 2], MatrixRaftMessageType::AdminCommand),
            (847, vec![1], MatrixRaftMessageType::AdminCommand),
        ]
    );
    assert_eq!(
        message_plan.route_keys_by_group(),
        vec![
            (
                846,
                vec![
                    MatrixRaftRouteKey::new(846, 1),
                    MatrixRaftRouteKey::new(846, 2),
                ],
            ),
            (847, vec![MatrixRaftRouteKey::new(847, 1)]),
        ]
    );
    assert_eq!(
        message_plan.node_ids_by_group(),
        vec![(846, vec![1, 2]), (847, vec![1])]
    );
    assert_eq!(
        message_plan.node_counts_by_group(),
        vec![(846, 2), (847, 1)]
    );
    assert_eq!(
        message_plan.message_types_by_group(),
        vec![
            (846, MatrixRaftMessageType::AdminCommand),
            (847, MatrixRaftMessageType::AdminCommand),
        ]
    );
    assert_eq!(
        message_plan.messages_by_group(),
        vec![(846, fanout_message.clone()), (847, fanout_message.clone()),]
    );
    assert_eq!(
        message_plan.messages_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), fanout_message.clone()),
            (MatrixRaftRouteKey::new(846, 2), fanout_message.clone()),
            (MatrixRaftRouteKey::new(847, 1), fanout_message.clone()),
        ]
    );
    assert_eq!(
        message_plan.message_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftMessageType::AdminCommand
            ),
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftMessageType::AdminCommand
            ),
            (
                MatrixRaftRouteKey::new(847, 1),
                MatrixRaftMessageType::AdminCommand
            ),
        ]
    );
    assert_eq!(
        message_plan.sender_receiver_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), (Some(1), Some(1))),
            (MatrixRaftRouteKey::new(846, 2), (Some(1), Some(1))),
            (MatrixRaftRouteKey::new(847, 1), (Some(1), Some(1))),
        ]
    );
    assert_eq!(
        message_plan.terms_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        message_plan.committed_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        message_plan.message_bytes_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), 0),
            (MatrixRaftRouteKey::new(846, 2), 0),
            (MatrixRaftRouteKey::new(847, 1), 0),
        ]
    );
    assert_eq!(
        message_plan.propose_request_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        message_plan.snapshot_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        message_plan.snapshot_chunk_offsets_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        message_plan.snapshot_chunk_done_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    assert_eq!(
        message_plan.snapshot_chunk_payload_bytes_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), None),
            (MatrixRaftRouteKey::new(846, 2), None),
            (MatrixRaftRouteKey::new(847, 1), None),
        ]
    );
    let message_results = server
        .route_message_to_groups(
            [846, 847],
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        )
        .expect("route message to meta and data groups");
    assert_eq!(message_results.len(), 3);
    assert!(message_results.iter().all(|result| result.handled));

    let grouped_message_results = server
        .route_message_to_groups_grouped(
            [846, 847],
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        )
        .expect("route grouped message to meta and data groups");
    assert_eq!(
        grouped_message_results
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 1)]
    );
    assert!(grouped_message_results
        .iter()
        .all(|(_, results)| results.iter().all(|result| result.status()
            == MatrixRaftRouteResultStatus::Handled
            && !result.is_unhandled()
            && result.handled
            && result.message_type == MatrixRaftMessageType::AdminCommand)));
    let grouped_message_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&grouped_message_results);
    assert_eq!(
        grouped_message_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.result_count,
                summary.handled_count,
                summary.unhandled_count,
                summary.message_types.clone(),
                summary.kinds.clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                2,
                2,
                0,
                vec![MatrixRaftMessageType::AdminCommand],
                vec![MatrixRaftRouteResultKind::Delivered],
            ),
            (
                847,
                1,
                1,
                0,
                vec![MatrixRaftMessageType::AdminCommand],
                vec![MatrixRaftRouteResultKind::Delivered],
            ),
        ]
    );
    assert!(grouped_message_summaries.iter().all(|summary| summary
        .proposed_log_ids_by_route_key
        .iter()
        .all(|(_, proposed_log_id)| proposed_log_id.is_none())));
    assert!(grouped_message_summaries.iter().all(|summary| summary
        .read_index_responses_by_route_key
        .iter()
        .all(|(_, read_index_response)| read_index_response.is_none())));
    assert!(grouped_message_summaries.iter().all(|summary| summary
        .released_memory_by_route_key
        .iter()
        .all(|(_, released_memory)| released_memory.is_some())));
    assert!(grouped_message_summaries.iter().all(|summary| summary
        .released_memory_presence_by_route_key()
        .iter()
        .all(|(_, present)| *present)));
    assert_eq!(
        grouped_message_summaries
            .iter()
            .map(|summary| (summary.group_id, summary.released_memory_by_route_key.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 1)]
    );
    assert_eq!(
        grouped_message_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.handled_message_types.clone(),
                summary.unhandled_message_types.clone(),
                summary.handled_kinds.clone(),
                summary.unhandled_kinds.clone(),
                summary.counts_by_message_type.clone(),
                summary.counts_by_kind.clone(),
                summary.result_counts_by_status(),
                summary.route_key_counts_by_status(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                vec![MatrixRaftMessageType::AdminCommand],
                Vec::new(),
                vec![MatrixRaftRouteResultKind::Delivered],
                Vec::new(),
                vec![(MatrixRaftMessageType::AdminCommand, 2, 2, 0)],
                vec![(MatrixRaftRouteResultKind::Delivered, 2, 2, 0)],
                (2, 0),
                (2, 0),
            ),
            (
                847,
                vec![MatrixRaftMessageType::AdminCommand],
                Vec::new(),
                vec![MatrixRaftRouteResultKind::Delivered],
                Vec::new(),
                vec![(MatrixRaftMessageType::AdminCommand, 1, 1, 0)],
                vec![(MatrixRaftRouteResultKind::Delivered, 1, 1, 0)],
                (1, 0),
                (1, 0),
            ),
        ]
    );
    assert_eq!(
        grouped_message_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.message_types_by_route_key(),
                summary.handled_message_types_by_route_key(),
                summary.unhandled_message_types_by_route_key(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                vec![
                    (
                        MatrixRaftRouteKey::new(846, 1),
                        MatrixRaftMessageType::AdminCommand,
                    ),
                    (
                        MatrixRaftRouteKey::new(846, 2),
                        MatrixRaftMessageType::AdminCommand,
                    ),
                ],
                vec![
                    (
                        MatrixRaftRouteKey::new(846, 1),
                        MatrixRaftMessageType::AdminCommand,
                    ),
                    (
                        MatrixRaftRouteKey::new(846, 2),
                        MatrixRaftMessageType::AdminCommand,
                    ),
                ],
                Vec::<(MatrixRaftRouteKey, MatrixRaftMessageType)>::new(),
            ),
            (
                847,
                vec![(
                    MatrixRaftRouteKey::new(847, 1),
                    MatrixRaftMessageType::AdminCommand,
                )],
                vec![(
                    MatrixRaftRouteKey::new(847, 1),
                    MatrixRaftMessageType::AdminCommand,
                )],
                Vec::<(MatrixRaftRouteKey, MatrixRaftMessageType)>::new(),
            ),
        ]
    );
    assert_eq!(
        grouped_message_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.kinds_by_route_key(),
                summary.handled_kinds_by_route_key(),
                summary.unhandled_kinds_by_route_key(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                vec![
                    (
                        MatrixRaftRouteKey::new(846, 1),
                        MatrixRaftRouteResultKind::Delivered,
                    ),
                    (
                        MatrixRaftRouteKey::new(846, 2),
                        MatrixRaftRouteResultKind::Delivered,
                    ),
                ],
                vec![
                    (
                        MatrixRaftRouteKey::new(846, 1),
                        MatrixRaftRouteResultKind::Delivered,
                    ),
                    (
                        MatrixRaftRouteKey::new(846, 2),
                        MatrixRaftRouteResultKind::Delivered,
                    ),
                ],
                Vec::<(MatrixRaftRouteKey, MatrixRaftRouteResultKind)>::new(),
            ),
            (
                847,
                vec![(
                    MatrixRaftRouteKey::new(847, 1),
                    MatrixRaftRouteResultKind::Delivered,
                )],
                vec![(
                    MatrixRaftRouteKey::new(847, 1),
                    MatrixRaftRouteResultKind::Delivered,
                )],
                Vec::<(MatrixRaftRouteKey, MatrixRaftRouteResultKind)>::new(),
            ),
        ]
    );
    assert_eq!(
        grouped_message_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.node_ids.clone(),
                summary.handled_node_ids.clone(),
                summary.unhandled_node_ids.clone(),
                summary.handled_by_route_key.clone(),
                summary.status_by_route_key(),
                summary.handled_presence_by_route_key(),
                summary.unhandled_presence_by_route_key(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                vec![1, 2],
                vec![1, 2],
                Vec::new(),
                vec![
                    (MatrixRaftRouteKey::new(846, 1), true),
                    (MatrixRaftRouteKey::new(846, 2), true),
                ],
                vec![
                    (
                        MatrixRaftRouteKey::new(846, 1),
                        MatrixRaftRouteResultStatus::Handled,
                    ),
                    (
                        MatrixRaftRouteKey::new(846, 2),
                        MatrixRaftRouteResultStatus::Handled,
                    ),
                ],
                vec![
                    (MatrixRaftRouteKey::new(846, 1), true),
                    (MatrixRaftRouteKey::new(846, 2), true),
                ],
                vec![
                    (MatrixRaftRouteKey::new(846, 1), false),
                    (MatrixRaftRouteKey::new(846, 2), false),
                ],
            ),
            (
                847,
                vec![1],
                vec![1],
                Vec::new(),
                vec![(MatrixRaftRouteKey::new(847, 1), true)],
                vec![(
                    MatrixRaftRouteKey::new(847, 1),
                    MatrixRaftRouteResultStatus::Handled,
                )],
                vec![(MatrixRaftRouteKey::new(847, 1), true)],
                vec![(MatrixRaftRouteKey::new(847, 1), false)],
            ),
        ]
    );
    assert_eq!(
        grouped_message_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.route_keys.clone(),
                summary.handled_route_keys.clone(),
                summary.unhandled_route_keys.clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                vec![
                    MatrixRaftRouteKey::new(846, 1),
                    MatrixRaftRouteKey::new(846, 2),
                ],
                vec![
                    MatrixRaftRouteKey::new(846, 1),
                    MatrixRaftRouteKey::new(846, 2),
                ],
                Vec::<MatrixRaftRouteKey>::new(),
            ),
            (
                847,
                vec![MatrixRaftRouteKey::new(847, 1)],
                vec![MatrixRaftRouteKey::new(847, 1)],
                Vec::<MatrixRaftRouteKey>::new(),
            ),
        ]
    );
    assert_eq!(
        grouped_message_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.node_ids_by_route_key(),
                summary.handled_node_ids_by_route_key(),
                summary.unhandled_node_ids_by_route_key(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                vec![
                    (MatrixRaftRouteKey::new(846, 1), 1),
                    (MatrixRaftRouteKey::new(846, 2), 2),
                ],
                vec![
                    (MatrixRaftRouteKey::new(846, 1), 1),
                    (MatrixRaftRouteKey::new(846, 2), 2),
                ],
                Vec::<(MatrixRaftRouteKey, u64)>::new(),
            ),
            (
                847,
                vec![(MatrixRaftRouteKey::new(847, 1), 1)],
                vec![(MatrixRaftRouteKey::new(847, 1), 1)],
                Vec::new(),
            ),
        ]
    );
    assert!(grouped_message_summaries
        .iter()
        .all(MatrixRaftRouteGroupSummary::is_handled));
    assert_eq!(
        grouped_message_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary
                    .results_by_route_key
                    .iter()
                    .map(|(key, result)| (*key, result.message_type, result.handled))
                    .collect::<Vec<_>>(),
                summary.handled_results_by_route_key.len(),
                summary.unhandled_results_by_route_key.len()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                vec![
                    (
                        MatrixRaftRouteKey::new(846, 1),
                        MatrixRaftMessageType::AdminCommand,
                        true
                    ),
                    (
                        MatrixRaftRouteKey::new(846, 2),
                        MatrixRaftMessageType::AdminCommand,
                        true
                    ),
                ],
                2,
                0
            ),
            (
                847,
                vec![(
                    MatrixRaftRouteKey::new(847, 1),
                    MatrixRaftMessageType::AdminCommand,
                    true
                )],
                1,
                0
            ),
        ]
    );

    let best_effort_propose = server
        .route_message_to_groups_best_effort(
            [846, 847],
            MatrixRaftMessage::propose(
                1,
                1,
                MatrixRaftPropose {
                    request_id: Some(846_847),
                    data: b"cross-plane-membership-and-shard-update".to_vec(),
                    context: b"meta-data".to_vec(),
                    is_command: true,
                },
            ),
        )
        .expect("best-effort cross-plane propose");
    assert_eq!(best_effort_propose.len(), 3);
    assert_eq!(
        best_effort_propose
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        2
    );
    assert!(best_effort_propose
        .iter()
        .filter(|result| result.is_ok())
        .all(|result| result
            .result
            .as_ref()
            .and_then(|route| route.proposed_log_id.as_ref())
            .is_some()));
    assert_eq!(
        best_effort_propose
            .iter()
            .filter(|result| result.error.is_some())
            .count(),
        1
    );
    assert_eq!(
        best_effort_propose
            .iter()
            .map(|result| (result.route_key(), result.status(), result.is_error()))
            .collect::<Vec<_>>(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftBatchRouteResultStatus::Ok,
                false
            ),
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftBatchRouteResultStatus::Error,
                true
            ),
            (
                MatrixRaftRouteKey::new(847, 1),
                MatrixRaftBatchRouteResultStatus::Ok,
                false
            ),
        ]
    );
    let meta_propose_results = best_effort_propose
        .iter()
        .filter(|result| result.group_id == 846)
        .cloned()
        .collect::<Vec<_>>();
    let meta_propose_summary =
        MatrixRaftBatchRouteGroupSummary::from_results(846, &meta_propose_results);
    assert_eq!(meta_propose_summary.result_count, 2);
    assert_eq!(meta_propose_summary.ok_count, 1);
    assert_eq!(meta_propose_summary.error_count, 1);
    assert_eq!(meta_propose_summary.node_ids, vec![1, 2]);
    assert_eq!(meta_propose_summary.ok_node_ids, vec![1]);
    assert_eq!(meta_propose_summary.error_node_ids, vec![2]);
    assert_eq!(
        meta_propose_summary.statuses_by_route_key,
        vec![
            (MatrixRaftRouteKey::new(846, 1), true),
            (MatrixRaftRouteKey::new(846, 2), false),
        ]
    );
    assert_eq!(
        meta_propose_summary.status_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftBatchRouteResultStatus::Ok,
            ),
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftBatchRouteResultStatus::Error,
            ),
        ]
    );
    assert_eq!(
        meta_propose_summary.ok_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), true),
            (MatrixRaftRouteKey::new(846, 2), false),
        ]
    );
    assert_eq!(
        meta_propose_summary.error_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(846, 1), false),
            (MatrixRaftRouteKey::new(846, 2), true),
        ]
    );
    assert_eq!(
        meta_propose_summary.message_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(846, 1),
                MatrixRaftMessageType::Propose,
            ),
            (
                MatrixRaftRouteKey::new(846, 2),
                MatrixRaftMessageType::Propose,
            ),
        ]
    );
    assert_eq!(
        meta_propose_summary.ok_message_types_by_route_key(),
        vec![(
            MatrixRaftRouteKey::new(846, 1),
            MatrixRaftMessageType::Propose,
        )]
    );
    assert_eq!(
        meta_propose_summary.error_message_types_by_route_key(),
        vec![(
            MatrixRaftRouteKey::new(846, 2),
            MatrixRaftMessageType::Propose,
        )]
    );
    assert!(meta_propose_summary
        .errors_by_route_key
        .iter()
        .any(|(key, error)| *key == MatrixRaftRouteKey::new(846, 2) && error.is_some()));

    let best_effort = server
        .route_message_to_groups_best_effort(
            [846, 847],
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        )
        .expect("best-effort cross-plane route");
    assert_eq!(best_effort.len(), 3);
    assert!(best_effort.iter().all(|result| result.is_ok()));
    assert!(best_effort
        .iter()
        .all(|result| result.message_type == MatrixRaftMessageType::AdminCommand));

    let grouped_best_effort = server
        .route_message_to_groups_grouped_best_effort(
            [846, 847],
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        )
        .expect("best-effort grouped cross-plane route");
    assert_eq!(
        grouped_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 1)]
    );
    assert!(grouped_best_effort
        .iter()
        .all(|(_, results)| results
            .iter()
            .all(|result| result.is_ok()
                && result.message_type == MatrixRaftMessageType::AdminCommand)));
    let grouped_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&grouped_best_effort);
    assert_eq!(
        grouped_best_effort_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.result_count,
                summary.ok_count,
                summary.error_count,
                summary.message_types.clone(),
                summary.ok_message_types.clone(),
                summary.error_message_types.clone(),
                summary.counts_by_message_type.clone(),
                summary.result_counts_by_status(),
                summary.route_key_counts_by_status(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                2,
                2,
                0,
                vec![MatrixRaftMessageType::AdminCommand],
                vec![MatrixRaftMessageType::AdminCommand],
                Vec::new(),
                vec![(MatrixRaftMessageType::AdminCommand, 2, 2, 0)],
                (2, 0),
                (2, 0),
            ),
            (
                847,
                1,
                1,
                0,
                vec![MatrixRaftMessageType::AdminCommand],
                vec![MatrixRaftMessageType::AdminCommand],
                Vec::new(),
                vec![(MatrixRaftMessageType::AdminCommand, 1, 1, 0)],
                (1, 0),
                (1, 0),
            ),
        ]
    );
    assert_eq!(
        grouped_best_effort_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.message_types_by_route_key(),
                summary.ok_message_types_by_route_key(),
                summary.error_message_types_by_route_key(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                vec![
                    (
                        MatrixRaftRouteKey::new(846, 1),
                        MatrixRaftMessageType::AdminCommand,
                    ),
                    (
                        MatrixRaftRouteKey::new(846, 2),
                        MatrixRaftMessageType::AdminCommand,
                    ),
                ],
                vec![
                    (
                        MatrixRaftRouteKey::new(846, 1),
                        MatrixRaftMessageType::AdminCommand,
                    ),
                    (
                        MatrixRaftRouteKey::new(846, 2),
                        MatrixRaftMessageType::AdminCommand,
                    ),
                ],
                Vec::<(MatrixRaftRouteKey, MatrixRaftMessageType)>::new(),
            ),
            (
                847,
                vec![(
                    MatrixRaftRouteKey::new(847, 1),
                    MatrixRaftMessageType::AdminCommand,
                )],
                vec![(
                    MatrixRaftRouteKey::new(847, 1),
                    MatrixRaftMessageType::AdminCommand,
                )],
                Vec::<(MatrixRaftRouteKey, MatrixRaftMessageType)>::new(),
            ),
        ]
    );
    assert!(grouped_best_effort_summaries.iter().all(|summary| summary
        .proposed_log_ids_by_route_key
        .iter()
        .all(|(_, proposed_log_id)| proposed_log_id.is_none())));
    assert!(grouped_best_effort_summaries.iter().all(|summary| summary
        .read_index_responses_by_route_key
        .iter()
        .all(|(_, read_index_response)| read_index_response.is_none())));
    assert!(grouped_best_effort_summaries.iter().all(|summary| summary
        .released_memory_by_route_key
        .iter()
        .all(|(_, released_memory)| released_memory.is_some())));
    assert_eq!(
        grouped_best_effort_summaries
            .iter()
            .map(|summary| (summary.group_id, summary.released_memory_by_route_key.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 1)]
    );
    assert_eq!(
        grouped_best_effort_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.node_ids.clone(),
                summary.ok_node_ids.clone(),
                summary.error_node_ids.clone(),
                summary.statuses_by_route_key.clone(),
                summary.status_by_route_key(),
                summary.ok_presence_by_route_key(),
                summary.error_presence_by_route_key(),
                summary.errors_by_route_key.clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                vec![1, 2],
                vec![1, 2],
                Vec::new(),
                vec![
                    (MatrixRaftRouteKey::new(846, 1), true),
                    (MatrixRaftRouteKey::new(846, 2), true),
                ],
                vec![
                    (
                        MatrixRaftRouteKey::new(846, 1),
                        MatrixRaftBatchRouteResultStatus::Ok,
                    ),
                    (
                        MatrixRaftRouteKey::new(846, 2),
                        MatrixRaftBatchRouteResultStatus::Ok,
                    ),
                ],
                vec![
                    (MatrixRaftRouteKey::new(846, 1), true),
                    (MatrixRaftRouteKey::new(846, 2), true),
                ],
                vec![
                    (MatrixRaftRouteKey::new(846, 1), false),
                    (MatrixRaftRouteKey::new(846, 2), false),
                ],
                vec![
                    (MatrixRaftRouteKey::new(846, 1), None),
                    (MatrixRaftRouteKey::new(846, 2), None),
                ],
            ),
            (
                847,
                vec![1],
                vec![1],
                Vec::new(),
                vec![(MatrixRaftRouteKey::new(847, 1), true)],
                vec![(
                    MatrixRaftRouteKey::new(847, 1),
                    MatrixRaftBatchRouteResultStatus::Ok,
                )],
                vec![(MatrixRaftRouteKey::new(847, 1), true)],
                vec![(MatrixRaftRouteKey::new(847, 1), false)],
                vec![(MatrixRaftRouteKey::new(847, 1), None)],
            ),
        ]
    );
    assert_eq!(
        grouped_best_effort_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.route_keys.clone(),
                summary.ok_route_keys.clone(),
                summary.error_route_keys.clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                846,
                vec![
                    MatrixRaftRouteKey::new(846, 1),
                    MatrixRaftRouteKey::new(846, 2),
                ],
                vec![
                    MatrixRaftRouteKey::new(846, 1),
                    MatrixRaftRouteKey::new(846, 2),
                ],
                Vec::new(),
            ),
            (
                847,
                vec![MatrixRaftRouteKey::new(847, 1)],
                vec![MatrixRaftRouteKey::new(847, 1)],
                Vec::new(),
            ),
        ]
    );
    assert!(grouped_best_effort_summaries
        .iter()
        .all(MatrixRaftBatchRouteGroupSummary::is_ok));

    let admin_best_effort = server
        .route_admin_command_to_groups_best_effort(
            [846, 847],
            MatrixRaftAdminCommand::release_memory(),
        )
        .expect("best-effort cross-plane admin route");
    assert_eq!(admin_best_effort.len(), 3);
    assert!(admin_best_effort.iter().all(|result| result.is_ok()));

    let grouped_admin_best_effort = server
        .route_admin_command_to_groups_grouped_best_effort(
            [846, 847],
            MatrixRaftAdminCommand::release_memory(),
        )
        .expect("best-effort grouped cross-plane admin route");
    assert_eq!(
        grouped_admin_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 1)]
    );
    assert!(grouped_admin_best_effort
        .iter()
        .all(|(_, results)| results.iter().all(|result| result.is_ok())));

    let mixed_admin_best_effort = server
        .route_admin_commands_for_groups_best_effort([
            (846, MatrixRaftAdminCommand::set_node_healthy(3, false)),
            (847, MatrixRaftAdminCommand::set_leader_lease_valid(true)),
        ])
        .expect("best-effort mixed admin route");
    assert_eq!(mixed_admin_best_effort.len(), 3);
    assert!(mixed_admin_best_effort.iter().all(|result| result.is_ok()));
    assert!(mixed_admin_best_effort
        .iter()
        .filter(|result| result.group_id == 846)
        .all(|result| result
            .result
            .as_ref()
            .is_some_and(|route| route.node_healthy == Some(false))));
    assert!(mixed_admin_best_effort
        .iter()
        .filter(|result| result.group_id == 847)
        .all(|result| result
            .result
            .as_ref()
            .is_some_and(|route| route.leader_lease_valid == Some(true))));

    let mixed_admin_grouped_best_effort = server
        .route_admin_commands_for_groups_grouped_best_effort([
            (846, MatrixRaftAdminCommand::set_node_healthy(3, true)),
            (847, MatrixRaftAdminCommand::set_leader_lease_valid(false)),
        ])
        .expect("best-effort grouped mixed admin route");
    assert_eq!(
        mixed_admin_grouped_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 1)]
    );
    assert!(mixed_admin_grouped_best_effort
        .iter()
        .all(|(_, results)| results.iter().all(|result| result.is_ok())));

    assert_invalid_request_contains(
        server.route_message_to_groups(
            [846, 899],
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        ),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_route_message_to_groups(
            [846, 899],
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::release_memory()),
        ),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.route_admin_command_to_groups_grouped(
            [846, 899],
            MatrixRaftAdminCommand::release_memory(),
        ),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_route_admin_command_to_groups(
            [846, 899],
            MatrixRaftAdminCommand::release_memory(),
        ),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_route_admin_commands_for_groups([
            (846, MatrixRaftAdminCommand::release_memory()),
            (899, MatrixRaftAdminCommand::set_node_healthy(3, true)),
        ]),
        "group 899 is not registered",
    );

    server.shutdown_all().expect("shutdown cross-plane server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_reports_group_statuses_and_leaders() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let group_wal_1 = temp_dir("status-group-1-wal");
    let group_snap_1 = temp_dir("status-group-1-snapshot");
    let group_wal_2 = temp_dir("status-group-2-wal");
    let group_snap_2 = temp_dir("status-group-2-snapshot");
    let data_wal = temp_dir("status-data-wal");
    let data_snap = temp_dir("status-data-snapshot");
    server
        .create_node(options_for_peer(822, 1, &group_wal_1, &group_snap_1), 1)
        .expect("group node 1");
    server
        .create_node(options_for_peer(822, 2, &group_wal_2, &group_snap_2), 1)
        .expect("group node 2");
    server
        .create_node(options(823, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start status server");

    let statuses = server.group_statuses(822).expect("group statuses");
    assert_eq!(statuses.len(), 2);
    assert_eq!(
        statuses
            .iter()
            .map(|status| status.node_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(statuses.iter().all(|status| status.group_id == 822));
    assert!(statuses.iter().all(|status| status.leader_id == Some(1)));
    assert_eq!(
        server
            .group_route_key_list(822)
            .expect("group route keys")
            .iter()
            .map(|key| (key.group_id, key.node_id))
            .collect::<Vec<_>>(),
        vec![(822, 1), (822, 2)]
    );
    assert_eq!(
        server
            .route_keys_for_groups([822, 823])
            .expect("meta and data route keys")
            .iter()
            .map(|(group_id, keys)| (*group_id, keys.len()))
            .collect::<Vec<_>>(),
        vec![(822, 2), (823, 1)]
    );
    let route_key_plan = server
        .plan_route_keys_for_groups([822, 823])
        .expect("plan route keys for meta and data groups");
    assert_eq!(route_key_plan.operation, "route_keys");
    assert_eq!(route_key_plan.operation_name(), "route_keys");
    assert_eq!(route_key_plan.operation_arguments(), Vec::<String>::new());
    assert_eq!(route_key_plan.operation_argument_count(), 0);
    assert_eq!(route_key_plan.group_count, 2);
    assert_eq!(route_key_plan.group_ids, vec![822, 823]);
    assert_eq!(route_key_plan.node_count, 3);
    assert_eq!(
        route_key_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(822, 1),
            MatrixRaftRouteKey::new(822, 2),
            MatrixRaftRouteKey::new(823, 1),
        ]
    );
    assert_eq!(
        route_key_plan.route_keys_by_group(),
        vec![
            (
                822,
                vec![
                    MatrixRaftRouteKey::new(822, 1),
                    MatrixRaftRouteKey::new(822, 2),
                ],
            ),
            (823, vec![MatrixRaftRouteKey::new(823, 1)]),
        ]
    );
    assert_eq!(
        route_key_plan.node_ids_by_group(),
        vec![(822, vec![1, 2]), (823, vec![1])]
    );
    assert_eq!(
        route_key_plan.node_counts_by_group(),
        vec![(822, 2), (823, 1)]
    );
    assert_eq!(
        route_key_plan.route_key_counts_by_group(),
        vec![(822, 2), (823, 1)]
    );
    assert_eq!(
        route_key_plan.fanout_counts_by_group(),
        vec![(822, 2, 2), (823, 1, 1)]
    );
    assert_eq!(
        route_key_plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(822, 1), 1),
            (MatrixRaftRouteKey::new(822, 2), 2),
            (MatrixRaftRouteKey::new(823, 1), 1),
        ]
    );
    assert_eq!(
        route_key_plan.operations_by_group(),
        vec![
            (822, "route_keys".to_string()),
            (823, "route_keys".to_string()),
        ]
    );
    assert_eq!(
        route_key_plan.operations_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(822, 1), "route_keys".to_string()),
            (MatrixRaftRouteKey::new(822, 2), "route_keys".to_string()),
            (MatrixRaftRouteKey::new(823, 1), "route_keys".to_string()),
        ]
    );
    assert_eq!(
        route_key_plan.operation_names_by_group(),
        vec![
            (822, "route_keys".to_string()),
            (823, "route_keys".to_string()),
        ]
    );
    assert_eq!(
        route_key_plan.operation_names_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(822, 1), "route_keys".to_string()),
            (MatrixRaftRouteKey::new(822, 2), "route_keys".to_string()),
            (MatrixRaftRouteKey::new(823, 1), "route_keys".to_string()),
        ]
    );
    assert_eq!(
        route_key_plan.operation_arguments_by_group(),
        vec![(822, Vec::<String>::new()), (823, Vec::<String>::new())]
    );
    assert_eq!(
        route_key_plan.operation_arguments_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(822, 1), Vec::<String>::new()),
            (MatrixRaftRouteKey::new(822, 2), Vec::<String>::new()),
            (MatrixRaftRouteKey::new(823, 1), Vec::<String>::new()),
        ]
    );
    assert_eq!(
        route_key_plan.operation_argument_counts_by_group(),
        vec![(822, 0), (823, 0)]
    );
    assert_eq!(
        route_key_plan.operation_argument_counts_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(822, 1), 0),
            (MatrixRaftRouteKey::new(822, 2), 0),
            (MatrixRaftRouteKey::new(823, 1), 0),
        ]
    );
    assert_eq!(
        route_key_plan.fanout_counts_by_operation(),
        vec![("route_keys".to_string(), 2, 3, 3)]
    );
    assert_eq!(
        server
            .node_id_on_node(822, 1)
            .expect("node descriptor on meta node")
            .raft_addr,
        peer(822, 1).raft_addr
    );
    assert_eq!(
        server
            .node_ids_on_group(822)
            .expect("node descriptors on meta group")
            .iter()
            .map(|node| (node.peer_id, node.raft_addr.clone()))
            .collect::<Vec<_>>(),
        vec![(1, peer(822, 1).raft_addr), (2, peer(822, 2).raft_addr)]
    );
    assert_eq!(
        server
            .node_ids_for_groups([822, 823])
            .expect("node descriptors on selected groups")
            .iter()
            .map(|(group_id, nodes)| {
                (
                    *group_id,
                    nodes.iter().map(|node| node.peer_id).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(822, vec![1, 2]), (823, vec![1])]
    );
    let node_id_plan = server
        .plan_node_ids_for_groups([822, 823])
        .expect("plan node descriptors on selected groups");
    assert_eq!(node_id_plan.operation, "node_ids");
    assert_eq!(node_id_plan.group_count, 2);
    assert_eq!(node_id_plan.group_ids, vec![822, 823]);
    assert_eq!(node_id_plan.node_count, 3);
    assert_eq!(
        node_id_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(822, 1),
            MatrixRaftRouteKey::new(822, 2),
            MatrixRaftRouteKey::new(823, 1),
        ]
    );
    assert_eq!(
        node_id_plan.route_keys_by_group(),
        vec![
            (
                822,
                vec![
                    MatrixRaftRouteKey::new(822, 1),
                    MatrixRaftRouteKey::new(822, 2),
                ],
            ),
            (823, vec![MatrixRaftRouteKey::new(823, 1)]),
        ]
    );
    assert_eq!(
        node_id_plan.node_ids_by_group(),
        vec![(822, vec![1, 2]), (823, vec![1])]
    );
    assert_eq!(
        node_id_plan.node_counts_by_group(),
        vec![(822, 2), (823, 1)]
    );
    server
        .publish_snapshot_route(
            822,
            1,
            MatrixRaftSnapshotDesc::from_snapshot_meta(&SnapshotMetadata {
                snapshot_id: "topology-meta-snapshot".to_string(),
                last_log_id: LogId { term: 1, index: 1 },
                membership: vec![1, 2],
                members: vec![peer(822, 1), peer(822, 2)],
            }),
        )
        .expect("publish topology snapshot route");
    let meta_topology = server.group_topology(822).expect("meta topology");
    assert_eq!(meta_topology.group_id, 822);
    assert_eq!(meta_topology.node_ids, vec![1, 2]);
    assert_eq!(
        meta_topology
            .route_keys
            .iter()
            .map(|key| (key.group_id, key.node_id))
            .collect::<Vec<_>>(),
        vec![(822, 1), (822, 2)]
    );
    assert_eq!(meta_topology.node_count, 2);
    assert_eq!(meta_topology.runtime_wiring_count, 2);
    assert_eq!(meta_topology.snapshot_route_count, 1);

    let selected_topologies = server
        .topologies_for_groups([822, 823])
        .expect("selected meta and data topologies");
    let topology_plan = server
        .plan_topologies_for_groups([822, 823])
        .expect("plan selected meta and data topologies");
    assert_eq!(topology_plan.operation, "topologies");
    assert_eq!(topology_plan.group_count, 2);
    assert_eq!(topology_plan.group_ids, vec![822, 823]);
    assert_eq!(topology_plan.node_count, 3);
    assert_eq!(
        selected_topologies
            .iter()
            .map(|topology| {
                (
                    topology.group_id,
                    topology.node_count,
                    topology.runtime_wiring_count,
                    topology.snapshot_route_count,
                )
            })
            .collect::<Vec<_>>(),
        vec![(822, 2, 2, 1), (823, 1, 1, 0)]
    );
    let cluster_topology = server.topology();
    assert_eq!(cluster_topology.group_count, 2);
    assert_eq!(cluster_topology.node_count, 3);
    assert_eq!(cluster_topology.runtime_wiring_count, 3);
    assert_eq!(cluster_topology.snapshot_route_count, 1);
    assert_eq!(
        cluster_topology
            .groups
            .iter()
            .map(|topology| (topology.group_id, topology.node_ids.clone()))
            .collect::<Vec<_>>(),
        vec![(822, vec![1, 2]), (823, vec![1])]
    );
    assert_eq!(
        cluster_topology.route_keys_by_group(),
        vec![
            (
                822,
                vec![
                    MatrixRaftRouteKey::new(822, 1),
                    MatrixRaftRouteKey::new(822, 2),
                ],
            ),
            (823, vec![MatrixRaftRouteKey::new(823, 1)]),
        ]
    );
    assert_eq!(
        cluster_topology.node_ids_by_group(),
        vec![(822, vec![1, 2]), (823, vec![1])]
    );
    assert_eq!(
        cluster_topology.counts_by_group(),
        vec![(822, 2, 2, 1), (823, 1, 1, 0)]
    );
    assert_eq!(
        server
            .statuses_for_groups([822, 823])
            .expect("meta and data statuses")
            .iter()
            .map(|(group_id, statuses)| {
                (
                    *group_id,
                    statuses
                        .iter()
                        .map(|status| status.node_id)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(822, vec![1, 2]), (823, vec![1])]
    );
    let status_plan = server
        .plan_statuses_for_groups([822, 823])
        .expect("plan meta and data statuses");
    assert_eq!(status_plan.operation, "statuses");
    assert_eq!(status_plan.group_count, 2);
    assert_eq!(status_plan.node_count, 3);

    let local_statuses = server
        .group_local_statuses(822)
        .expect("group local statuses");
    assert_eq!(local_statuses.len(), 2);
    assert_eq!(
        local_statuses
            .iter()
            .map(|status| status.node_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(local_statuses.iter().all(|status| status.worker_running));
    assert_eq!(
        server
            .local_statuses_for_groups([822, 823])
            .expect("meta and data local statuses")
            .iter()
            .map(|(group_id, statuses)| (*group_id, statuses.len()))
            .collect::<Vec<_>>(),
        vec![(822, 2), (823, 1)]
    );
    assert_eq!(
        server
            .plan_local_statuses_for_groups([822, 823])
            .expect("plan meta and data local statuses")
            .operation,
        "local_statuses"
    );

    let leaders = server.group_leaders(822).expect("group leaders");
    assert_eq!(leaders.len(), 2);
    assert!(leaders
        .iter()
        .all(|leader| leader.as_ref().map(|node| node.peer_id) == Some(1)));
    assert_eq!(
        server
            .leaders_for_groups([822, 823])
            .expect("meta and data leaders")
            .iter()
            .map(|(group_id, leaders)| (*group_id, leaders.len()))
            .collect::<Vec<_>>(),
        vec![(822, 2), (823, 1)]
    );
    let leader_plan = server
        .plan_leaders_for_groups([822, 823])
        .expect("plan meta and data leaders");
    assert_eq!(leader_plan.operation, "leaders");
    assert_eq!(leader_plan.node_count, 3);
    // Assert the invariant, not an equality between two live reads.
    //
    // This used to compare `in_lease_on_group` against `statuses` captured just
    // after `start_all`, ~370 lines earlier. The leader lease is wall-clock
    // based (`leader_lease_ms` defaults to 500) and is established by the
    // operations in between, so the stale snapshot said "no lease" while the
    // live call could say "lease held": the assertion passed only when the run
    // happened to be slow enough for the lease to have lapsed again. Re-reading
    // the statuses immediately beforehand narrows the window but does not close
    // it -- two independent reads of a time-based predicate can still straddle
    // the expiry instant, which was still failing about once in sixty runs.
    //
    // What is actually invariant here is that only a leader can hold a lease.
    // Node 1 leads throughout (asserted above); the follower must never report
    // one. The leader's own lease is left unasserted because its value is a
    // function of when the assertion happens to run.
    let group_leases = server
        .in_lease_on_group(822, None)
        .expect("group lease states");
    assert_eq!(group_leases.len(), 2);
    let lease_roles = server
        .group_statuses(822)
        .expect("group statuses for lease comparison")
        .iter()
        .map(|status| status.role)
        .collect::<Vec<_>>();
    for (node_index, holds_lease) in group_leases.iter().enumerate() {
        if *holds_lease {
            assert_eq!(
                lease_roles[node_index],
                StateRole::Leader,
                "only a leader may report an active lease"
            );
        }
    }
    // Same race as above -- the leader's own lease value depends on when the
    // call lands, so assert the two things that hold whenever it lands: the
    // follower never reports a lease, and a term that does not match the
    // leader's never does either.
    assert!(!server
        .in_lease_on_node(822, 2, None)
        .expect("follower node lease state"));
    assert!(!server
        .in_lease_on_node(822, 1, Some(u64::MAX))
        .expect("mismatched term lease state"));
    assert_eq!(
        server
            .in_lease_for_groups([822, 823], None)
            .expect("meta and data lease states")
            .iter()
            .map(|(group_id, leases)| (*group_id, leases.len()))
            .collect::<Vec<_>>(),
        vec![(822, 2), (823, 1)]
    );
    let lease_plan = server
        .plan_in_lease_for_groups([822, 823], Some(1))
        .expect("plan meta and data lease states");
    assert_eq!(lease_plan.operation, "in_lease:1");
    assert_eq!(lease_plan.operation_name(), "in_lease");
    assert_eq!(lease_plan.operation_arguments(), vec!["1".to_string()]);
    assert_eq!(lease_plan.operation_argument_count(), 1);
    assert_eq!(lease_plan.group_count, 2);
    assert_eq!(lease_plan.node_count, 3);
    assert_eq!(
        lease_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(822, 1),
            MatrixRaftRouteKey::new(822, 2),
            MatrixRaftRouteKey::new(823, 1),
        ]
    );
    assert_eq!(
        lease_plan.operations_by_group(),
        vec![
            (822, "in_lease:1".to_string()),
            (823, "in_lease:1".to_string()),
        ]
    );
    assert_eq!(
        lease_plan.operation_names_by_group(),
        vec![(822, "in_lease".to_string()), (823, "in_lease".to_string()),]
    );
    assert_eq!(
        lease_plan.operation_arguments_by_group(),
        vec![(822, vec!["1".to_string()]), (823, vec!["1".to_string()])]
    );
    assert_eq!(
        lease_plan.operation_argument_counts_by_group(),
        vec![(822, 1), (823, 1)]
    );
    assert_eq!(
        lease_plan.fanout_counts_by_operation(),
        vec![("in_lease".to_string(), 2, 3, 3)]
    );
    assert_eq!(server.group_statuses(823).expect("data statuses").len(), 1);
    assert_invalid_request_contains(server.group_statuses(899), "group 899 is not registered");
    assert_eq!(
        server.in_lease_on_node(822, 99, None),
        Err(RaftError::NodeNotFound(99))
    );
    assert_eq!(
        server.node_id_on_node(822, 99),
        Err(RaftError::NodeNotFound(99))
    );
    assert_invalid_request_contains(
        server.statuses_for_groups([822, 899]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_node_ids_for_groups([899, 822]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_leaders_for_groups([899, 822]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_in_lease_for_groups([899, 822], None),
        "group 899 is not registered",
    );

    server.shutdown_all().expect("shutdown status server");

    let _ = fs::remove_dir_all(group_wal_1);
    let _ = fs::remove_dir_all(group_snap_1);
    let _ = fs::remove_dir_all(group_wal_2);
    let _ = fs::remove_dir_all(group_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_syncs_fsm_runtime_bindings_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("fsm-sync-meta-1-wal");
    let meta_snap_1 = temp_dir("fsm-sync-meta-1-snapshot");
    let meta_wal_2 = temp_dir("fsm-sync-meta-2-wal");
    let meta_snap_2 = temp_dir("fsm-sync-meta-2-snapshot");
    let data_wal = temp_dir("fsm-sync-data-wal");
    let data_snap = temp_dir("fsm-sync-data-snapshot");
    server
        .create_node(options_for_peer(844, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(844, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(845, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start fsm sync server");

    let mut meta_bindings = BTreeMap::from([
        (
            MatrixRaftRouteKey::new(844, 1),
            MatrixRaftFsmRuntimeBinding::new(RuntimeSyncFsm::default()),
        ),
        (
            MatrixRaftRouteKey::new(844, 2),
            MatrixRaftFsmRuntimeBinding::new(RuntimeSyncFsm::default()),
        ),
    ]);
    let meta_reports = server
        .sync_fsm_runtimes_on_group(844, &mut meta_bindings)
        .expect("sync meta group fsm runtimes");
    assert_eq!(
        meta_reports
            .iter()
            .map(|(key, _)| key.node_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(meta_reports.iter().all(|(key, report)| key.group_id == 844
        && report.opened
        && report.term == 1
        && report.leader_id == Some(1)));
    assert_eq!(
        meta_bindings
            .get(&MatrixRaftRouteKey::new(844, 1))
            .expect("meta binding")
            .fsm()
            .events,
        vec!["open".to_string(), "lead:1".to_string()]
    );

    let mut node_binding = MatrixRaftFsmRuntimeBinding::new(RuntimeSyncFsm::default());
    let node_report = server
        .sync_fsm_runtime_on_node(844, 2, &mut node_binding)
        .expect("sync single node fsm runtime");
    assert!(node_report.opened);
    assert_eq!(node_report.leader_id, Some(1));
    assert!(node_binding
        .fsm()
        .events
        .iter()
        .any(|event| event == "follow:1:1"));

    let mut data_bindings = BTreeMap::from([(
        MatrixRaftRouteKey::new(845, 1),
        MatrixRaftFsmRuntimeBinding::new(RuntimeSyncFsm::default()),
    )]);
    let data_reports = server
        .sync_fsm_runtimes_on_group(845, &mut data_bindings)
        .expect("sync data group fsm runtime");
    assert_eq!(data_reports.len(), 1);
    assert_eq!(data_reports[0].0, MatrixRaftRouteKey::new(845, 1));

    let sync_plan = server
        .plan_sync_fsm_runtimes_for_groups([844, 845])
        .expect("plan selected meta and data fsm runtime sync");
    assert_eq!(sync_plan.operation, "sync_fsm_runtimes");
    assert_eq!(sync_plan.group_count, 2);
    assert_eq!(sync_plan.node_count, 3);
    assert_eq!(
        sync_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(844, 1),
            MatrixRaftRouteKey::new(844, 2),
            MatrixRaftRouteKey::new(845, 1),
        ]
    );

    let mut selected_bindings = BTreeMap::from([
        (
            MatrixRaftRouteKey::new(844, 1),
            MatrixRaftFsmRuntimeBinding::new(RuntimeSyncFsm::default()),
        ),
        (
            MatrixRaftRouteKey::new(844, 2),
            MatrixRaftFsmRuntimeBinding::new(RuntimeSyncFsm::default()),
        ),
        (
            MatrixRaftRouteKey::new(845, 1),
            MatrixRaftFsmRuntimeBinding::new(RuntimeSyncFsm::default()),
        ),
    ]);
    let selected_reports = server
        .sync_fsm_runtimes_for_groups([844, 845], &mut selected_bindings)
        .expect("sync selected meta and data fsm runtimes");
    assert_eq!(
        selected_reports
            .iter()
            .map(|(group_id, reports)| (*group_id, reports.len()))
            .collect::<Vec<_>>(),
        vec![(844, 2), (845, 1)]
    );
    assert!(selected_reports.iter().all(|(group_id, reports)| {
        reports.iter().all(|(key, report)| {
            key.group_id == *group_id
                && report.opened
                && report.term == 1
                && report.leader_id == Some(1)
        })
    }));
    assert_eq!(
        selected_bindings
            .get(&MatrixRaftRouteKey::new(845, 1))
            .expect("selected data binding")
            .fsm()
            .events,
        vec!["open".to_string(), "lead:1".to_string()]
    );

    let mut partial_bindings = BTreeMap::from([
        (
            MatrixRaftRouteKey::new(844, 1),
            MatrixRaftFsmRuntimeBinding::new(RuntimeSyncFsm::default()),
        ),
        (
            MatrixRaftRouteKey::new(845, 1),
            MatrixRaftFsmRuntimeBinding::new(RuntimeSyncFsm::default()),
        ),
    ]);
    let meta_best_effort = server
        .sync_fsm_runtimes_on_group_best_effort(844, &mut partial_bindings)
        .expect("best-effort meta fsm runtime sync");
    assert_eq!(meta_best_effort.group_id, 844);
    assert_eq!(meta_best_effort.node_count, 2);
    assert_eq!(meta_best_effort.ok_count, 1);
    assert_eq!(meta_best_effort.error_count, 1);
    assert!(meta_best_effort.results.iter().any(|result| {
        result.key == MatrixRaftRouteKey::new(844, 1)
            && result.is_ok()
            && result.report.as_ref().is_some_and(|report| report.opened)
    }));
    assert!(meta_best_effort.results.iter().any(|result| {
        result.key == MatrixRaftRouteKey::new(844, 2)
            && !result.ok
            && result
                .error
                .as_ref()
                .is_some_and(|error| error.contains("binding missing for node 2 in group 844"))
    }));
    assert_eq!(
        meta_best_effort.route_keys(),
        vec![
            MatrixRaftRouteKey::new(844, 1),
            MatrixRaftRouteKey::new(844, 2),
        ]
    );
    assert_eq!(
        meta_best_effort.ok_route_keys(),
        vec![MatrixRaftRouteKey::new(844, 1)]
    );
    assert_eq!(
        meta_best_effort.error_route_keys(),
        vec![MatrixRaftRouteKey::new(844, 2)]
    );
    assert_eq!(meta_best_effort.node_ids(), vec![1, 2]);
    assert_eq!(meta_best_effort.ok_node_ids(), vec![1]);
    assert_eq!(meta_best_effort.error_node_ids(), vec![2]);
    assert_eq!(
        meta_best_effort.statuses_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), true),
            (MatrixRaftRouteKey::new(844, 2), false),
        ]
    );
    assert_eq!(
        meta_best_effort.report_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), true),
            (MatrixRaftRouteKey::new(844, 2), false),
        ]
    );
    assert!(meta_best_effort
        .errors_by_route_key()
        .iter()
        .any(
            |(route_key, error)| *route_key == MatrixRaftRouteKey::new(844, 2)
                && error
                    .as_ref()
                    .is_some_and(|error| error.contains("binding missing for node 2 in group 844"))
        ));
    assert_eq!(
        meta_best_effort.opened_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), Some(true)),
            (MatrixRaftRouteKey::new(844, 2), None),
        ]
    );
    assert_eq!(
        meta_best_effort.opened_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), true),
            (MatrixRaftRouteKey::new(844, 2), false),
        ]
    );
    assert_eq!(
        meta_best_effort
            .reports_by_route_key()
            .iter()
            .map(|(route_key, report)| (*route_key, report.as_ref().map(|report| report.opened)))
            .collect::<Vec<_>>(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), Some(true)),
            (MatrixRaftRouteKey::new(844, 2), None),
        ]
    );
    assert_eq!(
        meta_best_effort
            .ok_reports_by_route_key()
            .iter()
            .map(|(route_key, report)| (*route_key, report.leader_started))
            .collect::<Vec<_>>(),
        vec![(MatrixRaftRouteKey::new(844, 1), true)]
    );
    assert_eq!(
        meta_best_effort.error_reports_by_route_key(),
        vec![(MatrixRaftRouteKey::new(844, 2), None)]
    );
    assert_eq!(
        meta_best_effort.leader_started_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), Some(true)),
            (MatrixRaftRouteKey::new(844, 2), None),
        ]
    );
    assert_eq!(
        meta_best_effort.leader_started_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), true),
            (MatrixRaftRouteKey::new(844, 2), false),
        ]
    );
    assert_eq!(
        meta_best_effort.terms_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), Some(1)),
            (MatrixRaftRouteKey::new(844, 2), None),
        ]
    );
    assert_eq!(
        meta_best_effort.term_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), true),
            (MatrixRaftRouteKey::new(844, 2), false),
        ]
    );
    assert_eq!(
        meta_best_effort.leader_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), Some(1)),
            (MatrixRaftRouteKey::new(844, 2), None),
        ]
    );
    assert_eq!(
        meta_best_effort.leader_id_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), true),
            (MatrixRaftRouteKey::new(844, 2), false),
        ]
    );
    let selected_best_effort = server
        .sync_fsm_runtimes_for_groups_best_effort([844, 845], &mut partial_bindings)
        .expect("best-effort selected fsm runtime sync");
    assert_eq!(
        selected_best_effort
            .iter()
            .map(|group| (
                group.group_id,
                group.node_count,
                group.ok_count,
                group.error_count
            ))
            .collect::<Vec<_>>(),
        vec![(844, 2, 1, 1), (845, 1, 1, 0)]
    );
    assert!(selected_best_effort
        .iter()
        .find(|group| group.group_id == 845)
        .expect("data group best-effort sync")
        .is_ok());
    let selected_data_sync = selected_best_effort
        .iter()
        .find(|group| group.group_id == 845)
        .expect("data sync metadata");
    assert_eq!(
        selected_data_sync.route_keys(),
        vec![MatrixRaftRouteKey::new(845, 1)]
    );
    assert_eq!(
        selected_data_sync.ok_route_keys(),
        vec![MatrixRaftRouteKey::new(845, 1)]
    );
    assert!(selected_data_sync.error_route_keys().is_empty());
    assert_eq!(selected_data_sync.node_ids(), vec![1]);
    assert_eq!(selected_data_sync.ok_node_ids(), vec![1]);
    assert!(selected_data_sync.error_node_ids().is_empty());
    assert_eq!(
        selected_data_sync.statuses_by_route_key(),
        vec![(MatrixRaftRouteKey::new(845, 1), true)]
    );
    assert_eq!(
        selected_data_sync.report_presence_by_route_key(),
        vec![(MatrixRaftRouteKey::new(845, 1), true)]
    );
    assert_eq!(
        selected_data_sync.errors_by_route_key(),
        vec![(MatrixRaftRouteKey::new(845, 1), None)]
    );
    assert_eq!(
        selected_data_sync.opened_by_route_key(),
        vec![(MatrixRaftRouteKey::new(845, 1), Some(true))]
    );
    assert_eq!(
        selected_data_sync.opened_presence_by_route_key(),
        vec![(MatrixRaftRouteKey::new(845, 1), true)]
    );
    assert_eq!(
        selected_data_sync.closed_by_route_key(),
        vec![(MatrixRaftRouteKey::new(845, 1), Some(false))]
    );
    assert_eq!(
        selected_data_sync.closed_presence_by_route_key(),
        vec![(MatrixRaftRouteKey::new(845, 1), true)]
    );
    assert_eq!(
        selected_data_sync.following_started_presence_by_route_key(),
        vec![(MatrixRaftRouteKey::new(845, 1), true)]
    );
    assert_eq!(
        selected_data_sync.roles_by_route_key(),
        vec![(
            MatrixRaftRouteKey::new(845, 1),
            Some(matrixraft::StateRole::Leader)
        )]
    );
    assert_eq!(
        selected_data_sync.role_presence_by_route_key(),
        vec![(MatrixRaftRouteKey::new(845, 1), true)]
    );
    assert_eq!(
        selected_data_sync
            .reports_by_route_key()
            .iter()
            .map(|(route_key, report)| (*route_key, report.as_ref().map(|report| report.role)))
            .collect::<Vec<_>>(),
        vec![(
            MatrixRaftRouteKey::new(845, 1),
            Some(matrixraft::StateRole::Leader)
        )]
    );
    assert_eq!(
        selected_data_sync.ok_reports_by_route_key().len(),
        selected_data_sync.results.len()
    );
    assert!(selected_data_sync.error_reports_by_route_key().is_empty());

    let mut incomplete_bindings = BTreeMap::from([(
        MatrixRaftRouteKey::new(844, 1),
        MatrixRaftFsmRuntimeBinding::new(RuntimeSyncFsm::default()),
    )]);
    assert_invalid_request_contains(
        server.sync_fsm_runtimes_on_group(844, &mut incomplete_bindings),
        "binding missing for node 2 in group 844",
    );
    assert_invalid_request_contains(
        server.sync_fsm_runtimes_on_group(899, &mut meta_bindings),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.sync_fsm_runtimes_for_groups([845, 899], &mut selected_bindings),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_sync_fsm_runtimes_for_groups([899, 844]),
        "group 899 is not registered",
    );

    server.shutdown_all().expect("shutdown fsm sync server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_resolves_membership_addresses_and_timeout_now_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("lookup-meta-1-wal");
    let meta_snap_1 = temp_dir("lookup-meta-1-snapshot");
    let meta_wal_2 = temp_dir("lookup-meta-2-wal");
    let meta_snap_2 = temp_dir("lookup-meta-2-snapshot");
    let data_wal = temp_dir("lookup-data-wal");
    let data_snap = temp_dir("lookup-data-snapshot");
    server
        .create_node(options_for_peer(842, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(842, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(843, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start lookup server");

    let resolved = server
        .resolve_address_on_group(842, 2)
        .expect("resolve peer address on group");
    assert_eq!(resolved.len(), 2);
    assert!(resolved
        .iter()
        .all(|node| node.peer_id == 2 && node.raft_addr == peer(842, 2).raft_addr));
    assert_eq!(
        server
            .resolve_address_for_groups([842, 843], 2)
            .expect("resolve peer address on meta and data groups")
            .iter()
            .map(|(group_id, nodes)| {
                (
                    *group_id,
                    nodes.iter().map(|node| node.peer_id).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(842, vec![2, 2]), (843, vec![2])]
    );
    let resolve_plan = server
        .plan_resolve_address_for_groups([842, 843], 2)
        .expect("plan resolve peer address on meta and data groups");
    assert_eq!(resolve_plan.operation, "resolve_address:2");
    assert_eq!(resolve_plan.group_count, 2);
    assert_eq!(resolve_plan.node_count, 3);
    assert_eq!(
        server
            .resolve_address_on_node(842, 1, 1)
            .expect("resolve peer on node")
            .snapshot_addr,
        peer(842, 1).snapshot_addr
    );

    let memberships = server
        .memberships_on_group(842)
        .expect("memberships on meta group");
    assert_eq!(memberships.len(), 2);
    assert!(memberships.iter().all(|members| {
        members.iter().any(|member| member.peer_id == 1)
            && members.iter().any(|member| member.peer_id == 2)
    }));
    assert_eq!(
        server
            .memberships_for_groups([842, 843])
            .expect("memberships on meta and data groups")
            .iter()
            .map(|(group_id, memberships)| (*group_id, memberships.len()))
            .collect::<Vec<_>>(),
        vec![(842, 2), (843, 1)]
    );
    let membership_plan = server
        .plan_memberships_for_groups([842, 843])
        .expect("plan memberships on meta and data groups");
    assert_eq!(membership_plan.operation, "memberships");
    assert_eq!(membership_plan.node_count, 3);
    let member_details = server
        .membership_members_on_group(842)
        .expect("membership member details on meta group");
    assert_eq!(member_details.len(), 2);
    assert!(member_details.iter().all(|members| {
        members.iter().any(|member| {
            member.id == 2
                && member.conf_state == MatrixRaftConfState::Voter
                && member.raft_addr == peer(842, 2).raft_addr
        })
    }));
    assert_eq!(
        server
            .membership_members_for_groups([842, 843])
            .expect("membership member details on meta and data groups")
            .iter()
            .map(|(group_id, members)| (*group_id, members.len()))
            .collect::<Vec<_>>(),
        vec![(842, 2), (843, 1)]
    );
    let member_plan = server
        .plan_membership_members_for_groups([842, 843])
        .expect("plan membership member details on meta and data groups");
    assert_eq!(member_plan.operation, "membership_members");
    assert_eq!(
        member_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(842, 1),
            MatrixRaftRouteKey::new(842, 2),
            MatrixRaftRouteKey::new(843, 1),
        ]
    );
    assert_eq!(
        server
            .memberships_on_group(843)
            .expect("data memberships")
            .len(),
        1
    );

    assert_eq!(
        server
            .callback_scheduler_len_on_node(842, 1)
            .expect("callback scheduler length on node"),
        0
    );
    assert_eq!(
        server
            .callback_scheduler_lens_on_group(842)
            .expect("callback scheduler lengths on meta group"),
        vec![0, 0]
    );
    assert_eq!(
        server
            .callback_scheduler_lens_for_groups([842, 843])
            .expect("callback scheduler lengths on selected groups"),
        vec![(842, vec![0, 0]), (843, vec![0])]
    );
    let callback_len_plan = server
        .plan_callback_scheduler_lens_for_groups([842, 843])
        .expect("plan selected callback scheduler lengths");
    assert_eq!(callback_len_plan.operation, "callback_scheduler_lens");
    assert_eq!(callback_len_plan.group_count, 2);
    assert_eq!(callback_len_plan.node_count, 3);
    assert_eq!(
        server
            .callback_scheduler_next_timeout_ms_on_node(842, 1, 100)
            .expect("callback scheduler next timeout on node"),
        u64::MAX
    );
    assert_eq!(
        server
            .callback_scheduler_next_timeout_ms_on_group(842, 100)
            .expect("callback scheduler next timeout on meta group"),
        vec![u64::MAX, u64::MAX]
    );
    assert_eq!(
        server
            .callback_scheduler_next_timeout_ms_for_groups([842, 843], 100)
            .expect("callback scheduler next timeout on selected groups"),
        vec![(842, vec![u64::MAX, u64::MAX]), (843, vec![u64::MAX])]
    );
    let callback_timeout_plan = server
        .plan_callback_scheduler_next_timeout_ms_for_groups([842, 843], 100)
        .expect("plan selected callback scheduler next timeout");
    assert_eq!(
        callback_timeout_plan.operation,
        "callback_scheduler_next_timeout_ms:100"
    );
    assert_eq!(callback_timeout_plan.node_count, 3);
    assert_eq!(
        server
            .drain_lapsed_callbacks_on_node(842, 1, 100, 1)
            .expect("drain lapsed callbacks on node"),
        Vec::<MatrixRaftAsyncResult>::new()
    );
    let timeout_result = MatrixRaftAsyncResult::timeout(MatrixRaftAsyncOperation::Propose, 250);
    assert_eq!(
        timeout_result.status(),
        MatrixRaftAsyncResultStatus::TimedOut
    );
    assert!(timeout_result.is_timed_out());
    assert!(!timeout_result.is_ok());
    assert!(!timeout_result.is_error());
    let timeout_summary = MatrixRaftAsyncGroupSummary::from_results(
        842,
        &[(MatrixRaftRouteKey::new(842, 1), timeout_result)],
    );
    assert_eq!(
        timeout_summary.status_by_route_key(),
        vec![(
            MatrixRaftRouteKey::new(842, 1),
            MatrixRaftAsyncResultStatus::TimedOut
        )]
    );
    assert_eq!(
        timeout_summary.timed_out_presence_by_route_key(),
        vec![(MatrixRaftRouteKey::new(842, 1), true)]
    );
    assert_eq!(
        server
            .drain_lapsed_callbacks_on_group(842, 100, 1)
            .expect("drain lapsed callbacks on meta group"),
        vec![
            Vec::<MatrixRaftAsyncResult>::new(),
            Vec::<MatrixRaftAsyncResult>::new()
        ]
    );
    assert_eq!(
        server
            .drain_lapsed_callbacks_for_groups([842, 843], 100, 1)
            .expect("drain lapsed callbacks on selected groups"),
        vec![
            (
                842,
                vec![
                    Vec::<MatrixRaftAsyncResult>::new(),
                    Vec::<MatrixRaftAsyncResult>::new()
                ]
            ),
            (843, vec![Vec::<MatrixRaftAsyncResult>::new()])
        ]
    );
    let drain_callback_plan = server
        .plan_drain_lapsed_callbacks_for_groups([842, 843], 100, 1)
        .expect("plan selected lapsed callback drain");
    assert_eq!(
        drain_callback_plan.operation,
        "drain_lapsed_callbacks:100:1"
    );
    assert_eq!(
        drain_callback_plan.operation_name(),
        "drain_lapsed_callbacks"
    );
    assert_eq!(
        drain_callback_plan.operation_arguments(),
        vec!["100".to_string(), "1".to_string()]
    );
    assert_eq!(drain_callback_plan.operation_argument_count(), 2);
    assert_eq!(drain_callback_plan.node_count, 3);
    assert_eq!(
        drain_callback_plan.fanout_counts_by_operation(),
        vec![("drain_lapsed_callbacks".to_string(), 2, 3, 3)]
    );
    assert_eq!(
        drain_callback_plan.operation_names_by_group(),
        vec![
            (842, "drain_lapsed_callbacks".to_string()),
            (843, "drain_lapsed_callbacks".to_string()),
        ]
    );
    assert_eq!(
        drain_callback_plan.operation_arguments_by_group(),
        vec![
            (842, vec!["100".to_string(), "1".to_string()]),
            (843, vec!["100".to_string(), "1".to_string()]),
        ]
    );
    assert_eq!(
        drain_callback_plan.operation_argument_counts_by_group(),
        vec![(842, 2), (843, 2)]
    );
    assert_eq!(
        server
            .cancel_callback_on_node(842, 1, 9001)
            .expect("cancel missing callback on node"),
        None
    );
    assert_eq!(
        server
            .cancel_callback_on_group(842, 9001)
            .expect("cancel missing callback on meta group"),
        vec![None, None]
    );
    assert_eq!(
        server
            .cancel_callback_for_groups([842, 843], 9001)
            .expect("cancel missing callback on selected groups"),
        vec![(842, vec![None, None]), (843, vec![None])]
    );
    let cancel_callback_plan = server
        .plan_cancel_callback_for_groups([842, 843], 9001)
        .expect("plan selected callback cancel");
    assert_eq!(cancel_callback_plan.operation, "cancel_callback:9001");
    assert_eq!(cancel_callback_plan.node_count, 3);

    let timeout = server
        .timeout_now_on_node(842, 2, 1, 2)
        .expect("timeout-now on node");
    assert_eq!(timeout.node_id, 2);
    assert_eq!(timeout.from, 1);
    let best_effort = server
        .timeout_now_on_group_best_effort(842, 1, 2)
        .expect("timeout-now best effort on group");
    assert_eq!(best_effort.len(), 2);
    assert!(best_effort.iter().all(|result| {
        result.result.as_ref().is_some_and(|route| {
            route
                .timeout_now_response
                .as_ref()
                .is_some_and(|response| response.from == 1)
        }) || result.error.is_some()
    }));
    assert_eq!(
        server
            .timeout_now_for_groups([842, 843], 1, 2)
            .expect("timeout-now on selected meta and data groups")
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(842, 2), (843, 1)]
    );
    let timeout_plan = server
        .plan_timeout_now_for_groups([842, 843], 1, 2)
        .expect("plan timeout-now on selected meta and data groups");
    assert_eq!(timeout_plan.message_type, MatrixRaftMessageType::TimeoutNow);
    assert_eq!(timeout_plan.group_count, 2);
    assert_eq!(timeout_plan.node_count, 3);
    let selected_timeout = server
        .timeout_now_for_groups_best_effort([842, 843], 1, 2)
        .expect("timeout-now best effort on selected meta and data groups");
    assert_eq!(
        selected_timeout
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(842, 2), (843, 1)]
    );
    assert!(selected_timeout.iter().all(|(_, results)| {
        results.iter().any(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .timeout_now_response
                    .as_ref()
                    .is_some_and(|response| response.from == 1 && response.node_id == 2)
            })
        })
    }));
    let selected_timeout_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_timeout);
    assert!(selected_timeout_summaries.iter().all(|summary| {
        summary
            .timeout_now_response_presence_by_route_key()
            .iter()
            .any(|(_, present)| *present)
            && summary
                .vote_response_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));

    assert_eq!(
        server.resolve_address_on_node(842, 99, 1),
        Err(RaftError::NodeNotFound(99))
    );
    assert_invalid_request_contains(
        server.resolve_address_on_group(899, 1),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.memberships_for_groups([842, 899]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_membership_members_for_groups([899, 842]),
        "group 899 is not registered",
    );
    assert_eq!(
        server.callback_scheduler_len_on_node(842, 99),
        Err(RaftError::NodeNotFound(99))
    );
    assert_invalid_request_contains(
        server.plan_callback_scheduler_lens_for_groups([899, 842]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_drain_lapsed_callbacks_for_groups([899, 842], 100, 1),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.timeout_now_for_groups([899, 842], 1, 2),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_timeout_now_for_groups([899, 842], 1, 2),
        "group 899 is not registered",
    );

    server.shutdown_all().expect("shutdown lookup server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_routes_admin_commands_to_group_nodes() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let group_wal_1 = temp_dir("admin-group-1-wal");
    let group_snap_1 = temp_dir("admin-group-1-snapshot");
    let group_wal_2 = temp_dir("admin-group-2-wal");
    let group_snap_2 = temp_dir("admin-group-2-snapshot");
    let data_wal = temp_dir("admin-data-wal");
    let data_snap = temp_dir("admin-data-snapshot");
    server
        .create_node(options_for_peer(824, 1, &group_wal_1, &group_snap_1), 1)
        .expect("group node 1");
    server
        .create_node(options_for_peer(824, 2, &group_wal_2, &group_snap_2), 1)
        .expect("group node 2");
    server
        .create_node(options(825, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start admin group server");

    let admin_results = server
        .route_admin_command_to_group(824, MatrixRaftAdminCommand::set_node_healthy(3, false))
        .expect("group admin route");
    assert_eq!(admin_results.len(), 2);
    assert_eq!(
        admin_results
            .iter()
            .map(|result| result.key.node_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(admin_results
        .iter()
        .all(|result| result.key.group_id == 824));
    assert!(admin_results.iter().all(|result| result.handled));
    assert!(admin_results
        .iter()
        .all(|result| result.node_healthy == Some(false)));
    assert!(admin_results.iter().all(|result| {
        result
            .snapshot_peer_report
            .as_ref()
            .map(|report| report.peer_id)
            == Some(3)
    }));
    assert_eq!(server.group_statuses(825).expect("data group").len(), 1);

    let best_effort = server
        .route_admin_command_to_group_best_effort(824, MatrixRaftAdminCommand::release_memory())
        .expect("best-effort group admin route");
    assert_eq!(best_effort.len(), 2);
    assert!(best_effort.iter().all(|result| result.is_ok()));
    assert!(best_effort.iter().all(|result| result.group_id == 824));
    assert!(best_effort
        .iter()
        .all(|result| result.message_type == MatrixRaftMessageType::AdminCommand));

    assert_invalid_request_contains(
        server.route_admin_command_to_group(899, MatrixRaftAdminCommand::release_memory()),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.route_admin_command_to_group_best_effort(
            899,
            MatrixRaftAdminCommand::release_memory(),
        ),
        "group 899 is not registered",
    );

    server.shutdown_all().expect("shutdown admin group server");

    let _ = fs::remove_dir_all(group_wal_1);
    let _ = fs::remove_dir_all(group_snap_1);
    let _ = fs::remove_dir_all(group_wal_2);
    let _ = fs::remove_dir_all(group_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_controls_peer_health_and_reorder_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("peer-health-meta-1-wal");
    let meta_snap_1 = temp_dir("peer-health-meta-1-snapshot");
    let meta_wal_2 = temp_dir("peer-health-meta-2-wal");
    let meta_snap_2 = temp_dir("peer-health-meta-2-snapshot");
    let data_wal = temp_dir("peer-health-data-wal");
    let data_snap = temp_dir("peer-health-data-snapshot");
    server
        .create_node(options_for_peer(856, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(856, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(857, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start peer health server");

    let peer_health_plans = [
        server
            .plan_partition_peer_for_groups([856, 857], 3)
            .expect("plan selected peer partition"),
        server
            .plan_heal_peer_for_groups([856, 857], 3)
            .expect("plan selected peer heal"),
        server
            .plan_set_node_healthy_for_groups([856, 857], 3, false)
            .expect("plan selected node health"),
        server
            .plan_fire_fatal_event_for_groups([856, 857], 3, "selected fatal")
            .expect("plan selected fatal event"),
        server
            .plan_expire_peer_reorder_queue_for_groups([856, 857], 3)
            .expect("plan selected reorder expiry"),
    ];
    assert_eq!(
        peer_health_plans
            .iter()
            .map(|plan| plan.command_type)
            .collect::<Vec<_>>(),
        vec![
            MatrixRaftAdminCommandType::PartitionPeer,
            MatrixRaftAdminCommandType::HealPeer,
            MatrixRaftAdminCommandType::SetNodeHealthy,
            MatrixRaftAdminCommandType::FireFatalEvent,
            MatrixRaftAdminCommandType::ExpirePeerReorderQueue,
        ]
    );
    assert!(peer_health_plans.iter().all(|plan| {
        plan.group_count == 2
            && plan.node_count == 3
            && plan.route_keys
                == vec![
                    MatrixRaftRouteKey::new(856, 1),
                    MatrixRaftRouteKey::new(856, 2),
                    MatrixRaftRouteKey::new(857, 1),
                ]
    }));

    let partitioned = server
        .partition_peer_for_groups([856, 857], 3)
        .expect("partition peer on selected groups");
    assert!(partitioned.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| report.peer_id == 3 && report.peer_healthy == Some(false))
        })
    }));
    let healthy = server
        .set_node_healthy_for_groups([856, 857], 3, true)
        .expect("mark peer healthy on selected groups");
    assert!(healthy.iter().all(|(_, results)| results
        .iter()
        .all(|result| result.node_healthy == Some(true))));
    let healthy_summaries = MatrixRaftRouteGroupSummary::from_grouped_results(&healthy);
    assert!(healthy_summaries.iter().all(|summary| summary
        .node_healthy_presence_by_route_key()
        .iter()
        .all(|(_, present)| *present)));
    assert!(healthy_summaries.iter().all(|summary| summary
        .node_healthy_values_by_route_key()
        .iter()
        .all(|(_, healthy)| *healthy == Some(true))));
    let healed = server
        .heal_peer_for_groups_best_effort([856, 857], 3)
        .expect("best-effort heal peer on selected groups");
    assert!(healed.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .snapshot_peer_report
                    .as_ref()
                    .is_some_and(|report| report.peer_id == 3)
            })
        })
    }));

    let network_plan = server
        .plan_network_error_for_groups([856, 857], 1, 3)
        .expect("plan selected network errors");
    assert_eq!(
        network_plan.message_type,
        MatrixRaftMessageType::NetworkError
    );
    assert_eq!(network_plan.group_count, 2);
    assert_eq!(network_plan.node_count, 3);
    let network_errors = server
        .network_error_for_groups([856, 857], 1, 3)
        .expect("record selected network errors");
    assert!(network_errors.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| report.status.packet_loss_events == 1)
        })
    }));

    server
        .begin_snapshot_send_for_groups([856, 857], 3, "peer-health-progress", 3, 2)
        .expect("begin selected snapshot send for progress");
    let progress = MatrixRaftSnapshotProgress {
        remote_receiving: true,
        elapsed_since_last_receiving_ms: 25,
        send_timeout_ms: 100,
    };
    let progress_plan = server
        .plan_snapshot_progress_for_groups([856, 857], 3, 1, progress.clone())
        .expect("plan selected snapshot progress");
    assert_eq!(
        progress_plan.message_type,
        MatrixRaftMessageType::SnapshotProgress
    );
    assert_eq!(progress_plan.node_count, 3);
    let progressed = server
        .snapshot_progress_for_groups_best_effort([856, 857], 3, 1, progress)
        .expect("best-effort selected snapshot progress");
    assert!(progressed.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .snapshot_peer_report
                    .as_ref()
                    .is_some_and(|report| report.status.snapshot_sending)
            })
        })
    }));

    let future_entry = MatrixRaftEntry {
        term: 1,
        index: 9,
        entry_type: MatrixRaftEntryType::Normal,
        propose: Some(MatrixRaftPropose {
            request_id: Some(856_009),
            data: b"selected-future-entry".to_vec(),
            context: Vec::new(),
            is_command: true,
        }),
        config_change: None,
        memberships: Vec::new(),
        request_id: 856_009,
        bytes_size: 21,
    };
    let reorder_plan = server
        .plan_receive_out_of_order_append_for_groups([856, 857], 3, future_entry.clone())
        .expect("plan selected out-of-order append");
    assert_eq!(
        reorder_plan.command_type,
        MatrixRaftAdminCommandType::ReceiveOutOfOrderAppend
    );
    assert_eq!(reorder_plan.node_count, 3);
    let reordered = server
        .receive_out_of_order_append_for_groups([856, 857], 3, future_entry)
        .expect("receive out-of-order append on selected groups");
    assert!(reordered.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| report.status.reorder_queue_depth == 1)
        })
    }));
    let expired = server
        .expire_peer_reorder_queue_for_groups([856, 857], 3)
        .expect("expire peer reorder queue on selected groups");
    assert!(expired.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.reorder_queue_dropped == Some(1)
                && result
                    .snapshot_peer_report
                    .as_ref()
                    .is_some_and(|report| report.status.reorder_queue_depth == 0)
        })
    }));
    let expired_summaries = MatrixRaftRouteGroupSummary::from_grouped_results(&expired);
    assert!(expired_summaries.iter().all(|summary| summary
        .reorder_queue_dropped_presence_by_route_key()
        .iter()
        .all(|(_, present)| *present)));
    assert!(expired_summaries.iter().all(|summary| summary
        .reorder_queue_dropped_values_by_route_key()
        .iter()
        .all(|(_, dropped)| *dropped == Some(1))));

    let fatal = server
        .fire_fatal_event_for_groups_best_effort([856, 857], 3, "selected fatal")
        .expect("best-effort fatal event on selected groups");
    assert!(fatal
        .iter()
        .all(|(_, results)| results.iter().all(|result| result.is_ok())));
    let fatal_summaries = MatrixRaftBatchRouteGroupSummary::from_grouped_results(&fatal);
    assert!(fatal_summaries.iter().all(|summary| summary
        .fatal_event_transfer_target_presence_by_route_key()
        .iter()
        .all(|(_, present)| !*present)));
    assert!(fatal_summaries.iter().all(|summary| summary
        .fatal_event_transfer_targets_by_route_key()
        .iter()
        .all(|(_, target)| target.is_none())));
    let fatal_blocker_plan = server
        .plan_fatal_blockers_for_groups([856, 857])
        .expect("plan selected fatal blockers");
    assert_eq!(fatal_blocker_plan.operation, "fatal_blockers");
    assert_eq!(fatal_blocker_plan.group_count, 2);
    assert_eq!(fatal_blocker_plan.node_count, 3);
    assert_eq!(
        fatal_blocker_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(856, 1),
            MatrixRaftRouteKey::new(856, 2),
            MatrixRaftRouteKey::new(857, 1),
        ]
    );
    assert_eq!(
        server
            .fatal_blockers_on_group(856)
            .expect("fatal blockers on meta group")
            .len(),
        2
    );
    assert!(server
        .fatal_blockers_on_node(856, 1)
        .expect("fatal blockers on node")
        .iter()
        .any(|blocker| blocker.id.contains("fatal_event:3:")));
    let selected_blockers = server
        .fatal_blockers_for_groups([856, 857])
        .expect("selected fatal blockers");
    assert_eq!(
        selected_blockers
            .iter()
            .map(|(group_id, blockers)| (*group_id, blockers.len()))
            .collect::<Vec<_>>(),
        vec![(856, 2), (857, 1)]
    );
    assert!(selected_blockers.iter().all(|(_, blockers)| {
        blockers.iter().all(|node_blockers| {
            node_blockers
                .iter()
                .any(|blocker| blocker.id.contains("fatal_event:3:"))
        })
    }));
    let fatal_event_plan = server
        .plan_fatal_events_for_groups([856, 857])
        .expect("plan selected fatal events");
    assert_eq!(fatal_event_plan.operation, "fatal_events");
    assert_eq!(fatal_event_plan.group_count, 2);
    assert_eq!(fatal_event_plan.node_count, 3);
    assert_eq!(
        server
            .fatal_events_on_group(856)
            .expect("fatal events on meta group")
            .len(),
        2
    );
    assert!(server
        .fatal_events_on_node(856, 1)
        .expect("fatal events on node")
        .iter()
        .any(|event| event.node_id == Some(3) && event.reason == "selected fatal"));
    let selected_events = server
        .fatal_events_for_groups([856, 857])
        .expect("selected fatal events");
    assert_eq!(
        selected_events
            .iter()
            .map(|(group_id, events)| (*group_id, events.len()))
            .collect::<Vec<_>>(),
        vec![(856, 2), (857, 1)]
    );
    assert!(selected_events.iter().all(|(_, events)| {
        events.iter().all(|node_events| {
            node_events
                .iter()
                .any(|event| event.node_id == Some(3) && event.reason == "selected fatal")
        })
    }));
    assert_eq!(
        server
            .set_node_healthy_on_node(856, 1, 3, true)
            .expect("restore peer health on node")
            .node_healthy,
        Some(true)
    );

    assert_eq!(
        server.partition_peer_on_node(856, 99, 3),
        Err(RaftError::NodeNotFound(99))
    );
    assert_invalid_request_contains(
        server.plan_set_node_healthy_for_groups([899, 856], 3, false),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_network_error_for_groups([899, 856], 1, 3),
        "group 899 is not registered",
    );
    assert_eq!(
        server.fatal_events_on_node(856, 99),
        Err(RaftError::NodeNotFound(99))
    );
    assert_invalid_request_contains(
        server.plan_fatal_events_for_groups([899, 856]),
        "group 899 is not registered",
    );

    server.shutdown_all().expect("shutdown peer health server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
// Forwarded reads fail closed (return errors, never panic) for unknown targets.
fn matrixraft_forwarded_read_index_reports_errors_for_unknown_targets() {
    let transport = MatrixRaftTransportBuilder::new()
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let server = MatrixRaftMultiRaftServer::new(context);
    let opts = MatrixRaftReadIndexOptions::quorum_read(1);

    // A group with no registered nodes is an explicit error, not a panic.
    assert!(server.forwarded_read_index_for_group(4242, opts).is_err());
    // An unknown node in the group is a NodeNotFound error, not a panic.
    assert!(server.forwarded_read_index_on_node(4242, 7, opts).is_err());
}

#[test]
// Option B (docs/read_index_safety_review.md): linearizable follower reads via
// leader-confirmed ReadIndex forwarding. A follower's own read reports `not_leader`;
// forwarding obtains a quorum-confirmed read index from the leader and serves the
// follower's read as safe only once it has applied up to that index.
fn matrixraft_forwarded_follower_read_index_is_linearizable() {
    let transport = MatrixRaftTransportBuilder::new()
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let wal_1 = temp_dir("fwd-read-1-wal");
    let snap_1 = temp_dir("fwd-read-1-snapshot");
    let wal_2 = temp_dir("fwd-read-2-wal");
    let snap_2 = temp_dir("fwd-read-2-snapshot");
    server
        .create_node(options_for_peer(900, 1, &wal_1, &snap_1), 1)
        .expect("node 1");
    server
        .create_node(options_for_peer(900, 2, &wal_2, &snap_2), 1)
        .expect("node 2");
    server.start_all(1).expect("start");

    // Leader (node 1) commits an entry, establishing a quorum-confirmed read index.
    let log = server
        .propose_to_node_with_options(
            900,
            1,
            MatrixRaftProposeOptions {
                with_term: Some(1),
                is_command: true,
            },
            b"fwd-read-write".to_vec(),
        )
        .expect("leader propose");
    let opts = MatrixRaftReadIndexOptions::quorum_read(log.index);

    // Leader is served directly: forwarding matches the leader's own read.
    let leader_direct = server
        .node(900, 1)
        .expect("node 1")
        .read_index_with_options(opts)
        .expect("leader read");
    assert!(leader_direct.safe);
    assert_eq!(leader_direct.read_index, log.index);
    assert_eq!(
        server
            .forwarded_read_index_on_node(900, 1, opts)
            .expect("leader forwarded"),
        leader_direct
    );

    // A follower cannot certify a linearizable read from its local state alone.
    let follower_local = server
        .node(900, 2)
        .expect("node 2")
        .read_index_with_options(opts)
        .expect("follower local read");
    assert!(!follower_local.safe);
    assert_eq!(follower_local.reason, "not_leader");

    // Forwarding hands the follower the leader-confirmed read index and reports
    // safety honestly against the follower's applied index — never faking it.
    let follower_applied = server
        .node(900, 2)
        .expect("node 2")
        .get_status()
        .expect("follower status")
        .applied_index;
    let follower_fwd = server
        .forwarded_read_index_on_node(900, 2, opts)
        .expect("follower forwarded");
    assert_eq!(follower_fwd.read_index, leader_direct.read_index);
    assert!(!follower_fwd.lease_read);
    assert_eq!(
        follower_fwd.safe,
        follower_applied >= leader_direct.read_index
    );
    // A freshly-created follower has not applied the leader's new entry, so its
    // forwarded read is honestly pending rather than falsely safe.
    assert!(!follower_fwd.safe);
    assert_eq!(follower_fwd.reason, "follower_apply_pending");

    // The group fanout serves the leader safely and each follower via forwarding;
    // every response carries the leader-confirmed read index, and no node reports
    // safety it cannot back with applied state.
    let group = server
        .forwarded_read_index_for_group(900, opts)
        .expect("group forwarded");
    assert_eq!(
        group.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
        vec![
            MatrixRaftRouteKey::new(900, 1),
            MatrixRaftRouteKey::new(900, 2),
        ]
    );
    assert!(group[0].1.safe);
    for (key, resp) in &group {
        assert_eq!(resp.read_index, leader_direct.read_index);
        let applied = server
            .node(900, key.node_id)
            .expect("node")
            .get_status()
            .expect("status")
            .applied_index;
        assert_eq!(resp.safe, applied >= leader_direct.read_index);
    }

    // Forwarding is strictly safer than a follower's local read: the follower's own
    // read can never be safe (`not_leader`), whereas the forwarded read carries the
    // leader's quorum-confirmed index and would certify safe once the follower has
    // applied up to it. The `follower_fwd.safe == (applied >= read_index)` assertion
    // above exercises that decision for the follower's real applied state.
    assert!(!follower_local.safe && follower_fwd.read_index == leader_direct.read_index);
}

#[test]
// A healthy multi-raft server exposes direct group propose and read paths. The
// read-index fanout certifies the quorum-confirmed leader's read as safe and
// reports honest bounded-stale status (Some(false) + reason) for followers that
// have not yet applied up to the read floor — it never fakes linearizable safety.
// See docs/read_index_safety_review.md.
fn matrixraft_multi_raft_server_exposes_direct_group_propose_and_read_paths() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("direct-meta-1-wal");
    let meta_snap_1 = temp_dir("direct-meta-1-snapshot");
    let meta_wal_2 = temp_dir("direct-meta-2-wal");
    let meta_snap_2 = temp_dir("direct-meta-2-snapshot");
    let data_wal = temp_dir("direct-data-wal");
    let data_snap = temp_dir("direct-data-snapshot");
    server
        .create_node(options_for_peer(826, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(826, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(827, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start direct api server");

    let meta_log = server
        .propose_to_node_with_options(
            826,
            1,
            MatrixRaftProposeOptions {
                with_term: Some(1),
                is_command: true,
            },
            b"meta-group-direct-write".to_vec(),
        )
        .expect("direct meta propose");
    assert_eq!(meta_log.index, 2);
    assert_eq!(
        server
            .read_index_on_node(826, 1, meta_log.index)
            .expect("direct meta read")
            .read_index,
        meta_log.index
    );

    assert!(server
        .propose_to_group_nodes(826, b"meta-group-strict-fanout".to_vec())
        .is_err());

    let group_propose_payload = b"meta-group-planned-fanout".to_vec();
    let group_propose_plan = server
        .plan_propose_with_options_on_group(
            826,
            MatrixRaftProposeOptions {
                with_term: Some(1),
                is_command: true,
            },
            &group_propose_payload,
        )
        .expect("group propose fanout plan");
    assert_eq!(group_propose_plan.group_id, 826);
    assert_eq!(group_propose_plan.node_ids, vec![1, 2]);
    assert_eq!(
        group_propose_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(826, 1),
            MatrixRaftRouteKey::new(826, 2)
        ]
    );
    assert_eq!(group_propose_plan.node_count, 2);
    assert_eq!(
        group_propose_plan.payload_bytes,
        group_propose_payload.len()
    );
    assert_eq!(
        group_propose_plan.options,
        MatrixRaftProposeOptions {
            with_term: Some(1),
            is_command: true,
        }
    );

    let fanout_results = server
        .propose_to_group_nodes_best_effort(826, b"meta-group-fanout-write".to_vec())
        .expect("best-effort group fanout propose");
    assert_eq!(fanout_results.len(), 2);
    assert_eq!(
        fanout_results
            .iter()
            .map(|result| result.runtime_node_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        fanout_results
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        1
    );
    assert_eq!(
        fanout_results
            .iter()
            .filter(|result| result.error.is_some())
            .count(),
        1
    );
    let fanout_log = fanout_results
        .iter()
        .find_map(|result| result.result.as_ref())
        .and_then(|result| result.proposed_log_id.as_ref())
        .expect("fanout leader log");
    assert_eq!(
        fanout_results
            .iter()
            .find(|result| result.is_ok())
            .map(|result| result.runtime_node_id),
        Some(1)
    );
    assert_eq!(
        fanout_results
            .iter()
            .find(|result| result.error.is_some())
            .map(|result| result.runtime_node_id),
        Some(2)
    );
    assert_eq!(
        server
            .node(826, 1)
            .expect("meta leader node")
            .get_status()
            .expect("meta leader status")
            .last_log_index,
        fanout_log.index
    );

    let reads = server
        .group_read_indexes_with_options(
            826,
            MatrixRaftReadIndexOptions::quorum_read(fanout_log.index),
        )
        .expect("group read indexes");
    assert_eq!(reads.len(), 2);
    assert!(reads.iter().any(|read| read.safe));
    assert!(reads.iter().any(|read| !read.safe));
    assert!(reads.iter().all(|read| read.read_index <= fanout_log.index));
    assert_eq!(
        server
            .read_index_on_node_with_options(
                826,
                1,
                MatrixRaftReadIndexOptions::quorum_read(fanout_log.index),
            )
            .expect("direct leader quorum read")
            .read_index,
        fanout_log.index
    );

    let data_log = server
        .propose_to_node(827, 1, b"data-node-direct-write".to_vec())
        .expect("direct data propose");
    assert_eq!(data_log.index, 2);
    assert_eq!(
        server
            .group_read_indexes(827, data_log.index)
            .expect("data group read")
            .len(),
        1
    );
    assert!(server
        .propose_to_groups([826, 827], b"selected-strict-fanout".to_vec())
        .is_err());
    let selected_propose_payload = b"selected-planned-fanout".to_vec();
    let selected_propose_plan = server
        .plan_propose_with_options_for_groups(
            [826, 827],
            MatrixRaftProposeOptions {
                with_term: Some(1),
                is_command: true,
            },
            &selected_propose_payload,
        )
        .expect("selected propose fanout plan");
    assert_eq!(selected_propose_plan.group_count, 2);
    assert_eq!(selected_propose_plan.group_ids, vec![826, 827]);
    assert_eq!(selected_propose_plan.node_count, 3);
    assert_eq!(
        selected_propose_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(826, 1),
            MatrixRaftRouteKey::new(826, 2),
            MatrixRaftRouteKey::new(827, 1),
        ]
    );
    assert_eq!(
        selected_propose_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone(), group.payload_bytes))
            .collect::<Vec<_>>(),
        vec![
            (826, vec![1, 2], selected_propose_payload.len()),
            (827, vec![1], selected_propose_payload.len()),
        ]
    );
    assert_eq!(
        selected_propose_plan.route_keys_by_group(),
        vec![
            (
                826,
                vec![
                    MatrixRaftRouteKey::new(826, 1),
                    MatrixRaftRouteKey::new(826, 2),
                ],
            ),
            (827, vec![MatrixRaftRouteKey::new(827, 1)]),
        ]
    );
    assert_eq!(
        selected_propose_plan.node_ids_by_group(),
        vec![(826, vec![1, 2]), (827, vec![1])]
    );
    assert_eq!(
        selected_propose_plan.node_counts_by_group(),
        vec![(826, 2), (827, 1)]
    );
    assert_eq!(
        selected_propose_plan.route_key_counts_by_group(),
        vec![(826, 2), (827, 1)]
    );
    assert_eq!(
        selected_propose_plan.fanout_counts_by_group(),
        vec![(826, 2, 2), (827, 1, 1)]
    );
    assert_eq!(
        selected_propose_plan.options_by_group(),
        vec![
            (
                826,
                MatrixRaftProposeOptions {
                    with_term: Some(1),
                    is_command: true,
                },
            ),
            (
                827,
                MatrixRaftProposeOptions {
                    with_term: Some(1),
                    is_command: true,
                },
            ),
        ]
    );
    assert_eq!(
        selected_propose_plan.options_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(826, 1),
                MatrixRaftProposeOptions {
                    with_term: Some(1),
                    is_command: true,
                },
            ),
            (
                MatrixRaftRouteKey::new(826, 2),
                MatrixRaftProposeOptions {
                    with_term: Some(1),
                    is_command: true,
                },
            ),
            (
                MatrixRaftRouteKey::new(827, 1),
                MatrixRaftProposeOptions {
                    with_term: Some(1),
                    is_command: true,
                },
            ),
        ]
    );
    assert_eq!(
        selected_propose_plan.terms_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(826, 1), Some(1)),
            (MatrixRaftRouteKey::new(826, 2), Some(1)),
            (MatrixRaftRouteKey::new(827, 1), Some(1)),
        ]
    );
    assert_eq!(
        selected_propose_plan.terms_by_group(),
        vec![(826, Some(1)), (827, Some(1))]
    );
    assert_eq!(
        selected_propose_plan.command_values_by_group(),
        vec![(826, true), (827, true)]
    );
    assert_eq!(
        selected_propose_plan.command_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(826, 1), true),
            (MatrixRaftRouteKey::new(826, 2), true),
            (MatrixRaftRouteKey::new(827, 1), true),
        ]
    );
    assert_eq!(
        selected_propose_plan.payload_bytes_by_group(),
        vec![
            (826, selected_propose_payload.len()),
            (827, selected_propose_payload.len()),
        ]
    );
    assert_eq!(
        selected_propose_plan.payload_bytes_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(826, 1),
                selected_propose_payload.len(),
            ),
            (
                MatrixRaftRouteKey::new(826, 2),
                selected_propose_payload.len(),
            ),
            (
                MatrixRaftRouteKey::new(827, 1),
                selected_propose_payload.len(),
            ),
        ]
    );
    assert_eq!(
        selected_propose_plan.payload_bytes,
        selected_propose_payload.len()
    );
    assert_eq!(
        selected_propose_plan.options,
        MatrixRaftProposeOptions {
            with_term: Some(1),
            is_command: true,
        }
    );
    let selected_fanout = server
        .propose_to_groups_with_options_best_effort(
            [826, 827],
            MatrixRaftProposeOptions {
                with_term: Some(1),
                is_command: true,
            },
            b"selected-best-effort-fanout".to_vec(),
        )
        .expect("best-effort selected meta and data propose");
    assert_eq!(
        selected_fanout
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(826, 2), (827, 1)]
    );
    assert_eq!(
        selected_fanout
            .iter()
            .flat_map(|(_, results)| results.iter())
            .filter(|result| result.is_ok())
            .count(),
        2
    );
    assert_eq!(
        selected_fanout
            .iter()
            .flat_map(|(_, results)| results.iter())
            .filter(|result| result.error.is_some())
            .count(),
        1
    );
    assert!(selected_fanout.iter().all(|(_, results)| {
        results.iter().any(|result| {
            result
                .result
                .as_ref()
                .and_then(|route| route.proposed_log_id.as_ref())
                .is_some()
        })
    }));
    let propose_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, bool, Option<LogId>)>::new());
    let propose_callback_results = server
        .propose_with_options_callbacks_for_groups(
            [826, 827],
            MatrixRaftProposeOptions {
                with_term: Some(1),
                is_command: true,
            },
            b"selected-callback-fanout".to_vec(),
            |key| {
                let hits = &propose_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.ok, result.log_id));
                }
            },
            1_000,
        )
        .expect("selected propose callback fanout");
    assert_eq!(
        propose_callback_results
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(826, 2), (827, 1)]
    );
    assert!(propose_callback_results.iter().all(|(_, results)| {
        results.iter().all(|(_, result)| {
            result.has_node_id()
                && result.has_request_id()
                && result.has_deadline()
                && result.status()
                    == if result.ok {
                        MatrixRaftAsyncResultStatus::Ok
                    } else {
                        MatrixRaftAsyncResultStatus::Error
                    }
                && result.has_log_id() == result.ok
                && result.has_error() != result.ok
                && !result.read_index_presence()
                && !result.membership_presence()
                && !result.snapshot_presence()
                && !result.auto_promote_presence()
                && !result.remove_presence()
                && !result.transfer_leader_presence()
                && !result.timeout_now_presence()
                && !result.step_down_presence()
                && !result.resign_presence()
        })
    }));
    let propose_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&propose_callback_results);
    assert_eq!(
        propose_callback_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.result_count,
                summary.ok_count,
                summary.error_count,
                summary.timed_out_count,
                summary.operations.clone(),
                summary.ok_operations.clone(),
                summary.error_operations.clone(),
                summary.timed_out_operations.clone(),
                summary.counts_by_operation.clone(),
                summary.result_counts_by_status(),
                summary.route_key_counts_by_status(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                2,
                1,
                1,
                0,
                vec![MatrixRaftAsyncOperation::Propose],
                vec![MatrixRaftAsyncOperation::Propose],
                vec![MatrixRaftAsyncOperation::Propose],
                Vec::new(),
                vec![(MatrixRaftAsyncOperation::Propose, 2, 1, 1, 0)],
                (1, 1, 0),
                (1, 1, 0),
            ),
            (
                827,
                1,
                1,
                0,
                0,
                vec![MatrixRaftAsyncOperation::Propose],
                vec![MatrixRaftAsyncOperation::Propose],
                Vec::new(),
                Vec::new(),
                vec![(MatrixRaftAsyncOperation::Propose, 1, 1, 0, 0)],
                (1, 0, 0),
                (1, 0, 0),
            ),
        ]
    );
    assert_eq!(
        propose_callback_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.node_ids.clone(),
                summary.ok_node_ids.clone(),
                summary.error_node_ids.clone(),
                summary.timed_out_node_ids.clone(),
                summary.statuses_by_route_key.clone(),
                summary.status_by_route_key(),
                summary.ok_presence_by_route_key(),
                summary.error_presence_by_route_key(),
                summary.timed_out_presence_by_route_key(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![Some(1), Some(2)],
                vec![Some(1)],
                vec![Some(2)],
                Vec::new(),
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
                vec![
                    (
                        MatrixRaftRouteKey::new(826, 1),
                        MatrixRaftAsyncResultStatus::Ok,
                    ),
                    (
                        MatrixRaftRouteKey::new(826, 2),
                        MatrixRaftAsyncResultStatus::Error,
                    ),
                ],
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (
                827,
                vec![Some(1)],
                vec![Some(1)],
                Vec::new(),
                Vec::new(),
                vec![(MatrixRaftRouteKey::new(827, 1), true)],
                vec![(
                    MatrixRaftRouteKey::new(827, 1),
                    MatrixRaftAsyncResultStatus::Ok,
                )],
                vec![(MatrixRaftRouteKey::new(827, 1), true)],
                vec![(MatrixRaftRouteKey::new(827, 1), false)],
                vec![(MatrixRaftRouteKey::new(827, 1), false)],
            ),
        ]
    );
    assert_eq!(
        propose_callback_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.node_ids_by_route_key(),
                summary.ok_node_ids_by_route_key(),
                summary.error_node_ids_by_route_key(),
                summary.timed_out_node_ids_by_route_key(),
                summary.operations_by_route_key.clone(),
                summary.ok_operations_by_route_key(),
                summary.error_operations_by_route_key(),
                summary.timed_out_operations_by_route_key(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), Some(1)),
                    (MatrixRaftRouteKey::new(826, 2), Some(2)),
                ],
                vec![(MatrixRaftRouteKey::new(826, 1), Some(1))],
                vec![(MatrixRaftRouteKey::new(826, 2), Some(2))],
                Vec::<(MatrixRaftRouteKey, Option<u64>)>::new(),
                vec![
                    (
                        MatrixRaftRouteKey::new(826, 1),
                        MatrixRaftAsyncOperation::Propose,
                    ),
                    (
                        MatrixRaftRouteKey::new(826, 2),
                        MatrixRaftAsyncOperation::Propose,
                    ),
                ],
                vec![(
                    MatrixRaftRouteKey::new(826, 1),
                    MatrixRaftAsyncOperation::Propose,
                )],
                vec![(
                    MatrixRaftRouteKey::new(826, 2),
                    MatrixRaftAsyncOperation::Propose,
                )],
                Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)>::new(),
            ),
            (
                827,
                vec![(MatrixRaftRouteKey::new(827, 1), Some(1))],
                vec![(MatrixRaftRouteKey::new(827, 1), Some(1))],
                Vec::<(MatrixRaftRouteKey, Option<u64>)>::new(),
                Vec::<(MatrixRaftRouteKey, Option<u64>)>::new(),
                vec![(
                    MatrixRaftRouteKey::new(827, 1),
                    MatrixRaftAsyncOperation::Propose,
                )],
                vec![(
                    MatrixRaftRouteKey::new(827, 1),
                    MatrixRaftAsyncOperation::Propose,
                )],
                Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)>::new(),
                Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)>::new(),
            ),
        ]
    );
    assert!(propose_callback_summaries[0]
        .errors_by_route_key
        .iter()
        .any(|(key, error)| *key == MatrixRaftRouteKey::new(826, 2) && error.is_some()));
    assert_eq!(
        propose_callback_summaries[1].errors_by_route_key,
        vec![(MatrixRaftRouteKey::new(827, 1), None)]
    );
    assert!(propose_callback_summaries.iter().all(|summary| {
        summary.request_ids.iter().all(Option::is_some)
            && summary.deadline_ms.iter().all(Option::is_some)
            && summary
                .timeout_ms
                .iter()
                .all(|timeout_ms| *timeout_ms == 1_000)
            && summary
                .callback_timing_by_route_key()
                .iter()
                .all(|(_, deadline_ms, timeout_ms)| deadline_ms.is_some() && *timeout_ms == 1_000)
    }));
    assert_eq!(
        propose_callback_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.request_ids.len(),
                summary.ok_request_ids.len(),
                summary.error_request_ids.len(),
                summary.timed_out_request_ids.len(),
                summary.request_ids_by_status(),
                summary.request_id_presence_by_route_key(),
                summary.deadline_presence_by_route_key(),
                summary.log_id_presence_by_route_key(),
                summary.read_index_presence_by_route_key.clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                2,
                1,
                1,
                0,
                (
                    vec![propose_callback_summaries[0].request_ids[0]],
                    vec![propose_callback_summaries[0].request_ids[1]],
                    Vec::new(),
                ),
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (
                827,
                1,
                1,
                0,
                0,
                (
                    vec![propose_callback_summaries[1].request_ids[0]],
                    Vec::new(),
                    Vec::new(),
                ),
                vec![(MatrixRaftRouteKey::new(827, 1), true)],
                vec![(MatrixRaftRouteKey::new(827, 1), true)],
                vec![(MatrixRaftRouteKey::new(827, 1), true)],
                vec![(MatrixRaftRouteKey::new(827, 1), false)],
            ),
        ]
    );
    assert!(propose_callback_summaries.iter().all(|summary| {
        summary
            .membership_presence_by_route_key()
            .iter()
            .all(|(_, present)| !*present)
            && summary
                .snapshot_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
            && summary
                .auto_promote_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
            && summary
                .transfer_leader_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    assert_eq!(
        propose_callback_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.route_keys.clone(),
                summary.ok_route_keys.clone(),
                summary.error_route_keys.clone(),
                summary.timed_out_route_keys.clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    MatrixRaftRouteKey::new(826, 1),
                    MatrixRaftRouteKey::new(826, 2),
                ],
                vec![MatrixRaftRouteKey::new(826, 1)],
                vec![MatrixRaftRouteKey::new(826, 2)],
                Vec::<MatrixRaftRouteKey>::new(),
            ),
            (
                827,
                vec![MatrixRaftRouteKey::new(827, 1)],
                vec![MatrixRaftRouteKey::new(827, 1)],
                Vec::<MatrixRaftRouteKey>::new(),
                Vec::<MatrixRaftRouteKey>::new(),
            ),
        ]
    );
    assert!(!propose_callback_summaries[0].is_ok());
    assert!(propose_callback_summaries[1].is_ok());
    assert_eq!(propose_callback_hits.borrow().len(), 3);
    assert!(propose_callback_hits
        .borrow()
        .iter()
        .any(|(key, ok, log_id)| *key == MatrixRaftRouteKey::new(826, 1)
            && *ok
            && log_id.is_some()));
    assert!(propose_callback_hits
        .borrow()
        .iter()
        .any(|(key, ok, log_id)| *key == MatrixRaftRouteKey::new(827, 1)
            && *ok
            && log_id.is_some()));
    let group_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, bool, Option<LogId>)>::new());
    let group_callback_results = server
        .propose_callbacks_on_group(
            827,
            b"data-group-callback-fanout".to_vec(),
            |key| {
                let hits = &group_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.ok, result.log_id));
                }
            },
            1_000,
        )
        .expect("data group propose callback fanout");
    assert_eq!(group_callback_results.len(), 1);
    assert_eq!(group_callback_results[0].0, MatrixRaftRouteKey::new(827, 1));
    assert!(group_callback_results[0].1.ok);
    assert!(group_callback_results[0].1.log_id.is_some());
    assert_eq!(group_callback_hits.borrow().len(), 1);
    assert_eq!(
        group_callback_hits.borrow()[0].0,
        MatrixRaftRouteKey::new(827, 1)
    );
    assert!(group_callback_hits.borrow()[0].1);
    let selected_default_fanout = server
        .propose_to_groups_best_effort([826, 827], b"selected-default-fanout".to_vec())
        .expect("default best-effort selected propose");
    assert_eq!(
        selected_default_fanout
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(826, 2), (827, 1)]
    );
    let selected_reads = server
        .read_indexes_for_groups_with_options(
            [826, 827],
            MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index)),
        )
        .expect("selected meta and data read indexes");
    let selected_read_plan = server
        .plan_read_indexes_with_options_for_groups(
            [826, 827],
            MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index)),
        )
        .expect("selected read-index fanout plan");
    assert_eq!(selected_read_plan.group_count, 2);
    assert_eq!(selected_read_plan.group_ids, vec![826, 827]);
    assert_eq!(selected_read_plan.node_count, 3);
    assert_eq!(
        selected_read_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(826, 1),
            MatrixRaftRouteKey::new(826, 2),
            MatrixRaftRouteKey::new(827, 1),
        ]
    );
    assert_eq!(
        selected_read_plan.options,
        MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index))
    );
    assert_eq!(
        selected_read_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone(), group.options))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![1, 2],
                MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index))
            ),
            (
                827,
                vec![1],
                MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index))
            ),
        ]
    );
    assert_eq!(
        selected_read_plan.options_by_group(),
        vec![
            (
                826,
                MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index))
            ),
            (
                827,
                MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index))
            ),
        ]
    );
    assert_eq!(
        selected_read_plan.options_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(826, 1),
                MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index))
            ),
            (
                MatrixRaftRouteKey::new(826, 2),
                MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index))
            ),
            (
                MatrixRaftRouteKey::new(827, 1),
                MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index))
            ),
        ]
    );
    assert_eq!(
        selected_read_plan.min_commit_indices_by_group(),
        vec![
            (826, fanout_log.index.min(data_log.index)),
            (827, fanout_log.index.min(data_log.index)),
        ]
    );
    assert_eq!(
        selected_read_plan.min_commit_indices_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(826, 1),
                fanout_log.index.min(data_log.index)
            ),
            (
                MatrixRaftRouteKey::new(826, 2),
                fanout_log.index.min(data_log.index)
            ),
            (
                MatrixRaftRouteKey::new(827, 1),
                fanout_log.index.min(data_log.index)
            ),
        ]
    );
    assert_eq!(
        selected_read_plan.modes_by_group(),
        vec![
            (826, MatrixRaftReadIndexMode::QuorumRead),
            (827, MatrixRaftReadIndexMode::QuorumRead),
        ]
    );
    assert_eq!(
        selected_read_plan.modes_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(826, 1),
                MatrixRaftReadIndexMode::QuorumRead
            ),
            (
                MatrixRaftRouteKey::new(826, 2),
                MatrixRaftReadIndexMode::QuorumRead
            ),
            (
                MatrixRaftRouteKey::new(827, 1),
                MatrixRaftReadIndexMode::QuorumRead
            ),
        ]
    );
    assert_eq!(
        selected_reads
            .iter()
            .map(|(group_id, reads)| (*group_id, reads.len()))
            .collect::<Vec<_>>(),
        vec![(826, 2), (827, 1)]
    );
    assert!(selected_reads
        .iter()
        .all(|(_, reads)| reads.iter().all(|read| !read.reason.is_empty())));
    let best_effort_reads = server
        .read_indexes_with_options_for_groups_best_effort(
            [826, 827],
            MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index)),
        )
        .expect("best-effort selected meta and data read indexes");
    assert_eq!(
        best_effort_reads
            .iter()
            .map(|group| (
                group.group_id,
                group.node_count,
                group.ok_count,
                group.error_count
            ))
            .collect::<Vec<_>>(),
        vec![(826, 2, 2, 0), (827, 1, 1, 0)]
    );
    assert!(best_effort_reads.iter().all(|group| group.is_ok()
        && group
            .results
            .iter()
            .all(|result| result.is_ok() && result.read_index.is_some())));
    assert_eq!(
        best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.route_keys(), group.ok_route_keys()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    MatrixRaftRouteKey::new(826, 1),
                    MatrixRaftRouteKey::new(826, 2),
                ],
                vec![
                    MatrixRaftRouteKey::new(826, 1),
                    MatrixRaftRouteKey::new(826, 2),
                ]
            ),
            (
                827,
                vec![MatrixRaftRouteKey::new(827, 1)],
                vec![MatrixRaftRouteKey::new(827, 1)]
            ),
        ]
    );
    assert!(best_effort_reads
        .iter()
        .all(|group| group.error_route_keys().is_empty()));
    assert!(best_effort_reads.iter().all(|group| {
        group.responses_by_route_key().iter().all(|(_, response)| {
            response
                .as_ref()
                .is_some_and(|response| !response.reason.is_empty())
        })
    }));
    assert_eq!(
        best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.response_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.safe_values_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), Some(true)),
                    (MatrixRaftRouteKey::new(826, 2), Some(false)),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), Some(true))],),
        ]
    );
    assert_eq!(
        best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.safe_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.lease_read_values_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), Some(false)),
                    (MatrixRaftRouteKey::new(826, 2), Some(false)),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), Some(false))],),
        ]
    );
    assert_eq!(
        best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.lease_read_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.read_index_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.reason_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert!(best_effort_reads.iter().all(|group| {
        group
            .read_indices_by_route_key()
            .iter()
            .all(|(_, read_index)| read_index.is_some())
            && group
                .reasons_by_route_key()
                .iter()
                .all(|(_, reason)| reason.as_ref().is_some_and(|reason| !reason.is_empty()))
    }));
    let data_group_best_effort_reads = server
        .read_indexes_on_group_best_effort(827, 1)
        .expect("best-effort data group lease read indexes");
    assert_eq!(data_group_best_effort_reads.group_id, 827);
    assert_eq!(data_group_best_effort_reads.node_count, 1);
    assert_eq!(data_group_best_effort_reads.ok_count, 1);
    assert_eq!(data_group_best_effort_reads.error_count, 0);
    let bounded_options =
        MatrixRaftBoundedStaleReadOptions::new(fanout_log.index.min(data_log.index), 16);
    let bounded_plan = server
        .plan_bounded_stale_reads_with_options_for_groups([826, 827], bounded_options)
        .expect("plan bounded-stale selected reads");
    assert_eq!(bounded_plan.group_count, 2);
    assert_eq!(bounded_plan.group_ids, vec![826, 827]);
    assert_eq!(bounded_plan.node_count, 3);
    assert_eq!(
        bounded_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(826, 1),
            MatrixRaftRouteKey::new(826, 2),
            MatrixRaftRouteKey::new(827, 1),
        ]
    );
    assert_eq!(bounded_plan.options, bounded_options);
    assert_eq!(
        bounded_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone(), group.options))
            .collect::<Vec<_>>(),
        vec![
            (826, vec![1, 2], bounded_options),
            (827, vec![1], bounded_options),
        ]
    );
    assert_eq!(
        bounded_plan.options_by_group(),
        vec![(826, bounded_options), (827, bounded_options)]
    );
    assert_eq!(
        bounded_plan.options_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(826, 1), bounded_options),
            (MatrixRaftRouteKey::new(826, 2), bounded_options),
            (MatrixRaftRouteKey::new(827, 1), bounded_options),
        ]
    );
    assert_eq!(
        bounded_plan.min_commit_indices_by_group(),
        vec![
            (826, fanout_log.index.min(data_log.index)),
            (827, fanout_log.index.min(data_log.index)),
        ]
    );
    assert_eq!(
        bounded_plan.min_commit_indices_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(826, 1),
                fanout_log.index.min(data_log.index)
            ),
            (
                MatrixRaftRouteKey::new(826, 2),
                fanout_log.index.min(data_log.index)
            ),
            (
                MatrixRaftRouteKey::new(827, 1),
                fanout_log.index.min(data_log.index)
            ),
        ]
    );
    assert_eq!(
        bounded_plan.max_stale_index_lags_by_group(),
        vec![(826, 16), (827, 16)]
    );
    assert_eq!(
        bounded_plan.max_stale_index_lags_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(826, 1), 16),
            (MatrixRaftRouteKey::new(826, 2), 16),
            (MatrixRaftRouteKey::new(827, 1), 16),
        ]
    );
    let direct_bounded_read = server
        .bounded_stale_read_on_node_with_options(827, 1, bounded_options)
        .expect("direct bounded-stale data read");
    assert!(direct_bounded_read.bounded_stale.is_some());
    assert!(!direct_bounded_read.reason.is_empty());
    let bounded_reads = server
        .bounded_stale_reads_with_options_for_groups([826, 827], bounded_options)
        .expect("bounded-stale selected reads");
    assert_eq!(
        bounded_reads
            .iter()
            .map(|(group_id, reads)| (*group_id, reads.len()))
            .collect::<Vec<_>>(),
        vec![(826, 2), (827, 1)]
    );
    assert!(bounded_reads.iter().all(|(_, reads)| {
        reads
            .iter()
            .all(|read| read.bounded_stale.is_some() && !read.reason.is_empty())
    }));
    let bounded_best_effort_reads = server
        .bounded_stale_reads_with_options_for_groups_best_effort([826, 827], bounded_options)
        .expect("best-effort bounded-stale selected reads");
    assert_eq!(
        bounded_best_effort_reads
            .iter()
            .map(|group| (
                group.group_id,
                group.node_count,
                group.ok_count,
                group.error_count
            ))
            .collect::<Vec<_>>(),
        vec![(826, 2, 2, 0), (827, 1, 1, 0)]
    );
    assert!(bounded_best_effort_reads.iter().all(|group| group.is_ok()
        && group.results.iter().all(|result| {
            result.is_ok()
                && result
                    .report
                    .as_ref()
                    .is_some_and(|report| report.bounded_stale.is_some())
        })));
    assert!(bounded_best_effort_reads.iter().all(|group| {
        group.reports_by_route_key().iter().all(|(_, report)| {
            report
                .as_ref()
                .is_some_and(|report| !report.reason.is_empty() && report.bounded_stale.is_some())
        })
    }));
    assert_eq!(
        bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.report_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.bounded_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.safe_values_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), Some(true)),
                    (MatrixRaftRouteKey::new(826, 2), Some(false)),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), Some(true))],),
        ]
    );
    assert_eq!(
        bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.safe_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.bounded_allowed_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), Some(true)),
                    (MatrixRaftRouteKey::new(826, 2), Some(true)),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), Some(true))],),
        ]
    );
    assert_eq!(
        bounded_best_effort_reads
            .iter()
            .map(|group| (
                group.group_id,
                group.bounded_allowed_presence_by_route_key()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.read_index_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.bounded_lag_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.reason_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), true),
                    (MatrixRaftRouteKey::new(826, 2), true),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert!(bounded_best_effort_reads.iter().all(|group| {
        group
            .read_indices_by_route_key()
            .iter()
            .all(|(_, read_index)| read_index.is_some())
            && group
                .bounded_lags_by_route_key()
                .iter()
                .all(|(_, lag)| lag.is_some())
            && group
                .reasons_by_route_key()
                .iter()
                .all(|(_, reason)| reason.as_ref().is_some_and(|reason| !reason.is_empty()))
    }));
    let data_group_bounded_best_effort = server
        .bounded_stale_reads_on_group_best_effort(827, 1, 16)
        .expect("best-effort bounded-stale data group reads");
    assert_eq!(data_group_bounded_best_effort.group_id, 827);
    assert_eq!(data_group_bounded_best_effort.node_count, 1);
    assert_eq!(data_group_bounded_best_effort.ok_count, 1);
    assert_eq!(data_group_bounded_best_effort.error_count, 0);
    let read_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, bool, Option<u64>)>::new());
    let read_callback_results = server
        .read_index_with_options_callbacks_for_groups(
            [826, 827],
            MatrixRaftReadIndexOptions::quorum_read(fanout_log.index.min(data_log.index)),
            |key| {
                let hits = &read_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((
                        key,
                        result.ok,
                        result.read_index.as_ref().map(|read| read.read_index),
                    ));
                }
            },
            1_000,
        )
        .expect("selected read callback fanout");
    assert_eq!(
        read_callback_results
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(826, 2), (827, 1)]
    );
    let read_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&read_callback_results);
    assert!(read_callback_summaries.iter().all(|summary| {
        summary.operations == vec![MatrixRaftAsyncOperation::ReadIndex]
            && summary.ok_operations == vec![MatrixRaftAsyncOperation::ReadIndex]
            && summary.timed_out_operations.is_empty()
            && summary.request_ids.iter().all(Option::is_some)
            && summary.deadline_ms.iter().all(Option::is_some)
            && summary
                .timeout_ms
                .iter()
                .all(|timeout_ms| *timeout_ms == 1_000)
            && summary.proposed_log_ids.iter().all(Option::is_none)
            && summary.read_index_present.iter().all(|present| *present)
            && summary
                .read_index_presence_by_route_key
                .iter()
                .all(|(_, present)| *present)
            && summary
                .read_index_responses_by_route_key()
                .iter()
                .all(|(_, response)| response.is_some())
            && summary
                .read_indices_by_route_key()
                .iter()
                .all(|(_, read_index)| read_index.is_some())
            && {
                // Honest read-index safety contract for a fanned-out group: every
                // node computes a read index (Some); the quorum-confirmed leader
                // certifies the read as safe; and a follower that has not yet
                // applied up to the read floor honestly reports Some(false) (with a
                // reason, asserted below) rather than faking linearizable safety.
                let safe_by_key = summary.read_index_safe_by_route_key();
                safe_by_key.iter().all(|(_, safe)| safe.is_some())
                    && safe_by_key.iter().any(|(_, safe)| *safe == Some(true))
            }
            && summary
                .read_index_lease_read_by_route_key()
                .iter()
                .all(|(_, lease_read)| lease_read.is_some())
            && summary
                .read_index_reasons_by_route_key()
                .iter()
                .all(|(_, reason)| reason.as_ref().is_some_and(|reason| !reason.is_empty()))
    }));
    assert_eq!(
        read_callback_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.counts_by_operation.clone(),
                summary.result_counts_by_status(),
                summary.route_key_counts_by_status(),
                summary.log_id_presence_by_route_key(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![(MatrixRaftAsyncOperation::ReadIndex, 2, 1, 1, 0)],
                (1, 1, 0),
                (1, 1, 0),
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (
                827,
                vec![(MatrixRaftAsyncOperation::ReadIndex, 1, 1, 0, 0)],
                (1, 0, 0),
                (1, 0, 0),
                vec![(MatrixRaftRouteKey::new(827, 1), false)],
            ),
        ]
    );
    assert_eq!(
        read_callback_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.node_ids_by_route_key(),
                summary.ok_node_ids_by_route_key(),
                summary.error_node_ids_by_route_key(),
                summary.operations_by_route_key.clone(),
                summary.ok_operations_by_route_key(),
                summary.error_operations_by_route_key(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), Some(1)),
                    (MatrixRaftRouteKey::new(826, 2), Some(2)),
                ],
                vec![(MatrixRaftRouteKey::new(826, 1), Some(1))],
                vec![(MatrixRaftRouteKey::new(826, 2), Some(2))],
                vec![
                    (
                        MatrixRaftRouteKey::new(826, 1),
                        MatrixRaftAsyncOperation::ReadIndex,
                    ),
                    (
                        MatrixRaftRouteKey::new(826, 2),
                        MatrixRaftAsyncOperation::ReadIndex,
                    ),
                ],
                vec![(
                    MatrixRaftRouteKey::new(826, 1),
                    MatrixRaftAsyncOperation::ReadIndex,
                )],
                vec![(
                    MatrixRaftRouteKey::new(826, 2),
                    MatrixRaftAsyncOperation::ReadIndex,
                )],
            ),
            (
                827,
                vec![(MatrixRaftRouteKey::new(827, 1), Some(1))],
                vec![(MatrixRaftRouteKey::new(827, 1), Some(1))],
                Vec::<(MatrixRaftRouteKey, Option<u64>)>::new(),
                vec![(
                    MatrixRaftRouteKey::new(827, 1),
                    MatrixRaftAsyncOperation::ReadIndex,
                )],
                vec![(
                    MatrixRaftRouteKey::new(827, 1),
                    MatrixRaftAsyncOperation::ReadIndex,
                )],
                Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)>::new(),
            ),
        ]
    );
    assert_eq!(read_callback_hits.borrow().len(), 3);
    assert!(read_callback_hits
        .borrow()
        .iter()
        .all(|(_, _, read_index)| read_index.is_some()));
    let group_read_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, bool, Option<u64>)>::new());
    let group_read_callback_results = server
        .read_index_callbacks_on_group(
            827,
            1,
            |key| {
                let hits = &group_read_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((
                        key,
                        result.ok,
                        result.read_index.as_ref().map(|read| read.read_index),
                    ));
                }
            },
            1_000,
        )
        .expect("data group read callback fanout");
    assert_eq!(group_read_callback_results.len(), 1);
    assert_eq!(group_read_callback_hits.borrow().len(), 1);
    assert_eq!(
        group_read_callback_hits.borrow()[0].0,
        MatrixRaftRouteKey::new(827, 1)
    );
    assert!(group_read_callback_hits.borrow()[0].2.is_some());
    assert_eq!(
        server
            .read_indexes_for_groups([826, 827], 1)
            .expect("selected meta and data lease reads")
            .iter()
            .map(|(group_id, reads)| (*group_id, reads.len()))
            .collect::<Vec<_>>(),
        vec![(826, 2), (827, 1)]
    );
    server
        .shutdown_group_best_effort(826)
        .expect("shutdown meta group before best-effort read error fanout");
    let shutdown_best_effort_reads = server
        .read_indexes_for_groups_best_effort([826, 827], 1)
        .expect("best-effort read indexes with shutdown meta group");
    assert_eq!(
        shutdown_best_effort_reads
            .iter()
            .map(|group| (
                group.group_id,
                group.node_count,
                group.ok_count,
                group.error_count
            ))
            .collect::<Vec<_>>(),
        vec![(826, 2, 0, 2), (827, 1, 1, 0)]
    );
    assert!(shutdown_best_effort_reads
        .iter()
        .find(|group| group.group_id == 826)
        .expect("shutdown meta group read results")
        .results
        .iter()
        .all(|result| result.error.is_some()));
    assert_eq!(
        shutdown_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.response_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        shutdown_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.read_index_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        shutdown_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.safe_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        shutdown_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.reason_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    let shutdown_bounded_best_effort_reads = server
        .bounded_stale_reads_for_groups_best_effort([826, 827], 1, 16)
        .expect("best-effort bounded-stale reads with shutdown meta group");
    assert_eq!(
        shutdown_bounded_best_effort_reads
            .iter()
            .map(|group| (
                group.group_id,
                group.node_count,
                group.ok_count,
                group.error_count
            ))
            .collect::<Vec<_>>(),
        vec![(826, 2, 0, 2), (827, 1, 1, 0)]
    );
    assert!(shutdown_bounded_best_effort_reads
        .iter()
        .find(|group| group.group_id == 826)
        .expect("shutdown meta group bounded-stale read results")
        .results
        .iter()
        .all(|result| result.error.is_some()));
    assert!(shutdown_bounded_best_effort_reads
        .iter()
        .find(|group| group.group_id == 827)
        .expect("running data group bounded-stale read results")
        .results
        .iter()
        .all(|result| result
            .report
            .as_ref()
            .is_some_and(|report| report.bounded_stale.is_some())));
    assert_eq!(
        shutdown_bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.report_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        shutdown_bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.bounded_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        shutdown_bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.read_index_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        shutdown_bounded_best_effort_reads
            .iter()
            .map(|group| (
                group.group_id,
                group.bounded_allowed_presence_by_route_key()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        shutdown_bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.bounded_lag_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_eq!(
        shutdown_bounded_best_effort_reads
            .iter()
            .map(|group| (group.group_id, group.reason_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                826,
                vec![
                    (MatrixRaftRouteKey::new(826, 1), false),
                    (MatrixRaftRouteKey::new(826, 2), false),
                ],
            ),
            (827, vec![(MatrixRaftRouteKey::new(827, 1), true)],),
        ]
    );
    assert_invalid_request_contains(
        server.propose_to_groups([899, 826], b"missing-group".to_vec()),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_propose_for_groups([899, 826], &b"missing-group".to_vec()),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.read_indexes_for_groups([826, 899], 0),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_read_indexes_for_groups([826, 899], 0),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_bounded_stale_reads_for_groups([826, 899], 0, 16),
        "group 899 is not registered",
    );
    assert_eq!(
        server.read_index_on_node(826, 99, 0),
        Err(RaftError::NodeNotFound(99))
    );
    assert_eq!(
        server.bounded_stale_read_on_node(826, 99, 0, 16),
        Err(RaftError::NodeNotFound(99))
    );

    server.shutdown_all().expect("shutdown direct api server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_routes_membership_operations_to_group_nodes() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("membership-meta-1-wal");
    let meta_snap_1 = temp_dir("membership-meta-1-snapshot");
    let meta_wal_2 = temp_dir("membership-meta-2-wal");
    let meta_snap_2 = temp_dir("membership-meta-2-snapshot");
    let data_wal = temp_dir("membership-data-wal");
    let data_snap = temp_dir("membership-data-snapshot");
    server
        .create_node(options_for_peer(828, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(828, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(829, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start membership server");

    let learner = peer_with_role(828, 4, ReplicaRole::Learner);
    let add_reports = server
        .route_membership_operation_to_group(828, MembershipOperation::AddLearner(learner.clone()))
        .expect("add learner to group");
    assert_eq!(add_reports.len(), 2);
    assert!(add_reports.iter().all(|report| report.success));
    assert!(add_reports
        .iter()
        .all(|report| report.after.learners.contains(&4)));
    assert!(server
        .group_statuses(828)
        .expect("meta statuses")
        .iter()
        .all(|status| status.membership.learners.contains(&4)));
    assert!(server
        .group_statuses(829)
        .expect("data statuses")
        .iter()
        .all(|status| !status.membership.learners.contains(&4)));
    let selected_membership_plan = server
        .plan_membership_operation_for_groups(
            [828, 829],
            MembershipOperation::AddLearner(peer_with_role(828, 6, ReplicaRole::Learner)),
        )
        .expect("plan selected membership fanout");
    assert_eq!(selected_membership_plan.group_count, 2);
    assert_eq!(selected_membership_plan.group_ids, vec![828, 829]);
    assert_eq!(selected_membership_plan.node_count, 3);
    assert_eq!(
        selected_membership_plan
            .route_keys
            .iter()
            .map(|key| (key.group_id, key.node_id))
            .collect::<Vec<_>>(),
        vec![(828, 1), (828, 2), (829, 1)]
    );
    assert_eq!(
        selected_membership_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone(), group.node_count))
            .collect::<Vec<_>>(),
        vec![(828, vec![1, 2], 2), (829, vec![1], 1)]
    );
    assert!(matches!(
        selected_membership_plan.operation,
        MembershipOperation::AddLearner(_)
    ));
    assert!(selected_membership_plan
        .operations_by_group()
        .iter()
        .all(|(group_id, operation)| {
            [828, 829].contains(group_id)
                && matches!(operation, MembershipOperation::AddLearner(peer) if peer.node_id == 6)
        }));
    assert_eq!(
        selected_membership_plan.operation_types_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), "add_learner".to_string()),
            (MatrixRaftRouteKey::new(828, 2), "add_learner".to_string()),
            (MatrixRaftRouteKey::new(829, 1), "add_learner".to_string()),
        ]
    );
    assert_eq!(
        selected_membership_plan.operation_member_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), 6),
            (MatrixRaftRouteKey::new(828, 2), 6),
            (MatrixRaftRouteKey::new(829, 1), 6),
        ]
    );
    assert_eq!(
        selected_membership_plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), 1),
            (MatrixRaftRouteKey::new(828, 2), 2),
            (MatrixRaftRouteKey::new(829, 1), 1),
        ]
    );
    assert!(selected_membership_plan
        .operations_by_route_key()
        .iter()
        .all(|(route_key, operation)| {
            [
                MatrixRaftRouteKey::new(828, 1),
                MatrixRaftRouteKey::new(828, 2),
                MatrixRaftRouteKey::new(829, 1),
            ]
            .contains(route_key)
                && matches!(operation, MembershipOperation::AddLearner(peer) if peer.node_id == 6)
        }));
    let selected_add_reports = server
        .route_membership_operation_to_groups(
            [828, 829],
            MembershipOperation::AddLearner(peer_with_role(828, 6, ReplicaRole::Learner)),
        )
        .expect("add learner to selected groups");
    assert_eq!(
        selected_add_reports
            .iter()
            .map(|(group_id, reports)| (*group_id, reports.len()))
            .collect::<Vec<_>>(),
        vec![(828, 2), (829, 1)]
    );
    assert!(selected_add_reports.iter().all(|(_, reports)| reports
        .iter()
        .all(|report| report.success && report.after.learners.contains(&6))));
    let membership_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation, bool)>::new());
    let selected_membership_callbacks = server
        .membership_operation_callbacks_for_groups(
            [828, 829],
            MembershipOperation::AddLearner(peer_with_role(828, 7, ReplicaRole::Learner)),
            |key| {
                let hits = &membership_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation, result.ok));
                }
            },
            1_000,
        )
        .expect("selected membership callbacks");
    assert_eq!(
        selected_membership_callbacks
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(828, 2), (829, 1)]
    );
    assert_eq!(membership_callback_hits.borrow().len(), 3);
    assert!(membership_callback_hits
        .borrow()
        .iter()
        .all(|(_, operation, ok)| *operation == MatrixRaftAsyncOperation::AddLearner && *ok));
    assert!(selected_membership_callbacks.iter().all(|(_, results)| {
        results.iter().all(|(_, result)| {
            result.membership_presence()
                && !result.remove_presence()
                && result
                    .membership
                    .as_ref()
                    .is_some_and(|report| report.success && report.after.learners.contains(&7))
        })
    }));
    let membership_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&selected_membership_callbacks);
    assert!(membership_callback_summaries.iter().all(|summary| {
        summary
            .membership_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .membership_success_by_route_key()
                .iter()
                .all(|(_, success)| *success == Some(true))
            && summary
                .membership_reasons_by_route_key()
                .iter()
                .all(|(_, reason)| reason.is_some())
            && summary
                .remove_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    let witness_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation, bool)>::new());
    let witness_callbacks = server
        .membership_operation_callbacks_on_group(
            829,
            MembershipOperation::AddWitness(peer_with_role(829, 8, ReplicaRole::Witness)),
            |key| {
                let hits = &witness_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation, result.ok));
                }
            },
            1_000,
        )
        .expect("data group witness membership callbacks");
    assert_eq!(witness_callbacks.len(), 1);
    assert_eq!(witness_callbacks[0].0, MatrixRaftRouteKey::new(829, 1));
    assert_eq!(witness_callback_hits.borrow().len(), 1);
    assert_eq!(
        witness_callback_hits.borrow()[0],
        (
            MatrixRaftRouteKey::new(829, 1),
            MatrixRaftAsyncOperation::AddWitness,
            true
        )
    );
    let remove_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation, bool)>::new());
    let selected_remove_callbacks = server
        .membership_operation_callbacks_for_groups(
            [828, 829],
            MembershipOperation::Remove(7),
            |key| {
                let hits = &remove_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation, result.ok));
                }
            },
            1_000,
        )
        .expect("selected membership remove callbacks");
    assert_eq!(
        selected_remove_callbacks
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(828, 2), (829, 1)]
    );
    assert_eq!(remove_callback_hits.borrow().len(), 3);
    assert!(remove_callback_hits
        .borrow()
        .iter()
        .all(|(_, operation, ok)| *operation == MatrixRaftAsyncOperation::RemoveNode && *ok));
    assert!(selected_remove_callbacks.iter().any(|(_, results)| {
        results
            .iter()
            .any(|(_, result)| result.remove_presence() && !result.snapshot_presence())
    }));
    let remove_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&selected_remove_callbacks);
    assert!(remove_callback_summaries
        .iter()
        .flat_map(|summary| summary.remove_presence_by_route_key())
        .any(|(_, present)| present));
    assert!(remove_callback_summaries.iter().all(|summary| {
        summary
            .removed_ids_by_route_key()
            .iter()
            .all(|(_, removed_id)| *removed_id == Some(7))
            && summary
                .removed_values_by_route_key()
                .iter()
                .all(|(_, removed)| *removed == Some(true))
            && summary
                .remove_membership_success_by_route_key()
                .iter()
                .all(|(_, success)| *success == Some(true))
    }));
    assert!(remove_callback_summaries
        .iter()
        .flat_map(|summary| summary.removed_conf_states_by_route_key())
        .all(|(_, conf_state)| conf_state.is_some()));
    assert!(remove_callback_summaries.iter().all(|summary| summary
        .snapshot_presence_by_route_key()
        .iter()
        .all(|(_, present)| !*present)));

    let duplicate_add = server
        .route_membership_operation_to_group_best_effort(
            828,
            MembershipOperation::AddLearner(learner),
        )
        .expect("duplicate add learner best effort");
    assert_eq!(duplicate_add.len(), 2);
    assert!(duplicate_add.iter().all(|result| result.error.is_some()));

    let remove_reports = server
        .route_membership_operation_to_group_best_effort(828, MembershipOperation::Remove(4))
        .expect("remove learner best effort");
    assert_eq!(remove_reports.len(), 2);
    assert!(remove_reports.iter().all(|result| result.is_ok()));
    assert!(remove_reports.iter().all(|result| {
        result
            .result
            .as_ref()
            .and_then(|route| route.membership.as_ref())
            .is_some_and(|report| report.success && !report.after.learners.contains(&4))
    }));
    let selected_remove_reports = server
        .route_membership_operation_to_groups_best_effort(
            [828, 829],
            MembershipOperation::Remove(6),
        )
        .expect("remove learner from selected groups");
    assert_eq!(
        selected_remove_reports
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(828, 2), (829, 1)]
    );
    assert!(selected_remove_reports.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .result
                .as_ref()
                .and_then(|route| route.membership.as_ref())
                .is_some_and(|report| report.success && !report.after.learners.contains(&6))
        })
    }));
    let selected_remove_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_remove_reports);
    assert!(selected_remove_summaries.iter().all(|summary| {
        summary
            .membership_success_by_route_key()
            .iter()
            .all(|(_, success)| *success == Some(true))
            && summary
                .membership_reasons_by_route_key()
                .iter()
                .all(|(_, reason)| reason.is_some())
    }));

    let membership_workflow = vec![
        MembershipOperation::AddLearner(peer_with_role(828, 10, ReplicaRole::Learner)),
        MembershipOperation::Promote(10),
        MembershipOperation::Remove(10),
    ];
    let workflow_plan = server
        .plan_membership_workflow_for_groups([828, 829], membership_workflow.clone())
        .expect("plan selected membership workflow");
    assert_eq!(workflow_plan.group_count, 2);
    assert_eq!(workflow_plan.group_ids, vec![828, 829]);
    assert_eq!(workflow_plan.node_count, 3);
    assert_eq!(workflow_plan.operation_count, 3);
    assert_eq!(
        workflow_plan
            .route_keys
            .iter()
            .map(|key| (key.group_id, key.node_id))
            .collect::<Vec<_>>(),
        vec![(828, 1), (828, 2), (829, 1)]
    );
    assert_eq!(
        workflow_plan
            .groups
            .iter()
            .map(|group| (
                group.group_id,
                group.node_ids.clone(),
                group.operation_count
            ))
            .collect::<Vec<_>>(),
        vec![(828, vec![1, 2], 3), (829, vec![1], 3)]
    );
    assert_eq!(
        workflow_plan.operation_counts_by_group(),
        vec![(828, 3), (829, 3)]
    );
    assert_eq!(
        workflow_plan.operation_counts_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), 3),
            (MatrixRaftRouteKey::new(828, 2), 3),
            (MatrixRaftRouteKey::new(829, 1), 3),
        ]
    );
    assert_eq!(
        workflow_plan.operations_by_group(),
        vec![
            (828, membership_workflow.clone()),
            (829, membership_workflow.clone()),
        ]
    );
    assert_eq!(
        workflow_plan.operations_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), membership_workflow.clone()),
            (MatrixRaftRouteKey::new(828, 2), membership_workflow.clone()),
            (MatrixRaftRouteKey::new(829, 1), membership_workflow.clone()),
        ]
    );
    assert_eq!(
        workflow_plan.operation_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(828, 1),
                vec![
                    "add_learner".to_string(),
                    "promote".to_string(),
                    "remove".to_string()
                ]
            ),
            (
                MatrixRaftRouteKey::new(828, 2),
                vec![
                    "add_learner".to_string(),
                    "promote".to_string(),
                    "remove".to_string()
                ]
            ),
            (
                MatrixRaftRouteKey::new(829, 1),
                vec![
                    "add_learner".to_string(),
                    "promote".to_string(),
                    "remove".to_string()
                ]
            ),
        ]
    );
    assert_eq!(
        workflow_plan.operation_member_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), vec![10, 10, 10]),
            (MatrixRaftRouteKey::new(828, 2), vec![10, 10, 10]),
            (MatrixRaftRouteKey::new(829, 1), vec![10, 10, 10]),
        ]
    );
    let workflow_reports = server
        .route_membership_workflow_to_groups([828, 829], membership_workflow)
        .expect("run selected membership workflow");
    assert_eq!(
        workflow_reports
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(828, 2), (829, 1)]
    );
    assert!(workflow_reports.iter().all(|(_, results)| {
        results
            .iter()
            .all(|(_, reports)| reports.len() == 3 && reports.iter().all(|report| report.success))
    }));
    let workflow_group_results = server
        .route_membership_workflow_to_groups_best_effort(
            [828, 829],
            [
                MembershipOperation::AddLearner(peer_with_role(828, 14, ReplicaRole::Learner)),
                MembershipOperation::Promote(14),
                MembershipOperation::Remove(14),
            ],
        )
        .expect("best-effort selected membership workflow result metadata");
    assert_eq!(
        workflow_group_results
            .iter()
            .map(|group| (group.group_id, group.report_counts_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                828,
                vec![
                    (MatrixRaftRouteKey::new(828, 1), 3),
                    (MatrixRaftRouteKey::new(828, 2), 3),
                ],
            ),
            (829, vec![(MatrixRaftRouteKey::new(829, 1), 3)],),
        ]
    );
    assert_eq!(
        workflow_group_results
            .iter()
            .map(|group| (group.group_id, group.report_presence_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                828,
                vec![
                    (MatrixRaftRouteKey::new(828, 1), true),
                    (MatrixRaftRouteKey::new(828, 2), true),
                ],
            ),
            (829, vec![(MatrixRaftRouteKey::new(829, 1), true)],),
        ]
    );
    assert_eq!(
        workflow_group_results
            .iter()
            .map(|group| (group.group_id, group.operation_member_ids_by_route_key()))
            .collect::<Vec<_>>(),
        vec![
            (
                828,
                vec![
                    (MatrixRaftRouteKey::new(828, 1), vec![14, 14, 14]),
                    (MatrixRaftRouteKey::new(828, 2), vec![14, 14, 14]),
                ],
            ),
            (
                829,
                vec![(MatrixRaftRouteKey::new(829, 1), vec![14, 14, 14])],
            ),
        ]
    );
    assert!(workflow_group_results.iter().all(|group| {
        group
            .reports_by_route_key()
            .iter()
            .all(|(_, reports)| reports.as_ref().is_some_and(|reports| reports.len() == 3))
            && group.ok_reports_by_route_key().iter().all(|(_, reports)| {
                reports.len() == 3 && reports.iter().all(|report| report.success)
            })
            && group.error_reports_by_route_key().is_empty()
    }));
    assert!(workflow_group_results.iter().all(|group| {
        group.is_ok()
            && group
                .success_values_by_route_key()
                .iter()
                .all(|(_, values)| values == &vec![true, true, true])
            && group
                .validation_values_by_route_key()
                .iter()
                .all(|(_, values)| values == &vec![true, true, true])
            && group
                .rollback_values_by_route_key()
                .iter()
                .all(|(_, values)| values == &vec![false, false, false])
            && group
                .reasons_by_route_key()
                .iter()
                .all(|(_, reasons)| reasons.iter().all(|reason| !reason.is_empty()))
    }));
    assert!(server
        .statuses_for_groups([828, 829])
        .expect("statuses after membership workflow")
        .iter()
        .flat_map(|(_, statuses)| statuses.iter())
        .all(|status| {
            !status.membership.learners.contains(&10) && !status.membership.voters.contains(&10)
        }));
    let data_workflow = server
        .route_membership_workflow_to_node(
            829,
            1,
            [
                MembershipOperation::AddLearner(peer_with_role(829, 11, ReplicaRole::Learner)),
                MembershipOperation::Remove(11),
            ],
        )
        .expect("direct data membership workflow");
    assert_eq!(data_workflow.len(), 2);
    assert!(data_workflow.iter().all(|report| report.success));
    let rollback_attempt = server
        .route_membership_workflow_to_group_best_effort(
            829,
            [
                MembershipOperation::AddWitness(peer_with_role(829, 12, ReplicaRole::Witness)),
                MembershipOperation::Remove(99),
            ],
        )
        .expect("best-effort rollback workflow");
    assert_eq!(rollback_attempt.group_id, 829);
    assert_eq!(rollback_attempt.node_count, 1);
    assert_eq!(rollback_attempt.ok_count, 0);
    assert_eq!(rollback_attempt.error_count, 1);
    assert!(rollback_attempt.results[0].error.is_some());
    assert!(server
        .group_statuses(829)
        .expect("data statuses after rollback workflow")
        .iter()
        .all(|status| !status.membership.witnesses.contains(&12)));

    let node_report = server
        .route_membership_operation_to_node(
            829,
            1,
            MembershipOperation::AddWitness(peer_with_role(829, 5, ReplicaRole::Witness)),
        )
        .expect("data group direct witness");
    assert!(node_report.success);
    assert!(node_report.after.witnesses.contains(&5));

    server
        .shutdown_group_best_effort(828)
        .expect("shutdown meta group before membership workflow partial failure");
    let partial_workflow = server
        .route_membership_workflow_to_groups_best_effort(
            [828, 829],
            [
                MembershipOperation::AddLearner(peer_with_role(829, 13, ReplicaRole::Learner)),
                MembershipOperation::Remove(13),
            ],
        )
        .expect("best-effort membership workflow with shutdown meta group");
    assert_eq!(
        partial_workflow
            .iter()
            .map(|group| (
                group.group_id,
                group.node_count,
                group.ok_count,
                group.error_count
            ))
            .collect::<Vec<_>>(),
        vec![(828, 2, 0, 2), (829, 1, 1, 0)]
    );
    assert!(partial_workflow
        .iter()
        .find(|group| group.group_id == 828)
        .expect("shutdown meta workflow results")
        .results
        .iter()
        .all(|result| result.error.is_some()));
    assert!(partial_workflow
        .iter()
        .find(|group| group.group_id == 829)
        .expect("data workflow results")
        .results
        .iter()
        .all(|result| result.reports.as_ref().is_some_and(|reports| {
            reports.len() == 2 && reports.iter().all(|report| report.success)
        })));
    let partial_meta_workflow = partial_workflow
        .iter()
        .find(|group| group.group_id == 828)
        .expect("shutdown meta workflow result accessors");
    assert_eq!(
        partial_meta_workflow.route_keys(),
        vec![
            MatrixRaftRouteKey::new(828, 1),
            MatrixRaftRouteKey::new(828, 2),
        ]
    );
    assert_eq!(partial_meta_workflow.node_ids(), vec![1, 2]);
    assert!(partial_meta_workflow.ok_node_ids().is_empty());
    assert_eq!(partial_meta_workflow.error_node_ids(), vec![1, 2]);
    assert_eq!(
        partial_meta_workflow.statuses_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), false),
            (MatrixRaftRouteKey::new(828, 2), false),
        ]
    );
    assert!(partial_meta_workflow
        .errors_by_route_key()
        .iter()
        .all(|(_, error)| error.is_some()));
    assert!(partial_meta_workflow
        .reports_by_route_key()
        .iter()
        .all(|(_, reports)| reports.is_none()));
    assert!(partial_meta_workflow.ok_reports_by_route_key().is_empty());
    assert_eq!(
        partial_meta_workflow.error_reports_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), None),
            (MatrixRaftRouteKey::new(828, 2), None),
        ]
    );
    assert_eq!(
        partial_meta_workflow.report_counts_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), 0),
            (MatrixRaftRouteKey::new(828, 2), 0),
        ]
    );
    assert_eq!(
        partial_meta_workflow.report_presence_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), false),
            (MatrixRaftRouteKey::new(828, 2), false),
        ]
    );
    assert_eq!(
        partial_meta_workflow.operation_member_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(828, 1), Vec::new()),
            (MatrixRaftRouteKey::new(828, 2), Vec::new()),
        ]
    );
    let partial_data_workflow = partial_workflow
        .iter()
        .find(|group| group.group_id == 829)
        .expect("data workflow result accessors");
    assert_eq!(partial_data_workflow.node_ids(), vec![1]);
    assert_eq!(partial_data_workflow.ok_node_ids(), vec![1]);
    assert!(partial_data_workflow.error_node_ids().is_empty());
    assert_eq!(
        partial_data_workflow.statuses_by_route_key(),
        vec![(MatrixRaftRouteKey::new(829, 1), true)]
    );
    assert_eq!(
        partial_data_workflow.errors_by_route_key(),
        vec![(MatrixRaftRouteKey::new(829, 1), None)]
    );
    assert!(partial_data_workflow
        .reports_by_route_key()
        .iter()
        .all(|(_, reports)| reports.as_ref().is_some_and(|reports| reports.len() == 2)));
    assert!(partial_data_workflow
        .ok_reports_by_route_key()
        .iter()
        .all(|(_, reports)| reports.len() == 2));
    assert!(partial_data_workflow
        .error_reports_by_route_key()
        .is_empty());
    assert_eq!(
        partial_data_workflow.report_counts_by_route_key(),
        vec![(MatrixRaftRouteKey::new(829, 1), 2)]
    );
    assert_eq!(
        partial_data_workflow.report_presence_by_route_key(),
        vec![(MatrixRaftRouteKey::new(829, 1), true)]
    );
    assert_eq!(
        partial_data_workflow.operation_member_ids_by_route_key(),
        vec![(MatrixRaftRouteKey::new(829, 1), vec![13, 13])]
    );
    assert_eq!(
        partial_data_workflow.success_values_by_route_key(),
        vec![(MatrixRaftRouteKey::new(829, 1), vec![true, true])]
    );
    assert_eq!(
        partial_data_workflow.rollback_values_by_route_key(),
        vec![(MatrixRaftRouteKey::new(829, 1), vec![false, false])]
    );

    assert_invalid_request_contains(
        server.plan_membership_operation_for_groups([899, 828], MembershipOperation::Remove(4)),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_membership_workflow_for_groups([899, 828], [MembershipOperation::Remove(4)]),
        "group 899 is not registered",
    );
    assert_eq!(
        server.route_membership_operation_to_node(828, 99, MembershipOperation::Remove(4)),
        Err(RaftError::NodeNotFound(99))
    );
    assert_eq!(
        server.route_membership_workflow_to_node(828, 99, [MembershipOperation::Remove(4)]),
        Err(RaftError::NodeNotFound(99))
    );

    server.shutdown_all().expect("shutdown membership server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_routes_config_changes_to_group_nodes() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("config-meta-1-wal");
    let meta_snap_1 = temp_dir("config-meta-1-snapshot");
    let meta_wal_2 = temp_dir("config-meta-2-wal");
    let meta_snap_2 = temp_dir("config-meta-2-snapshot");
    let data_wal = temp_dir("config-data-wal");
    let data_snap = temp_dir("config-data-snapshot");
    server
        .create_node(options_for_peer(830, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(830, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(831, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start config-change server");

    let add_learner = MatrixRaftConfigChange {
        request_id: Some(60),
        change_type: MatrixRaftConfigChangeType::AddNode,
        member_id: 4,
        raft_addr: "127.0.0.1:83004".to_string(),
        snapshot_addr: "127.0.0.1:83104".to_string(),
        old_members: Vec::new(),
        conf_state: MatrixRaftConfState::Learner,
        auto_promote: false,
    };
    let add_results = server
        .route_config_change_to_group(830, add_learner.clone())
        .expect("add learner config change to group");
    assert_eq!(add_results.len(), 2);
    assert!(add_results.iter().all(|result| result.handled));
    assert!(add_results.iter().all(|result| {
        result
            .membership
            .as_ref()
            .is_some_and(|report| report.success && report.after.learners.contains(&4))
    }));
    assert!(server
        .group_statuses(830)
        .expect("meta group statuses")
        .iter()
        .all(|status| status.membership.learners.contains(&4)));
    assert!(server
        .group_statuses(831)
        .expect("data group statuses")
        .iter()
        .all(|status| !status.membership.learners.contains(&4)));
    let selected_add_learner = MatrixRaftConfigChange {
        request_id: Some(65),
        change_type: MatrixRaftConfigChangeType::AddNode,
        member_id: 6,
        raft_addr: "127.0.0.1:83006".to_string(),
        snapshot_addr: "127.0.0.1:83106".to_string(),
        old_members: Vec::new(),
        conf_state: MatrixRaftConfState::Learner,
        auto_promote: false,
    };
    let selected_config_plan = server
        .plan_config_change_for_groups([830, 831], selected_add_learner.clone())
        .expect("plan selected config-change fanout");
    assert_eq!(selected_config_plan.group_count, 2);
    assert_eq!(selected_config_plan.group_ids, vec![830, 831]);
    assert_eq!(selected_config_plan.node_count, 3);
    assert_eq!(selected_config_plan.change.member_id, 6);
    assert_eq!(
        selected_config_plan
            .groups
            .iter()
            .map(|group| (
                group.group_id,
                group.node_ids.clone(),
                group.change.member_id
            ))
            .collect::<Vec<_>>(),
        vec![(830, vec![1, 2], 6), (831, vec![1], 6)]
    );
    assert_eq!(
        selected_config_plan.route_keys_by_group(),
        vec![
            (
                830,
                vec![
                    MatrixRaftRouteKey::new(830, 1),
                    MatrixRaftRouteKey::new(830, 2),
                ],
            ),
            (831, vec![MatrixRaftRouteKey::new(831, 1)]),
        ]
    );
    assert_eq!(
        selected_config_plan.node_ids_by_group(),
        vec![(830, vec![1, 2]), (831, vec![1])]
    );
    assert_eq!(
        selected_config_plan.route_key_counts_by_group(),
        vec![(830, 2), (831, 1)]
    );
    assert_eq!(
        selected_config_plan.fanout_counts_by_group(),
        vec![(830, 2, 2), (831, 1, 1)]
    );
    assert_eq!(
        selected_config_plan.change_types_by_group(),
        vec![
            (830, MatrixRaftConfigChangeType::AddNode),
            (831, MatrixRaftConfigChangeType::AddNode),
        ]
    );
    assert_eq!(
        selected_config_plan.change_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(830, 1),
                MatrixRaftConfigChangeType::AddNode,
            ),
            (
                MatrixRaftRouteKey::new(830, 2),
                MatrixRaftConfigChangeType::AddNode,
            ),
            (
                MatrixRaftRouteKey::new(831, 1),
                MatrixRaftConfigChangeType::AddNode,
            ),
        ]
    );
    assert_eq!(
        selected_config_plan.member_ids_by_group(),
        vec![(830, 6), (831, 6)]
    );
    assert_eq!(
        selected_config_plan.member_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(830, 1), 6),
            (MatrixRaftRouteKey::new(830, 2), 6),
            (MatrixRaftRouteKey::new(831, 1), 6),
        ]
    );
    assert_eq!(
        selected_config_plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(830, 1), 1),
            (MatrixRaftRouteKey::new(830, 2), 2),
            (MatrixRaftRouteKey::new(831, 1), 1),
        ]
    );
    assert_eq!(
        selected_config_plan
            .changes_by_group()
            .iter()
            .map(|(group_id, change)| (
                *group_id,
                change.request_id,
                change.change_type,
                change.member_id,
                change.conf_state,
                change.auto_promote
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                830,
                Some(65),
                MatrixRaftConfigChangeType::AddNode,
                6,
                MatrixRaftConfState::Learner,
                false
            ),
            (
                831,
                Some(65),
                MatrixRaftConfigChangeType::AddNode,
                6,
                MatrixRaftConfState::Learner,
                false
            ),
        ]
    );
    assert_eq!(
        selected_config_plan
            .changes_by_route_key()
            .iter()
            .map(|(key, change)| (
                *key,
                change.request_id,
                change.change_type,
                change.member_id
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                MatrixRaftRouteKey::new(830, 1),
                Some(65),
                MatrixRaftConfigChangeType::AddNode,
                6
            ),
            (
                MatrixRaftRouteKey::new(830, 2),
                Some(65),
                MatrixRaftConfigChangeType::AddNode,
                6
            ),
            (
                MatrixRaftRouteKey::new(831, 1),
                Some(65),
                MatrixRaftConfigChangeType::AddNode,
                6
            ),
        ]
    );
    assert_eq!(
        selected_config_plan.request_ids_by_group(),
        vec![(830, Some(65)), (831, Some(65))]
    );
    assert_eq!(
        selected_config_plan.request_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(830, 1), Some(65)),
            (MatrixRaftRouteKey::new(830, 2), Some(65)),
            (MatrixRaftRouteKey::new(831, 1), Some(65)),
        ]
    );
    assert_eq!(
        selected_config_plan.conf_states_by_group(),
        vec![
            (830, MatrixRaftConfState::Learner),
            (831, MatrixRaftConfState::Learner),
        ]
    );
    assert_eq!(
        selected_config_plan.conf_states_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(830, 1),
                MatrixRaftConfState::Learner
            ),
            (
                MatrixRaftRouteKey::new(830, 2),
                MatrixRaftConfState::Learner
            ),
            (
                MatrixRaftRouteKey::new(831, 1),
                MatrixRaftConfState::Learner
            ),
        ]
    );
    assert_eq!(
        selected_config_plan.auto_promote_values_by_group(),
        vec![(830, false), (831, false)]
    );
    assert_eq!(
        selected_config_plan.auto_promote_values_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(830, 1), false),
            (MatrixRaftRouteKey::new(830, 2), false),
            (MatrixRaftRouteKey::new(831, 1), false),
        ]
    );
    let selected_add_results = server
        .route_config_change_to_groups([830, 831], selected_add_learner)
        .expect("add learner config change to selected groups");
    assert_eq!(
        selected_add_results
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(830, 2), (831, 1)]
    );
    assert!(selected_add_results.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .membership
                .as_ref()
                .is_some_and(|report| report.success && report.after.learners.contains(&6))
        })
    }));

    let duplicate_add = server
        .route_config_change_to_group_best_effort(830, add_learner)
        .expect("duplicate config change best effort");
    assert_eq!(duplicate_add.len(), 2);
    assert!(duplicate_add.iter().all(|result| result.error.is_some()));

    let remove_learner = MatrixRaftConfigChange {
        request_id: Some(61),
        change_type: MatrixRaftConfigChangeType::RemoveNode,
        member_id: 4,
        raft_addr: String::new(),
        snapshot_addr: String::new(),
        old_members: Vec::new(),
        conf_state: MatrixRaftConfState::Learner,
        auto_promote: false,
    };
    let remove_results = server
        .route_config_change_to_group_best_effort(830, remove_learner)
        .expect("remove learner config change");
    assert_eq!(remove_results.len(), 2);
    assert!(remove_results.iter().all(|result| result.is_ok()));
    assert!(remove_results.iter().all(|result| {
        result
            .result
            .as_ref()
            .and_then(|route| route.membership.as_ref())
            .is_some_and(|report| report.success && !report.after.learners.contains(&4))
    }));
    let selected_remove_learner = MatrixRaftConfigChange {
        request_id: Some(66),
        change_type: MatrixRaftConfigChangeType::RemoveNode,
        member_id: 6,
        raft_addr: String::new(),
        snapshot_addr: String::new(),
        old_members: Vec::new(),
        conf_state: MatrixRaftConfState::Learner,
        auto_promote: false,
    };
    let selected_remove_results = server
        .route_config_change_to_groups_best_effort([830, 831], selected_remove_learner)
        .expect("remove learner config change from selected groups");
    assert_eq!(
        selected_remove_results
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(830, 2), (831, 1)]
    );
    assert!(selected_remove_results.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .result
                .as_ref()
                .and_then(|route| route.membership.as_ref())
                .is_some_and(|report| report.success && !report.after.learners.contains(&6))
        })
    }));

    let direct_witness = server
        .route_config_change_to_node(
            831,
            1,
            MatrixRaftConfigChange {
                request_id: Some(62),
                change_type: MatrixRaftConfigChangeType::AddNode,
                member_id: 5,
                raft_addr: "127.0.0.1:83105".to_string(),
                snapshot_addr: "127.0.0.1:83205".to_string(),
                old_members: Vec::new(),
                conf_state: MatrixRaftConfState::Witness,
                auto_promote: false,
            },
        )
        .expect("direct data witness config change");
    assert!(direct_witness
        .membership
        .as_ref()
        .is_some_and(|report| report.success && report.after.witnesses.contains(&5)));

    assert_invalid_request_contains(
        server.plan_config_change_for_groups(
            [899, 830],
            MatrixRaftConfigChange {
                request_id: Some(63),
                change_type: MatrixRaftConfigChangeType::RemoveNode,
                member_id: 4,
                raft_addr: String::new(),
                snapshot_addr: String::new(),
                old_members: Vec::new(),
                conf_state: MatrixRaftConfState::Learner,
                auto_promote: false,
            },
        ),
        "group 899 is not registered",
    );
    assert_eq!(
        server.route_config_change_to_node(
            830,
            99,
            MatrixRaftConfigChange {
                request_id: Some(64),
                change_type: MatrixRaftConfigChangeType::RemoveNode,
                member_id: 4,
                raft_addr: String::new(),
                snapshot_addr: String::new(),
                old_members: Vec::new(),
                conf_state: MatrixRaftConfState::Learner,
                auto_promote: false,
            },
        ),
        Err(RaftError::NodeNotFound(99))
    );

    server
        .shutdown_all()
        .expect("shutdown config-change server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_controls_peer_catchup_and_promotion_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("peer-control-meta-1-wal");
    let meta_snap_1 = temp_dir("peer-control-meta-1-snapshot");
    let meta_wal_2 = temp_dir("peer-control-meta-2-wal");
    let meta_snap_2 = temp_dir("peer-control-meta-2-snapshot");
    let data_wal = temp_dir("peer-control-data-wal");
    let data_snap = temp_dir("peer-control-data-snapshot");
    server
        .create_node(options_for_peer(832, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(832, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(833, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start peer-control server");

    server
        .route_membership_operation_to_group(
            832,
            MembershipOperation::AddLearner(peer_with_role(832, 4, ReplicaRole::Learner)),
        )
        .expect("add learner");
    server
        .route_membership_operation_to_group(
            833,
            MembershipOperation::AddLearner(peer_with_role(833, 4, ReplicaRole::Learner)),
        )
        .expect("add data learner");
    let catchups = server
        .catch_up_peer_on_group(832, 4)
        .expect("catch up learner by group");
    assert_eq!(catchups.len(), 2);
    assert!(catchups
        .iter()
        .all(|report| report.learner_id == 4 && report.caught_up));
    assert!(
        server
            .catch_up_peer_on_node(832, 1, 4)
            .expect("single node catch-up")
            .caught_up
    );

    let promotions = server
        .promote_peer_on_group(832, 4)
        .expect("promote learner by group");
    assert_eq!(promotions.len(), 2);
    assert!(promotions.iter().all(|report| report.promoted));
    assert!(server
        .group_statuses(832)
        .expect("meta statuses after promote")
        .iter()
        .all(|status| {
            status.membership.voters.contains(&4) && !status.membership.learners.contains(&4)
        }));
    for group_id in [832, 833] {
        server
            .route_membership_operation_to_group(
                group_id,
                MembershipOperation::AddLearner(peer_with_role(group_id, 6, ReplicaRole::Learner)),
            )
            .expect("add selected learner");
    }
    let catch_up_plan = server
        .plan_catch_up_peer_for_groups([832, 833], 6)
        .expect("plan selected learner catch-up");
    assert_eq!(catch_up_plan.group_count, 2);
    assert_eq!(catch_up_plan.node_count, 3);
    assert_eq!(
        catch_up_plan.message_type,
        MatrixRaftMessageType::CatchUpPeer
    );
    assert_eq!(
        catch_up_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(832, 1),
            MatrixRaftRouteKey::new(832, 2),
            MatrixRaftRouteKey::new(833, 1),
        ]
    );
    let selected_catchups = server
        .catch_up_peer_for_groups([832, 833], 6)
        .expect("catch up learner on selected groups");
    assert_eq!(
        selected_catchups
            .iter()
            .map(|(group_id, reports)| (*group_id, reports.len()))
            .collect::<Vec<_>>(),
        vec![(832, 2), (833, 1)]
    );
    assert!(selected_catchups.iter().all(|(_, reports)| {
        reports
            .iter()
            .all(|report| report.learner_id == 6 && report.caught_up)
    }));
    let selected_catchups_best_effort = server
        .catch_up_peer_for_groups_best_effort([832, 833], 6)
        .expect("best-effort catch up learner on selected groups");
    assert_eq!(
        selected_catchups_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(832, 2), (833, 1)]
    );
    let selected_catchup_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_catchups_best_effort);
    assert!(selected_catchup_best_effort_summaries
        .iter()
        .all(|summary| summary
            .catch_up_learner_ids_by_route_key()
            .iter()
            .all(|(_, learner_id)| *learner_id == Some(6))
            && summary
                .catch_up_caught_up_by_route_key()
                .iter()
                .all(|(_, caught_up)| *caught_up == Some(true))));
    let promote_plan = server
        .plan_promote_peer_for_groups([832, 833], 6)
        .expect("plan selected learner promote");
    assert_eq!(promote_plan.group_count, 2);
    assert_eq!(promote_plan.node_count, 3);
    assert_eq!(
        promote_plan.message_type,
        MatrixRaftMessageType::PromotePeer
    );
    let selected_promotions = server
        .promote_peer_for_groups([832, 833], 6)
        .expect("promote learner on selected groups");
    assert_eq!(
        selected_promotions
            .iter()
            .map(|(group_id, reports)| (*group_id, reports.len()))
            .collect::<Vec<_>>(),
        vec![(832, 2), (833, 1)]
    );
    assert!(selected_promotions
        .iter()
        .all(|(_, reports)| reports.iter().all(|report| report.promoted)));
    assert!(server
        .promote_peer_on_group_best_effort(832, 4)
        .expect("duplicate promote best effort")
        .iter()
        .all(|result| result.error.is_some()));
    assert!(server
        .group_statuses(833)
        .expect("data statuses after selected promote")
        .iter()
        .all(|status| status.membership.voters.contains(&6)));

    for group_id in [832, 833] {
        server
            .route_membership_operation_to_group(
                group_id,
                MembershipOperation::AddLearner(peer_with_role(group_id, 10, ReplicaRole::Learner)),
            )
            .expect("add best-effort promote learner");
    }
    server
        .catch_up_peer_for_groups([832, 833], 10)
        .expect("catch up best-effort promote learner");
    let selected_promotions_best_effort = server
        .promote_peer_for_groups_best_effort([832, 833], 10)
        .expect("best-effort promote learner on selected groups");
    assert_eq!(
        selected_promotions_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(832, 2), (833, 1)]
    );
    let selected_promotion_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_promotions_best_effort);
    assert!(selected_promotion_best_effort_summaries
        .iter()
        .all(|summary| summary
            .promote_learner_ids_by_route_key()
            .iter()
            .all(|(_, learner_id)| *learner_id == Some(10))
            && summary
                .promote_promoted_by_route_key()
                .iter()
                .all(|(_, promoted)| *promoted == Some(true))
            && summary
                .promote_membership_success_by_route_key()
                .iter()
                .all(|(_, success)| *success == Some(true))));

    for group_id in [832, 833] {
        server
            .route_membership_operation_to_group(
                group_id,
                MembershipOperation::AddLearner(peer_with_role(group_id, 8, ReplicaRole::Learner)),
            )
            .expect("add callback learner");
    }
    server
        .catch_up_peer_for_groups([832, 833], 8)
        .expect("catch up callback learner");
    let promote_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation, bool)>::new());
    let promote_callbacks = server
        .promote_peer_callbacks_for_groups(
            [832, 833],
            8,
            |key| {
                let hits = &promote_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation, result.ok));
                }
            },
            1_000,
        )
        .expect("selected promote callbacks");
    assert_eq!(
        promote_callbacks
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(832, 2), (833, 1)]
    );
    assert_eq!(promote_callback_hits.borrow().len(), 3);
    assert!(promote_callback_hits
        .borrow()
        .iter()
        .all(|(_, operation, ok)| *operation == MatrixRaftAsyncOperation::Promote && *ok));
    assert!(promote_callbacks.iter().all(|(_, results)| {
        results.iter().all(|(_, result)| {
            result
                .membership
                .as_ref()
                .is_some_and(|report| report.success && report.after.voters.contains(&8))
        })
    }));
    let promote_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&promote_callbacks);
    assert!(promote_callback_summaries.iter().all(|summary| {
        summary
            .membership_success_by_route_key()
            .iter()
            .all(|(_, success)| *success == Some(true))
            && summary
                .membership_reasons_by_route_key()
                .iter()
                .all(|(_, reason)| reason.is_some())
    }));

    server
        .route_config_change_to_group(
            832,
            MatrixRaftConfigChange {
                request_id: Some(70),
                change_type: MatrixRaftConfigChangeType::AddNode,
                member_id: 5,
                raft_addr: "127.0.0.1:83205".to_string(),
                snapshot_addr: "127.0.0.1:83305".to_string(),
                old_members: Vec::new(),
                conf_state: MatrixRaftConfState::Learner,
                auto_promote: true,
            },
        )
        .expect("add auto learner");
    server
        .route_config_change_to_group(
            833,
            MatrixRaftConfigChange {
                request_id: Some(71),
                change_type: MatrixRaftConfigChangeType::AddNode,
                member_id: 5,
                raft_addr: "127.0.0.1:83305".to_string(),
                snapshot_addr: "127.0.0.1:83405".to_string(),
                old_members: Vec::new(),
                conf_state: MatrixRaftConfState::Learner,
                auto_promote: true,
            },
        )
        .expect("add data auto learner");
    let auto_promotions = server
        .auto_promote_learner_on_group(832, 5)
        .expect("auto-promote learner by group");
    assert_eq!(auto_promotions.len(), 2);
    assert!(auto_promotions.iter().all(|report| {
        report.learner_id == 5
            && report.auto_promote
            && report.promoted
            && report.state_after == LearnerAutoPromoteState::Promoted
    }));
    assert!(
        server
            .auto_promote_learner_on_node(832, 1, 5)
            .expect("single auto-promote after promoted")
            .promoted
    );
    for group_id in [832, 833] {
        server
            .route_config_change_to_group(
                group_id,
                MatrixRaftConfigChange {
                    request_id: Some(80 + group_id),
                    change_type: MatrixRaftConfigChangeType::AddNode,
                    member_id: 7,
                    raft_addr: format!("127.0.0.1:{group_id}07"),
                    snapshot_addr: format!("127.0.0.1:{group_id}17"),
                    old_members: Vec::new(),
                    conf_state: MatrixRaftConfState::Learner,
                    auto_promote: true,
                },
            )
            .expect("add selected auto learner");
    }
    let auto_promote_plan = server
        .plan_auto_promote_learner_for_groups([832, 833], 7)
        .expect("plan selected auto-promote learner");
    assert_eq!(auto_promote_plan.group_count, 2);
    assert_eq!(auto_promote_plan.node_count, 3);
    assert_eq!(
        auto_promote_plan.message_type,
        MatrixRaftMessageType::AutoPromoteLearner
    );
    let selected_auto_promotions = server
        .auto_promote_learner_for_groups([832, 833], 7)
        .expect("auto-promote learner on selected groups");
    assert_eq!(
        selected_auto_promotions
            .iter()
            .map(|(group_id, reports)| (*group_id, reports.len()))
            .collect::<Vec<_>>(),
        vec![(832, 2), (833, 1)]
    );
    assert!(selected_auto_promotions.iter().all(|(_, reports)| {
        reports.iter().all(|report| {
            report.learner_id == 7
                && report.auto_promote
                && report.promoted
                && report.state_after == LearnerAutoPromoteState::Promoted
        })
    }));
    for group_id in [832, 833] {
        server
            .route_config_change_to_group(
                group_id,
                MatrixRaftConfigChange {
                    request_id: Some(85 + group_id),
                    change_type: MatrixRaftConfigChangeType::AddNode,
                    member_id: 11,
                    raft_addr: format!("127.0.0.1:{group_id}11"),
                    snapshot_addr: format!("127.0.0.1:{group_id}21"),
                    old_members: Vec::new(),
                    conf_state: MatrixRaftConfState::Learner,
                    auto_promote: true,
                },
            )
            .expect("add best-effort auto learner");
    }
    let selected_auto_promotions_best_effort = server
        .auto_promote_learner_for_groups_best_effort([832, 833], 11)
        .expect("best-effort auto-promote learner on selected groups");
    assert_eq!(
        selected_auto_promotions_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(832, 2), (833, 1)]
    );
    let selected_auto_promotion_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(
            &selected_auto_promotions_best_effort,
        );
    assert!(selected_auto_promotion_best_effort_summaries
        .iter()
        .all(|summary| summary
            .auto_promote_learner_ids_by_route_key()
            .iter()
            .all(|(_, learner_id)| *learner_id == Some(11))
            && summary
                .auto_promote_enabled_by_route_key()
                .iter()
                .all(|(_, enabled)| *enabled == Some(true))
            && summary
                .auto_promote_promoted_by_route_key()
                .iter()
                .all(|(_, promoted)| *promoted == Some(true))));
    for group_id in [832, 833] {
        server
            .route_config_change_to_group(
                group_id,
                MatrixRaftConfigChange {
                    request_id: Some(90 + group_id),
                    change_type: MatrixRaftConfigChangeType::AddNode,
                    member_id: 9,
                    raft_addr: format!("127.0.0.1:{group_id}09"),
                    snapshot_addr: format!("127.0.0.1:{group_id}19"),
                    old_members: Vec::new(),
                    conf_state: MatrixRaftConfState::Learner,
                    auto_promote: true,
                },
            )
            .expect("add callback auto learner");
    }
    let auto_promote_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation, bool)>::new());
    let auto_promote_callbacks = server
        .auto_promote_learner_callbacks_for_groups(
            [832, 833],
            9,
            |key| {
                let hits = &auto_promote_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation, result.ok));
                }
            },
            1_000,
        )
        .expect("selected auto-promote callbacks");
    assert_eq!(
        auto_promote_callbacks
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(832, 2), (833, 1)]
    );
    assert_eq!(auto_promote_callback_hits.borrow().len(), 3);
    assert!(auto_promote_callback_hits
        .borrow()
        .iter()
        .all(
            |(_, operation, ok)| *operation == MatrixRaftAsyncOperation::AutoPromoteLearner && *ok
        ));
    assert!(auto_promote_callbacks.iter().all(|(_, results)| {
        results.iter().all(|(_, result)| {
            result.auto_promote_presence()
                && !result.membership_presence()
                && result.auto_promote.as_ref().is_some_and(|report| {
                    report.learner_id == 9
                        && report.auto_promote
                        && report.promoted
                        && report.state_after == LearnerAutoPromoteState::Promoted
                })
        })
    }));
    let auto_promote_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&auto_promote_callbacks);
    assert!(auto_promote_callback_summaries.iter().all(|summary| {
        summary
            .auto_promote_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .auto_promote_learner_ids_by_route_key()
                .iter()
                .all(|(_, learner_id)| *learner_id == Some(9))
            && summary
                .auto_promote_enabled_by_route_key()
                .iter()
                .all(|(_, enabled)| *enabled == Some(true))
            && summary
                .auto_promote_promoted_by_route_key()
                .iter()
                .all(|(_, promoted)| *promoted == Some(true))
            && summary
                .membership_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    assert!(server
        .auto_promote_learner_for_groups_best_effort([832, 833], 99)
        .expect("missing learner auto-promote best effort")
        .iter()
        .all(|(_, results)| results.iter().all(|result| result.error.is_some())));

    assert_invalid_request_contains(
        server.catch_up_peer_for_groups([899, 832], 4),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_promote_peer_for_groups([899, 832], 4),
        "group 899 is not registered",
    );
    assert_eq!(
        server.promote_peer_on_node(832, 99, 4),
        Err(RaftError::NodeNotFound(99))
    );

    server.shutdown_all().expect("shutdown peer-control server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_routes_explicit_candidate_campaigns() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let wal_dir = temp_dir("candidate-campaign-wal");
    let snapshot_dir = temp_dir("candidate-campaign-snapshot");
    server
        .create_node(options(813, &wal_dir, &snapshot_dir), 1)
        .expect("campaign node");
    server.start_all(1).expect("start campaign server");

    let campaign_result = server
        .route_message(
            813,
            1,
            MatrixRaftMessage::admin(1, 1, MatrixRaftAdminCommand::campaign(2, true)),
        )
        .expect("explicit candidate campaign route");
    assert!(campaign_result.handled);
    assert_eq!(campaign_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(campaign_result.campaign_candidate_id_presence());
    assert!(campaign_result.campaign_forced_presence());
    assert_eq!(campaign_result.campaign_candidate_id, Some(2));
    assert_eq!(campaign_result.campaign_forced, Some(true));
    assert_eq!(
        server
            .node(813, 1)
            .expect("node")
            .get_status()
            .expect("status after explicit campaign")
            .leader_id,
        Some(2)
    );

    server.shutdown_all().expect("shutdown campaign server");

    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);
}

#[test]
fn matrixraft_multi_raft_server_controls_leadership_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("leadership-meta-1-wal");
    let meta_snap_1 = temp_dir("leadership-meta-1-snapshot");
    let meta_wal_2 = temp_dir("leadership-meta-2-wal");
    let meta_snap_2 = temp_dir("leadership-meta-2-snapshot");
    let data_wal = temp_dir("leadership-data-wal");
    let data_snap = temp_dir("leadership-data-snapshot");
    server
        .create_node(options_for_peer(834, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(834, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(835, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start leadership server");

    let campaigns = server
        .campaign_on_group(834, 1, true)
        .expect("campaign meta group");
    assert_eq!(campaigns.len(), 2);
    assert!(campaigns.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.campaign_candidate_id == Some(1)
            && result.campaign_forced == Some(true)
    }));
    assert!(server
        .group_statuses(834)
        .expect("meta statuses after campaign")
        .iter()
        .all(|status| status.leader_id == Some(1)));
    assert!(server
        .group_statuses(835)
        .expect("data statuses before leadership changes")
        .iter()
        .all(|status| status.leader_id != Some(2)));
    let campaign_plan = server
        .plan_campaigns_for_groups([834, 835], 1, true)
        .expect("plan selected campaigns");
    assert_eq!(campaign_plan.group_count, 2);
    assert_eq!(campaign_plan.node_count, 3);
    assert_eq!(
        campaign_plan.command_type,
        MatrixRaftAdminCommandType::Election
    );
    assert_eq!(
        campaign_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(834, 1),
            MatrixRaftRouteKey::new(834, 2),
            MatrixRaftRouteKey::new(835, 1),
        ]
    );
    assert_eq!(
        campaign_plan.command_types_by_group(),
        vec![
            (834, MatrixRaftAdminCommandType::Election),
            (835, MatrixRaftAdminCommandType::Election),
        ]
    );
    assert_eq!(
        campaign_plan.command_node_ids_by_group(),
        vec![(834, Some(1)), (835, Some(1))]
    );
    assert_eq!(
        campaign_plan.forced_campaigns_by_group(),
        vec![(834, true), (835, true)]
    );
    let selected_campaigns = server
        .campaigns_for_groups([834, 835], 1, true)
        .expect("campaign selected meta and data groups");
    assert_eq!(
        selected_campaigns
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_campaigns.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.kind == MatrixRaftRouteResultKind::Delivered
                && result.campaign_candidate_id == Some(1)
                && result.campaign_forced == Some(true)
        })
    }));
    let selected_campaign_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_campaigns);
    assert!(selected_campaign_summaries.iter().all(|summary| {
        summary
            .campaign_candidate_ids_by_route_key()
            .iter()
            .all(|(_, candidate_id)| *candidate_id == Some(1))
            && summary
                .campaign_forced_by_route_key()
                .iter()
                .all(|(_, forced)| *forced == Some(true))
    }));
    let selected_campaigns_best_effort = server
        .campaigns_for_groups_best_effort([834, 835], 1, true)
        .expect("best-effort campaign selected meta and data groups");
    assert_eq!(
        selected_campaigns_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_campaigns_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered
                    && route.campaign_candidate_id == Some(1)
                    && route.campaign_forced == Some(true)
            })
        })
    }));
    let selected_campaign_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_campaigns_best_effort);
    assert!(selected_campaign_best_effort_summaries
        .iter()
        .all(|summary| summary
            .campaign_candidate_ids_by_route_key()
            .iter()
            .all(|(_, candidate_id)| *candidate_id == Some(1))
            && summary
                .campaign_forced_by_route_key()
                .iter()
                .all(|(_, forced)| *forced == Some(true))));
    let timeout_now_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)>::new());
    let timeout_now_callbacks = server
        .timeout_now_callbacks_for_groups(
            [834, 835],
            1,
            1,
            |key| {
                let hits = &timeout_now_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation));
                }
            },
            1_000,
        )
        .expect("timeout-now callbacks for selected groups");
    assert_eq!(
        timeout_now_callbacks
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert_eq!(timeout_now_callback_hits.borrow().len(), 3);
    assert!(timeout_now_callback_hits
        .borrow()
        .iter()
        .all(|(_, operation)| *operation == MatrixRaftAsyncOperation::TimeoutNow));
    assert!(timeout_now_callbacks.iter().all(|(_, results)| {
        results
            .iter()
            .all(|(_, result)| result.timeout_now_presence() && !result.transfer_leader_presence())
    }));
    let timeout_now_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&timeout_now_callbacks);
    assert!(timeout_now_callback_summaries.iter().all(|summary| {
        summary
            .timeout_now_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .transfer_leader_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
            && summary
                .timeout_now_responses_by_route_key()
                .iter()
                .all(|(_, response)| response.is_some())
            && summary
                .timeout_now_node_ids_by_route_key()
                .iter()
                .all(|(_, node_id)| node_id.is_some())
            && summary
                .timeout_now_from_ids_by_route_key()
                .iter()
                .all(|(_, from)| from.is_some())
            && summary
                .timeout_now_campaigned_by_route_key()
                .iter()
                .all(|(_, campaigned)| campaigned.is_some())
            && summary
                .timeout_now_terms_by_route_key()
                .iter()
                .all(|(_, term)| term.is_some())
            && summary
                .timeout_now_reasons_by_route_key()
                .iter()
                .all(|(_, reason)| reason.as_ref().is_some_and(|reason| !reason.is_empty()))
    }));

    let transfer_plan = server
        .plan_transfer_leader_for_groups([834, 835], 2)
        .expect("plan selected transfer leadership");
    assert_eq!(transfer_plan.group_count, 2);
    assert_eq!(transfer_plan.node_count, 3);
    assert_eq!(
        transfer_plan.command_type,
        MatrixRaftAdminCommandType::TransferLeader
    );
    assert_eq!(
        transfer_plan.transferee_ids_by_group(),
        vec![(834, Some(2)), (835, Some(2))]
    );
    let transfers = server
        .transfer_leader_on_group(834, 2)
        .expect("transfer leadership by group");
    assert_eq!(transfers.len(), 2);
    assert!(transfers.iter().all(|result| {
        result
            .transfer_leader
            .as_ref()
            .is_some_and(|report| report.transferred && report.transferee_id == 2)
    }));
    let selected_transfers = server
        .transfer_leader_for_groups([834, 835], 1)
        .expect("transfer selected meta and data groups");
    assert_eq!(
        selected_transfers
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_transfers.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .transfer_leader
                .as_ref()
                .is_some_and(|report| report.transferred && report.transferee_id == 1)
        })
    }));
    let selected_transfer_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_transfers);
    assert!(selected_transfer_summaries.iter().all(|summary| {
        summary
            .transfer_leader_transferee_ids_by_route_key()
            .iter()
            .all(|(_, transferee_id)| *transferee_id == Some(1))
            && summary
                .transfer_leader_transferred_by_route_key()
                .iter()
                .all(|(_, transferred)| *transferred == Some(true))
    }));
    let selected_transfers_best_effort = server
        .transfer_leader_for_groups_best_effort([834, 835], 1)
        .expect("best-effort transfer selected meta and data groups");
    assert_eq!(
        selected_transfers_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_transfers_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .transfer_leader
                    .as_ref()
                    .is_some_and(|report| report.transferred && report.transferee_id == 1)
            })
        })
    }));
    let selected_transfer_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_transfers_best_effort);
    assert!(selected_transfer_best_effort_summaries
        .iter()
        .all(|summary| summary
            .transfer_leader_transferee_ids_by_route_key()
            .iter()
            .all(|(_, transferee_id)| *transferee_id == Some(1))
            && summary
                .transfer_leader_transferred_by_route_key()
                .iter()
                .all(|(_, transferred)| *transferred == Some(true))));
    let transfer_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)>::new());
    let transfer_callbacks = server
        .transfer_leader_callbacks_for_groups(
            [834, 835],
            1,
            |key| {
                let hits = &transfer_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation));
                }
            },
            1_000,
        )
        .expect("transfer callbacks for selected groups");
    assert_eq!(
        transfer_callbacks
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert_eq!(transfer_callback_hits.borrow().len(), 3);
    assert!(transfer_callback_hits
        .borrow()
        .iter()
        .all(|(_, operation)| *operation == MatrixRaftAsyncOperation::TransferLeader));
    assert!(transfer_callbacks.iter().all(|(_, results)| {
        results
            .iter()
            .all(|(_, result)| result.transfer_leader_presence() && !result.timeout_now_presence())
    }));
    let transfer_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&transfer_callbacks);
    assert!(transfer_callback_summaries.iter().all(|summary| {
        summary
            .transfer_leader_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .transfer_leader_transferee_ids_by_route_key()
                .iter()
                .all(|(_, transferee_id)| *transferee_id == Some(1))
            && summary
                .transfer_leader_transferred_by_route_key()
                .iter()
                .all(|(_, transferred)| *transferred == Some(true))
            && summary
                .timeout_now_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    let aborts = server
        .abort_leader_transfer_on_group(834, "group leadership abort")
        .expect("abort transfer by group");
    assert_eq!(aborts.len(), 2);
    assert!(aborts
        .iter()
        .all(|result| result.leader_transfer_aborted.is_some()));
    let selected_aborts = server
        .abort_leader_transfer_for_groups([834, 835], "selected leadership abort")
        .expect("abort selected meta and data transfers");
    assert_eq!(
        selected_aborts
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_aborts.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.leader_transfer_aborted.is_some())
    }));
    let selected_abort_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_aborts);
    assert!(selected_abort_summaries.iter().all(|summary| {
        summary
            .leader_transfer_aborted_by_route_key()
            .iter()
            .all(|(_, aborted)| aborted.is_some())
    }));
    let selected_aborts_best_effort = server
        .abort_leader_transfer_for_groups_best_effort([834, 835], "selected leadership abort")
        .expect("best-effort abort selected meta and data transfers");
    assert_eq!(
        selected_aborts_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_aborts_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.leader_transfer_aborted.is_some())
        })
    }));
    let selected_abort_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_aborts_best_effort);
    assert!(selected_abort_best_effort_summaries
        .iter()
        .all(|summary| summary
            .leader_transfer_aborted_by_route_key()
            .iter()
            .all(|(_, aborted)| aborted.is_some())));
    let complete_plan = server
        .plan_complete_leader_transfer_for_groups([834, 835])
        .expect("plan selected complete transfer");
    assert_eq!(complete_plan.group_count, 2);
    assert_eq!(complete_plan.node_count, 3);
    assert_eq!(
        complete_plan.command_type,
        MatrixRaftAdminCommandType::CompleteLeaderTransfer
    );
    let completes = server
        .complete_leader_transfer_on_group(834)
        .expect("complete transfer by group");
    assert_eq!(completes.len(), 2);
    assert!(completes
        .iter()
        .all(|result| result.leader_transfer_completed == Some(false)));
    let selected_completes = server
        .complete_leader_transfer_for_groups([834, 835])
        .expect("complete selected meta and data transfers");
    assert_eq!(
        selected_completes
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_completes.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.leader_transfer_completed.is_some())
    }));
    let selected_complete_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_completes);
    assert!(selected_complete_summaries.iter().all(|summary| {
        summary
            .leader_transfer_completed_by_route_key()
            .iter()
            .all(|(_, completed)| completed.is_some())
    }));
    let selected_completes_best_effort = server
        .complete_leader_transfer_for_groups_best_effort([834, 835])
        .expect("best-effort complete selected meta and data transfers");
    assert_eq!(
        selected_completes_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_completes_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.leader_transfer_completed.is_some())
        })
    }));
    let selected_complete_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_completes_best_effort);
    assert!(selected_complete_best_effort_summaries
        .iter()
        .all(|summary| summary
            .leader_transfer_completed_by_route_key()
            .iter()
            .all(|(_, completed)| completed.is_some())));

    server
        .campaign_on_node(834, 1, 1, true)
        .expect("restore node leadership for direct abort");
    server
        .transfer_leader_on_node(834, 1, 2)
        .expect("transfer leadership on one node");
    let abort_node = server
        .abort_leader_transfer_on_node(834, 1, "node leadership abort")
        .expect("abort transfer on one node");
    assert!(abort_node.leader_transfer_aborted.is_some());

    let step_down = server
        .step_down_on_node(834, 1, Some(2))
        .expect("step down single meta node");
    assert!(step_down.step_down.as_ref().is_some_and(|report| {
        report.stepped_down
            && report.requested_transferee_id == Some(2)
            && report.transferee_id == Some(2)
    }));
    server
        .campaigns_for_groups([834, 835], 1, true)
        .expect("restore selected group leadership before step-down");
    let step_down_plan = server
        .plan_step_down_for_groups([834, 835], Some(1))
        .expect("plan selected step-down");
    assert_eq!(step_down_plan.group_count, 2);
    assert_eq!(step_down_plan.node_count, 3);
    assert_eq!(
        step_down_plan.command_type,
        MatrixRaftAdminCommandType::StepDown
    );
    let selected_step_down = server
        .step_down_for_groups([834, 835], Some(1))
        .expect("step down selected meta and data groups");
    assert_eq!(
        selected_step_down
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_step_down.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .step_down
                .as_ref()
                .is_some_and(|report| report.stepped_down && report.transferee_id == Some(1))
        })
    }));
    let selected_step_down_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_step_down);
    assert!(selected_step_down_summaries.iter().all(|summary| {
        summary
            .step_down_requested_transferee_ids_by_route_key()
            .iter()
            .all(|(_, transferee_id)| *transferee_id == Some(1))
            && summary
                .step_down_transferee_ids_by_route_key()
                .iter()
                .all(|(_, transferee_id)| *transferee_id == Some(1))
            && summary
                .step_down_stepped_down_by_route_key()
                .iter()
                .all(|(_, stepped_down)| *stepped_down == Some(true))
    }));
    server
        .campaigns_for_groups([834, 835], 1, true)
        .expect("restore selected leadership before best-effort step-down");
    let selected_step_down_best_effort = server
        .step_down_for_groups_best_effort([834, 835], Some(1))
        .expect("best-effort step down selected meta and data groups");
    assert_eq!(
        selected_step_down_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_step_down_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .step_down
                    .as_ref()
                    .is_some_and(|report| report.stepped_down && report.transferee_id == Some(1))
            })
        })
    }));
    let selected_step_down_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_step_down_best_effort);
    assert!(selected_step_down_best_effort_summaries
        .iter()
        .all(|summary| summary
            .step_down_transferee_ids_by_route_key()
            .iter()
            .all(|(_, transferee_id)| *transferee_id == Some(1))
            && summary
                .step_down_stepped_down_by_route_key()
                .iter()
                .all(|(_, stepped_down)| *stepped_down == Some(true))));
    server
        .campaigns_for_groups([834, 835], 1, true)
        .expect("restore selected leadership before callback step-down");
    let step_down_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)>::new());
    let step_down_callbacks = server
        .step_down_callbacks_for_groups(
            [834, 835],
            Some(1),
            |key| {
                let hits = &step_down_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation));
                }
            },
            1_000,
        )
        .expect("step-down callbacks for selected groups");
    assert_eq!(
        step_down_callbacks
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert_eq!(step_down_callback_hits.borrow().len(), 3);
    assert!(step_down_callback_hits
        .borrow()
        .iter()
        .all(|(_, operation)| *operation == MatrixRaftAsyncOperation::StepDown));
    assert!(step_down_callbacks.iter().all(|(_, results)| {
        results
            .iter()
            .all(|(_, result)| result.step_down_presence() && !result.resign_presence())
    }));
    let step_down_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&step_down_callbacks);
    assert!(step_down_callback_summaries.iter().all(|summary| {
        summary
            .step_down_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .step_down_requested_transferee_ids_by_route_key()
                .iter()
                .all(|(_, transferee_id)| *transferee_id == Some(1))
            && summary
                .step_down_transferee_ids_by_route_key()
                .iter()
                .all(|(_, transferee_id)| *transferee_id == Some(1))
            && summary
                .step_down_stepped_down_by_route_key()
                .iter()
                .all(|(_, stepped_down)| *stepped_down == Some(true))
            && summary
                .resign_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    server
        .campaign_on_group(834, 1, true)
        .expect("restore meta leadership");
    let campaign_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation, bool)>::new());
    let campaign_callbacks = server
        .campaign_callbacks_on_group(
            835,
            |key| {
                let hits = &campaign_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation, result.ok));
                }
            },
            1_000,
        )
        .expect("campaign callbacks on data group");
    assert_eq!(campaign_callbacks.len(), 1);
    assert_eq!(campaign_callbacks[0].0, MatrixRaftRouteKey::new(835, 1));
    assert_eq!(
        campaign_callbacks[0].1.operation,
        MatrixRaftAsyncOperation::Campaign
    );
    assert_eq!(campaign_callback_hits.borrow().len(), 1);
    assert_eq!(
        campaign_callback_hits.borrow()[0].0,
        MatrixRaftRouteKey::new(835, 1)
    );
    assert_eq!(
        campaign_callback_hits.borrow()[0].1,
        MatrixRaftAsyncOperation::Campaign
    );

    let resigns = server
        .resign_leader_on_group(834)
        .expect("resign leadership by group");
    assert_eq!(resigns.len(), 2);
    assert!(resigns
        .iter()
        .all(|result| result.resign.as_ref().is_some_and(|report| report.resigned)));
    assert!(server
        .group_statuses(834)
        .expect("meta statuses after resign")
        .iter()
        .all(|status| status.leader_id.is_none()));
    server
        .campaigns_for_groups([834, 835], 1, true)
        .expect("restore selected leadership before resign");
    let forced_campaign_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)>::new());
    let forced_campaign_callbacks = server
        .forced_campaign_callbacks_for_groups(
            [834, 835],
            |key| {
                let hits = &forced_campaign_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation));
                }
            },
            1_000,
        )
        .expect("forced campaign callbacks for selected groups");
    assert_eq!(
        forced_campaign_callbacks
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert_eq!(forced_campaign_callback_hits.borrow().len(), 3);
    assert!(forced_campaign_callback_hits
        .borrow()
        .iter()
        .all(|(_, operation)| *operation == MatrixRaftAsyncOperation::ForcedCampaign));
    let resign_callback_hits =
        std::cell::RefCell::new(Vec::<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)>::new());
    let resign_callbacks = server
        .resign_leader_callbacks_for_groups(
            [834, 835],
            "selected callback resign",
            |key| {
                let hits = &resign_callback_hits;
                move |result: MatrixRaftAsyncResult| {
                    hits.borrow_mut().push((key, result.operation));
                }
            },
            1_000,
        )
        .expect("resign callbacks for selected groups");
    assert_eq!(
        resign_callbacks
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert_eq!(resign_callback_hits.borrow().len(), 3);
    assert!(resign_callback_hits
        .borrow()
        .iter()
        .all(|(_, operation)| *operation == MatrixRaftAsyncOperation::ResignLeader));
    assert!(resign_callbacks.iter().all(|(_, results)| {
        results
            .iter()
            .all(|(_, result)| result.resign_presence() && !result.step_down_presence())
    }));
    let resign_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&resign_callbacks);
    assert!(resign_callback_summaries.iter().all(|summary| {
        summary
            .resign_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .resign_reasons_by_route_key()
                .iter()
                .all(|(_, reason)| reason.as_deref() == Some("selected callback resign"))
            && summary
                .resign_resigned_by_route_key()
                .iter()
                .all(|(_, resigned)| *resigned == Some(true))
            && summary
                .step_down_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    server
        .campaigns_for_groups([834, 835], 1, true)
        .expect("restore selected leadership after callback resign");
    let resign_plan = server
        .plan_resign_leader_for_groups([834, 835])
        .expect("plan selected resign");
    assert_eq!(resign_plan.group_count, 2);
    assert_eq!(resign_plan.node_count, 3);
    assert_eq!(resign_plan.command_type, MatrixRaftAdminCommandType::Resign);
    let selected_resigns_best_effort = server
        .resign_leader_for_groups_best_effort([834, 835])
        .expect("best-effort resign selected meta and data groups");
    assert_eq!(
        selected_resigns_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_resigns_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.resign.as_ref().is_some_and(|report| report.resigned))
        })
    }));
    let selected_resign_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_resigns_best_effort);
    assert!(selected_resign_best_effort_summaries
        .iter()
        .all(|summary| summary
            .resign_resigned_by_route_key()
            .iter()
            .all(|(_, resigned)| *resigned == Some(true))));
    server
        .campaigns_for_groups([834, 835], 1, true)
        .expect("restore selected leadership before strict resign");
    let selected_resigns = server
        .resign_leader_for_groups([834, 835])
        .expect("resign selected meta and data groups");
    assert_eq!(
        selected_resigns
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(834, 2), (835, 1)]
    );
    assert!(selected_resigns.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.resign.as_ref().is_some_and(|report| report.resigned))
    }));
    let selected_resign_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_resigns);
    assert!(selected_resign_summaries.iter().all(|summary| {
        summary
            .resign_reasons_by_route_key()
            .iter()
            .all(|(_, reason)| reason.is_some())
            && summary
                .resign_resigned_by_route_key()
                .iter()
                .all(|(_, resigned)| *resigned == Some(true))
    }));
    assert_eq!(
        server
            .group_statuses(835)
            .expect("data statuses after meta leadership changes")
            .len(),
        1
    );

    assert_invalid_request_contains(
        server.campaign_on_group(899, 1, true),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.campaigns_for_groups([899, 834], 1, true),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_transfer_leader_for_groups([899, 834], 1),
        "group 899 is not registered",
    );
    assert_eq!(
        server.transfer_leader_on_node(834, 99, 2),
        Err(RaftError::NodeNotFound(99))
    );
    // A transfer to an unknown peer is *ignored*, not rejected: the admission
    // returns `IgnoredUnknownPeer` ("ignored_unknown_transferee") and
    // `transfer_leader` reports `Ok`, which matches how etcd/raft drops a
    // `MsgTransferLeader` naming a peer it does not know.
    //
    // This used to assert that every routed result carried an error. That only
    // held when the nodes happened to answer `NoLeader` first -- a state that
    // depends on where the wall-clock leader lease sits when the call lands --
    // so it failed roughly twice in sixty runs. The guarantee worth asserting
    // is the one the ignore exists to provide: an unknown transferee cannot
    // move leadership.
    let invalid_transfer = server
        .transfer_leader_on_group_best_effort(834, 99)
        .expect("invalid transfer target best effort");
    assert!(!invalid_transfer.is_empty());
    // Comparing leadership before and after would NOT be stable: leadership
    // converges asynchronously, so two reads can differ for reasons unrelated
    // to the transfer -- under CPU load this was seen going from [None, None]
    // to [Some(1), Some(2)]. What does hold regardless of timing is that a peer
    // the group does not know can never become its leader.
    assert!(server
        .group_leaders(834)
        .expect("group leaders after invalid transfer")
        .iter()
        .all(|leader| leader.as_ref().map(|node| node.peer_id) != Some(99)));

    server.shutdown_all().expect("shutdown leadership server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_controls_snapshot_lifecycle_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("snapshot-meta-1-wal");
    let meta_snap_1 = temp_dir("snapshot-meta-1-snapshot");
    let meta_wal_2 = temp_dir("snapshot-meta-2-wal");
    let meta_snap_2 = temp_dir("snapshot-meta-2-snapshot");
    let data_wal_1 = temp_dir("snapshot-data-1-wal");
    let data_snap_1 = temp_dir("snapshot-data-1-snapshot");
    let data_wal_2 = temp_dir("snapshot-data-2-wal");
    let data_snap_2 = temp_dir("snapshot-data-2-snapshot");
    server
        .create_node(options_for_peer(836, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(836, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options_for_peer(837, 1, &data_wal_1, &data_snap_1), 1)
        .expect("data node 1");
    server
        .create_node(options_for_peer(837, 2, &data_wal_2, &data_snap_2), 1)
        .expect("data node 2");
    server.start_all(1).expect("start snapshot server");

    let direct_snapshot_plan = server
        .plan_async_snapshots_for_groups([836, 837])
        .expect("plan direct selected snapshots");
    assert_eq!(
        direct_snapshot_plan.command_type,
        MatrixRaftAdminCommandType::TriggerSnapshot
    );
    assert_eq!(direct_snapshot_plan.group_count, 2);
    assert_eq!(direct_snapshot_plan.node_count, 4);
    let direct_group_snapshots = server
        .async_snapshots_on_group(836)
        .expect("direct async snapshots on meta group");
    assert_eq!(
        direct_group_snapshots
            .iter()
            .map(|(key, snapshot)| (key.group_id, key.node_id, snapshot.snapshot_id.is_empty()))
            .collect::<Vec<_>>(),
        vec![(836, 1, false), (836, 2, false)]
    );
    let direct_group_snapshot_ids = direct_group_snapshots
        .iter()
        .map(|(key, snapshot)| (*key, snapshot.snapshot_id.clone()))
        .collect::<Vec<_>>();
    let direct_group_ready_plan = server
        .plan_async_snapshot_ready_for_nodes(direct_group_snapshot_ids.clone(), true)
        .expect("plan direct group snapshot ready");
    assert_eq!(
        direct_group_ready_plan.operation,
        "async_snapshot_ready:true"
    );
    assert_eq!(direct_group_ready_plan.group_count, 1);
    assert_eq!(direct_group_ready_plan.node_count, 2);
    assert_eq!(
        direct_group_ready_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(836, 1),
            MatrixRaftRouteKey::new(836, 2)
        ]
    );
    assert_eq!(
        server
            .async_snapshot_ready_for_nodes(direct_group_snapshot_ids, true)
            .expect("mark direct group snapshots ready"),
        vec![
            MatrixRaftRouteKey::new(836, 1),
            MatrixRaftRouteKey::new(836, 2)
        ]
    );
    let direct_node_snapshot = server
        .async_snapshot_on_node(836, 1)
        .expect("direct async snapshot on node");
    assert!(!direct_node_snapshot.snapshot_id.is_empty());
    server
        .async_snapshot_applied_on_node(836, 1, &direct_node_snapshot.snapshot_id)
        .expect("mark direct node snapshot applied");
    let direct_selected_snapshots = server
        .async_snapshots_for_groups([836, 837])
        .expect("direct async snapshots on selected groups");
    assert_eq!(
        direct_selected_snapshots
            .iter()
            .map(|(group_id, snapshots)| (*group_id, snapshots.len()))
            .collect::<Vec<_>>(),
        vec![(836, 2), (837, 2)]
    );
    assert!(direct_selected_snapshots
        .iter()
        .all(|(group_id, snapshots)| {
            snapshots.iter().all(|(key, snapshot)| {
                key.group_id == *group_id && !snapshot.snapshot_id.is_empty()
            })
        }));
    let direct_selected_snapshot_ids = direct_selected_snapshots
        .iter()
        .flat_map(|(_, snapshots)| {
            snapshots
                .iter()
                .map(|(key, snapshot)| (*key, snapshot.snapshot_id.clone()))
        })
        .collect::<Vec<_>>();
    let direct_selected_ready_plan = server
        .plan_async_snapshot_ready_for_nodes(direct_selected_snapshot_ids.clone(), true)
        .expect("plan direct selected snapshots ready");
    assert_eq!(
        direct_selected_ready_plan.operation,
        "async_snapshot_ready:true"
    );
    assert_eq!(direct_selected_ready_plan.group_count, 2);
    assert_eq!(direct_selected_ready_plan.node_count, 4);
    assert_eq!(
        direct_selected_ready_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect::<Vec<_>>(),
        vec![(836, vec![1, 2]), (837, vec![1, 2])]
    );
    assert_eq!(
        server
            .async_snapshot_ready_for_nodes(direct_selected_snapshot_ids.clone(), true)
            .expect("mark direct selected snapshots ready"),
        vec![
            MatrixRaftRouteKey::new(836, 1),
            MatrixRaftRouteKey::new(836, 2),
            MatrixRaftRouteKey::new(837, 1),
            MatrixRaftRouteKey::new(837, 2),
        ]
    );
    let direct_selected_ready_best_effort_snapshots = server
        .async_snapshots_for_groups([836, 837])
        .expect("direct snapshots for best-effort ready");
    let mut direct_selected_ready_best_effort_ids = direct_selected_ready_best_effort_snapshots
        .iter()
        .flat_map(|(_, snapshots)| {
            snapshots
                .iter()
                .map(|(key, snapshot)| (*key, snapshot.snapshot_id.clone()))
        })
        .collect::<Vec<_>>();
    direct_selected_ready_best_effort_ids.push((
        MatrixRaftRouteKey::new(837, 99),
        "missing-direct-ready".to_string(),
    ));
    direct_selected_ready_best_effort_ids.push(direct_selected_ready_best_effort_ids[0].clone());
    let direct_ready_best_effort = server
        .async_snapshot_ready_for_nodes_best_effort(direct_selected_ready_best_effort_ids, true);
    assert_eq!(direct_ready_best_effort.len(), 6);
    assert_eq!(
        direct_ready_best_effort
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        4
    );
    assert!(direct_ready_best_effort.iter().any(|result| {
        result.key == MatrixRaftRouteKey::new(837, 99) && result.error.is_some()
    }));
    assert!(direct_ready_best_effort.iter().any(|result| {
        result.key == MatrixRaftRouteKey::new(836, 1)
            && result
                .error
                .as_ref()
                .is_some_and(|error| error.contains("appears more than once"))
    }));
    let direct_selected_applied_snapshots = server
        .async_snapshots_for_groups([836, 837])
        .expect("direct async snapshots for selected apply");
    let direct_selected_applied_ids = direct_selected_applied_snapshots
        .iter()
        .flat_map(|(_, snapshots)| {
            snapshots
                .iter()
                .map(|(key, snapshot)| (*key, snapshot.snapshot_id.clone()))
        })
        .collect::<Vec<_>>();
    let direct_selected_applied_plan = server
        .plan_async_snapshot_applied_for_nodes(direct_selected_applied_ids.clone())
        .expect("plan direct selected snapshots applied");
    assert_eq!(
        direct_selected_applied_plan.operation,
        "async_snapshot_applied"
    );
    assert_eq!(direct_selected_applied_plan.group_count, 2);
    assert_eq!(direct_selected_applied_plan.node_count, 4);
    assert_eq!(
        server
            .async_snapshot_applied_for_nodes(direct_selected_applied_ids)
            .expect("mark direct selected snapshots applied"),
        vec![
            MatrixRaftRouteKey::new(836, 1),
            MatrixRaftRouteKey::new(836, 2),
            MatrixRaftRouteKey::new(837, 1),
            MatrixRaftRouteKey::new(837, 2),
        ]
    );
    let direct_selected_applied_best_effort_snapshots = server
        .async_snapshots_for_groups([836, 837])
        .expect("direct snapshots for best-effort applied");
    let mut direct_selected_applied_best_effort_ids = direct_selected_applied_best_effort_snapshots
        .iter()
        .flat_map(|(_, snapshots)| {
            snapshots
                .iter()
                .map(|(key, snapshot)| (*key, snapshot.snapshot_id.clone()))
        })
        .collect::<Vec<_>>();
    direct_selected_applied_best_effort_ids.push((
        MatrixRaftRouteKey::new(836, 99),
        "missing-direct-applied".to_string(),
    ));
    direct_selected_applied_best_effort_ids
        .push(direct_selected_applied_best_effort_ids[0].clone());
    let direct_applied_best_effort = server
        .async_snapshot_applied_for_nodes_best_effort(direct_selected_applied_best_effort_ids);
    assert_eq!(direct_applied_best_effort.len(), 6);
    assert_eq!(
        direct_applied_best_effort
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        4
    );
    assert!(direct_applied_best_effort.iter().any(|result| {
        result.key == MatrixRaftRouteKey::new(836, 99) && result.error.is_some()
    }));
    assert!(direct_applied_best_effort.iter().any(|result| {
        result.key == MatrixRaftRouteKey::new(836, 1)
            && result
                .error
                .as_ref()
                .is_some_and(|error| error.contains("appears more than once"))
    }));
    let snapshot_callback_hits = std::cell::RefCell::new(Vec::<(
        MatrixRaftRouteKey,
        MatrixRaftAsyncOperation,
        Option<String>,
    )>::new());
    let snapshot_callbacks = server
        .async_snapshot_callbacks_for_groups([836, 837], |key| {
            let hits = &snapshot_callback_hits;
            move |result: MatrixRaftAsyncResult| {
                hits.borrow_mut().push((
                    key,
                    result.operation,
                    result
                        .snapshot
                        .as_ref()
                        .map(|snapshot| snapshot.snapshot_id.clone()),
                ));
            }
        })
        .expect("selected async snapshot callbacks");
    assert_eq!(
        snapshot_callbacks
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(836, 2), (837, 2)]
    );
    assert_eq!(snapshot_callback_hits.borrow().len(), 4);
    assert!(snapshot_callback_hits
        .borrow()
        .iter()
        .all(|(_, operation, snapshot_id)| {
            *operation == MatrixRaftAsyncOperation::AsyncSnapshot && snapshot_id.is_some()
        }));
    assert!(snapshot_callbacks.iter().all(|(_, results)| {
        results
            .iter()
            .all(|(_, result)| result.snapshot_presence() && !result.auto_promote_presence())
    }));
    let snapshot_callback_summaries =
        MatrixRaftAsyncGroupSummary::from_grouped_results(&snapshot_callbacks);
    assert!(snapshot_callback_summaries.iter().all(|summary| {
        summary
            .snapshot_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .snapshot_ids_by_route_key()
                .iter()
                .all(|(_, snapshot_id)| snapshot_id.is_some())
            && summary
                .snapshot_indices_by_route_key()
                .iter()
                .all(|(_, index)| index.is_some())
            && summary
                .auto_promote_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    let callback_snapshot_ids = snapshot_callbacks
        .iter()
        .flat_map(|(_, results)| {
            results.iter().map(|(key, result)| {
                (
                    *key,
                    result
                        .snapshot
                        .as_ref()
                        .expect("callback snapshot")
                        .snapshot_id
                        .clone(),
                )
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        server
            .async_snapshot_ready_for_nodes(callback_snapshot_ids, true)
            .expect("mark callback snapshots ready"),
        vec![
            MatrixRaftRouteKey::new(836, 1),
            MatrixRaftRouteKey::new(836, 2),
            MatrixRaftRouteKey::new(837, 1),
            MatrixRaftRouteKey::new(837, 2),
        ]
    );

    let triggers = server
        .trigger_snapshot_on_group(836)
        .expect("trigger snapshots by group");
    assert_eq!(triggers.len(), 2);
    assert!(triggers.iter().all(|result| result.snapshot.is_some()));
    for result in &triggers {
        let snapshot = result.snapshot.as_ref().expect("triggered snapshot");
        let ready_id = format!("836-{}", snapshot.index);
        assert_eq!(
            server
                .mark_snapshot_ready_on_node(836, result.key.node_id, &ready_id, true)
                .expect("mark snapshot ready")
                .kind,
            MatrixRaftRouteResultKind::Delivered
        );
    }

    let applied = server
        .trigger_snapshot_on_node(836, 1)
        .expect("trigger single snapshot")
        .snapshot
        .expect("single snapshot");
    let applied_id = format!("836-{}", applied.index);
    assert_eq!(
        server
            .mark_snapshot_applied_on_node(836, 1, &applied_id)
            .expect("mark snapshot applied")
            .kind,
        MatrixRaftRouteResultKind::Delivered
    );

    let begin_send = server
        .begin_snapshot_send_on_group(836, 2, "group-send-836", 12, 2)
        .expect("begin group snapshot send");
    assert_eq!(begin_send.len(), 2);
    assert!(begin_send.iter().all(|result| {
        result
            .snapshot_peer_report
            .as_ref()
            .is_some_and(|report| report.peer_id == 2 && report.status.snapshot_sending)
    }));
    let sent = server
        .record_snapshot_chunk_sent_on_group(836, 2, 8)
        .expect("record group snapshot chunk");
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|result| {
        result
            .snapshot_peer_report
            .as_ref()
            .is_some_and(|report| report.status.snapshot_sending)
    }));
    let retries = server
        .retry_snapshot_chunk_on_group(836, 2)
        .expect("retry group snapshot chunk");
    assert_eq!(retries.len(), 2);
    assert!(retries.iter().all(|result| {
        result
            .snapshot_peer_report
            .as_ref()
            .is_some_and(|report| report.status.snapshot_chunk_retry_count >= 1)
    }));
    let acks = server
        .acknowledge_snapshot_chunk_on_group(836, 2)
        .expect("acknowledge group snapshot chunk");
    assert_eq!(acks.len(), 2);
    assert!(acks.iter().all(|result| {
        result
            .snapshot_peer_report
            .as_ref()
            .is_some_and(|report| report.peer_id == 2)
    }));
    let cancels = server
        .cancel_snapshot_send_on_group(836, 2)
        .expect("cancel group snapshot send");
    assert_eq!(cancels.len(), 2);
    assert!(cancels.iter().all(|result| {
        result
            .snapshot_peer_report
            .as_ref()
            .is_some_and(|report| !report.status.snapshot_sending)
    }));

    let begin_install = server
        .begin_snapshot_install_on_group(836, 2, "group-install-836", 13, 2)
        .expect("begin group snapshot install");
    assert_eq!(begin_install.len(), 2);
    assert!(begin_install.iter().all(|result| {
        result
            .snapshot_peer_report
            .as_ref()
            .is_some_and(|report| report.status.snapshot_installing)
    }));
    let received = server
        .receive_snapshot_chunk_on_group(836, 2, 8, false)
        .expect("receive group snapshot chunk");
    assert_eq!(received.len(), 2);
    assert!(received.iter().all(|result| {
        result
            .snapshot_peer_report
            .as_ref()
            .is_some_and(|report| report.status.snapshot_install_progress_per_mille == 500)
    }));
    let rollbacks = server
        .rollback_snapshot_install_on_group(836, 2)
        .expect("rollback group snapshot install");
    assert_eq!(rollbacks.len(), 2);
    assert!(rollbacks.iter().all(|result| {
        result
            .snapshot_peer_report
            .as_ref()
            .is_some_and(|report| !report.status.snapshot_installing)
    }));
    let trigger_plan = server
        .plan_trigger_snapshot_for_groups([836, 837])
        .expect("plan selected trigger snapshots");
    assert_eq!(trigger_plan.group_count, 2);
    assert_eq!(trigger_plan.node_count, 4);
    assert_eq!(
        trigger_plan.command_type,
        MatrixRaftAdminCommandType::TriggerSnapshot
    );
    assert_eq!(
        trigger_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(836, 1),
            MatrixRaftRouteKey::new(836, 2),
            MatrixRaftRouteKey::new(837, 1),
            MatrixRaftRouteKey::new(837, 2),
        ]
    );
    let selected_triggers = server
        .trigger_snapshot_for_groups([836, 837])
        .expect("trigger selected meta and data snapshots");
    assert_eq!(
        selected_triggers
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(836, 2), (837, 2)]
    );
    assert!(selected_triggers
        .iter()
        .all(|(_, results)| results.iter().all(|result| result.snapshot.is_some())));
    let selected_triggers_best_effort = server
        .trigger_snapshot_for_groups_best_effort([836, 837])
        .expect("best-effort trigger selected meta and data snapshots");
    assert_eq!(
        selected_triggers_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(836, 2), (837, 2)]
    );
    assert!(selected_triggers_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.snapshot.is_some())
        })
    }));
    let ready_plan = server
        .plan_mark_snapshot_ready_for_groups([836, 837], "selected-ready-snapshot", true)
        .expect("plan selected snapshots ready");
    assert_eq!(ready_plan.group_count, 2);
    assert_eq!(ready_plan.node_count, 4);
    assert_eq!(
        ready_plan.command_type,
        MatrixRaftAdminCommandType::SnapshotReady
    );
    let selected_ready = server
        .mark_snapshot_ready_for_groups([836, 837], "selected-ready-snapshot", true)
        .expect("mark selected snapshots ready");
    assert_eq!(
        selected_ready
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(836, 2), (837, 2)]
    );
    assert!(selected_ready.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.kind == MatrixRaftRouteResultKind::Delivered && result.handled)
    }));
    let selected_ready_best_effort = server
        .mark_snapshot_ready_for_groups_best_effort(
            [836, 837],
            "selected-ready-snapshot-best-effort",
            true,
        )
        .expect("best-effort mark selected snapshots ready");
    assert!(selected_ready_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered && route.handled
            })
        })
    }));
    let selected_snapshot_ids = selected_triggers
        .iter()
        .map(|(group_id, results)| {
            let snapshot = results
                .first()
                .and_then(|result| result.snapshot.as_ref())
                .expect("selected group snapshot");
            (*group_id, format!("{}-{}", group_id, snapshot.index))
        })
        .collect::<Vec<_>>();
    let selected_best_effort_snapshot_ids = selected_triggers_best_effort
        .iter()
        .map(|(group_id, results)| {
            let snapshot = results
                .first()
                .and_then(|result| result.result.as_ref())
                .and_then(|route| route.snapshot.as_ref())
                .expect("best-effort selected group snapshot");
            (*group_id, format!("{}-{}", group_id, snapshot.index))
        })
        .collect::<Vec<_>>();
    let selected_group_ready = server
        .mark_snapshot_ready_for_group_snapshots(selected_snapshot_ids.clone(), true)
        .expect("mark selected group snapshots ready");
    assert_eq!(
        selected_group_ready
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(836, 2), (837, 2)]
    );
    assert!(selected_group_ready.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.kind == MatrixRaftRouteResultKind::Delivered && result.handled)
    }));
    let selected_group_ready_best_effort = server
        .mark_snapshot_ready_for_group_snapshots_best_effort(
            selected_best_effort_snapshot_ids.clone(),
            true,
        )
        .expect("best-effort mark selected group snapshots ready");
    assert!(selected_group_ready_best_effort
        .iter()
        .all(|(_, results)| results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered && route.handled
            })
        })));
    let selected_node_ready_ids = selected_triggers
        .iter()
        .flat_map(|(_, results)| {
            results.iter().map(|result| {
                let snapshot = result.snapshot.as_ref().expect("selected snapshot");
                (
                    result.key,
                    format!("{}-{}", result.key.group_id, snapshot.index),
                )
            })
        })
        .collect::<Vec<_>>();
    let selected_node_ready_plan = server
        .plan_mark_snapshot_ready_for_node_snapshots(selected_node_ready_ids.clone(), true)
        .expect("plan route-key selected snapshots ready");
    assert_eq!(
        selected_node_ready_plan.operation,
        "mark_snapshot_ready:true"
    );
    assert_eq!(selected_node_ready_plan.group_count, 2);
    assert_eq!(selected_node_ready_plan.node_count, 4);
    let selected_node_ready = server
        .mark_snapshot_ready_for_node_snapshots(selected_node_ready_ids, true)
        .expect("mark route-key selected snapshots ready");
    assert_eq!(selected_node_ready.len(), 4);
    assert!(selected_node_ready
        .iter()
        .all(|result| result.kind == MatrixRaftRouteResultKind::Delivered && result.handled));
    let selected_node_ready_best_effort_triggers = server
        .trigger_snapshot_for_groups([836, 837])
        .expect("trigger selected snapshots for best-effort route-key ready");
    let mut selected_node_ready_best_effort_ids = selected_node_ready_best_effort_triggers
        .iter()
        .flat_map(|(_, results)| {
            results.iter().map(|result| {
                let snapshot = result.snapshot.as_ref().expect("selected snapshot");
                (
                    result.key,
                    format!("{}-{}", result.key.group_id, snapshot.index),
                )
            })
        })
        .collect::<Vec<_>>();
    selected_node_ready_best_effort_ids.push((
        MatrixRaftRouteKey::new(837, 99),
        "missing-route-ready".to_string(),
    ));
    selected_node_ready_best_effort_ids.push(selected_node_ready_best_effort_ids[0].clone());
    let selected_node_ready_best_effort = server
        .mark_snapshot_ready_for_node_snapshots_best_effort(
            selected_node_ready_best_effort_ids,
            true,
        );
    assert_eq!(selected_node_ready_best_effort.len(), 6);
    assert_eq!(
        selected_node_ready_best_effort
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        4
    );
    assert!(selected_node_ready_best_effort
        .iter()
        .any(|result| { result.runtime_node_id == 99 && result.error.is_some() }));
    assert!(selected_node_ready_best_effort.iter().any(|result| {
        result.runtime_node_id == 1
            && result
                .error
                .as_ref()
                .is_some_and(|error| error.contains("appears more than once"))
    }));
    let selected_applied_triggers = server
        .trigger_snapshot_for_groups([836, 837])
        .expect("trigger selected snapshots for applied callbacks");
    let selected_applied_snapshot_ids = selected_applied_triggers
        .iter()
        .flat_map(|(_, results)| {
            results.iter().map(|result| {
                let snapshot = result
                    .snapshot
                    .as_ref()
                    .expect("selected applied node snapshot");
                (
                    result.key,
                    format!("{}-{}", result.key.group_id, snapshot.index),
                )
            })
        })
        .collect::<Vec<_>>();
    let applied_plan = server
        .plan_mark_snapshot_applied_for_node_snapshots(selected_applied_snapshot_ids.clone())
        .expect("plan route-key selected snapshots applied");
    assert_eq!(applied_plan.group_count, 2);
    assert_eq!(applied_plan.node_count, 4);
    assert_eq!(applied_plan.operation, "mark_snapshot_applied");
    let selected_applied = server
        .mark_snapshot_applied_for_node_snapshots(selected_applied_snapshot_ids)
        .expect("mark route-key selected snapshots applied");
    assert_eq!(selected_applied.len(), 4);
    assert!(selected_applied
        .iter()
        .all(|result| result.kind == MatrixRaftRouteResultKind::Delivered && result.handled));
    let selected_applied_node_best_effort_triggers = server
        .trigger_snapshot_for_groups([836, 837])
        .expect("trigger selected snapshots for best-effort route-key applied");
    let mut selected_applied_node_best_effort_ids = selected_applied_node_best_effort_triggers
        .iter()
        .flat_map(|(_, results)| {
            results.iter().map(|result| {
                let snapshot = result
                    .snapshot
                    .as_ref()
                    .expect("selected applied node snapshot");
                (
                    result.key,
                    format!("{}-{}", result.key.group_id, snapshot.index),
                )
            })
        })
        .collect::<Vec<_>>();
    selected_applied_node_best_effort_ids.push((
        MatrixRaftRouteKey::new(836, 99),
        "missing-route-applied".to_string(),
    ));
    selected_applied_node_best_effort_ids.push(selected_applied_node_best_effort_ids[0].clone());
    let selected_applied_node_best_effort = server
        .mark_snapshot_applied_for_node_snapshots_best_effort(
            selected_applied_node_best_effort_ids,
        );
    assert_eq!(selected_applied_node_best_effort.len(), 6);
    assert_eq!(
        selected_applied_node_best_effort
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        4
    );
    assert!(selected_applied_node_best_effort
        .iter()
        .any(|result| { result.runtime_node_id == 99 && result.error.is_some() }));
    assert!(selected_applied_node_best_effort.iter().any(|result| {
        result.runtime_node_id == 1
            && result
                .error
                .as_ref()
                .is_some_and(|error| error.contains("appears more than once"))
    }));
    let selected_applied_best_effort_triggers = server
        .trigger_snapshot_for_groups_best_effort([836, 837])
        .expect("best-effort trigger selected snapshots for applied callbacks");
    let selected_applied_best_effort_snapshot_ids = selected_applied_best_effort_triggers
        .iter()
        .map(|(group_id, results)| {
            let snapshot = results
                .first()
                .and_then(|result| result.result.as_ref())
                .and_then(|route| route.snapshot.as_ref())
                .expect("best-effort selected applied group snapshot");
            (*group_id, format!("{}-{}", group_id, snapshot.index))
        })
        .collect::<Vec<_>>();
    let selected_applied_best_effort = server
        .mark_snapshot_applied_for_group_snapshots_best_effort(
            selected_applied_best_effort_snapshot_ids,
        )
        .expect("best-effort mark selected snapshots applied");
    assert_eq!(
        selected_applied_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(836, 2), (837, 2)]
    );
    assert!(selected_applied_best_effort.iter().all(|(_, results)| {
        results
            .iter()
            .any(|result| result.result.as_ref().is_some_and(|route| route.handled))
    }));
    assert!(selected_applied_best_effort
        .iter()
        .flat_map(|(_, results)| results.iter())
        .all(|result| result.result.is_some() || result.error.is_some()));
    let begin_send_plan = server
        .plan_begin_snapshot_send_for_groups([836, 837], 2, "selected-send-snapshot", 21, 2)
        .expect("plan selected snapshot send");
    assert_eq!(begin_send_plan.group_count, 2);
    assert_eq!(begin_send_plan.node_count, 4);
    assert_eq!(
        begin_send_plan.command_type,
        MatrixRaftAdminCommandType::BeginSnapshotSend
    );
    let selected_begin_send = server
        .begin_snapshot_send_for_groups([836, 837], 2, "selected-send-snapshot", 21, 2)
        .expect("begin selected snapshot send");
    assert_eq!(
        selected_begin_send
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(836, 2), (837, 2)]
    );
    assert!(selected_begin_send.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| report.peer_id == 2 && report.status.snapshot_sending)
        })
    }));
    let selected_sent = server
        .record_snapshot_chunk_sent_for_groups([836, 837], 2, 8)
        .expect("record selected snapshot chunks");
    let snapshot_transfer_plans = [
        server
            .plan_record_snapshot_chunk_sent_for_groups([836, 837], 2, 8)
            .expect("plan selected snapshot chunk sent"),
        server
            .plan_retry_snapshot_chunk_for_groups([836, 837], 2)
            .expect("plan selected snapshot chunk retry"),
        server
            .plan_acknowledge_snapshot_chunk_for_groups([836, 837], 2)
            .expect("plan selected snapshot chunk acknowledge"),
        server
            .plan_cancel_snapshot_send_for_groups([836, 837], 2)
            .expect("plan selected snapshot send cancel"),
        server
            .plan_rollback_snapshot_install_for_groups([836, 837], 2)
            .expect("plan selected snapshot install rollback"),
    ];
    assert_eq!(
        snapshot_transfer_plans
            .iter()
            .map(|plan| plan.command_type)
            .collect::<Vec<_>>(),
        vec![
            MatrixRaftAdminCommandType::RecordSnapshotChunkSent,
            MatrixRaftAdminCommandType::RetrySnapshotChunk,
            MatrixRaftAdminCommandType::AcknowledgeSnapshotChunk,
            MatrixRaftAdminCommandType::CancelSnapshotSend,
            MatrixRaftAdminCommandType::RollbackSnapshotInstall,
        ]
    );
    assert!(snapshot_transfer_plans.iter().all(|plan| {
        plan.group_count == 2
            && plan.node_count == 4
            && plan.route_keys
                == vec![
                    MatrixRaftRouteKey::new(836, 1),
                    MatrixRaftRouteKey::new(836, 2),
                    MatrixRaftRouteKey::new(837, 1),
                    MatrixRaftRouteKey::new(837, 2),
                ]
    }));
    assert!(selected_sent.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| report.status.snapshot_sending)
        })
    }));
    let selected_retries = server
        .retry_snapshot_chunk_for_groups([836, 837], 2)
        .expect("retry selected snapshot chunks");
    assert!(selected_retries.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| report.status.snapshot_chunk_retry_count >= 1)
        })
    }));
    let selected_acks = server
        .acknowledge_snapshot_chunk_for_groups([836, 837], 2)
        .expect("acknowledge selected snapshot chunks");
    assert!(selected_acks.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| report.peer_id == 2)
        })
    }));
    let selected_cancels = server
        .cancel_snapshot_send_for_groups([836, 837], 2)
        .expect("cancel selected snapshot sends");
    assert!(selected_cancels.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| !report.status.snapshot_sending)
        })
    }));
    let begin_install_plan = server
        .plan_begin_snapshot_install_for_groups([836, 837], 2, "selected-install-snapshot", 22, 2)
        .expect("plan selected snapshot install");
    assert_eq!(begin_install_plan.group_count, 2);
    assert_eq!(begin_install_plan.node_count, 4);
    assert_eq!(
        begin_install_plan.command_type,
        MatrixRaftAdminCommandType::BeginSnapshotInstall
    );
    let selected_begin_install = server
        .begin_snapshot_install_for_groups([836, 837], 2, "selected-install-snapshot", 22, 2)
        .expect("begin selected snapshot install");
    assert!(selected_begin_install.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| report.status.snapshot_installing)
        })
    }));
    let receive_chunk_plan = server
        .plan_receive_snapshot_chunk_for_groups([836, 837], 2, 8, false)
        .expect("plan selected snapshot chunk receive");
    assert_eq!(receive_chunk_plan.group_count, 2);
    assert_eq!(receive_chunk_plan.node_count, 4);
    assert_eq!(
        receive_chunk_plan.command_type,
        MatrixRaftAdminCommandType::ReceiveSnapshotChunk
    );
    let selected_received = server
        .receive_snapshot_chunk_for_groups([836, 837], 2, 8, false)
        .expect("receive selected snapshot chunks");
    assert!(selected_received.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| report.status.snapshot_install_progress_per_mille == 500)
        })
    }));
    let selected_rollbacks = server
        .rollback_snapshot_install_for_groups([836, 837], 2)
        .expect("rollback selected snapshot installs");
    assert!(selected_rollbacks.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .snapshot_peer_report
                .as_ref()
                .is_some_and(|report| !report.status.snapshot_installing)
        })
    }));
    let selected_begin_send_best_effort = server
        .begin_snapshot_send_for_groups_best_effort(
            [836, 837],
            2,
            "selected-best-effort-send-snapshot",
            31,
            2,
        )
        .expect("best-effort begin selected snapshot send");
    assert_eq!(
        selected_begin_send_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(836, 2), (837, 2)]
    );
    assert!(selected_begin_send_best_effort
        .iter()
        .all(|(_, results)| results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .snapshot_peer_report
                    .as_ref()
                    .is_some_and(|report| report.peer_id == 2 && report.status.snapshot_sending)
            })
        })));
    let selected_sent_best_effort = server
        .record_snapshot_chunk_sent_for_groups_best_effort([836, 837], 2, 8)
        .expect("best-effort record selected snapshot chunks");
    assert!(selected_sent_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .snapshot_peer_report
                    .as_ref()
                    .is_some_and(|report| report.status.snapshot_sending)
            })
        })
    }));
    let selected_retries_best_effort = server
        .retry_snapshot_chunk_for_groups_best_effort([836, 837], 2)
        .expect("best-effort retry selected snapshot chunks");
    assert!(selected_retries_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .snapshot_peer_report
                    .as_ref()
                    .is_some_and(|report| report.status.snapshot_chunk_retry_count >= 1)
            })
        })
    }));
    let selected_acks_best_effort = server
        .acknowledge_snapshot_chunk_for_groups_best_effort([836, 837], 2)
        .expect("best-effort acknowledge selected snapshot chunks");
    assert!(selected_acks_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .snapshot_peer_report
                    .as_ref()
                    .is_some_and(|report| report.peer_id == 2)
            })
        })
    }));
    let selected_cancels_best_effort = server
        .cancel_snapshot_send_for_groups_best_effort([836, 837], 2)
        .expect("best-effort cancel selected snapshot sends");
    assert!(selected_cancels_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .snapshot_peer_report
                    .as_ref()
                    .is_some_and(|report| !report.status.snapshot_sending)
            })
        })
    }));
    let selected_begin_install_best_effort = server
        .begin_snapshot_install_for_groups_best_effort(
            [836, 837],
            2,
            "selected-best-effort-install-snapshot",
            32,
            2,
        )
        .expect("best-effort begin selected snapshot install");
    assert!(selected_begin_install_best_effort
        .iter()
        .all(|(_, results)| {
            results.iter().all(|result| {
                result.result.as_ref().is_some_and(|route| {
                    route
                        .snapshot_peer_report
                        .as_ref()
                        .is_some_and(|report| report.status.snapshot_installing)
                })
            })
        }));
    let selected_received_best_effort = server
        .receive_snapshot_chunk_for_groups_best_effort([836, 837], 2, 8, false)
        .expect("best-effort receive selected snapshot chunks");
    assert!(selected_received_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .snapshot_peer_report
                    .as_ref()
                    .is_some_and(|report| report.status.snapshot_install_progress_per_mille == 500)
            })
        })
    }));
    let selected_rollbacks_best_effort = server
        .rollback_snapshot_install_for_groups_best_effort([836, 837], 2)
        .expect("best-effort rollback selected snapshot installs");
    assert!(selected_rollbacks_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .snapshot_peer_report
                    .as_ref()
                    .is_some_and(|report| !report.status.snapshot_installing)
            })
        })
    }));
    assert!(server
        .group_statuses(837)
        .expect("data group status after meta snapshot controls")
        .iter()
        .all(|status| status.group_id == 837));

    let install_fence = ApplySnapshotFence {
        applied_index: 10_000,
        commit_index: 10_000,
        installed_snapshot_index: 10_000,
        first_retained_log_index: 10_001,
    };
    let meta_install_snapshot = RaftSnapshot {
        group_id: 836,
        meta: SnapshotMetadata {
            snapshot_id: "direct-install-836".to_string(),
            last_log_id: LogId {
                term: 1,
                index: 10_000,
            },
            membership: vec![1, 2],
            members: vec![
                peer_with_role(836, 1, ReplicaRole::Voter),
                peer_with_role(836, 2, ReplicaRole::Voter),
            ],
        },
        payload: b"direct meta install snapshot".to_vec(),
    };
    let direct_install = server
        .install_snapshot_on_node(
            836,
            1,
            2,
            meta_install_snapshot.clone(),
            install_fence.clone(),
        )
        .expect("direct full snapshot install");
    assert!(direct_install.is_ok());
    assert_eq!(direct_install.key, MatrixRaftRouteKey::new(836, 1));
    assert_eq!(direct_install.target, 2);
    assert_eq!(direct_install.snapshot_id, "direct-install-836");
    let meta_group_install_snapshot = RaftSnapshot {
        group_id: 836,
        meta: SnapshotMetadata {
            snapshot_id: "group-install-836".to_string(),
            last_log_id: LogId {
                term: 1,
                index: 10_010,
            },
            membership: vec![1, 2],
            members: vec![
                peer_with_role(836, 1, ReplicaRole::Voter),
                peer_with_role(836, 2, ReplicaRole::Voter),
            ],
        },
        payload: b"group meta install snapshot".to_vec(),
    };
    let meta_group_fence = ApplySnapshotFence {
        applied_index: 10_010,
        commit_index: 10_010,
        installed_snapshot_index: 10_010,
        first_retained_log_index: 10_011,
    };
    let meta_install_plan = server
        .plan_install_snapshot_on_group(
            836,
            2,
            meta_group_install_snapshot.clone(),
            meta_group_fence.clone(),
        )
        .expect("plan meta full snapshot install");
    assert_eq!(meta_install_plan.group_id, 836);
    assert_eq!(meta_install_plan.target, 2);
    assert_eq!(meta_install_plan.node_count, 2);
    assert_eq!(
        meta_install_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(836, 1),
            MatrixRaftRouteKey::new(836, 2)
        ]
    );
    let meta_group_installs = server
        .install_snapshot_on_group(836, 2, meta_group_install_snapshot, meta_group_fence)
        .expect("install full snapshot on meta group");
    assert_eq!(meta_group_installs.len(), 2);
    assert!(meta_group_installs.iter().all(|result| result.is_ok()));
    let selected_install_fence = ApplySnapshotFence {
        applied_index: 10_020,
        commit_index: 10_020,
        installed_snapshot_index: 10_020,
        first_retained_log_index: 10_021,
    };
    let selected_meta_snapshot = RaftSnapshot {
        group_id: 836,
        meta: SnapshotMetadata {
            snapshot_id: "selected-install-836".to_string(),
            last_log_id: LogId {
                term: 1,
                index: 10_020,
            },
            membership: vec![1, 2],
            members: vec![
                peer_with_role(836, 1, ReplicaRole::Voter),
                peer_with_role(836, 2, ReplicaRole::Voter),
            ],
        },
        payload: b"selected meta install snapshot".to_vec(),
    };
    let selected_data_snapshot = RaftSnapshot {
        group_id: 837,
        meta: SnapshotMetadata {
            snapshot_id: "selected-install-837".to_string(),
            last_log_id: LogId {
                term: 1,
                index: 10_020,
            },
            membership: vec![1, 2],
            members: vec![
                peer_with_role(837, 1, ReplicaRole::Voter),
                peer_with_role(837, 2, ReplicaRole::Voter),
            ],
        },
        payload: b"selected data install snapshot".to_vec(),
    };
    let selected_install_plan = server
        .plan_install_snapshots_for_groups([
            (
                836,
                2,
                selected_meta_snapshot.clone(),
                selected_install_fence.clone(),
            ),
            (
                837,
                2,
                selected_data_snapshot.clone(),
                selected_install_fence.clone(),
            ),
        ])
        .expect("plan selected full snapshot installs");
    assert_eq!(selected_install_plan.group_count, 2);
    assert_eq!(selected_install_plan.group_ids, vec![836, 837]);
    assert_eq!(selected_install_plan.node_count, 4);
    assert_eq!(
        selected_install_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.target, group.node_ids.clone()))
            .collect::<Vec<_>>(),
        vec![(836, 2, vec![1, 2]), (837, 2, vec![1, 2])]
    );
    assert_eq!(
        selected_install_plan.targets_by_group(),
        vec![(836, 2), (837, 2)]
    );
    assert_eq!(
        selected_install_plan.snapshots_by_group(),
        vec![
            (836, selected_meta_snapshot.clone()),
            (837, selected_data_snapshot.clone()),
        ]
    );
    assert_eq!(
        selected_install_plan.snapshots_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(836, 1),
                selected_meta_snapshot.clone()
            ),
            (
                MatrixRaftRouteKey::new(836, 2),
                selected_meta_snapshot.clone()
            ),
            (
                MatrixRaftRouteKey::new(837, 1),
                selected_data_snapshot.clone()
            ),
            (
                MatrixRaftRouteKey::new(837, 2),
                selected_data_snapshot.clone()
            ),
        ]
    );
    assert_eq!(
        selected_install_plan.snapshot_ids_by_group(),
        vec![
            (836, "selected-install-836".to_string()),
            (837, "selected-install-837".to_string()),
        ]
    );
    assert_eq!(
        selected_install_plan.snapshot_indices_by_group(),
        vec![(836, 10_020), (837, 10_020)]
    );
    assert_eq!(
        selected_install_plan.fences_by_group(),
        vec![
            (836, selected_install_fence.clone()),
            (837, selected_install_fence.clone()),
        ]
    );
    assert_eq!(
        selected_install_plan.fences_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(836, 1),
                selected_install_fence.clone()
            ),
            (
                MatrixRaftRouteKey::new(836, 2),
                selected_install_fence.clone()
            ),
            (
                MatrixRaftRouteKey::new(837, 1),
                selected_install_fence.clone()
            ),
            (
                MatrixRaftRouteKey::new(837, 2),
                selected_install_fence.clone()
            ),
        ]
    );
    assert_eq!(
        selected_install_plan.fence_applied_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(836, 1), 10_020),
            (MatrixRaftRouteKey::new(836, 2), 10_020),
            (MatrixRaftRouteKey::new(837, 1), 10_020),
            (MatrixRaftRouteKey::new(837, 2), 10_020),
        ]
    );
    assert_eq!(
        selected_install_plan.fence_commit_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(836, 1), 10_020),
            (MatrixRaftRouteKey::new(836, 2), 10_020),
            (MatrixRaftRouteKey::new(837, 1), 10_020),
            (MatrixRaftRouteKey::new(837, 2), 10_020),
        ]
    );
    assert_eq!(
        selected_install_plan.fence_installed_snapshot_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(836, 1), 10_020),
            (MatrixRaftRouteKey::new(836, 2), 10_020),
            (MatrixRaftRouteKey::new(837, 1), 10_020),
            (MatrixRaftRouteKey::new(837, 2), 10_020),
        ]
    );
    assert_eq!(
        selected_install_plan.fence_first_retained_log_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(836, 1), 10_021),
            (MatrixRaftRouteKey::new(836, 2), 10_021),
            (MatrixRaftRouteKey::new(837, 1), 10_021),
            (MatrixRaftRouteKey::new(837, 2), 10_021),
        ]
    );
    let selected_installs = server
        .install_snapshots_for_groups([
            (
                836,
                2,
                selected_meta_snapshot,
                selected_install_fence.clone(),
            ),
            (
                837,
                2,
                selected_data_snapshot,
                selected_install_fence.clone(),
            ),
        ])
        .expect("install selected full snapshots");
    assert_eq!(
        selected_installs
            .iter()
            .map(|group| (
                group.group_id,
                group.target,
                group.node_count,
                group.ok_count
            ))
            .collect::<Vec<_>>(),
        vec![(836, 2, 2, 2), (837, 2, 2, 2)]
    );
    assert!(selected_installs.iter().all(|group| group.is_ok()));
    server
        .shutdown_group_best_effort(836)
        .expect("shutdown meta group before best-effort full snapshot install");
    let retry_install_fence = ApplySnapshotFence {
        applied_index: 10_030,
        commit_index: 10_030,
        installed_snapshot_index: 10_030,
        first_retained_log_index: 10_031,
    };
    let retry_meta_snapshot = RaftSnapshot {
        group_id: 836,
        meta: SnapshotMetadata {
            snapshot_id: "retry-install-836".to_string(),
            last_log_id: LogId {
                term: 1,
                index: 10_030,
            },
            membership: vec![1, 2],
            members: vec![
                peer_with_role(836, 1, ReplicaRole::Voter),
                peer_with_role(836, 2, ReplicaRole::Voter),
            ],
        },
        payload: b"retry meta install snapshot".to_vec(),
    };
    let retry_data_snapshot = RaftSnapshot {
        group_id: 837,
        meta: SnapshotMetadata {
            snapshot_id: "retry-install-837".to_string(),
            last_log_id: LogId {
                term: 1,
                index: 10_030,
            },
            membership: vec![1, 2],
            members: vec![
                peer_with_role(837, 1, ReplicaRole::Voter),
                peer_with_role(837, 2, ReplicaRole::Voter),
            ],
        },
        payload: b"retry data install snapshot".to_vec(),
    };
    let retry_installs = server
        .install_snapshots_for_groups_best_effort([
            (836, 2, retry_meta_snapshot, retry_install_fence.clone()),
            (837, 2, retry_data_snapshot, retry_install_fence),
        ])
        .expect("best-effort selected full snapshot installs");
    assert_eq!(
        retry_installs
            .iter()
            .map(|group| (
                group.group_id,
                group.node_count,
                group.ok_count,
                group.error_count
            ))
            .collect::<Vec<_>>(),
        vec![(836, 2, 0, 2), (837, 2, 2, 0)]
    );
    assert!(retry_installs
        .iter()
        .find(|group| group.group_id == 836)
        .expect("shutdown meta install results")
        .results
        .iter()
        .all(|result| result.error.is_some()));
    assert!(retry_installs
        .iter()
        .find(|group| group.group_id == 837)
        .expect("data install results")
        .is_ok());
    let retry_meta_installs = retry_installs
        .iter()
        .find(|group| group.group_id == 836)
        .expect("shutdown meta install results by key");
    assert_eq!(
        retry_meta_installs.route_keys(),
        vec![
            MatrixRaftRouteKey::new(836, 1),
            MatrixRaftRouteKey::new(836, 2),
        ]
    );
    assert!(retry_meta_installs.ok_route_keys().is_empty());
    assert_eq!(
        retry_meta_installs.error_route_keys(),
        vec![
            MatrixRaftRouteKey::new(836, 1),
            MatrixRaftRouteKey::new(836, 2),
        ]
    );
    assert_eq!(retry_meta_installs.node_ids(), vec![1, 2]);
    assert!(retry_meta_installs.ok_node_ids().is_empty());
    assert_eq!(retry_meta_installs.error_node_ids(), vec![1, 2]);
    assert_eq!(
        retry_meta_installs.statuses_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(836, 1), false),
            (MatrixRaftRouteKey::new(836, 2), false),
        ]
    );
    assert!(retry_meta_installs
        .results_by_route_key()
        .iter()
        .all(|(_, result)| !result.is_ok() && result.error.is_some()));
    assert!(retry_meta_installs.ok_results_by_route_key().is_empty());
    assert_eq!(
        retry_meta_installs
            .error_results_by_route_key()
            .iter()
            .map(|(route_key, result)| (*route_key, result.target, result.snapshot_index))
            .collect::<Vec<_>>(),
        vec![
            (MatrixRaftRouteKey::new(836, 1), 2, 10_030),
            (MatrixRaftRouteKey::new(836, 2), 2, 10_030),
        ]
    );
    assert!(retry_meta_installs
        .errors_by_route_key()
        .iter()
        .all(|(_, error)| error.is_some()));
    assert_eq!(
        retry_meta_installs.targets_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(836, 1), 2),
            (MatrixRaftRouteKey::new(836, 2), 2),
        ]
    );
    assert_eq!(
        retry_meta_installs.snapshot_ids_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(836, 1),
                "retry-install-836".to_string()
            ),
            (
                MatrixRaftRouteKey::new(836, 2),
                "retry-install-836".to_string()
            ),
        ]
    );
    assert_eq!(
        retry_meta_installs.snapshot_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(836, 1), 10_030),
            (MatrixRaftRouteKey::new(836, 2), 10_030),
        ]
    );
    assert!(retry_meta_installs
        .ok_snapshot_ids_by_route_key()
        .is_empty());
    assert!(retry_meta_installs
        .ok_snapshot_indices_by_route_key()
        .is_empty());
    assert_eq!(
        retry_meta_installs.error_snapshot_ids_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(836, 1),
                "retry-install-836".to_string()
            ),
            (
                MatrixRaftRouteKey::new(836, 2),
                "retry-install-836".to_string()
            ),
        ]
    );
    assert_eq!(
        retry_meta_installs.error_snapshot_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(836, 1), 10_030),
            (MatrixRaftRouteKey::new(836, 2), 10_030),
        ]
    );
    let retry_data_installs = retry_installs
        .iter()
        .find(|group| group.group_id == 837)
        .expect("data install results by key");
    assert_eq!(
        retry_data_installs.ok_route_keys(),
        vec![
            MatrixRaftRouteKey::new(837, 1),
            MatrixRaftRouteKey::new(837, 2),
        ]
    );
    assert!(retry_data_installs.error_route_keys().is_empty());
    assert_eq!(retry_data_installs.node_ids(), vec![1, 2]);
    assert_eq!(retry_data_installs.ok_node_ids(), vec![1, 2]);
    assert!(retry_data_installs.error_node_ids().is_empty());
    assert_eq!(
        retry_data_installs.statuses_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(837, 1), true),
            (MatrixRaftRouteKey::new(837, 2), true),
        ]
    );
    assert!(retry_data_installs
        .results_by_route_key()
        .iter()
        .all(|(_, result)| result.is_ok() && result.error.is_none()));
    assert_eq!(
        retry_data_installs
            .ok_results_by_route_key()
            .iter()
            .map(|(route_key, result)| (*route_key, result.target, result.snapshot_index))
            .collect::<Vec<_>>(),
        vec![
            (MatrixRaftRouteKey::new(837, 1), 2, 10_030),
            (MatrixRaftRouteKey::new(837, 2), 2, 10_030),
        ]
    );
    assert!(retry_data_installs.error_results_by_route_key().is_empty());
    assert_eq!(
        retry_data_installs.errors_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(837, 1), None),
            (MatrixRaftRouteKey::new(837, 2), None),
        ]
    );
    assert_eq!(
        retry_data_installs.targets_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(837, 1), 2),
            (MatrixRaftRouteKey::new(837, 2), 2),
        ]
    );
    assert_eq!(
        retry_data_installs.ok_snapshot_ids_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(837, 1),
                "retry-install-837".to_string()
            ),
            (
                MatrixRaftRouteKey::new(837, 2),
                "retry-install-837".to_string()
            ),
        ]
    );
    assert_eq!(
        retry_data_installs.ok_snapshot_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(837, 1), 10_030),
            (MatrixRaftRouteKey::new(837, 2), 10_030),
        ]
    );
    assert!(retry_data_installs
        .error_snapshot_ids_by_route_key()
        .is_empty());
    assert!(retry_data_installs
        .error_snapshot_indices_by_route_key()
        .is_empty());

    assert_invalid_request_contains(
        server.trigger_snapshot_for_groups([899, 836]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.async_snapshots_for_groups([899, 836]),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.mark_snapshot_ready_for_groups([899, 836], "missing-ready", true),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_begin_snapshot_send_for_groups([899, 836], 2, "missing-send", 1, 1),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_cancel_snapshot_send_for_groups([899, 836], 2),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_install_snapshots_for_groups([(899, 2, meta_install_snapshot, install_fence)]),
        "group 899 is not registered",
    );
    assert_eq!(
        server.begin_snapshot_send_on_node(836, 99, 2, "missing-node-snapshot", 1, 1),
        Err(RaftError::NodeNotFound(99))
    );
    assert_eq!(
        server.install_snapshot_on_node(
            836,
            99,
            2,
            RaftSnapshot {
                group_id: 836,
                meta: SnapshotMetadata {
                    snapshot_id: "missing-install-node".to_string(),
                    last_log_id: LogId {
                        term: 1,
                        index: 10_040,
                    },
                    membership: vec![1, 2],
                    members: Vec::new(),
                },
                payload: Vec::new(),
            },
            ApplySnapshotFence {
                applied_index: 10_040,
                commit_index: 10_040,
                installed_snapshot_index: 10_040,
                first_retained_log_index: 10_041,
            },
        ),
        Err(RaftError::NodeNotFound(99))
    );
    assert_eq!(
        server.async_snapshot_on_node(836, 99),
        Err(RaftError::NodeNotFound(99))
    );
    assert_eq!(
        server.async_snapshot_ready_on_node(836, 99, "missing-ready", true),
        Err(RaftError::NodeNotFound(99))
    );
    assert_invalid_request_contains(
        server.plan_async_snapshot_ready_for_nodes(
            [
                (MatrixRaftRouteKey::new(836, 1), "duplicate-ready"),
                (MatrixRaftRouteKey::new(836, 1), "duplicate-ready"),
            ],
            true,
        ),
        "node 1 in group 836 appears more than once in async_snapshot_ready:true batch",
    );
    assert_invalid_request_contains(
        server.plan_mark_snapshot_ready_for_node_snapshots(
            [
                (MatrixRaftRouteKey::new(836, 1), "duplicate-ready"),
                (MatrixRaftRouteKey::new(836, 1), "duplicate-ready"),
            ],
            true,
        ),
        "node 1 in group 836 appears more than once in mark_snapshot_ready:true batch",
    );
    assert_eq!(
        server.async_snapshot_applied_for_nodes([(
            MatrixRaftRouteKey::new(836, 99),
            "missing-applied"
        )]),
        Err(RaftError::NodeNotFound(99))
    );
    assert_invalid_request_contains(
        server.async_snapshot_applied_for_nodes([
            (MatrixRaftRouteKey::new(836, 1), "duplicate-applied"),
            (MatrixRaftRouteKey::new(836, 1), "duplicate-applied"),
        ]),
        "node 1 in group 836 appears more than once in async_snapshot_applied batch",
    );
    assert_invalid_request_contains(
        server.mark_snapshot_applied_for_node_snapshots([
            (MatrixRaftRouteKey::new(836, 1), "duplicate-applied"),
            (MatrixRaftRouteKey::new(836, 1), "duplicate-applied"),
        ]),
        "node 1 in group 836 appears more than once in mark_snapshot_applied batch",
    );
    assert!(server
        .begin_snapshot_send_on_group_best_effort(836, 99, "missing-peer-snapshot", 1, 1)
        .expect("missing peer snapshot best effort")
        .iter()
        .all(|result| result.error.is_some()));

    server.shutdown_all().expect("shutdown snapshot server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal_1);
    let _ = fs::remove_dir_all(data_snap_1);
    let _ = fs::remove_dir_all(data_wal_2);
    let _ = fs::remove_dir_all(data_snap_2);
}

#[test]
fn matrixraft_multi_raft_server_controls_lease_and_attributes_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("lease-meta-1-wal");
    let meta_snap_1 = temp_dir("lease-meta-1-snapshot");
    let meta_wal_2 = temp_dir("lease-meta-2-wal");
    let meta_snap_2 = temp_dir("lease-meta-2-snapshot");
    let data_wal = temp_dir("lease-data-wal");
    let data_snap = temp_dir("lease-data-snapshot");
    server
        .create_node(options_for_peer(838, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(838, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options(839, &data_wal, &data_snap), 1)
        .expect("data node");
    server.start_all(1).expect("start lease server");

    let prohibits = server
        .set_prohibits_election_on_group(838, true)
        .expect("set prohibits election on meta group");
    assert_eq!(prohibits.len(), 2);
    assert!(prohibits
        .iter()
        .all(|result| { result.kind == MatrixRaftRouteResultKind::Delivered && result.handled }));
    let ignore_witness = server
        .set_ignore_witness_on_group(838, true)
        .expect("set ignore witness on meta group");
    assert_eq!(ignore_witness.len(), 2);
    assert!(ignore_witness
        .iter()
        .all(|result| { result.kind == MatrixRaftRouteResultKind::Delivered && result.handled }));
    let prohibits_plan = server
        .plan_set_prohibits_election_for_groups([838, 839], false)
        .expect("plan selected prohibits election");
    assert_eq!(prohibits_plan.group_count, 2);
    assert_eq!(prohibits_plan.node_count, 3);
    assert_eq!(
        prohibits_plan.command_type,
        MatrixRaftAdminCommandType::ProhibitsElection
    );
    assert_eq!(
        prohibits_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(838, 1),
            MatrixRaftRouteKey::new(838, 2),
            MatrixRaftRouteKey::new(839, 1),
        ]
    );
    let selected_prohibits = server
        .set_prohibits_election_for_groups([838, 839], false)
        .expect("set prohibits election on selected groups");
    assert_eq!(
        selected_prohibits
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(838, 2), (839, 1)]
    );
    assert!(selected_prohibits.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.kind == MatrixRaftRouteResultKind::Delivered && result.handled)
    }));
    let selected_prohibits_best_effort = server
        .set_prohibits_election_for_groups_best_effort([838, 839], true)
        .expect("best-effort set prohibits election on selected groups");
    assert_eq!(
        selected_prohibits_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(838, 2), (839, 1)]
    );
    assert!(selected_prohibits_best_effort
        .iter()
        .all(|(_, results)| results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered && route.handled
            })
        })));
    let ignore_plan = server
        .plan_set_ignore_witness_for_groups([838, 839], false)
        .expect("plan selected ignore witness");
    assert_eq!(ignore_plan.group_count, 2);
    assert_eq!(ignore_plan.node_count, 3);
    assert_eq!(
        ignore_plan.command_type,
        MatrixRaftAdminCommandType::IgnoreWitness
    );
    let selected_ignore_witness = server
        .set_ignore_witness_for_groups([838, 839], false)
        .expect("set ignore witness on selected groups");
    assert_eq!(
        selected_ignore_witness
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(838, 2), (839, 1)]
    );
    assert!(selected_ignore_witness.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.kind == MatrixRaftRouteResultKind::Delivered && result.handled)
    }));
    let selected_ignore_witness_best_effort = server
        .set_ignore_witness_for_groups_best_effort([838, 839], true)
        .expect("best-effort set ignore witness on selected groups");
    assert_eq!(
        selected_ignore_witness_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(838, 2), (839, 1)]
    );
    assert!(selected_ignore_witness_best_effort
        .iter()
        .all(|(_, results)| results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered && route.handled
            })
        })));

    // KNOWN LOAD-SENSITIVE, and not fixable from here.
    //
    // This samples the data group's lease before and after invalidating the
    // meta group's, and requires the two to match. The lease clock advances on
    // its own, so the samples can differ for reasons unrelated to the meta
    // group: under 2x CPU oversubscription this fails roughly 3% of runs.
    //
    // Pinning the lease first does NOT help, and was tried: neither `true` nor
    // `false` is a stable state while the automatic tick runs, because with
    // `lease_duration_ms: 20` against a 10ms tick the lease both expires and
    // renews within a couple of ticks. The assertion two below
    // (`!status.leader_lease_valid` on the meta group) fails for the same
    // reason and predates this note.
    //
    // Making this deterministic needs the automatic tick suppressed for this
    // binary -- `tick_interval_ms` far larger than the test. That is a change
    // to the shared `options()` helper affecting all 33 tests here, several of
    // which rely on tick-driven timeouts (`election_cycle_tick`,
    // `transfer_timeout_tick`, `offline_timeout_tick`), so it needs doing
    // deliberately rather than as a drive-by.
    let data_lease_before: Vec<_> = server
        .group_statuses(839)
        .expect("data statuses before meta lease invalidation")
        .into_iter()
        .map(|status| status.leader_lease_valid)
        .collect();

    let lease_invalid = server
        .set_leader_lease_valid_on_group(838, false)
        .expect("invalidate leader lease on meta group");
    assert_eq!(lease_invalid.len(), 2);
    assert!(lease_invalid
        .iter()
        .all(|result| result.leader_lease_valid == Some(false)));
    assert!(server
        .group_statuses(838)
        .expect("meta statuses after lease invalidation")
        .iter()
        .all(|status| !status.leader_lease_valid));
    let data_lease_after: Vec<_> = server
        .group_statuses(839)
        .expect("data statuses after meta lease invalidation")
        .into_iter()
        .map(|status| status.leader_lease_valid)
        .collect();
    assert_eq!(data_lease_after, data_lease_before);
    let lease_plan = server
        .plan_set_leader_lease_valid_for_groups([838, 839], false)
        .expect("plan selected leader lease validity");
    assert_eq!(lease_plan.group_count, 2);
    assert_eq!(lease_plan.node_count, 3);
    assert_eq!(
        lease_plan.command_type,
        MatrixRaftAdminCommandType::SetLeaderLeaseValid
    );
    let selected_lease_invalid = server
        .set_leader_lease_valid_for_groups([838, 839], false)
        .expect("invalidate selected group leader leases");
    assert_eq!(
        selected_lease_invalid
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(838, 2), (839, 1)]
    );
    assert!(selected_lease_invalid.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.leader_lease_valid == Some(false))
    }));
    let selected_lease_invalid_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_lease_invalid);
    assert!(selected_lease_invalid_summaries
        .iter()
        .all(|summary| summary
            .leader_lease_valid_by_route_key
            .iter()
            .all(|(_, valid)| valid == &Some(false))));
    assert!(selected_lease_invalid_summaries
        .iter()
        .all(|summary| summary
            .leader_lease_valid_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));
    assert!(selected_lease_invalid_summaries
        .iter()
        .all(|summary| summary
            .leader_lease_valid_values_by_route_key()
            .iter()
            .all(|(_, valid)| *valid == Some(false))));
    let selected_lease_valid_best_effort = server
        .set_leader_lease_valid_for_groups_best_effort([838, 839], true)
        .expect("best-effort validate selected group leader leases");
    assert!(selected_lease_valid_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.leader_lease_valid == Some(true))
        })
    }));
    let selected_lease_valid_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_lease_valid_best_effort);
    assert!(selected_lease_valid_summaries.iter().all(|summary| summary
        .leader_lease_valid_by_route_key
        .iter()
        .all(|(_, valid)| valid == &Some(true))));
    assert!(selected_lease_valid_summaries.iter().all(|summary| summary
        .leader_lease_valid_presence_by_route_key()
        .iter()
        .all(|(_, present)| *present)));
    assert!(selected_lease_valid_summaries.iter().all(|summary| summary
        .leader_lease_valid_values_by_route_key()
        .iter()
        .all(|(_, valid)| *valid == Some(true))));

    let confirmations = server
        .receive_leader_lease_confirmation_on_group(838, 2, 77, Some(5))
        .expect("confirm leader lease on meta group");
    assert_eq!(confirmations.len(), 2);
    assert!(confirmations
        .iter()
        .all(|result| result.leader_lease_confirmed.is_some()));
    let leader_ticks = server
        .tick_leader_lease_on_group(838, 5)
        .expect("tick leader lease on meta group");
    assert_eq!(leader_ticks.len(), 2);
    assert!(leader_ticks
        .iter()
        .all(|result| result.leader_lease_expired.is_some()));
    let confirmation_plan = server
        .plan_receive_leader_lease_confirmation_for_groups([838, 839], 2, 78, Some(6))
        .expect("plan selected leader lease confirmation");
    assert_eq!(confirmation_plan.group_count, 2);
    assert_eq!(confirmation_plan.node_count, 3);
    assert_eq!(
        confirmation_plan.command_type,
        MatrixRaftAdminCommandType::ReceiveLeaderLeaseConfirmation
    );
    let selected_confirmations = server
        .receive_leader_lease_confirmation_for_groups([838, 839], 2, 78, Some(6))
        .expect("confirm selected group leader leases");
    assert_eq!(
        selected_confirmations
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(838, 2), (839, 1)]
    );
    assert!(selected_confirmations.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.leader_lease_confirmed.is_some())
    }));
    let selected_confirmation_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_confirmations);
    assert!(selected_confirmation_summaries.iter().all(|summary| summary
        .leader_lease_confirmed_presence_by_route_key()
        .iter()
        .all(|(_, present)| *present)));
    assert!(selected_confirmation_summaries.iter().all(|summary| summary
        .leader_lease_confirmed_values_by_route_key()
        .iter()
        .all(|(_, confirmed)| confirmed.is_some())));
    let selected_confirmations_best_effort = server
        .receive_leader_lease_confirmation_for_groups_best_effort([838, 839], 2, 79, Some(7))
        .expect("best-effort confirm selected group leader leases");
    assert_eq!(
        selected_confirmations_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(838, 2), (839, 1)]
    );
    assert!(selected_confirmations_best_effort
        .iter()
        .all(|(_, results)| results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.leader_lease_confirmed.is_some())
        })));
    let selected_confirmation_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_confirmations_best_effort);
    assert!(selected_confirmation_best_effort_summaries
        .iter()
        .all(|summary| summary
            .leader_lease_confirmed_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));
    assert!(selected_confirmation_best_effort_summaries
        .iter()
        .all(|summary| summary
            .leader_lease_confirmed_values_by_route_key()
            .iter()
            .all(|(_, confirmed)| confirmed.is_some())));
    let leader_tick_plan = server
        .plan_tick_leader_lease_for_groups([838, 839], 6)
        .expect("plan selected leader lease tick");
    assert_eq!(leader_tick_plan.group_count, 2);
    assert_eq!(leader_tick_plan.node_count, 3);
    assert_eq!(
        leader_tick_plan.command_type,
        MatrixRaftAdminCommandType::TickLeaderLease
    );
    let selected_leader_ticks = server
        .tick_leader_lease_for_groups([838, 839], 6)
        .expect("tick selected group leader leases");
    assert!(selected_leader_ticks.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.leader_lease_expired.is_some())
    }));
    let selected_leader_tick_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_leader_ticks);
    assert!(selected_leader_tick_summaries.iter().all(|summary| summary
        .leader_lease_expired_presence_by_route_key()
        .iter()
        .all(|(_, present)| *present)));
    assert!(selected_leader_tick_summaries.iter().all(|summary| summary
        .leader_lease_expired_values_by_route_key()
        .iter()
        .all(|(_, expired)| expired.is_some())));
    let selected_leader_ticks_best_effort = server
        .tick_leader_lease_for_groups_best_effort([838, 839], 7)
        .expect("best-effort tick selected group leader leases");
    assert!(selected_leader_ticks_best_effort
        .iter()
        .all(|(_, results)| {
            results.iter().all(|result| {
                result
                    .result
                    .as_ref()
                    .is_some_and(|route| route.leader_lease_expired.is_some())
            })
        }));
    let selected_leader_tick_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_leader_ticks_best_effort);
    assert!(selected_leader_tick_best_effort_summaries
        .iter()
        .all(|summary| summary
            .leader_lease_expired_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));
    assert!(selected_leader_tick_best_effort_summaries
        .iter()
        .all(|summary| summary
            .leader_lease_expired_values_by_route_key()
            .iter()
            .all(|(_, expired)| expired.is_some())));

    let follower_receives = server
        .receive_follower_lease_on_group(838, 88)
        .expect("receive follower lease on meta group");
    assert_eq!(follower_receives.len(), 2);
    assert!(follower_receives
        .iter()
        .all(|result| result.follower_lease_received == Some(true)));
    let follower_ticks = server
        .tick_follower_lease_on_group(838, 20)
        .expect("tick follower lease on meta group");
    assert_eq!(follower_ticks.len(), 2);
    assert!(follower_ticks
        .iter()
        .all(|result| result.follower_lease_expired.is_some()));
    let follower_receive_plan = server
        .plan_receive_follower_lease_for_groups([838, 839], 89)
        .expect("plan selected follower lease receive");
    assert_eq!(follower_receive_plan.group_count, 2);
    assert_eq!(follower_receive_plan.node_count, 3);
    assert_eq!(
        follower_receive_plan.command_type,
        MatrixRaftAdminCommandType::ReceiveFollowerLease
    );
    let selected_follower_receives = server
        .receive_follower_lease_for_groups([838, 839], 89)
        .expect("receive selected group follower leases");
    assert_eq!(
        selected_follower_receives
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(838, 2), (839, 1)]
    );
    assert!(selected_follower_receives.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.follower_lease_received == Some(true))
    }));
    let selected_follower_receive_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_follower_receives);
    assert!(selected_follower_receive_summaries
        .iter()
        .all(|summary| summary
            .follower_lease_received_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));
    assert!(selected_follower_receive_summaries
        .iter()
        .all(|summary| summary
            .follower_lease_received_values_by_route_key()
            .iter()
            .all(|(_, received)| *received == Some(true))));
    let selected_follower_receives_best_effort = server
        .receive_follower_lease_for_groups_best_effort([838, 839], 90)
        .expect("best-effort receive selected group follower leases");
    assert_eq!(
        selected_follower_receives_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(838, 2), (839, 1)]
    );
    assert!(selected_follower_receives_best_effort
        .iter()
        .all(|(_, results)| results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.follower_lease_received == Some(true))
        })));
    let selected_follower_receive_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(
            &selected_follower_receives_best_effort,
        );
    assert!(selected_follower_receive_best_effort_summaries
        .iter()
        .all(|summary| summary
            .follower_lease_received_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));
    assert!(selected_follower_receive_best_effort_summaries
        .iter()
        .all(|summary| summary
            .follower_lease_received_values_by_route_key()
            .iter()
            .all(|(_, received)| *received == Some(true))));
    let follower_tick_plan = server
        .plan_tick_follower_lease_for_groups([838, 839], 20)
        .expect("plan selected follower lease tick");
    assert_eq!(follower_tick_plan.group_count, 2);
    assert_eq!(follower_tick_plan.node_count, 3);
    assert_eq!(
        follower_tick_plan.command_type,
        MatrixRaftAdminCommandType::TickFollowerLease
    );
    let selected_follower_ticks = server
        .tick_follower_lease_for_groups([838, 839], 20)
        .expect("tick selected group follower leases");
    assert!(selected_follower_ticks.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.follower_lease_expired.is_some())
    }));
    let selected_follower_tick_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_follower_ticks);
    assert!(selected_follower_tick_summaries
        .iter()
        .all(|summary| summary
            .follower_lease_expired_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));
    assert!(selected_follower_tick_summaries
        .iter()
        .all(|summary| summary
            .follower_lease_expired_values_by_route_key()
            .iter()
            .all(|(_, expired)| expired.is_some())));
    let selected_follower_ticks_best_effort = server
        .tick_follower_lease_for_groups_best_effort([838, 839], 21)
        .expect("best-effort tick selected group follower leases");
    assert!(selected_follower_ticks_best_effort
        .iter()
        .all(|(_, results)| results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.follower_lease_expired.is_some())
        })));
    let selected_follower_tick_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(
            &selected_follower_ticks_best_effort,
        );
    assert!(selected_follower_tick_best_effort_summaries
        .iter()
        .all(|summary| summary
            .follower_lease_expired_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));
    assert!(selected_follower_tick_best_effort_summaries
        .iter()
        .all(|summary| summary
            .follower_lease_expired_values_by_route_key()
            .iter()
            .all(|(_, expired)| expired.is_some())));

    assert_eq!(
        server.set_ignore_witness_on_node(838, 99, true),
        Err(RaftError::NodeNotFound(99))
    );
    assert_invalid_request_contains(
        server.set_leader_lease_valid_for_groups([899, 838], false),
        "group 899 is not registered",
    );
    assert_invalid_request_contains(
        server.plan_tick_follower_lease_for_groups([899, 838], 20),
        "group 899 is not registered",
    );
    assert!(server
        .receive_leader_lease_confirmation_on_group_best_effort(838, 99, 1, None)
        .expect("best-effort lease confirmation")
        .iter()
        .all(|result| {
            result.is_ok()
                && result
                    .result
                    .as_ref()
                    .is_some_and(|route| route.leader_lease_confirmed.is_some())
        }));

    server.shutdown_all().expect("shutdown lease server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal);
    let _ = fs::remove_dir_all(data_snap);
}

#[test]
fn matrixraft_multi_raft_server_controls_storage_apply_and_witness_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("storage-meta-1-wal");
    let meta_snap_1 = temp_dir("storage-meta-1-snapshot");
    let meta_wal_2 = temp_dir("storage-meta-2-wal");
    let meta_snap_2 = temp_dir("storage-meta-2-snapshot");
    let data_wal_1 = temp_dir("storage-data-1-wal");
    let data_snap_1 = temp_dir("storage-data-1-snapshot");
    let data_wal_2 = temp_dir("storage-data-2-wal");
    let data_snap_2 = temp_dir("storage-data-2-snapshot");
    server
        .create_node(options_for_peer(840, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(840, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options_for_peer(841, 1, &data_wal_1, &data_snap_1), 1)
        .expect("data node 1");
    server
        .create_node(options_for_peer(841, 2, &data_wal_2, &data_snap_2), 1)
        .expect("data node 2");
    server.start_all(1).expect("start storage server");

    let log_id = server
        .propose_to_node(840, 1, b"group-storage-control".to_vec())
        .expect("propose meta entry");
    let data_log_id = server
        .propose_to_node(841, 1, b"data-storage-control".to_vec())
        .expect("propose data entry");
    let applied_index = server
        .node(840, 1)
        .expect("meta node 1")
        .get_status()
        .expect("meta status after propose")
        .applied_index;
    let data_last_before: Vec<_> = server
        .group_statuses(841)
        .expect("data status before storage controls")
        .into_iter()
        .map(|status| status.last_log_index)
        .collect();

    let storage_plans = [
        server
            .plan_synced_for_groups([840, 841], None, None, 0)
            .expect("plan synced selected groups"),
        server
            .plan_applied_for_groups([840, 841], 1, applied_index, false)
            .expect("plan applied selected groups"),
        server
            .plan_apply_task_inflight_for_groups([840, 841], 1, applied_index)
            .expect("plan apply inflight selected groups"),
        server
            .plan_replicated_for_groups([840, 841], 2, true)
            .expect("plan replicated selected groups"),
        server
            .plan_compact_logs_through_for_groups([840, 841], 0)
            .expect("plan compact selected groups"),
        server
            .plan_checkpoint_snapshot_for_groups([840, 841], 1, "planned-checkpoint")
            .expect("plan checkpoint selected groups"),
        server
            .plan_witness_quorum_for_groups([840, 841], [1, 2])
            .expect("plan witness selected groups"),
        server
            .plan_release_memory_for_groups([840, 841])
            .expect("plan release selected groups"),
    ];
    assert_eq!(
        storage_plans
            .iter()
            .map(|plan| plan.command_type)
            .collect::<Vec<_>>(),
        vec![
            MatrixRaftAdminCommandType::Synced,
            MatrixRaftAdminCommandType::Applied,
            MatrixRaftAdminCommandType::ApplyTaskInflight,
            MatrixRaftAdminCommandType::Replicated,
            MatrixRaftAdminCommandType::CompactLogsThrough,
            MatrixRaftAdminCommandType::CheckpointSnapshot,
            MatrixRaftAdminCommandType::WitnessQuorum,
            MatrixRaftAdminCommandType::ReleaseMemory,
        ]
    );
    assert!(storage_plans.iter().all(|plan| {
        plan.group_count == 2
            && plan.node_count == 4
            && plan.route_keys
                == vec![
                    MatrixRaftRouteKey::new(840, 1),
                    MatrixRaftRouteKey::new(840, 2),
                    MatrixRaftRouteKey::new(841, 1),
                    MatrixRaftRouteKey::new(841, 2),
                ]
            && plan.groups.iter().all(|group| group.node_count == 2)
    }));

    let applied = server
        .applied_on_node(840, 1, 1, applied_index, false)
        .expect("applied on meta node");
    assert!(applied.apply_result.as_ref().is_some_and(|report| {
        report.node_id == 1 && report.applied_index == applied_index && !report.rejected
    }));
    let applied_best_effort = server
        .applied_on_group_best_effort(840, 1, applied_index, false)
        .expect("applied best effort on meta group");
    assert_eq!(applied_best_effort.len(), 2);
    assert!(applied_best_effort.iter().any(|result| result.is_ok()));
    let selected_applied = server
        .applied_for_groups_best_effort([840, 841], 1, applied_index, false)
        .expect("applied on selected groups");
    assert_eq!(
        selected_applied
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(840, 2), (841, 2)]
    );
    assert!(selected_applied.iter().all(|(_, results)| {
        results.iter().any(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.apply_result.as_ref().is_some_and(|report| {
                    report.node_id == 1 && report.applied_index == applied_index
                })
            })
        }) && results.iter().any(|result| result.error.is_some())
    }));
    let selected_applied_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_applied);
    assert!(selected_applied_summaries.iter().all(|summary| {
        summary
            .apply_result_presence_by_route_key()
            .iter()
            .any(|(_, present)| *present)
            && summary
                .apply_result_node_ids_by_route_key()
                .iter()
                .any(|(_, node_id)| *node_id == Some(1))
            && summary
                .applied_indices_by_route_key()
                .iter()
                .any(|(_, index)| *index == Some(applied_index))
            && summary
                .apply_rejected_by_route_key()
                .iter()
                .any(|(_, rejected)| *rejected == Some(false))
            && summary
                .synced_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));

    let inflight = server
        .apply_task_inflight_on_node(840, 1, 1, applied_index)
        .expect("apply task inflight on meta node");
    assert!(inflight.apply_result.as_ref().is_some_and(|report| {
        report.node_id == 1 && report.applied_index == applied_index && !report.rejected
    }));
    let inflight_best_effort = server
        .apply_task_inflight_on_group_best_effort(840, 1, applied_index)
        .expect("apply task inflight best effort on meta group");
    assert_eq!(inflight_best_effort.len(), 2);
    assert!(inflight_best_effort.iter().any(|result| result.is_ok()));
    let selected_inflight = server
        .apply_task_inflight_for_groups_best_effort([840, 841], 1, applied_index)
        .expect("apply task inflight on selected groups");
    assert!(selected_inflight.iter().all(|(_, results)| {
        results.iter().any(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.apply_result.as_ref().is_some_and(|report| {
                    report.node_id == 1 && report.applied_index == applied_index
                })
            })
        }) && results.iter().any(|result| result.error.is_some())
    }));

    let synced = server
        .synced_on_group(840, Some(1), Some(log_id.index), 0)
        .expect("synced on meta group");
    assert_eq!(synced.len(), 2);
    assert!(synced.iter().all(|result| {
        result.synced.as_ref().is_some_and(|report| {
            report.first_index == Some(1)
                && report.last_index == Some(log_id.index)
                && report.stabled_config_change_index == 0
        })
    }));
    let selected_synced = server
        .synced_for_groups([840, 841], None, None, 0)
        .expect("synced on selected groups");
    assert!(selected_synced.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .synced
                .as_ref()
                .is_some_and(|report| report.first_index.is_none() && report.last_index.is_none())
        })
    }));
    let selected_synced_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_synced);
    assert!(selected_synced_summaries.iter().all(|summary| {
        summary
            .synced_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .synced_stabled_config_change_indices_by_route_key()
                .iter()
                .all(|(_, index)| *index == Some(0))
            && summary
                .apply_result_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    let selected_synced_best_effort = server
        .synced_for_groups_best_effort([840, 841], None, None, 0)
        .expect("best-effort synced on selected groups");
    assert!(selected_synced_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.synced.as_ref().is_some_and(|report| {
                    report.first_index.is_none() && report.last_index.is_none()
                })
            })
        })
    }));
    let selected_synced_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_synced_best_effort);
    assert!(selected_synced_best_effort_summaries
        .iter()
        .all(|summary| summary
            .synced_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));

    let replicated = server
        .replicated_on_group(840, 2, true)
        .expect("replicated on meta group");
    assert_eq!(replicated.len(), 2);
    assert!(replicated.iter().all(|result| {
        result
            .replicated
            .as_ref()
            .is_some_and(|report| report.peer_id == 2 && report.success)
    }));
    let selected_replicated = server
        .replicated_for_groups([840, 841], 2, true)
        .expect("replicated on selected groups");
    assert!(selected_replicated.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .replicated
                .as_ref()
                .is_some_and(|report| report.peer_id == 2 && report.success)
        })
    }));
    let selected_replicated_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_replicated);
    assert!(selected_replicated_summaries.iter().all(|summary| {
        summary
            .replicated_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .replicated_peer_ids_by_route_key()
                .iter()
                .all(|(_, peer_id)| *peer_id == Some(2))
            && summary
                .replicated_success_by_route_key()
                .iter()
                .all(|(_, success)| *success == Some(true))
            && summary
                .snapshot_peer_report_presence_by_route_key()
                .iter()
                .all(|(_, present)| *present)
            && summary
                .snapshot_peer_ids_by_route_key()
                .iter()
                .all(|(_, peer_id)| *peer_id == Some(2))
    }));
    let selected_replicated_best_effort = server
        .replicated_for_groups_best_effort([840, 841], 2, true)
        .expect("best-effort replicated on selected groups");
    assert!(selected_replicated_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .replicated
                    .as_ref()
                    .is_some_and(|report| report.peer_id == 2 && report.success)
            })
        })
    }));
    let selected_replicated_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_replicated_best_effort);
    assert!(selected_replicated_best_effort_summaries
        .iter()
        .all(|summary| summary
            .replicated_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));

    let compacted = server
        .compact_logs_through_on_group(840, applied_index)
        .expect("compact logs on meta group");
    assert_eq!(compacted.len(), 2);
    assert!(compacted
        .iter()
        .all(|result| result.compacted_logs.is_some()));
    let selected_compacted = server
        .compact_logs_through_for_groups([840, 841], 0)
        .expect("compact logs on selected groups");
    assert!(selected_compacted
        .iter()
        .all(|(_, results)| { results.iter().all(|result| result.compacted_logs.is_some()) }));
    let selected_compacted_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_compacted);
    assert!(selected_compacted_summaries.iter().all(|summary| {
        summary
            .compacted_logs_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .fenced_compaction_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    let selected_compacted_best_effort = server
        .compact_logs_through_for_groups_best_effort([840, 841], 0)
        .expect("best-effort compact logs on selected groups");
    assert!(selected_compacted_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.compacted_logs.is_some())
        })
    }));
    let selected_compacted_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_compacted_best_effort);
    assert!(selected_compacted_best_effort_summaries
        .iter()
        .all(|summary| summary
            .compacted_logs_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));

    let fence = StorageApplyFence {
        group_id: 840,
        node_id: 1,
        committed_index: log_id.index,
        applied_index,
        durable_applied_index: applied_index,
        storage_flushed_index: applied_index,
        installed_snapshot_index: 0,
        first_retained_log_index: 1,
    };
    let fenced = server
        .compact_logs_with_storage_fence_on_group(840, applied_index, fence.clone())
        .expect("fenced compaction on meta group");
    assert_eq!(fenced.len(), 2);
    assert!(fenced.iter().all(|result| {
        result
            .fenced_compaction
            .as_ref()
            .is_some_and(|report| report.requested_log_index == applied_index && report.fence_valid)
    }));
    let data_fence = StorageApplyFence {
        group_id: 841,
        node_id: 1,
        committed_index: data_log_id.index,
        applied_index: 0,
        durable_applied_index: 0,
        storage_flushed_index: 0,
        installed_snapshot_index: 0,
        first_retained_log_index: 1,
    };
    let selected_fence = StorageApplyFence {
        group_id: 840,
        node_id: 1,
        committed_index: log_id.index,
        applied_index: 0,
        durable_applied_index: 0,
        storage_flushed_index: 0,
        installed_snapshot_index: 0,
        first_retained_log_index: 1,
    };
    let fenced_plan = server
        .plan_compact_logs_with_storage_fences_for_groups(
            [(840, selected_fence.clone()), (841, data_fence.clone())],
            0,
        )
        .expect("plan fenced compaction selected groups");
    assert_eq!(
        fenced_plan.command_type,
        MatrixRaftAdminCommandType::CompactLogsWithStorageFence
    );
    assert_eq!(fenced_plan.group_count, 2);
    assert_eq!(fenced_plan.node_count, 4);
    assert_eq!(
        fenced_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(840, 1),
            MatrixRaftRouteKey::new(840, 2),
            MatrixRaftRouteKey::new(841, 1),
            MatrixRaftRouteKey::new(841, 2),
        ]
    );
    assert_eq!(
        fenced_plan.log_indices_by_group(),
        vec![(840, Some(0)), (841, Some(0))]
    );
    assert_eq!(
        fenced_plan.storage_fence_presence_by_group(),
        vec![(840, true), (841, true)]
    );
    assert_eq!(
        fenced_plan.storage_fences_by_group(),
        vec![
            (840, Some(selected_fence.clone())),
            (841, Some(data_fence.clone())),
        ]
    );
    let selected_fenced = server
        .compact_logs_with_storage_fences_for_groups([(840, selected_fence), (841, data_fence)], 0)
        .expect("fenced compaction on selected groups");
    assert!(selected_fenced.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .fenced_compaction
                .as_ref()
                .is_some_and(|report| report.requested_log_index == 0 && report.fence_valid)
        })
    }));
    let selected_fenced_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_fenced);
    assert!(selected_fenced_summaries.iter().all(|summary| {
        summary
            .fenced_compaction_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .compacted_logs_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    let selected_fence_best_effort = StorageApplyFence {
        group_id: 840,
        node_id: 1,
        committed_index: log_id.index,
        applied_index: 0,
        durable_applied_index: 0,
        storage_flushed_index: 0,
        installed_snapshot_index: 0,
        first_retained_log_index: 1,
    };
    let data_fence_best_effort = StorageApplyFence {
        group_id: 841,
        node_id: 1,
        committed_index: data_log_id.index,
        applied_index: 0,
        durable_applied_index: 0,
        storage_flushed_index: 0,
        installed_snapshot_index: 0,
        first_retained_log_index: 1,
    };
    let selected_fenced_best_effort = server
        .compact_logs_with_storage_fences_for_groups_best_effort(
            [
                (840, selected_fence_best_effort),
                (841, data_fence_best_effort),
            ],
            0,
        )
        .expect("best-effort fenced compaction on selected groups");
    assert!(selected_fenced_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .fenced_compaction
                    .as_ref()
                    .is_some_and(|report| report.requested_log_index == 0 && report.fence_valid)
            })
        })
    }));
    let selected_fenced_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_fenced_best_effort);
    assert!(selected_fenced_best_effort_summaries
        .iter()
        .all(|summary| summary
            .fenced_compaction_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)));

    let checkpoint = server
        .checkpoint_snapshot_on_group(840, 1, "group-checkpoint-840")
        .expect("checkpoint snapshot on meta group");
    assert_eq!(checkpoint.len(), 2);
    assert!(checkpoint.iter().all(|result| {
        result.checkpoint.as_ref().is_some_and(|snapshot| {
            snapshot.group_id == 840 && snapshot.meta.snapshot_id == "group-checkpoint-840"
        })
    }));
    let selected_checkpoint = server
        .checkpoint_snapshot_for_groups([840, 841], 1, "selected-checkpoint")
        .expect("checkpoint selected groups");
    assert_eq!(
        selected_checkpoint
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(840, 2), (841, 2)]
    );
    assert!(selected_checkpoint.iter().all(|(group_id, results)| {
        results.iter().all(|result| {
            result.checkpoint.as_ref().is_some_and(|snapshot| {
                snapshot.group_id == *group_id && snapshot.meta.snapshot_id == "selected-checkpoint"
            })
        })
    }));
    let selected_checkpoint_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_checkpoint);
    assert!(selected_checkpoint_summaries.iter().all(|summary| {
        summary
            .checkpoint_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .checkpoint_snapshot_ids_by_route_key()
                .iter()
                .all(|(_, snapshot_id)| snapshot_id.as_deref() == Some("selected-checkpoint"))
            && summary
                .checkpoint_last_log_indices_by_route_key()
                .iter()
                .all(|(_, index)| index.is_some())
            && summary
                .snapshot_presence_by_route_key()
                .iter()
                .all(|(_, present)| *present)
            && summary
                .snapshot_ids_by_route_key()
                .iter()
                .all(|(_, snapshot_id)| snapshot_id.as_deref() == Some("selected-checkpoint"))
            && summary
                .snapshot_indices_by_route_key()
                .iter()
                .all(|(_, index)| index.is_some())
            && summary
                .witness_quorum_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    let selected_checkpoint_best_effort = server
        .checkpoint_snapshot_for_groups_best_effort(
            [840, 841],
            1,
            "selected-checkpoint-best-effort",
        )
        .expect("best-effort checkpoint selected groups");
    assert!(selected_checkpoint_best_effort
        .iter()
        .all(|(group_id, results)| {
            results.iter().all(|result| {
                result.result.as_ref().is_some_and(|route| {
                    route.checkpoint.as_ref().is_some_and(|snapshot| {
                        snapshot.group_id == *group_id
                            && snapshot.meta.snapshot_id == "selected-checkpoint-best-effort"
                    })
                })
            })
        }));
    let selected_checkpoint_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_checkpoint_best_effort);
    assert!(selected_checkpoint_best_effort_summaries
        .iter()
        .all(|summary| summary
            .checkpoint_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .checkpoint_snapshot_ids_by_route_key()
                .iter()
                .all(|(_, snapshot_id)| {
                    snapshot_id.as_deref() == Some("selected-checkpoint-best-effort")
                })));

    let witness = server
        .witness_quorum_on_group(840, [1, 2])
        .expect("witness quorum on meta group");
    assert_eq!(witness.len(), 2);
    assert!(witness.iter().all(|result| {
        result
            .witness_quorum
            .as_ref()
            .is_some_and(|report| report.acknowledged == 2)
    }));
    let selected_witness = server
        .witness_quorum_for_groups([840, 841], [1, 2])
        .expect("witness quorum on selected groups");
    assert!(selected_witness.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .witness_quorum
                .as_ref()
                .is_some_and(|report| report.acknowledged == 2)
        })
    }));
    let selected_witness_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_witness);
    assert!(selected_witness_summaries.iter().all(|summary| {
        summary
            .witness_quorum_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .witness_quorum_acknowledged_by_route_key()
                .iter()
                .all(|(_, acknowledged)| *acknowledged == Some(2))
            && summary
                .witness_quorum_reached_by_route_key()
                .iter()
                .all(|(_, reached)| *reached == Some(true))
            && summary
                .checkpoint_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));
    let selected_witness_best_effort = server
        .witness_quorum_for_groups_best_effort([840, 841], [1, 2])
        .expect("best-effort witness quorum on selected groups");
    assert!(selected_witness_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route
                    .witness_quorum
                    .as_ref()
                    .is_some_and(|report| report.acknowledged == 2)
            })
        })
    }));
    let selected_witness_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_witness_best_effort);
    assert!(selected_witness_best_effort_summaries
        .iter()
        .all(|summary| summary
            .witness_quorum_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .witness_quorum_required_by_route_key()
                .iter()
                .all(|(_, required)| required.is_some())));

    let released = server
        .release_memory_on_group(840)
        .expect("release memory on meta group");
    assert_eq!(released.len(), 2);
    assert!(released
        .iter()
        .all(|result| result.released_memory.is_some()));
    let selected_released = server
        .release_memory_for_groups([840, 841])
        .expect("release memory on selected groups");
    assert!(selected_released.iter().all(|(_, results)| {
        results
            .iter()
            .all(|result| result.released_memory.is_some())
    }));
    let selected_released_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_released);
    assert!(selected_released_summaries.iter().all(|summary| summary
        .released_memory_values_by_route_key()
        .iter()
        .all(|(_, released)| released.is_some())));
    let selected_released_best_effort = server
        .release_memory_for_groups_best_effort([840, 841])
        .expect("best-effort release memory on selected groups");
    assert!(selected_released_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result
                .result
                .as_ref()
                .is_some_and(|route| route.released_memory.is_some())
        })
    }));
    let selected_released_best_effort_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&selected_released_best_effort);
    assert!(selected_released_best_effort_summaries
        .iter()
        .all(|summary| summary
            .released_memory_values_by_route_key()
            .iter()
            .all(|(_, released)| released.is_some())));
    let data_last_after: Vec<_> = server
        .group_statuses(841)
        .expect("data status after storage controls")
        .into_iter()
        .map(|status| status.last_log_index)
        .collect();
    assert_eq!(data_last_after, data_last_before);

    assert_eq!(
        server.applied_on_node(840, 99, 1, applied_index, false),
        Err(RaftError::NodeNotFound(99))
    );
    assert_invalid_request_contains(
        server.release_memory_for_groups([899, 840]),
        "group 899 is not registered",
    );
    assert!(server
        .checkpoint_snapshot_on_group_best_effort(840, 99, "missing-checkpoint")
        .expect("missing checkpoint target best effort")
        .iter()
        .all(|result| result.error.is_some()));

    server.shutdown_all().expect("shutdown storage server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal_1);
    let _ = fs::remove_dir_all(data_snap_1);
    let _ = fs::remove_dir_all(data_wal_2);
    let _ = fs::remove_dir_all(data_snap_2);
}

#[test]
fn matrixraft_multi_raft_server_fans_out_election_rpc_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("election-rpc-meta-1-wal");
    let meta_snap_1 = temp_dir("election-rpc-meta-1-snapshot");
    let meta_wal_2 = temp_dir("election-rpc-meta-2-wal");
    let meta_snap_2 = temp_dir("election-rpc-meta-2-snapshot");
    let data_wal_1 = temp_dir("election-rpc-data-1-wal");
    let data_snap_1 = temp_dir("election-rpc-data-1-snapshot");
    let data_wal_2 = temp_dir("election-rpc-data-2-wal");
    let data_snap_2 = temp_dir("election-rpc-data-2-snapshot");
    server
        .create_node(options_for_peer(842, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(842, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options_for_peer(843, 1, &data_wal_1, &data_snap_1), 1)
        .expect("data node 1");
    server
        .create_node(options_for_peer(843, 2, &data_wal_2, &data_snap_2), 1)
        .expect("data node 2");
    server.start_all(1).expect("start election rpc server");

    let simple_pre_vote_plan = server
        .plan_pre_vote_for_groups([842, 843], 2, 1)
        .expect("plan simple pre-vote fanout");
    assert_eq!(simple_pre_vote_plan.group_count, 2);
    assert_eq!(simple_pre_vote_plan.node_count, 4);
    assert_eq!(
        simple_pre_vote_plan.message_type,
        MatrixRaftMessageType::PreVote
    );
    assert_eq!(
        simple_pre_vote_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(842, 1),
            MatrixRaftRouteKey::new(842, 2),
            MatrixRaftRouteKey::new(843, 1),
            MatrixRaftRouteKey::new(843, 2),
        ]
    );
    let simple_pre_votes = server
        .pre_votes_for_groups([842, 843], 2, 1)
        .expect("simple pre-vote fanout");
    assert_eq!(
        simple_pre_votes
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(842, 2), (843, 2)]
    );
    assert!(simple_pre_votes.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.kind == MatrixRaftRouteResultKind::Delivered
                && result.message_type == MatrixRaftMessageType::PreVote
                && result.vote_response.is_some()
        })
    }));
    let simple_pre_vote_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&simple_pre_votes);
    assert!(simple_pre_vote_summaries.iter().all(|summary| {
        summary
            .vote_response_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .timeout_now_response_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));

    let vote_request = VoteRequest {
        group_id: 0,
        term: 2,
        candidate_id: 2,
        last_log_id: None,
        pre_vote: false,
        force: false,
    };
    let vote_request_plan = server
        .plan_vote_request_for_groups([842, 843], 2, 1, vote_request.clone(), true)
        .expect("plan vote-request fanout");
    assert_eq!(vote_request_plan.group_count, 2);
    assert_eq!(vote_request_plan.node_count, 4);
    assert_eq!(
        vote_request_plan.message_type,
        MatrixRaftMessageType::PreVoteRequest
    );
    assert!(vote_request_plan.groups.iter().all(|group| group
        .message
        .vote_request
        .as_ref()
        .is_some_and(|request| { request.group_id == 0 && !request.pre_vote })));
    let vote_requests = server
        .vote_requests_for_groups([842, 843], 2, 1, vote_request.clone(), true)
        .expect("vote-request fanout");
    assert_eq!(
        vote_requests
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(842, 2), (843, 2)]
    );
    assert!(vote_requests.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.kind == MatrixRaftRouteResultKind::Delivered
                && result.message_type == MatrixRaftMessageType::PreVoteRequest
                && result
                    .vote_response
                    .as_ref()
                    .is_some_and(|response| response.reason != "group_id_mismatch")
        })
    }));
    let group_vote_requests = server
        .vote_request_on_group(842, 2, 1, vote_request.clone(), false)
        .expect("vote-request on meta group");
    assert_eq!(group_vote_requests.len(), 2);
    assert!(group_vote_requests.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.message_type == MatrixRaftMessageType::VoteRequest
            && result.vote_response.is_some()
    }));
    let invalid_vote_targets = server
        .vote_requests_for_groups_best_effort([842], 2, 99, vote_request, true)
        .expect("best-effort vote request with missing target");
    assert_eq!(invalid_vote_targets.len(), 1);
    assert!(invalid_vote_targets[0]
        .1
        .iter()
        .all(|result| result.error.is_some()));

    let vote_response = VoteResponse {
        term: 2,
        vote_granted: true,
        reason: "pre_vote_granted".to_string(),
    };
    let response_plan = server
        .plan_vote_response_for_groups([842, 843], 1, 2, vote_response.clone(), true)
        .expect("plan vote-response fanout");
    assert_eq!(response_plan.group_count, 2);
    assert_eq!(response_plan.node_count, 4);
    assert_eq!(
        response_plan.message_type,
        MatrixRaftMessageType::PreVoteResponse
    );
    let group_vote_responses = server
        .vote_response_on_group(842, 1, 2, vote_response.clone(), true)
        .expect("vote-response on meta group");
    assert_eq!(group_vote_responses.len(), 2);
    assert!(group_vote_responses.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.message_type == MatrixRaftMessageType::PreVoteResponse
            && result.vote_response.is_some()
    }));
    let vote_responses = server
        .vote_responses_for_groups_best_effort([842, 843], 1, 2, vote_response, true)
        .expect("best-effort vote-response fanout");
    assert_eq!(
        vote_responses
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(842, 2), (843, 2)]
    );
    assert!(vote_responses.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered
                    && route.message_type == MatrixRaftMessageType::PreVoteResponse
                    && route.vote_response.is_some()
            })
        })
    }));
    let vote_response_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&vote_responses);
    assert!(vote_response_summaries.iter().all(|summary| {
        summary
            .vote_response_presence_by_route_key()
            .iter()
            .all(|(_, present)| *present)
            && summary
                .timeout_now_response_presence_by_route_key()
                .iter()
                .all(|(_, present)| !*present)
    }));

    assert_invalid_request_contains(
        server.plan_pre_vote_for_groups([899, 842], 2, 1),
        "group 899 is not registered",
    );

    server.shutdown_all().expect("shutdown election rpc server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal_1);
    let _ = fs::remove_dir_all(data_snap_1);
    let _ = fs::remove_dir_all(data_wal_2);
    let _ = fs::remove_dir_all(data_snap_2);
}

#[test]
fn matrixraft_multi_raft_server_fans_out_append_rpc_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("append-rpc-meta-1-wal");
    let meta_snap_1 = temp_dir("append-rpc-meta-1-snapshot");
    let meta_wal_2 = temp_dir("append-rpc-meta-2-wal");
    let meta_snap_2 = temp_dir("append-rpc-meta-2-snapshot");
    let data_wal_1 = temp_dir("append-rpc-data-1-wal");
    let data_snap_1 = temp_dir("append-rpc-data-1-snapshot");
    let data_wal_2 = temp_dir("append-rpc-data-2-wal");
    let data_snap_2 = temp_dir("append-rpc-data-2-snapshot");
    server
        .create_node(options_for_peer(844, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(844, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options_for_peer(845, 1, &data_wal_1, &data_snap_1), 1)
        .expect("data node 1");
    server
        .create_node(options_for_peer(845, 2, &data_wal_2, &data_snap_2), 1)
        .expect("data node 2");
    server.start_all(1).expect("start append rpc server");

    let append = AppendEntriesRequest {
        group_id: 0,
        term: 1,
        leader_id: 1,
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: 0,
        lease_epoch: 7,
    };
    let append_plan = server
        .plan_append_entries_for_groups([844, 845], 1, 2, &append)
        .expect("plan append fanout");
    assert_eq!(append_plan.group_count, 2);
    assert_eq!(append_plan.node_count, 4);
    assert_eq!(
        append_plan.message_type,
        MatrixRaftMessageType::AppendEntriesRequest
    );
    assert_eq!(
        append_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(844, 1),
            MatrixRaftRouteKey::new(844, 2),
            MatrixRaftRouteKey::new(845, 1),
            MatrixRaftRouteKey::new(845, 2),
        ]
    );
    assert_eq!(
        append_plan.route_keys_by_group(),
        vec![
            (
                844,
                vec![
                    MatrixRaftRouteKey::new(844, 1),
                    MatrixRaftRouteKey::new(844, 2),
                ],
            ),
            (
                845,
                vec![
                    MatrixRaftRouteKey::new(845, 1),
                    MatrixRaftRouteKey::new(845, 2),
                ],
            ),
        ]
    );
    assert_eq!(
        append_plan.node_ids_by_group(),
        vec![(844, vec![1, 2]), (845, vec![1, 2])]
    );
    assert_eq!(append_plan.node_counts_by_group(), vec![(844, 2), (845, 2)]);
    assert!(append_plan.groups.iter().all(|group| group
        .message
        .append_entries_request
        .as_ref()
        .is_some_and(|request| request.prev_index == 0 && request.entries.is_empty())));
    let meta_appends = server
        .append_entries_on_group(844, 1, 2, &append)
        .expect("append on meta group");
    assert_eq!(meta_appends.len(), 2);
    assert!(meta_appends.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.message_type == MatrixRaftMessageType::AppendEntriesRequest
            && result.append_entries_response.is_some()
    }));
    let selected_appends = server
        .append_entries_for_groups([844, 845], 1, 2, &append)
        .expect("append selected groups");
    assert_eq!(
        selected_appends
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(844, 2), (845, 2)]
    );
    assert!(selected_appends.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.kind == MatrixRaftRouteResultKind::Delivered
                && result.message_type == MatrixRaftMessageType::AppendEntriesRequest
                && result
                    .append_entries_response
                    .as_ref()
                    .is_some_and(|response| response.received)
        })
    }));
    let selected_append_summaries =
        MatrixRaftRouteGroupSummary::from_grouped_results(&selected_appends);
    assert_eq!(
        selected_append_summaries
            .iter()
            .map(|summary| (
                summary.group_id,
                summary.result_count,
                summary.handled_count,
                summary.unhandled_count,
                summary.route_keys.clone(),
                summary.message_types.clone(),
                summary.kinds.clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                844,
                2,
                2,
                0,
                vec![
                    MatrixRaftRouteKey::new(844, 1),
                    MatrixRaftRouteKey::new(844, 2),
                ],
                vec![MatrixRaftMessageType::AppendEntriesRequest],
                vec![MatrixRaftRouteResultKind::Delivered],
            ),
            (
                845,
                2,
                2,
                0,
                vec![
                    MatrixRaftRouteKey::new(845, 1),
                    MatrixRaftRouteKey::new(845, 2),
                ],
                vec![MatrixRaftMessageType::AppendEntriesRequest],
                vec![MatrixRaftRouteResultKind::Delivered],
            ),
        ]
    );
    assert!(selected_append_summaries
        .iter()
        .all(MatrixRaftRouteGroupSummary::is_handled));
    assert!(selected_append_summaries.iter().all(|summary| summary
        .proposed_log_id_presence_by_route_key()
        .iter()
        .all(|(_, present)| !present)));
    assert!(selected_append_summaries.iter().all(|summary| summary
        .append_entries_responses_by_route_key
        .iter()
        .all(|(_, response)| response.as_ref().is_some_and(|response| response.received))));
    assert!(selected_append_summaries.iter().all(|summary| summary
        .append_entries_response_presence_by_route_key()
        .iter()
        .all(|(_, present)| *present)));
    assert!(selected_append_summaries.iter().all(|summary| summary
        .read_index_response_presence_by_route_key()
        .iter()
        .all(|(_, present)| !present)));
    assert!(selected_append_summaries.iter().all(|summary| summary
        .install_snapshot_responses_by_route_key
        .iter()
        .all(|(_, response)| response.is_none())));
    assert!(selected_append_summaries.iter().all(|summary| summary
        .install_snapshot_response_presence_by_route_key()
        .iter()
        .all(|(_, present)| !present)));
    let invalid_targets = server
        .append_entries_for_groups_best_effort([844], 1, 99, &append)
        .expect("best-effort append with missing target");
    assert_eq!(invalid_targets.len(), 1);
    assert!(invalid_targets[0]
        .1
        .iter()
        .all(|result| result.error.is_some()));

    let heartbeat_plan = server
        .plan_merged_heartbeat_requests_for_groups([844, 845], 1, 2, &append)
        .expect("plan merged heartbeat requests");
    assert_eq!(heartbeat_plan.group_count, 2);
    assert_eq!(heartbeat_plan.group_ids, vec![844, 845]);
    assert_eq!(heartbeat_plan.message_count, 2);
    assert_eq!(heartbeat_plan.batch_count, 2);
    assert_eq!(
        heartbeat_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(844, 2),
            MatrixRaftRouteKey::new(845, 2),
        ]
    );
    assert_eq!(
        heartbeat_plan.message_type,
        MatrixRaftMessageType::AppendEntriesRequest
    );
    assert_eq!(
        heartbeat_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.route_key, group.message_type))
            .collect::<Vec<_>>(),
        vec![
            (
                844,
                MatrixRaftRouteKey::new(844, 2),
                MatrixRaftMessageType::AppendEntriesRequest,
            ),
            (
                845,
                MatrixRaftRouteKey::new(845, 2),
                MatrixRaftMessageType::AppendEntriesRequest,
            ),
        ]
    );
    assert_eq!(
        heartbeat_plan.route_keys_by_group(),
        vec![
            (844, vec![MatrixRaftRouteKey::new(844, 2)]),
            (845, vec![MatrixRaftRouteKey::new(845, 2)]),
        ]
    );
    assert_eq!(
        heartbeat_plan.message_counts_by_group(),
        vec![(844, 1), (845, 1)]
    );
    assert_eq!(
        heartbeat_plan.route_key_counts_by_group(),
        vec![(844, 1), (845, 1)]
    );
    assert_eq!(
        heartbeat_plan.fanout_counts_by_group(),
        vec![(844, 1, 1), (845, 1, 1)]
    );
    assert_eq!(
        heartbeat_plan.raft_addrs_by_group(),
        vec![(844, peer(844, 2).raft_addr), (845, peer(845, 2).raft_addr)]
    );
    assert_eq!(
        heartbeat_plan.raft_addrs_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 2), peer(844, 2).raft_addr),
            (MatrixRaftRouteKey::new(845, 2), peer(845, 2).raft_addr),
        ]
    );
    assert_eq!(
        heartbeat_plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 2), 2),
            (MatrixRaftRouteKey::new(845, 2), 2),
        ]
    );
    assert_eq!(
        heartbeat_plan.sender_receiver_by_group(),
        vec![(844, 1, 2), (845, 1, 2)]
    );
    assert_eq!(
        heartbeat_plan.sender_receiver_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 2), 1, 2),
            (MatrixRaftRouteKey::new(845, 2), 1, 2),
        ]
    );
    assert_eq!(
        heartbeat_plan.message_types_by_group(),
        vec![
            (844, MatrixRaftMessageType::AppendEntriesRequest),
            (845, MatrixRaftMessageType::AppendEntriesRequest),
        ]
    );
    assert!(heartbeat_plan
        .messages_by_group()
        .iter()
        .all(|(_, message)| message.term == Some(1)
            && message
                .append_entries_request
                .as_ref()
                .is_some_and(|request| request.prev_index == 0)));
    assert_eq!(
        heartbeat_plan.terms_by_group(),
        vec![(844, Some(1)), (845, Some(1))]
    );
    assert_eq!(
        heartbeat_plan.committed_indices_by_group(),
        vec![(844, Some(0)), (845, Some(0))]
    );
    assert_eq!(
        heartbeat_plan.message_bytes_by_group(),
        vec![(844, 0), (845, 0)]
    );
    assert_eq!(
        heartbeat_plan.message_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(844, 2),
                MatrixRaftMessageType::AppendEntriesRequest,
            ),
            (
                MatrixRaftRouteKey::new(845, 2),
                MatrixRaftMessageType::AppendEntriesRequest,
            ),
        ]
    );
    assert_eq!(
        heartbeat_plan.terms_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 2), Some(1)),
            (MatrixRaftRouteKey::new(845, 2), Some(1)),
        ]
    );
    assert_eq!(
        heartbeat_plan
            .messages_by_route_key()
            .iter()
            .map(|(key, message)| (
                *key,
                message.message_type,
                message.term == Some(1)
                    && message.committed_index == Some(0)
                    && message.append_entries_request.is_some()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                MatrixRaftRouteKey::new(844, 2),
                MatrixRaftMessageType::AppendEntriesRequest,
                true
            ),
            (
                MatrixRaftRouteKey::new(845, 2),
                MatrixRaftMessageType::AppendEntriesRequest,
                true
            ),
        ]
    );
    assert_eq!(
        heartbeat_plan.committed_indices_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 2), Some(0)),
            (MatrixRaftRouteKey::new(845, 2), Some(0)),
        ]
    );
    assert_eq!(
        heartbeat_plan.message_bytes_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 2), 0),
            (MatrixRaftRouteKey::new(845, 2), 0),
        ]
    );
    assert_eq!(
        heartbeat_plan
            .route_keys_by_raft_addr()
            .iter()
            .map(|(_, keys)| keys.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![MatrixRaftRouteKey::new(844, 2)],
            vec![MatrixRaftRouteKey::new(845, 2)],
        ]
    );
    assert_eq!(
        heartbeat_plan
            .messages_by_raft_addr()
            .iter()
            .map(|(_, messages)| {
                messages
                    .iter()
                    .filter(|message| {
                        message.message_type == MatrixRaftMessageType::AppendEntriesRequest
                            && message.append_entries_request.is_some()
                    })
                    .count()
            })
            .collect::<Vec<_>>(),
        vec![1, 1]
    );
    assert_eq!(
        heartbeat_plan
            .message_counts_by_raft_addr()
            .iter()
            .map(|(_, count)| *count)
            .collect::<Vec<_>>(),
        vec![1, 1]
    );
    assert_eq!(
        heartbeat_plan
            .route_key_counts_by_raft_addr()
            .iter()
            .map(|(_, count)| *count)
            .collect::<Vec<_>>(),
        vec![1, 1]
    );
    assert_eq!(
        heartbeat_plan
            .batch_fanout_counts_by_raft_addr()
            .iter()
            .map(|(_, message_count, route_key_count)| (*message_count, *route_key_count))
            .collect::<Vec<_>>(),
        vec![(1, 1), (1, 1)]
    );
    assert!(heartbeat_plan
        .batches
        .iter()
        .all(|batch| batch.message_count == 1 && batch.route_keys.len() == 1));
    let meta_heartbeats = server
        .merged_heartbeat_requests_on_group(844, 1, 2, &append)
        .expect("merged heartbeat requests on meta group");
    assert_eq!(meta_heartbeats.len(), 1);
    assert!(meta_heartbeats.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.key == MatrixRaftRouteKey::new(844, 2)
            && result.message_type == MatrixRaftMessageType::AppendEntriesRequest
            && result.append_entries_response.is_some()
    }));
    let selected_heartbeats = server
        .merged_heartbeat_requests_for_groups([844, 845], 1, 2, &append)
        .expect("merged heartbeat requests for selected groups");
    assert_eq!(
        selected_heartbeats
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(844, 1), (845, 1)]
    );
    assert!(selected_heartbeats.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.kind == MatrixRaftRouteResultKind::Delivered
                && result.message_type == MatrixRaftMessageType::AppendEntriesRequest
                && result.append_entries_response.is_some()
        })
    }));
    let selected_heartbeats_best_effort = server
        .merged_heartbeat_requests_for_groups_best_effort([844, 845], 1, 2, &append)
        .expect("best-effort merged heartbeat requests for selected groups");
    assert!(selected_heartbeats_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered
                    && route.message_type == MatrixRaftMessageType::AppendEntriesRequest
                    && route.append_entries_response.is_some()
            })
        })
    }));

    let lease_request = MatrixRaftLeaseRequest { epoch_id: 42 };
    let lease_request_plan = server
        .plan_lease_request_for_groups([844, 845], 1, 2, &append, lease_request.clone())
        .expect("plan lease request fanout");
    assert_eq!(lease_request_plan.group_count, 2);
    assert_eq!(lease_request_plan.node_count, 4);
    assert_eq!(
        lease_request_plan.message_type,
        MatrixRaftMessageType::AppendEntriesRequest
    );
    assert!(lease_request_plan.groups.iter().all(|group| group
        .message
        .lease_request
        .as_ref()
        .is_some_and(|request| request.epoch_id == 42)));
    let meta_lease_requests = server
        .lease_request_on_group(844, 1, 2, &append, lease_request.clone())
        .expect("lease request on meta group");
    assert_eq!(meta_lease_requests.len(), 2);
    assert!(meta_lease_requests.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.message_type == MatrixRaftMessageType::AppendEntriesRequest
            && result.append_entries_response.is_some()
    }));
    let selected_lease_requests = server
        .lease_requests_for_groups([844, 845], 1, 2, &append, lease_request.clone())
        .expect("lease request selected groups");
    assert_eq!(
        selected_lease_requests
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(844, 2), (845, 2)]
    );
    assert!(selected_lease_requests.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.kind == MatrixRaftRouteResultKind::Delivered
                && result.message_type == MatrixRaftMessageType::AppendEntriesRequest
                && result.append_entries_response.is_some()
        })
    }));
    let invalid_lease_targets = server
        .lease_requests_for_groups_best_effort([844], 1, 99, &append, lease_request)
        .expect("best-effort lease request with missing target");
    assert_eq!(invalid_lease_targets.len(), 1);
    assert!(invalid_lease_targets[0]
        .1
        .iter()
        .all(|result| result.error.is_some()));

    let append_response = AppendEntriesResponse {
        term: 1,
        success: true,
        match_index: 1,
        rejection_hint: None,
        rejected_index: None,
        require_snapshot: None,
        snapshot_state: SnapshotState::None,
        lease_confirmation_epoch: 8,
        lease_duration_ms: 25,
    };
    let response_plan = server
        .plan_append_entries_response_for_groups([844, 845], 2, 1, &append_response)
        .expect("plan append response fanout");
    assert_eq!(response_plan.group_count, 2);
    assert_eq!(response_plan.node_count, 4);
    assert_eq!(
        response_plan.message_type,
        MatrixRaftMessageType::AppendEntriesResponse
    );
    let meta_responses = server
        .append_entries_response_on_group(844, 2, 1, &append_response)
        .expect("append response on meta group");
    assert_eq!(meta_responses.len(), 2);
    assert!(meta_responses.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.message_type == MatrixRaftMessageType::AppendEntriesResponse
            && result.append_entries_response.is_some()
    }));
    let selected_responses = server
        .append_entries_responses_for_groups_best_effort([844, 845], 2, 1, &append_response)
        .expect("best-effort append responses");
    assert_eq!(
        selected_responses
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(844, 2), (845, 2)]
    );
    assert!(selected_responses.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered
                    && route.message_type == MatrixRaftMessageType::AppendEntriesResponse
                    && route.append_entries_response.is_some()
            })
        })
    }));
    let strict_responses = server
        .append_entries_responses_for_groups([844, 845], 2, 1, &append_response)
        .expect("strict append responses");
    assert_eq!(
        strict_responses
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(844, 2), (845, 2)]
    );

    let heartbeat_response_plan = server
        .plan_merged_heartbeat_responses_for_groups([844, 845], 2, 1, &append_response)
        .expect("plan merged heartbeat responses");
    assert_eq!(heartbeat_response_plan.group_count, 2);
    assert_eq!(heartbeat_response_plan.group_ids, vec![844, 845]);
    assert_eq!(heartbeat_response_plan.message_count, 2);
    assert_eq!(heartbeat_response_plan.batch_count, 2);
    assert_eq!(
        heartbeat_response_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(844, 1),
            MatrixRaftRouteKey::new(845, 1),
        ]
    );
    assert_eq!(
        heartbeat_response_plan.message_type,
        MatrixRaftMessageType::AppendEntriesResponse
    );
    assert_eq!(
        heartbeat_response_plan
            .groups
            .iter()
            .map(|group| (group.group_id, group.route_key, group.message_type))
            .collect::<Vec<_>>(),
        vec![
            (
                844,
                MatrixRaftRouteKey::new(844, 1),
                MatrixRaftMessageType::AppendEntriesResponse,
            ),
            (
                845,
                MatrixRaftRouteKey::new(845, 1),
                MatrixRaftMessageType::AppendEntriesResponse,
            ),
        ]
    );
    assert_eq!(
        heartbeat_response_plan.route_keys_by_group(),
        vec![
            (844, vec![MatrixRaftRouteKey::new(844, 1)]),
            (845, vec![MatrixRaftRouteKey::new(845, 1)]),
        ]
    );
    assert_eq!(
        heartbeat_response_plan.route_key_counts_by_group(),
        vec![(844, 1), (845, 1)]
    );
    assert_eq!(
        heartbeat_response_plan.fanout_counts_by_group(),
        vec![(844, 1, 1), (845, 1, 1)]
    );
    assert_eq!(
        heartbeat_response_plan.raft_addrs_by_group(),
        vec![(844, peer(844, 1).raft_addr), (845, peer(845, 1).raft_addr)]
    );
    assert_eq!(
        heartbeat_response_plan.raft_addrs_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), peer(844, 1).raft_addr),
            (MatrixRaftRouteKey::new(845, 1), peer(845, 1).raft_addr),
        ]
    );
    assert_eq!(
        heartbeat_response_plan.node_ids_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), 1),
            (MatrixRaftRouteKey::new(845, 1), 1),
        ]
    );
    assert_eq!(
        heartbeat_response_plan.sender_receiver_by_group(),
        vec![(844, 2, 1), (845, 2, 1)]
    );
    assert_eq!(
        heartbeat_response_plan.sender_receiver_by_route_key(),
        vec![
            (MatrixRaftRouteKey::new(844, 1), 2, 1),
            (MatrixRaftRouteKey::new(845, 1), 2, 1),
        ]
    );
    assert_eq!(
        heartbeat_response_plan.message_types_by_group(),
        vec![
            (844, MatrixRaftMessageType::AppendEntriesResponse),
            (845, MatrixRaftMessageType::AppendEntriesResponse),
        ]
    );
    assert_eq!(
        heartbeat_response_plan.message_types_by_route_key(),
        vec![
            (
                MatrixRaftRouteKey::new(844, 1),
                MatrixRaftMessageType::AppendEntriesResponse,
            ),
            (
                MatrixRaftRouteKey::new(845, 1),
                MatrixRaftMessageType::AppendEntriesResponse,
            ),
        ]
    );
    let meta_heartbeat_responses = server
        .merged_heartbeat_response_on_group(844, 2, 1, &append_response)
        .expect("merged heartbeat response on meta group");
    assert_eq!(meta_heartbeat_responses.len(), 1);
    assert!(meta_heartbeat_responses.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.key == MatrixRaftRouteKey::new(844, 1)
            && result.message_type == MatrixRaftMessageType::AppendEntriesResponse
            && result.append_entries_response.is_some()
    }));
    let selected_heartbeat_responses = server
        .merged_heartbeat_responses_for_groups([844, 845], 2, 1, &append_response)
        .expect("merged heartbeat responses for selected groups");
    assert_eq!(
        selected_heartbeat_responses
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(844, 1), (845, 1)]
    );
    let selected_heartbeat_responses_best_effort = server
        .merged_heartbeat_responses_for_groups_best_effort([844, 845], 2, 1, &append_response)
        .expect("best-effort merged heartbeat responses for selected groups");
    assert!(selected_heartbeat_responses_best_effort
        .iter()
        .all(|(_, results)| {
            results.iter().all(|result| {
                result.result.as_ref().is_some_and(|route| {
                    route.kind == MatrixRaftRouteResultKind::Delivered
                        && route.message_type == MatrixRaftMessageType::AppendEntriesResponse
                        && route.append_entries_response.is_some()
                })
            })
        }));

    let lease_response = MatrixRaftLeaseResponse {
        max_met_epoch_id: 43,
        duration_ms: 30,
    };
    let lease_response_plan = server
        .plan_lease_response_for_groups([844, 845], 2, 1, &append_response, lease_response.clone())
        .expect("plan lease response fanout");
    assert_eq!(lease_response_plan.group_count, 2);
    assert_eq!(lease_response_plan.node_count, 4);
    assert_eq!(
        lease_response_plan.message_type,
        MatrixRaftMessageType::AppendEntriesResponse
    );
    assert!(lease_response_plan.groups.iter().all(|group| group
        .message
        .lease_response
        .as_ref()
        .is_some_and(|response| response.max_met_epoch_id == 43 && response.duration_ms == 30)));
    let meta_lease_responses = server
        .lease_response_on_group(844, 2, 1, &append_response, lease_response.clone())
        .expect("lease response on meta group");
    assert_eq!(meta_lease_responses.len(), 2);
    assert!(meta_lease_responses.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.message_type == MatrixRaftMessageType::AppendEntriesResponse
            && result.append_entries_response.is_some()
    }));
    let selected_lease_responses = server
        .lease_responses_for_groups([844, 845], 2, 1, &append_response, lease_response.clone())
        .expect("lease responses for selected groups");
    assert_eq!(
        selected_lease_responses
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(844, 2), (845, 2)]
    );
    let lease_response_best_effort = server
        .lease_responses_for_groups_best_effort([844, 845], 2, 1, &append_response, lease_response)
        .expect("best-effort lease responses for selected groups");
    assert_eq!(
        lease_response_best_effort
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(844, 2), (845, 2)]
    );
    assert!(lease_response_best_effort.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered
                    && route.message_type == MatrixRaftMessageType::AppendEntriesResponse
                    && route.append_entries_response.is_some()
            })
        })
    }));

    assert_invalid_request_contains(
        server.plan_append_entries_for_groups([899, 844], 1, 2, &append),
        "group 899 is not registered",
    );

    server.shutdown_all().expect("shutdown append rpc server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal_1);
    let _ = fs::remove_dir_all(data_snap_1);
    let _ = fs::remove_dir_all(data_wal_2);
    let _ = fs::remove_dir_all(data_snap_2);
}

#[test]
fn matrixraft_multi_raft_server_fans_out_snapshot_chunk_rpc_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("snapshot-rpc-meta-1-wal");
    let meta_snap_1 = temp_dir("snapshot-rpc-meta-1-snapshot");
    let meta_wal_2 = temp_dir("snapshot-rpc-meta-2-wal");
    let meta_snap_2 = temp_dir("snapshot-rpc-meta-2-snapshot");
    let data_wal_1 = temp_dir("snapshot-rpc-data-1-wal");
    let data_snap_1 = temp_dir("snapshot-rpc-data-1-snapshot");
    let data_wal_2 = temp_dir("snapshot-rpc-data-2-wal");
    let data_snap_2 = temp_dir("snapshot-rpc-data-2-snapshot");
    server
        .create_node(options_for_peer(846, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(846, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options_for_peer(847, 1, &data_wal_1, &data_snap_1), 1)
        .expect("data node 1");
    server
        .create_node(options_for_peer(847, 2, &data_wal_2, &data_snap_2), 1)
        .expect("data node 2");
    server.start_all(1).expect("start snapshot rpc server");

    let snapshot_request = InstallSnapshotRequest {
        group_id: 0,
        term: 1,
        leader_id: 0,
        chunk: SnapshotChunk {
            meta: SnapshotMetadata {
                snapshot_id: "selected-snapshot-rpc-22".to_string(),
                last_log_id: LogId { term: 1, index: 22 },
                membership: vec![1, 2],
                members: vec![
                    peer_with_role(846, 1, ReplicaRole::Voter),
                    peer_with_role(846, 2, ReplicaRole::Voter),
                ],
            },
            offset: 0,
            data: b"selected snapshot rpc state".to_vec(),
            done: true,
        },
    };
    let request_plan = server
        .plan_install_snapshot_request_for_groups([846, 847], 1, 2, snapshot_request.clone())
        .expect("plan snapshot request fanout");
    assert_eq!(request_plan.group_count, 2);
    assert_eq!(request_plan.node_count, 4);
    assert_eq!(
        request_plan.message_type,
        MatrixRaftMessageType::InstallSnapshotRequest
    );
    assert_eq!(
        request_plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(846, 1),
            MatrixRaftRouteKey::new(846, 2),
            MatrixRaftRouteKey::new(847, 1),
            MatrixRaftRouteKey::new(847, 2),
        ]
    );
    assert!(request_plan.groups.iter().all(|group| group
        .message
        .install_snapshot_request
        .as_ref()
        .is_some_and(|request| request.group_id == 0 && request.chunk.done)));
    assert_eq!(
        request_plan.sender_receiver_by_group(),
        vec![(846, Some(1), Some(2)), (847, Some(1), Some(2))]
    );
    assert_eq!(
        request_plan.terms_by_group(),
        vec![(846, Some(1)), (847, Some(1))]
    );
    assert_eq!(
        request_plan.committed_indices_by_group(),
        vec![(846, Some(22)), (847, Some(22))]
    );
    assert_eq!(
        request_plan.snapshot_ids_by_group(),
        vec![
            (846, Some("selected-snapshot-rpc-22".to_string())),
            (847, Some("selected-snapshot-rpc-22".to_string())),
        ]
    );
    assert_eq!(
        request_plan.snapshot_chunk_offsets_by_group(),
        vec![(846, Some(0)), (847, Some(0))]
    );
    assert_eq!(
        request_plan.snapshot_chunk_done_by_group(),
        vec![(846, Some(true)), (847, Some(true))]
    );
    assert_eq!(
        request_plan.snapshot_chunk_payload_bytes_by_group(),
        vec![
            (846, Some(b"selected snapshot rpc state".len())),
            (847, Some(b"selected snapshot rpc state".len())),
        ]
    );
    let meta_requests = server
        .install_snapshot_request_on_group(846, 1, 2, snapshot_request.clone())
        .expect("snapshot request on meta group");
    assert_eq!(meta_requests.len(), 2);
    assert!(meta_requests.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.message_type == MatrixRaftMessageType::InstallSnapshotRequest
            && result
                .install_snapshot_response
                .as_ref()
                .is_some_and(|response| response.accepted)
    }));

    let selected_request = InstallSnapshotRequest {
        group_id: 0,
        term: 1,
        leader_id: 0,
        chunk: SnapshotChunk {
            meta: SnapshotMetadata {
                snapshot_id: "selected-snapshot-rpc-32".to_string(),
                last_log_id: LogId { term: 1, index: 32 },
                membership: vec![1, 2],
                members: Vec::new(),
            },
            offset: 0,
            data: b"selected snapshot rpc fanout state".to_vec(),
            done: true,
        },
    };
    let selected_requests = server
        .install_snapshot_requests_for_groups([846, 847], 1, 2, selected_request.clone())
        .expect("snapshot request selected groups");
    assert_eq!(
        selected_requests
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 2)]
    );
    assert!(selected_requests.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.kind == MatrixRaftRouteResultKind::Delivered
                && result.message_type == MatrixRaftMessageType::InstallSnapshotRequest
                && result
                    .install_snapshot_response
                    .as_ref()
                    .is_some_and(|response| {
                        response.accepted && response.reason != "group_id_mismatch"
                    })
        })
    }));
    let invalid_targets = server
        .install_snapshot_requests_for_groups_best_effort([846], 1, 99, selected_request)
        .expect("best-effort snapshot request with missing target");
    assert_eq!(invalid_targets.len(), 1);
    assert!(invalid_targets[0]
        .1
        .iter()
        .all(|result| result.error.is_some()));

    let snapshot_response = InstallSnapshotResponse {
        term: 2,
        accepted: false,
        next_offset: 0,
        committed_index: 32,
        reason: "selected_snapshot_response".to_string(),
    };
    let response_plan = server
        .plan_install_snapshot_response_for_groups([846, 847], 2, 1, snapshot_response.clone())
        .expect("plan snapshot response fanout");
    assert_eq!(response_plan.group_count, 2);
    assert_eq!(response_plan.node_count, 4);
    assert_eq!(
        response_plan.message_type,
        MatrixRaftMessageType::InstallSnapshotResponse
    );
    assert_eq!(
        response_plan.sender_receiver_by_group(),
        vec![(846, Some(2), Some(1)), (847, Some(2), Some(1))]
    );
    assert_eq!(
        response_plan.terms_by_group(),
        vec![(846, Some(2)), (847, Some(2))]
    );
    assert_eq!(
        response_plan.committed_indices_by_group(),
        vec![(846, Some(32)), (847, Some(32))]
    );
    let meta_responses = server
        .install_snapshot_response_on_group(846, 2, 1, snapshot_response.clone())
        .expect("snapshot response on meta group");
    assert_eq!(meta_responses.len(), 2);
    assert!(meta_responses.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.message_type == MatrixRaftMessageType::InstallSnapshotResponse
            && result.install_snapshot_response.is_some()
    }));
    let selected_responses = server
        .install_snapshot_responses_for_groups_best_effort(
            [846, 847],
            2,
            1,
            snapshot_response.clone(),
        )
        .expect("best-effort snapshot responses");
    assert_eq!(
        selected_responses
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 2)]
    );
    assert!(selected_responses.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered
                    && route.message_type == MatrixRaftMessageType::InstallSnapshotResponse
                    && route
                        .install_snapshot_response
                        .as_ref()
                        .is_some_and(|response| response.reason == "selected_snapshot_response")
            })
        })
    }));
    let strict_responses = server
        .install_snapshot_responses_for_groups([846, 847], 2, 1, snapshot_response)
        .expect("strict snapshot responses");
    assert_eq!(
        strict_responses
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(846, 2), (847, 2)]
    );

    assert_invalid_request_contains(
        server.plan_install_snapshot_request_for_groups([899, 846], 1, 2, snapshot_request),
        "group 899 is not registered",
    );

    server.shutdown_all().expect("shutdown snapshot rpc server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal_1);
    let _ = fs::remove_dir_all(data_snap_1);
    let _ = fs::remove_dir_all(data_wal_2);
    let _ = fs::remove_dir_all(data_snap_2);
}

#[test]
fn matrixraft_multi_raft_server_fans_out_read_index_rpc_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("read-index-rpc-meta-1-wal");
    let meta_snap_1 = temp_dir("read-index-rpc-meta-1-snapshot");
    let meta_wal_2 = temp_dir("read-index-rpc-meta-2-wal");
    let meta_snap_2 = temp_dir("read-index-rpc-meta-2-snapshot");
    let data_wal_1 = temp_dir("read-index-rpc-data-1-wal");
    let data_snap_1 = temp_dir("read-index-rpc-data-1-snapshot");
    let data_wal_2 = temp_dir("read-index-rpc-data-2-wal");
    let data_snap_2 = temp_dir("read-index-rpc-data-2-snapshot");
    server
        .create_node(options_for_peer(848, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(848, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options_for_peer(849, 1, &data_wal_1, &data_snap_1), 1)
        .expect("data node 1");
    server
        .create_node(options_for_peer(849, 2, &data_wal_2, &data_snap_2), 1)
        .expect("data node 2");
    server.start_all(1).expect("start read-index rpc server");

    let request = ReadIndexRequest {
        group_id: 0,
        requester_id: 2,
        min_commit_index: 0,
        allow_lease_read: false,
    };
    let plan = server
        .plan_read_index_request_for_groups([848, 849], 2, 1, request.clone())
        .expect("plan read-index request fanout");
    assert_eq!(plan.group_count, 2);
    assert_eq!(plan.node_count, 4);
    assert_eq!(plan.message_type, MatrixRaftMessageType::ReadIndexRequest);
    assert_eq!(
        plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(848, 1),
            MatrixRaftRouteKey::new(848, 2),
            MatrixRaftRouteKey::new(849, 1),
            MatrixRaftRouteKey::new(849, 2),
        ]
    );
    assert!(plan.groups.iter().all(|group| group
        .message
        .read_index_request
        .as_ref()
        .is_some_and(|request| request.group_id == 0 && !request.allow_lease_read)));
    assert_eq!(
        plan.sender_receiver_by_group(),
        vec![(848, Some(2), Some(1)), (849, Some(2), Some(1))]
    );
    assert_eq!(
        plan.committed_indices_by_group(),
        vec![(848, Some(0)), (849, Some(0))]
    );
    assert_eq!(plan.message_bytes_by_group(), vec![(848, 0), (849, 0)]);

    let meta_reads = server
        .read_index_request_on_group(848, 2, 1, request.clone())
        .expect("read-index request on meta group");
    assert_eq!(meta_reads.len(), 2);
    assert!(meta_reads.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.message_type == MatrixRaftMessageType::ReadIndexRequest
            && result.read_index_response.is_some()
    }));
    let selected_reads = server
        .read_index_requests_for_groups([848, 849], 2, 1, request.clone())
        .expect("read-index request selected groups");
    assert_eq!(
        selected_reads
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(848, 2), (849, 2)]
    );
    assert!(selected_reads.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.kind == MatrixRaftRouteResultKind::Delivered
                && result.message_type == MatrixRaftMessageType::ReadIndexRequest
                && result
                    .read_index_response
                    .as_ref()
                    .is_some_and(|response| !response.reason.is_empty())
        })
    }));
    let ignored_targets = server
        .read_index_requests_for_groups_best_effort([848], 2, 99, request)
        .expect("best-effort read-index request with protocol target metadata");
    assert_eq!(ignored_targets.len(), 1);
    assert!(ignored_targets[0].1.iter().all(|result| {
        result.result.as_ref().is_some_and(|route| {
            route.kind == MatrixRaftRouteResultKind::Delivered
                && route.message_type == MatrixRaftMessageType::ReadIndexRequest
                && route.read_index_response.is_some()
        })
    }));

    assert_invalid_request_contains(
        server.plan_read_index_request_for_groups(
            [899, 848],
            2,
            1,
            ReadIndexRequest {
                group_id: 0,
                requester_id: 2,
                min_commit_index: 0,
                allow_lease_read: false,
            },
        ),
        "group 899 is not registered",
    );

    server
        .shutdown_all()
        .expect("shutdown read-index rpc server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal_1);
    let _ = fs::remove_dir_all(data_snap_1);
    let _ = fs::remove_dir_all(data_wal_2);
    let _ = fs::remove_dir_all(data_snap_2);
}

#[test]
fn matrixraft_multi_raft_server_fans_out_proposal_rpc_by_group() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let meta_wal_1 = temp_dir("proposal-rpc-meta-1-wal");
    let meta_snap_1 = temp_dir("proposal-rpc-meta-1-snapshot");
    let meta_wal_2 = temp_dir("proposal-rpc-meta-2-wal");
    let meta_snap_2 = temp_dir("proposal-rpc-meta-2-snapshot");
    let data_wal_1 = temp_dir("proposal-rpc-data-1-wal");
    let data_snap_1 = temp_dir("proposal-rpc-data-1-snapshot");
    let data_wal_2 = temp_dir("proposal-rpc-data-2-wal");
    let data_snap_2 = temp_dir("proposal-rpc-data-2-snapshot");
    server
        .create_node(options_for_peer(850, 1, &meta_wal_1, &meta_snap_1), 1)
        .expect("meta node 1");
    server
        .create_node(options_for_peer(850, 2, &meta_wal_2, &meta_snap_2), 1)
        .expect("meta node 2");
    server
        .create_node(options_for_peer(851, 1, &data_wal_1, &data_snap_1), 1)
        .expect("data node 1");
    server
        .create_node(options_for_peer(851, 2, &data_wal_2, &data_snap_2), 1)
        .expect("data node 2");
    server.start_all(1).expect("start proposal rpc server");

    let propose = MatrixRaftPropose {
        request_id: Some(77),
        data: b"selected proposal rpc command".to_vec(),
        context: b"proxy context".to_vec(),
        is_command: true,
    };
    let plan = server
        .plan_propose_request_for_groups([850, 851], 1, propose.clone())
        .expect("plan proposal request fanout");
    assert_eq!(plan.group_count, 2);
    assert_eq!(plan.node_count, 2);
    assert_eq!(plan.message_type, MatrixRaftMessageType::Propose);
    assert_eq!(
        plan.route_keys,
        vec![
            MatrixRaftRouteKey::new(850, 1),
            MatrixRaftRouteKey::new(851, 1),
        ]
    );
    assert!(plan
        .groups
        .iter()
        .all(|group| group
            .message
            .propose
            .as_ref()
            .is_some_and(|message| message.request_id == Some(77)
                && message.context == b"proxy context"
                && message.is_command)));
    assert_eq!(
        plan.sender_receiver_by_group(),
        vec![(850, Some(1), Some(1)), (851, Some(1), Some(1))]
    );
    assert_eq!(
        plan.propose_request_ids_by_group(),
        vec![(850, Some(77)), (851, Some(77))]
    );
    assert_eq!(
        plan.message_bytes_by_group(),
        vec![
            (850, b"selected proposal rpc command".len() as u64),
            (851, b"selected proposal rpc command".len() as u64),
        ]
    );

    let meta_proposals = server
        .propose_request_on_group(850, 1, propose.clone())
        .expect("proposal request on meta group");
    assert_eq!(meta_proposals.len(), 1);
    assert!(meta_proposals.iter().all(|result| {
        result.kind == MatrixRaftRouteResultKind::Delivered
            && result.message_type == MatrixRaftMessageType::Propose
            && result.proposed_log_id.is_some()
    }));
    let selected_proposals = server
        .propose_requests_for_groups([850, 851], 1, propose.clone())
        .expect("proposal request selected groups");
    assert_eq!(
        selected_proposals
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(850, 1), (851, 1)]
    );
    assert!(selected_proposals.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.kind == MatrixRaftRouteResultKind::Delivered
                && result.message_type == MatrixRaftMessageType::Propose
                && result.proposed_log_id.is_some()
        })
    }));

    let noop = MatrixRaftPropose {
        request_id: Some(78),
        data: b"selected proposal rpc noop".to_vec(),
        context: b"noop context".to_vec(),
        is_command: false,
    };
    let best_effort_noops = server
        .propose_requests_for_groups_best_effort([850, 851], 1, noop)
        .expect("best-effort proposal request selected groups");
    assert_eq!(
        best_effort_noops
            .iter()
            .map(|(group_id, results)| (*group_id, results.len()))
            .collect::<Vec<_>>(),
        vec![(850, 1), (851, 1)]
    );
    assert!(best_effort_noops.iter().all(|(_, results)| {
        results.iter().all(|result| {
            result.result.as_ref().is_some_and(|route| {
                route.kind == MatrixRaftRouteResultKind::Delivered
                    && route.message_type == MatrixRaftMessageType::Propose
                    && route.proposed_log_id.is_some()
            })
        })
    }));
    let best_effort_noop_summaries =
        MatrixRaftBatchRouteGroupSummary::from_grouped_results(&best_effort_noops);
    assert!(best_effort_noop_summaries.iter().all(|summary| summary
        .proposed_log_ids_by_route_key
        .iter()
        .all(|(_, proposed_log_id)| proposed_log_id.is_some())));
    assert!(best_effort_noop_summaries.iter().all(|summary| summary
        .proposed_log_id_presence_by_route_key()
        .iter()
        .all(|(_, present)| *present)));
    assert!(best_effort_noop_summaries.iter().all(|summary| summary
        .append_entries_responses_by_route_key
        .iter()
        .all(|(_, response)| response.is_none())));
    assert!(best_effort_noop_summaries.iter().all(|summary| summary
        .append_entries_response_presence_by_route_key()
        .iter()
        .all(|(_, present)| !present)));
    assert!(best_effort_noop_summaries.iter().all(|summary| summary
        .read_index_response_presence_by_route_key()
        .iter()
        .all(|(_, present)| !present)));
    assert!(best_effort_noop_summaries.iter().all(|summary| summary
        .install_snapshot_response_presence_by_route_key()
        .iter()
        .all(|(_, present)| !present)));

    assert_invalid_request_contains(
        server.plan_propose_request_for_groups([899, 850], 1, propose),
        "group 899 is not registered",
    );

    server.shutdown_all().expect("shutdown proposal rpc server");

    let _ = fs::remove_dir_all(meta_wal_1);
    let _ = fs::remove_dir_all(meta_snap_1);
    let _ = fs::remove_dir_all(meta_wal_2);
    let _ = fs::remove_dir_all(meta_snap_2);
    let _ = fs::remove_dir_all(data_wal_1);
    let _ = fs::remove_dir_all(data_snap_1);
    let _ = fs::remove_dir_all(data_wal_2);
    let _ = fs::remove_dir_all(data_snap_2);
}

#[test]
fn matrixraft_multi_raft_server_rejects_vote_traffic_to_learners_but_accepts_append() {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(100)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .build()
        .expect("context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let wal_dir = temp_dir("learner-wal");
    let snapshot_dir = temp_dir("learner-snapshot");
    server
        .create_node(
            options_with_role(812, &wal_dir, &snapshot_dir, ReplicaRole::Learner),
            1,
        )
        .expect("learner node");
    server.start_all(1).expect("start learner");

    let append = AppendEntriesRequest {
        group_id: 812,
        term: 1,
        leader_id: 2,
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: 0,
        lease_epoch: 0,
    };
    let append_result = server
        .route_message(812, 1, MatrixRaftMessage::append_entries(2, 1, &append))
        .expect("append to learner");
    assert_eq!(append_result.kind, MatrixRaftRouteResultKind::Delivered);
    assert!(append_result.append_entries_response.is_some());

    let vote = VoteRequest {
        group_id: 812,
        term: 2,
        candidate_id: 2,
        last_log_id: None,
        pre_vote: false,
        force: false,
    };
    assert_invalid_request_contains(
        server.route_message(812, 1, MatrixRaftMessage::vote(2, 1, vote.clone(), false)),
        "does not accept vote traffic",
    );
    assert_invalid_request_contains(
        server.route_message(812, 1, MatrixRaftMessage::vote(2, 1, vote, true)),
        "does not accept vote traffic",
    );

    server.shutdown_all().expect("shutdown learner");

    let _ = fs::remove_dir_all(wal_dir);
    let _ = fs::remove_dir_all(snapshot_dir);
}
