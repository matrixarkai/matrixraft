// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    metrics::rustraft_metric_names,
    readiness::{
        rustraft_parity_report, rustraft_public_api_contract,
        rustraft_baseline_raft_parity_surface, RustRaftReadinessSnapshot,
    },
    status::{rustraft_fatal_blocker_report, RustRaftBlockerSeverity},
    transport::{
        AppendEntriesRequest, InstallSnapshotRequest, PreVoteRequest, PreVoteResponse,
        ReadIndexRequest, RustRaftSnapshotChunk, VoteRequest,
    },
    RustRaftInstallSnapshotResponse, RustRaftLogId, RustRaftSnapshotMeta,
};

fn ready_snapshot() -> RustRaftReadinessSnapshot {
    RustRaftReadinessSnapshot {
        rustraft_leader_write_authority_present: true,
        rustraft_operator_observability_present: true,
        rustraft_rpc_transport_contract_present: true,
        rustraft_log_retention_snapshot_trigger_present: true,
        rustraft_apply_snapshot_fence_present: true,
        raft_storage_apply_fence_present: true,
        rustraft_snapshot_floor_log_matching_present: true,
        rustraft_snapshot_tail_catchup_present: true,
        rustraft_compacted_entry_rejection_present: true,
        rustraft_metaserver_snapshot_floor_election_present: true,
        learner_catchup_promotion_present: true,
        metaserver_membership_workflow_present: true,
    }
}

#[test]
fn transport_contract_names_owned_rpc_types_including_prevote_and_snapshot_chunks() {
    let append = AppendEntriesRequest {
        group_id: 9,
        term: 4,
        leader_id: 1,
        prev_log_id: Some(RustRaftLogId { term: 4, index: 9 }),
        entries: Vec::new(),
        leader_commit: 9,
        lease_epoch: 0,
    };
    assert_eq!(append.leader_id, 1);

    let vote = VoteRequest {
        group_id: 9,
        term: 4,
        candidate_id: 2,
        last_log_id: None,
        pre_vote: false,
        force: false,
    };
    assert!(!vote.pre_vote);

    let pre_vote = PreVoteRequest {
        pre_vote: true,
        ..vote.clone()
    };
    assert!(pre_vote.pre_vote);

    let pre_vote_response = PreVoteResponse {
        term: 4,
        vote_granted: true,
        reason: "pre_vote_granted".to_string(),
    };
    assert!(pre_vote_response.vote_granted);

    let chunk = RustRaftSnapshotChunk {
        meta: RustRaftSnapshotMeta {
            snapshot_id: "transport-observability".to_string(),
            last_log_id: RustRaftLogId { term: 4, index: 10 },
            membership: vec![1, 2, 3],
            members: Vec::new(),
        },
        offset: 0,
        data: b"chunk".to_vec(),
        done: true,
    };
    let install = InstallSnapshotRequest {
        group_id: 9,
        term: 4,
        leader_id: 1,
        chunk,
    };
    assert!(install.chunk.done);
    let install_response = RustRaftInstallSnapshotResponse {
        term: 4,
        accepted: true,
        next_offset: 5,
        committed_index: 0,
        reason: "installed".to_string(),
    };
    assert!(install_response.accepted);

    let read = ReadIndexRequest {
        group_id: 9,
        requester_id: 1,
        min_commit_index: 10,
        allow_lease_read: true,
    };
    assert!(read.allow_lease_read);
}

#[test]
fn observability_contract_exports_metrics_parity_readiness_and_blocker_reports() {
    let metrics = rustraft_metric_names();
    assert_eq!(metrics.pre_vote_latency_ms, "rustraft_pre_vote_latency_ms");
    assert_eq!(metrics.blocker_total, "rustraft_blocker_total");
    assert_eq!(metrics.fatal_total, "rustraft_fatal_total");

    let api = rustraft_public_api_contract();
    assert!(api.rpc_messages.contains(&"PreVoteRequest".to_string()));
    assert!(api.rpc_messages.contains(&"PreVoteResponse".to_string()));
    assert!(api
        .rpc_messages
        .contains(&"RustRaftSnapshotChunk".to_string()));
    assert!(api
        .rpc_messages
        .contains(&"RustRaftTransportValidationReport".to_string()));
    assert!(api
        .rpc_messages
        .contains(&"InMemoryRaftTransport".to_string()));
    assert!(api
        .safety_helpers
        .contains(&"rustraft_fatal_blocker_report".to_string()));

    let surface = rustraft_baseline_raft_parity_surface();
    assert!(surface.transport_api.contains(&"pre_vote_rpc".to_string()));
    assert!(surface
        .transport_api
        .contains(&"install_snapshot_chunk_rpc".to_string()));
    assert!(surface
        .transport_api
        .contains(&"request_response_validation".to_string()));
    assert!(surface
        .transport_api
        .contains(&"in_memory_transport".to_string()));
    assert!(surface
        .transport_api
        .contains(&"tcp_reference_transport".to_string()));
    assert!(surface
        .observability_api
        .contains(&"blocker_report".to_string()));
    assert!(surface
        .observability_api
        .contains(&"readiness_report".to_string()));

    let readiness = rustraft_parity_report(&ready_snapshot());
    assert!(readiness.ready);

    let blockers = rustraft_fatal_blocker_report(
        "rustraft_transport_observability",
        vec!["leader_unavailable".to_string(), "wal_corrupt".to_string()],
        vec!["wal_corrupt".to_string()],
    );
    assert!(!blockers.ready);
    assert!(blockers.fatal);
    assert_eq!(blockers.blocker_count, 2);
    assert_eq!(blockers.fatal_count, 1);
    assert_eq!(
        blockers
            .blockers
            .iter()
            .find(|blocker| blocker.id == "wal_corrupt")
            .expect("fatal blocker")
            .severity,
        RustRaftBlockerSeverity::Fatal
    );
}
