// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Transport traits and RPC envelopes for append, vote, snapshot, and read-index paths.

pub use crate::{
    AppendEntriesRequest, AppendEntriesResponse, AuthenticatedRaftRpc, AuthenticatedRaftTransport,
    ClusterRaftTransport, InMemoryRaftTransport, InstallSnapshotRequest, InstallSnapshotResponse,
    PreVoteRequest, PreVoteResponse, RaftAuthPolicy, ReadIndexRequest, ReadIndexResponse,
    RustRaftAppendEntriesRequest, RustRaftAppendEntriesResponse, RustRaftHeartbeatAddressResolver,
    RustRaftHeartbeatMergeMessage, RustRaftHeartbeatMergeStats, RustRaftHeartbeatMerger,
    RustRaftInstallSnapshotRequest, RustRaftInstallSnapshotResponse, RustRaftMergedHeartbeatBatch,
    RustRaftReadIndexRequest, RustRaftReadIndexResponse, RustRaftSnapshotChunk,
    RustRaftTransportValidationReport, RustRaftVoteRequest, RustRaftVoteResponse,
    StaticRaftAuthToken, TcpRaftRpcResult, TcpRaftTransport, TcpRaftTransportRequest,
    TcpRaftTransportResponse, TcpRaftTransportServer, VoteRequest, VoteResponse,
};

use crate::{RaftError, RustRaftError, RustRaftLogId};

/// Network transport API for Raft append, vote, snapshot, and read-index RPCs.
pub trait RustRaftTransport {
    fn append_entries(
        &self,
        target: u64,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<RustRaftAppendEntriesResponse, RustRaftError>;
    fn vote(
        &self,
        target: u64,
        request: RustRaftVoteRequest,
    ) -> Result<RustRaftVoteResponse, RustRaftError>;
    fn install_snapshot(
        &self,
        target: u64,
        request: RustRaftInstallSnapshotRequest,
    ) -> Result<RustRaftInstallSnapshotResponse, RustRaftError>;
    fn read_index(
        &self,
        target: u64,
        request: RustRaftReadIndexRequest,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError>;
}

pub trait RaftTransport: RustRaftTransport {}

impl<T> RaftTransport for T where T: RustRaftTransport + ?Sized {}

impl RustRaftTransportValidationReport {
    fn new(rpc: impl Into<String>, blockers: Vec<String>) -> Self {
        Self {
            rpc: rpc.into(),
            valid: blockers.is_empty(),
            blockers,
        }
    }
}

fn validate_positive_id(blockers: &mut Vec<String>, field: &str, value: u64) {
    if value == 0 {
        blockers.push(format!("{field} must be greater than zero"));
    }
}

fn validate_log_id(blockers: &mut Vec<String>, field: &str, log_id: &RustRaftLogId) {
    if log_id.index == 0 {
        blockers.push(format!("{field}.index must be greater than zero"));
    }
}

fn validate_non_empty_reason(blockers: &mut Vec<String>, field: &str, reason: &str) {
    if reason.trim().is_empty() {
        blockers.push(format!("{field}.reason must not be empty"));
    }
}

pub(crate) fn require_transport_validation(
    report: RustRaftTransportValidationReport,
) -> Result<(), RaftError> {
    if report.valid {
        Ok(())
    } else {
        Err(RaftError::InvalidRequest(format!(
            "{} validation failed: {}",
            report.rpc,
            report.blockers.join("; ")
        )))
    }
}

pub fn rustraft_validate_append_entries_request(
    request: &AppendEntriesRequest,
) -> RustRaftTransportValidationReport {
    let mut blockers = Vec::new();
    validate_positive_id(&mut blockers, "group_id", request.group_id);
    validate_positive_id(&mut blockers, "leader_id", request.leader_id);
    if let Some(prev_log_id) = &request.prev_log_id {
        validate_log_id(&mut blockers, "prev_log_id", prev_log_id);
    }
    let mut expected_index = request
        .prev_log_id
        .as_ref()
        .map_or(1, |log_id| log_id.index + 1);
    for entry in &request.entries {
        validate_log_id(&mut blockers, "entries[].log_id", &entry.log_id);
        if entry.log_id.index != expected_index {
            blockers.push(format!(
                "entries must be contiguous: expected index {expected_index}, got {}",
                entry.log_id.index
            ));
        }
        expected_index = entry.log_id.index + 1;
    }
    RustRaftTransportValidationReport::new("append_entries_request", blockers)
}

pub fn rustraft_validate_append_entries_response(
    response: &AppendEntriesResponse,
) -> RustRaftTransportValidationReport {
    let mut blockers = Vec::new();
    if response.success && (response.rejection_hint.is_some() || response.rejected_index.is_some())
    {
        blockers.push(
            "successful append response must not include rejection_hint or rejected_index"
                .to_string(),
        );
    }
    RustRaftTransportValidationReport::new("append_entries_response", blockers)
}

pub fn rustraft_validate_vote_request(request: &VoteRequest) -> RustRaftTransportValidationReport {
    let mut blockers = Vec::new();
    validate_positive_id(&mut blockers, "group_id", request.group_id);
    validate_positive_id(&mut blockers, "candidate_id", request.candidate_id);
    if let Some(last_log_id) = &request.last_log_id {
        validate_log_id(&mut blockers, "last_log_id", last_log_id);
    }
    RustRaftTransportValidationReport::new("vote_request", blockers)
}

pub fn rustraft_validate_vote_response(
    response: &VoteResponse,
) -> RustRaftTransportValidationReport {
    let mut blockers = Vec::new();
    validate_non_empty_reason(&mut blockers, "vote_response", &response.reason);
    RustRaftTransportValidationReport::new("vote_response", blockers)
}

pub fn rustraft_validate_install_snapshot_request(
    request: &InstallSnapshotRequest,
) -> RustRaftTransportValidationReport {
    let mut blockers = Vec::new();
    validate_positive_id(&mut blockers, "group_id", request.group_id);
    validate_positive_id(&mut blockers, "leader_id", request.leader_id);
    validate_log_id(
        &mut blockers,
        "chunk.meta.last_log_id",
        &request.chunk.meta.last_log_id,
    );
    if request.chunk.meta.snapshot_id.trim().is_empty() {
        blockers.push("chunk.meta.snapshot_id must not be empty".to_string());
    }
    if request.chunk.meta.membership.is_empty() {
        blockers.push("chunk.meta.membership must not be empty".to_string());
    }
    RustRaftTransportValidationReport::new("install_snapshot_request", blockers)
}

pub fn rustraft_validate_install_snapshot_response(
    response: &InstallSnapshotResponse,
) -> RustRaftTransportValidationReport {
    let mut blockers = Vec::new();
    validate_non_empty_reason(&mut blockers, "install_snapshot_response", &response.reason);
    RustRaftTransportValidationReport::new("install_snapshot_response", blockers)
}

pub fn rustraft_validate_read_index_request(
    request: &ReadIndexRequest,
) -> RustRaftTransportValidationReport {
    let mut blockers = Vec::new();
    validate_positive_id(&mut blockers, "group_id", request.group_id);
    validate_positive_id(&mut blockers, "requester_id", request.requester_id);
    RustRaftTransportValidationReport::new("read_index_request", blockers)
}

pub fn rustraft_validate_read_index_response(
    response: &ReadIndexResponse,
) -> RustRaftTransportValidationReport {
    let mut blockers = Vec::new();
    validate_non_empty_reason(&mut blockers, "read_index_response", &response.reason);
    if !response.safe && response.lease_read {
        blockers.push("unsafe read-index response must not grant lease_read".to_string());
    }
    RustRaftTransportValidationReport::new("read_index_response", blockers)
}

pub fn rustraft_validate_tcp_transport_request(
    request: &TcpRaftTransportRequest,
) -> RustRaftTransportValidationReport {
    let mut report = match request {
        TcpRaftTransportRequest::AppendEntries { request, .. } => {
            rustraft_validate_append_entries_request(request)
        }
        TcpRaftTransportRequest::Vote { request, .. } => rustraft_validate_vote_request(request),
        TcpRaftTransportRequest::InstallSnapshot { request, .. } => {
            rustraft_validate_install_snapshot_request(request)
        }
        TcpRaftTransportRequest::ReadIndex { request, .. } => {
            rustraft_validate_read_index_request(request)
        }
        TcpRaftTransportRequest::Batch { requests } => {
            let mut blockers = Vec::new();
            if requests.is_empty() {
                blockers.push("batch requests must not be empty".to_string());
            }
            for (index, request) in requests.iter().enumerate() {
                if matches!(request, TcpRaftTransportRequest::Batch { .. }) {
                    blockers.push(format!("batch request {index} must not be nested"));
                    continue;
                }
                let report = rustraft_validate_tcp_transport_request(request);
                blockers.extend(
                    report
                        .blockers
                        .into_iter()
                        .map(|blocker| format!("batch request {index}: {blocker}")),
                );
            }
            return RustRaftTransportValidationReport::new("tcp_batch_request", blockers);
        }
    };
    let target = match request {
        TcpRaftTransportRequest::AppendEntries { target, .. }
        | TcpRaftTransportRequest::Vote { target, .. }
        | TcpRaftTransportRequest::InstallSnapshot { target, .. }
        | TcpRaftTransportRequest::ReadIndex { target, .. } => *target,
        TcpRaftTransportRequest::Batch { .. } => unreachable!("batch validation returns early"),
    };
    validate_positive_id(&mut report.blockers, "target", target);
    report.valid = report.blockers.is_empty();
    report
}
