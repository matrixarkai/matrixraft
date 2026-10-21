// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Transport traits and RPC envelopes for append, vote, snapshot, and read-index paths.

pub use crate::{
    AppendEntriesRequest, AppendEntriesResponse, AuthPolicy, AuthenticatedRaftRpc,
    AuthenticatedRaftTransport, ClusterRaftTransport, HeartbeatAddressResolver,
    HeartbeatMergeMessage, HeartbeatMergeStats, HeartbeatMerger, InMemoryRaftTransport,
    InstallSnapshotRequest, InstallSnapshotResponse, MergedHeartbeatBatch, PreVoteRequest,
    PreVoteResponse, ReadIndexRequest, ReadIndexResponse, SnapshotChunk, StaticRaftAuthToken,
    TcpRaftRpcResult, TcpRaftTransport, TcpRaftTransportRequest, TcpRaftTransportResponse,
    TcpRaftTransportServer, TransportValidationReport, VoteRequest, VoteResponse,
};

use crate::{LogId, RaftError};

/// Network transport API for Raft append, vote, snapshot, and read-index RPCs.
pub trait Transport {
    fn append_entries(
        &self,
        target: u64,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError>;
    fn vote(&self, target: u64, request: VoteRequest) -> Result<VoteResponse, RaftError>;
    fn install_snapshot(
        &self,
        target: u64,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError>;
    fn read_index(
        &self,
        target: u64,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError>;
}

impl TransportValidationReport {
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

/// The index rules, on their own, so every index that crosses the wire can be
/// held to them rather than only the ones inside a [`LogId`].
///
/// Keeping them inlined in `validate_log_id` meant the rules reached the three
/// indices that happen to be wrapped in a `LogId` and no others. A follower's
/// `require_snapshot` is just as much an index the leader acts on, and it was
/// not covered: a claim at the ceiling latched the leader's per-peer ratchet
/// permanently, after which the peer's own honest request for the snapshot it
/// needed was refused.
fn validate_log_index(blockers: &mut Vec<String>, field: &str, index: crate::LogIndex) {
    if index == 0 {
        blockers.push(format!("{field} must be greater than zero"));
    }
    if index == crate::LogIndex::MAX {
        // The protocol needs the index after this one to exist: a follower
        // stores `index + 1` as its next index, a leader builds its next entry
        // at `last + 1`, and a snapshot publishes `snapshot_index + 1` as the
        // first index it retains. An index at the ceiling makes all three
        // unrepresentable, so it is refused here rather than overflowed later.
        blockers.push(format!(
            "{field} must be below {}, because the protocol needs the index after it",
            crate::LogIndex::MAX
        ));
    }
}

fn validate_log_id(blockers: &mut Vec<String>, field: &str, log_id: &LogId) {
    validate_log_index(blockers, &format!("{field}.index"), log_id.index);
}

fn validate_non_empty_reason(blockers: &mut Vec<String>, field: &str, reason: &str) {
    if reason.trim().is_empty() {
        blockers.push(format!("{field}.reason must not be empty"));
    }
}

pub(crate) fn require_transport_validation(
    report: TransportValidationReport,
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

pub fn matrixraft_validate_append_entries_request(
    request: &AppendEntriesRequest,
) -> TransportValidationReport {
    let mut blockers = Vec::new();
    validate_positive_id(&mut blockers, "group_id", request.group_id);
    validate_positive_id(&mut blockers, "leader_id", request.leader_id);
    if let Some(prev_log_id) = &request.prev_log_id {
        validate_log_id(&mut blockers, "prev_log_id", prev_log_id);
    }
    // `checked_add`, not `saturating_add`: this is an EXPECTATION, and a
    // clamped expectation matches an index it should have rejected. `None`
    // means no index can follow, which the rule above has already recorded as
    // a blocker -- the contiguity check is simply skipped rather than compared
    // against a wrong number.
    let mut expected_index = match request.prev_log_id.as_ref() {
        None => Some(1),
        Some(log_id) => log_id.index.checked_add(1),
    };
    for entry in &request.entries {
        validate_log_id(&mut blockers, "entries[].log_id", &entry.log_id);
        if let Some(expected) = expected_index {
            if entry.log_id.index != expected {
                blockers.push(format!(
                    "entries must be contiguous: expected index {expected}, got {}",
                    entry.log_id.index
                ));
            }
        }
        expected_index = entry.log_id.index.checked_add(1);
    }
    TransportValidationReport::new("append_entries_request", blockers)
}

pub fn matrixraft_validate_append_entries_response(
    response: &AppendEntriesResponse,
) -> TransportValidationReport {
    let mut blockers = Vec::new();
    if response.success && (response.rejection_hint.is_some() || response.rejected_index.is_some())
    {
        blockers.push(
            "successful append response must not include rejection_hint or rejected_index"
                .to_string(),
        );
    }
    // The one index on this response that the leader turns into an action. It
    // drives a monotonic per-peer ratchet, so an unusable value is not merely
    // ignored -- it sticks, and shuts out the peer's later honest requests.
    //
    // The other numbers here are deliberately not held to this rule, and the
    // reasons are worth stating so a later sweep does not read their absence
    // as an oversight. `match_index` is an unbounded follower claim on purpose:
    // it paces replication and does not decide the commit, which is computed
    // from the leader's own node model. `rejected_index` and `rejection_hint`
    // only rewind `next_index` and are added to with `saturating_add`. A
    // commit or read index of zero is legitimate, meaning nothing is committed
    // yet, so the zero half of this rule would be wrong for them.
    if let Some(required_index) = response.require_snapshot {
        validate_log_index(&mut blockers, "require_snapshot", required_index);
    }
    TransportValidationReport::new("append_entries_response", blockers)
}

pub fn matrixraft_validate_vote_request(request: &VoteRequest) -> TransportValidationReport {
    let mut blockers = Vec::new();
    validate_positive_id(&mut blockers, "group_id", request.group_id);
    validate_positive_id(&mut blockers, "candidate_id", request.candidate_id);
    if let Some(last_log_id) = &request.last_log_id {
        validate_log_id(&mut blockers, "last_log_id", last_log_id);
    }
    TransportValidationReport::new("vote_request", blockers)
}

pub fn matrixraft_validate_vote_response(response: &VoteResponse) -> TransportValidationReport {
    let mut blockers = Vec::new();
    validate_non_empty_reason(&mut blockers, "vote_response", &response.reason);
    TransportValidationReport::new("vote_response", blockers)
}

pub fn matrixraft_validate_install_snapshot_request(
    request: &InstallSnapshotRequest,
) -> TransportValidationReport {
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
    TransportValidationReport::new("install_snapshot_request", blockers)
}

pub fn matrixraft_validate_install_snapshot_response(
    response: &InstallSnapshotResponse,
) -> TransportValidationReport {
    let mut blockers = Vec::new();
    validate_non_empty_reason(&mut blockers, "install_snapshot_response", &response.reason);
    TransportValidationReport::new("install_snapshot_response", blockers)
}

pub fn matrixraft_validate_read_index_request(
    request: &ReadIndexRequest,
) -> TransportValidationReport {
    let mut blockers = Vec::new();
    validate_positive_id(&mut blockers, "group_id", request.group_id);
    validate_positive_id(&mut blockers, "requester_id", request.requester_id);
    TransportValidationReport::new("read_index_request", blockers)
}

pub fn matrixraft_validate_read_index_response(
    response: &ReadIndexResponse,
) -> TransportValidationReport {
    let mut blockers = Vec::new();
    validate_non_empty_reason(&mut blockers, "read_index_response", &response.reason);
    if !response.safe && response.lease_read {
        blockers.push("unsafe read-index response must not grant lease_read".to_string());
    }
    TransportValidationReport::new("read_index_response", blockers)
}

pub fn matrixraft_validate_tcp_transport_request(
    request: &TcpRaftTransportRequest,
) -> TransportValidationReport {
    let mut report = match request {
        TcpRaftTransportRequest::AppendEntries { request, .. } => {
            matrixraft_validate_append_entries_request(request)
        }
        TcpRaftTransportRequest::Vote { request, .. } => matrixraft_validate_vote_request(request),
        TcpRaftTransportRequest::InstallSnapshot { request, .. } => {
            matrixraft_validate_install_snapshot_request(request)
        }
        TcpRaftTransportRequest::ReadIndex { request, .. } => {
            matrixraft_validate_read_index_request(request)
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
                let report = matrixraft_validate_tcp_transport_request(request);
                blockers.extend(
                    report
                        .blockers
                        .into_iter()
                        .map(|blocker| format!("batch request {index}: {blocker}")),
                );
            }
            return TransportValidationReport::new("tcp_batch_request", blockers);
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
