// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The raft RPC as protobuf behind a framed envelope.
//!
//! The transport wrote a newline-delimited JSON envelope with entry payloads as
//! base64. Measured, that cost about 64 bytes of envelope per log entry and 4/3
//! of every payload byte -- the same shape the WAL record carried before it
//! became protobuf, and by then the only JSON left on a path that moves bytes.
//!
//! ## Framing, and why it had to change with the encoding
//!
//! A JSON object is newline-safe and a protobuf message is not: its bytes can
//! contain any value, `\n` among them. So a reader cannot find the end of one
//! by looking for a newline, and the frame carries its own length instead --
//! four bytes of magic, a little-endian `u32` length, a little-endian `u32`
//! CRC-32C over the body, then the body. The same frame the WAL record uses,
//! with its own magic for the same reason the checkpoint has one: a magic that
//! said only "something framed" would let a decoder try an RPC against a WAL
//! record and report a torn message rather than the wrong one.
//!
//! ## Reading both shapes, and the order a cluster upgrades in
//!
//! A receiver reads either: a frame, or a JSON line from a peer that predates
//! this. It tells them apart by the first byte, since a JSON object starts with
//! `{` and this frame starts with `M`.
//!
//! **An old sender can talk to a new receiver; a new sender cannot talk to an
//! old receiver.** So a mixed-version cluster upgrades RECEIVERS FIRST -- which
//! is the same order the base64 change in `api_messages` documented for itself,
//! and for the same reason.
//!
//! ## An unknown enum value is refused, not defaulted
//!
//! `SnapshotState` travels as a small integer and an unrecognised one is an
//! error. `SnapshotState::default()` is `None`, and defaulting an unknown value
//! to it would turn a receiver that is mid-transfer into one that looks idle --
//! the same argument `role_from_wire` makes for a replica role, where a witness
//! read as a voter changes what a quorum is.

use crate::wal_binary::{FrameDigest, LogEntryProto, LogIdProto, SnapshotMetadataProto};
use crate::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RaftError, ReadIndexRequest, ReadIndexResponse, SnapshotChunk, SnapshotState, TcpRaftRpcResult,
    TcpRaftTransportEnvelope, TcpRaftTransportRequest, TcpRaftTransportResponse, VoteRequest,
    VoteResponse,
};

/// The four bytes that open a framed RPC.
pub(crate) const RPC_MAGIC: &[u8; 4] = b"MRRP";

/// A length and a CRC-32C, as in every other frame this crate writes.
pub(crate) const RPC_FRAME_BYTES: usize = 8;

/// The magic and the frame together, which is what a reader takes before it
/// knows how much body to expect.
pub(crate) const RPC_HEAD_BYTES: usize = 4 + RPC_FRAME_BYTES;

fn missing(what: &str) -> RaftError {
    RaftError::Transport(format!("a framed RPC is missing {what}"))
}

// ---------------------------------------------------------------------------
// Leaf messages. `LogIdProto`, `LogEntryProto` and `SnapshotMetadataProto` are
// reused from the WAL's mirrors rather than duplicated: an entry's shape is an
// entry's shape, and a second copy of it is how two encodings drift apart.

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct AppendEntriesRequestProto {
    #[prost(uint64, tag = "1")]
    pub group_id: u64,
    #[prost(uint64, tag = "2")]
    pub term: u64,
    #[prost(uint64, tag = "3")]
    pub leader_id: u64,
    #[prost(message, optional, tag = "4")]
    pub prev_log_id: Option<LogIdProto>,
    #[prost(message, repeated, tag = "5")]
    pub entries: Vec<LogEntryProto>,
    #[prost(uint64, tag = "6")]
    pub leader_commit: u64,
    #[prost(uint64, tag = "7")]
    pub lease_epoch: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct AppendEntriesResponseProto {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub success: bool,
    #[prost(uint64, tag = "3")]
    pub match_index: u64,
    #[prost(uint64, optional, tag = "4")]
    pub rejection_hint: Option<u64>,
    #[prost(uint64, optional, tag = "5")]
    pub rejected_index: Option<u64>,
    #[prost(uint64, optional, tag = "6")]
    pub require_snapshot: Option<u64>,
    #[prost(int32, tag = "7")]
    pub snapshot_state: i32,
    #[prost(uint64, tag = "8")]
    pub lease_confirmation_epoch: u64,
    #[prost(uint64, tag = "9")]
    pub lease_duration_ms: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct VoteRequestProto {
    #[prost(uint64, tag = "1")]
    pub group_id: u64,
    #[prost(uint64, tag = "2")]
    pub term: u64,
    #[prost(uint64, tag = "3")]
    pub candidate_id: u64,
    #[prost(message, optional, tag = "4")]
    pub last_log_id: Option<LogIdProto>,
    #[prost(bool, tag = "5")]
    pub pre_vote: bool,
    #[prost(bool, tag = "6")]
    pub force: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct VoteResponseProto {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub vote_granted: bool,
    #[prost(string, tag = "3")]
    pub reason: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct SnapshotChunkProto {
    #[prost(message, optional, tag = "1")]
    pub meta: Option<SnapshotMetadataProto>,
    #[prost(uint64, tag = "2")]
    pub offset: u64,
    /// `bytes`, not a string: this is what retires base64 and its 4/3 on the
    /// single widest field in the protocol, a chunk being 64 KiB by default.
    #[prost(bytes = "vec", tag = "3")]
    pub data: Vec<u8>,
    #[prost(bool, tag = "4")]
    pub done: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct InstallSnapshotRequestProto {
    #[prost(uint64, tag = "1")]
    pub group_id: u64,
    #[prost(uint64, tag = "2")]
    pub term: u64,
    #[prost(uint64, tag = "3")]
    pub leader_id: u64,
    #[prost(message, optional, tag = "4")]
    pub chunk: Option<SnapshotChunkProto>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct InstallSnapshotResponseProto {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub accepted: bool,
    #[prost(uint64, tag = "3")]
    pub next_offset: u64,
    #[prost(uint64, tag = "4")]
    pub committed_index: u64,
    #[prost(string, tag = "5")]
    pub reason: String,
    #[prost(bool, tag = "6")]
    pub throttled: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ReadIndexRequestProto {
    #[prost(uint64, tag = "1")]
    pub group_id: u64,
    #[prost(uint64, tag = "2")]
    pub requester_id: u64,
    #[prost(uint64, tag = "3")]
    pub min_commit_index: u64,
    #[prost(bool, tag = "4")]
    pub allow_lease_read: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ReadIndexResponseProto {
    #[prost(bool, tag = "1")]
    pub safe: bool,
    #[prost(uint64, tag = "2")]
    pub read_index: u64,
    #[prost(bool, tag = "3")]
    pub lease_read: bool,
    #[prost(string, tag = "4")]
    pub reason: String,
}

// ---------------------------------------------------------------------------
// The request tree. Each arm carries the target beside the request, as the
// enum it mirrors does.

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct TargetedAppendProto {
    #[prost(uint64, tag = "1")]
    pub target: u64,
    #[prost(message, optional, tag = "2")]
    pub request: Option<AppendEntriesRequestProto>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct TargetedVoteProto {
    #[prost(uint64, tag = "1")]
    pub target: u64,
    #[prost(message, optional, tag = "2")]
    pub request: Option<VoteRequestProto>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct TargetedInstallProto {
    #[prost(uint64, tag = "1")]
    pub target: u64,
    #[prost(message, optional, tag = "2")]
    pub request: Option<InstallSnapshotRequestProto>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct TargetedReadProto {
    #[prost(uint64, tag = "1")]
    pub target: u64,
    #[prost(message, optional, tag = "2")]
    pub request: Option<ReadIndexRequestProto>,
}

/// A batch, as its own message so the oneof arm holds one field.
///
/// Recursive through a `Vec`, which is a pointer, so the type stays finite.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct RequestBatchProto {
    #[prost(message, repeated, tag = "1")]
    pub requests: Vec<RpcRequestProto>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub(crate) enum RequestKind {
    #[prost(message, tag = "1")]
    AppendEntries(TargetedAppendProto),
    #[prost(message, tag = "2")]
    Vote(TargetedVoteProto),
    #[prost(message, tag = "3")]
    InstallSnapshot(TargetedInstallProto),
    #[prost(message, tag = "4")]
    ReadIndex(TargetedReadProto),
    #[prost(message, tag = "5")]
    Batch(RequestBatchProto),
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct RpcRequestProto {
    #[prost(oneof = "RequestKind", tags = "1, 2, 3, 4, 5")]
    pub kind: Option<RequestKind>,
}

// ---------------------------------------------------------------------------
// The response tree. Each arm is a result: one of `ok` or `error`.

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct AppendResultProto {
    #[prost(message, optional, tag = "1")]
    pub ok: Option<AppendEntriesResponseProto>,
    #[prost(string, optional, tag = "2")]
    pub error: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct VoteResultProto {
    #[prost(message, optional, tag = "1")]
    pub ok: Option<VoteResponseProto>,
    #[prost(string, optional, tag = "2")]
    pub error: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct InstallResultProto {
    #[prost(message, optional, tag = "1")]
    pub ok: Option<InstallSnapshotResponseProto>,
    #[prost(string, optional, tag = "2")]
    pub error: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ReadResultProto {
    #[prost(message, optional, tag = "1")]
    pub ok: Option<ReadIndexResponseProto>,
    #[prost(string, optional, tag = "2")]
    pub error: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ResponseBatchProto {
    #[prost(message, repeated, tag = "1")]
    pub responses: Vec<RpcResponseProto>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub(crate) enum ResponseKind {
    #[prost(message, tag = "1")]
    AppendEntries(AppendResultProto),
    #[prost(message, tag = "2")]
    Vote(VoteResultProto),
    #[prost(message, tag = "3")]
    InstallSnapshot(InstallResultProto),
    #[prost(message, tag = "4")]
    ReadIndex(ReadResultProto),
    #[prost(message, tag = "5")]
    Batch(ResponseBatchProto),
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct RpcResponseProto {
    #[prost(oneof = "ResponseKind", tags = "1, 2, 3, 4, 5")]
    pub kind: Option<ResponseKind>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct EnvelopeProto {
    #[prost(uint64, tag = "1")]
    pub cluster_id: u64,
    #[prost(string, optional, tag = "2")]
    pub user_payload: Option<String>,
    #[prost(message, optional, tag = "3")]
    pub request: Option<RpcRequestProto>,
}

// ---------------------------------------------------------------------------
// `SnapshotState` as a small integer, refused rather than defaulted.

fn snapshot_state_to_wire(state: SnapshotState) -> i32 {
    match state {
        SnapshotState::None => 0,
        SnapshotState::Creating => 1,
        SnapshotState::Receiving => 2,
        SnapshotState::Received => 3,
        SnapshotState::NotReady => 4,
    }
}

fn snapshot_state_from_wire(value: i32) -> Result<SnapshotState, RaftError> {
    match value {
        0 => Ok(SnapshotState::None),
        1 => Ok(SnapshotState::Creating),
        2 => Ok(SnapshotState::Receiving),
        3 => Ok(SnapshotState::Received),
        4 => Ok(SnapshotState::NotReady),
        other => Err(RaftError::Transport(format!(
            "a framed RPC carries snapshot state {other}, which this build does not recognise"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Conversions. Mechanical, and written out rather than derived, so that a
// field added to a request without a line here is a COMPILE error instead of a
// field that silently stops travelling.

impl From<&AppendEntriesRequest> for AppendEntriesRequestProto {
    fn from(value: &AppendEntriesRequest) -> Self {
        Self {
            group_id: value.group_id,
            term: value.term,
            leader_id: value.leader_id,
            prev_log_id: value.prev_log_id.as_ref().map(LogIdProto::from),
            entries: value.entries.iter().map(LogEntryProto::from).collect(),
            leader_commit: value.leader_commit,
            lease_epoch: value.lease_epoch,
        }
    }
}

impl TryFrom<AppendEntriesRequestProto> for AppendEntriesRequest {
    type Error = RaftError;

    fn try_from(proto: AppendEntriesRequestProto) -> Result<Self, Self::Error> {
        let mut entries = Vec::with_capacity(proto.entries.len());
        for entry in proto.entries {
            entries.push(crate::LogEntry::try_from(entry)?);
        }
        Ok(Self {
            group_id: proto.group_id,
            term: proto.term,
            leader_id: proto.leader_id,
            prev_log_id: proto.prev_log_id.as_ref().map(crate::LogId::from),
            entries,
            leader_commit: proto.leader_commit,
            lease_epoch: proto.lease_epoch,
        })
    }
}

impl From<&AppendEntriesResponse> for AppendEntriesResponseProto {
    fn from(value: &AppendEntriesResponse) -> Self {
        Self {
            term: value.term,
            success: value.success,
            match_index: value.match_index,
            rejection_hint: value.rejection_hint,
            rejected_index: value.rejected_index,
            require_snapshot: value.require_snapshot,
            snapshot_state: snapshot_state_to_wire(value.snapshot_state),
            lease_confirmation_epoch: value.lease_confirmation_epoch,
            lease_duration_ms: value.lease_duration_ms,
        }
    }
}

impl TryFrom<AppendEntriesResponseProto> for AppendEntriesResponse {
    type Error = RaftError;

    fn try_from(proto: AppendEntriesResponseProto) -> Result<Self, Self::Error> {
        Ok(Self {
            term: proto.term,
            success: proto.success,
            match_index: proto.match_index,
            rejection_hint: proto.rejection_hint,
            rejected_index: proto.rejected_index,
            require_snapshot: proto.require_snapshot,
            snapshot_state: snapshot_state_from_wire(proto.snapshot_state)?,
            lease_confirmation_epoch: proto.lease_confirmation_epoch,
            lease_duration_ms: proto.lease_duration_ms,
        })
    }
}

impl From<&VoteRequest> for VoteRequestProto {
    fn from(value: &VoteRequest) -> Self {
        Self {
            group_id: value.group_id,
            term: value.term,
            candidate_id: value.candidate_id,
            last_log_id: value.last_log_id.as_ref().map(LogIdProto::from),
            pre_vote: value.pre_vote,
            force: value.force,
        }
    }
}

impl From<VoteRequestProto> for VoteRequest {
    fn from(proto: VoteRequestProto) -> Self {
        Self {
            group_id: proto.group_id,
            term: proto.term,
            candidate_id: proto.candidate_id,
            last_log_id: proto.last_log_id.as_ref().map(crate::LogId::from),
            pre_vote: proto.pre_vote,
            force: proto.force,
        }
    }
}

impl From<&VoteResponse> for VoteResponseProto {
    fn from(value: &VoteResponse) -> Self {
        Self {
            term: value.term,
            vote_granted: value.vote_granted,
            reason: value.reason.clone(),
        }
    }
}

impl From<VoteResponseProto> for VoteResponse {
    fn from(proto: VoteResponseProto) -> Self {
        Self {
            term: proto.term,
            vote_granted: proto.vote_granted,
            reason: proto.reason,
        }
    }
}

impl From<&SnapshotChunk> for SnapshotChunkProto {
    fn from(value: &SnapshotChunk) -> Self {
        Self {
            meta: Some(SnapshotMetadataProto::from(&value.meta)),
            offset: value.offset,
            data: value.data.clone(),
            done: value.done,
        }
    }
}

impl TryFrom<SnapshotChunkProto> for SnapshotChunk {
    type Error = RaftError;

    fn try_from(proto: SnapshotChunkProto) -> Result<Self, Self::Error> {
        Ok(Self {
            meta: crate::SnapshotMetadata::try_from(
                proto.meta.ok_or_else(|| missing("a chunk's metadata"))?,
            )?,
            offset: proto.offset,
            data: proto.data,
            done: proto.done,
        })
    }
}

impl From<&InstallSnapshotRequest> for InstallSnapshotRequestProto {
    fn from(value: &InstallSnapshotRequest) -> Self {
        Self {
            group_id: value.group_id,
            term: value.term,
            leader_id: value.leader_id,
            chunk: Some(SnapshotChunkProto::from(&value.chunk)),
        }
    }
}

impl TryFrom<InstallSnapshotRequestProto> for InstallSnapshotRequest {
    type Error = RaftError;

    fn try_from(proto: InstallSnapshotRequestProto) -> Result<Self, Self::Error> {
        Ok(Self {
            group_id: proto.group_id,
            term: proto.term,
            leader_id: proto.leader_id,
            chunk: SnapshotChunk::try_from(
                proto.chunk.ok_or_else(|| missing("an install's chunk"))?,
            )?,
        })
    }
}

impl From<&InstallSnapshotResponse> for InstallSnapshotResponseProto {
    fn from(value: &InstallSnapshotResponse) -> Self {
        Self {
            term: value.term,
            accepted: value.accepted,
            next_offset: value.next_offset,
            committed_index: value.committed_index,
            reason: value.reason.clone(),
            throttled: value.throttled,
        }
    }
}

impl From<InstallSnapshotResponseProto> for InstallSnapshotResponse {
    fn from(proto: InstallSnapshotResponseProto) -> Self {
        Self {
            term: proto.term,
            accepted: proto.accepted,
            next_offset: proto.next_offset,
            committed_index: proto.committed_index,
            reason: proto.reason,
            throttled: proto.throttled,
        }
    }
}

impl From<&ReadIndexRequest> for ReadIndexRequestProto {
    fn from(value: &ReadIndexRequest) -> Self {
        Self {
            group_id: value.group_id,
            requester_id: value.requester_id,
            min_commit_index: value.min_commit_index,
            allow_lease_read: value.allow_lease_read,
        }
    }
}

impl From<ReadIndexRequestProto> for ReadIndexRequest {
    fn from(proto: ReadIndexRequestProto) -> Self {
        Self {
            group_id: proto.group_id,
            requester_id: proto.requester_id,
            min_commit_index: proto.min_commit_index,
            allow_lease_read: proto.allow_lease_read,
        }
    }
}

impl From<&ReadIndexResponse> for ReadIndexResponseProto {
    fn from(value: &ReadIndexResponse) -> Self {
        Self {
            safe: value.safe,
            read_index: value.read_index,
            lease_read: value.lease_read,
            reason: value.reason.clone(),
        }
    }
}

impl From<ReadIndexResponseProto> for ReadIndexResponse {
    fn from(proto: ReadIndexResponseProto) -> Self {
        Self {
            safe: proto.safe,
            read_index: proto.read_index,
            lease_read: proto.lease_read,
            reason: proto.reason,
        }
    }
}

// --- the request tree

impl From<&TcpRaftTransportRequest> for RpcRequestProto {
    fn from(value: &TcpRaftTransportRequest) -> Self {
        let kind = match value {
            TcpRaftTransportRequest::AppendEntries { target, request } => {
                RequestKind::AppendEntries(TargetedAppendProto {
                    target: *target,
                    request: Some(AppendEntriesRequestProto::from(request)),
                })
            }
            TcpRaftTransportRequest::Vote { target, request } => {
                RequestKind::Vote(TargetedVoteProto {
                    target: *target,
                    request: Some(VoteRequestProto::from(request)),
                })
            }
            TcpRaftTransportRequest::InstallSnapshot { target, request } => {
                RequestKind::InstallSnapshot(TargetedInstallProto {
                    target: *target,
                    request: Some(InstallSnapshotRequestProto::from(request)),
                })
            }
            TcpRaftTransportRequest::ReadIndex { target, request } => {
                RequestKind::ReadIndex(TargetedReadProto {
                    target: *target,
                    request: Some(ReadIndexRequestProto::from(request)),
                })
            }
            TcpRaftTransportRequest::Batch { requests } => RequestKind::Batch(RequestBatchProto {
                requests: requests.iter().map(RpcRequestProto::from).collect(),
            }),
        };
        Self { kind: Some(kind) }
    }
}

impl TryFrom<RpcRequestProto> for TcpRaftTransportRequest {
    type Error = RaftError;

    fn try_from(proto: RpcRequestProto) -> Result<Self, Self::Error> {
        match proto.kind.ok_or_else(|| missing("its request"))? {
            RequestKind::AppendEntries(arm) => Ok(Self::AppendEntries {
                target: arm.target,
                request: AppendEntriesRequest::try_from(
                    arm.request.ok_or_else(|| missing("an append request"))?,
                )?,
            }),
            RequestKind::Vote(arm) => Ok(Self::Vote {
                target: arm.target,
                request: VoteRequest::from(arm.request.ok_or_else(|| missing("a vote request"))?),
            }),
            RequestKind::InstallSnapshot(arm) => Ok(Self::InstallSnapshot {
                target: arm.target,
                request: InstallSnapshotRequest::try_from(
                    arm.request.ok_or_else(|| missing("an install request"))?,
                )?,
            }),
            RequestKind::ReadIndex(arm) => Ok(Self::ReadIndex {
                target: arm.target,
                request: ReadIndexRequest::from(
                    arm.request.ok_or_else(|| missing("a read index request"))?,
                ),
            }),
            RequestKind::Batch(arm) => {
                let mut requests = Vec::with_capacity(arm.requests.len());
                for request in arm.requests {
                    requests.push(Self::try_from(request)?);
                }
                Ok(Self::Batch { requests })
            }
        }
    }
}

// --- the response tree

impl From<&TcpRaftTransportResponse> for RpcResponseProto {
    fn from(value: &TcpRaftTransportResponse) -> Self {
        let kind = match value {
            TcpRaftTransportResponse::AppendEntries(result) => {
                ResponseKind::AppendEntries(AppendResultProto {
                    ok: result.ok.as_ref().map(AppendEntriesResponseProto::from),
                    error: result.error.clone(),
                })
            }
            TcpRaftTransportResponse::Vote(result) => ResponseKind::Vote(VoteResultProto {
                ok: result.ok.as_ref().map(VoteResponseProto::from),
                error: result.error.clone(),
            }),
            TcpRaftTransportResponse::InstallSnapshot(result) => {
                ResponseKind::InstallSnapshot(InstallResultProto {
                    ok: result.ok.as_ref().map(InstallSnapshotResponseProto::from),
                    error: result.error.clone(),
                })
            }
            TcpRaftTransportResponse::ReadIndex(result) => {
                ResponseKind::ReadIndex(ReadResultProto {
                    ok: result.ok.as_ref().map(ReadIndexResponseProto::from),
                    error: result.error.clone(),
                })
            }
            TcpRaftTransportResponse::Batch(responses) => ResponseKind::Batch(ResponseBatchProto {
                responses: responses.iter().map(RpcResponseProto::from).collect(),
            }),
        };
        Self { kind: Some(kind) }
    }
}

impl TryFrom<RpcResponseProto> for TcpRaftTransportResponse {
    type Error = RaftError;

    fn try_from(proto: RpcResponseProto) -> Result<Self, Self::Error> {
        match proto.kind.ok_or_else(|| missing("its response"))? {
            ResponseKind::AppendEntries(arm) => Ok(Self::AppendEntries(TcpRaftRpcResult {
                ok: arm.ok.map(AppendEntriesResponse::try_from).transpose()?,
                error: arm.error,
            })),
            ResponseKind::Vote(arm) => Ok(Self::Vote(TcpRaftRpcResult {
                ok: arm.ok.map(VoteResponse::from),
                error: arm.error,
            })),
            ResponseKind::InstallSnapshot(arm) => Ok(Self::InstallSnapshot(TcpRaftRpcResult {
                ok: arm.ok.map(InstallSnapshotResponse::from),
                error: arm.error,
            })),
            ResponseKind::ReadIndex(arm) => Ok(Self::ReadIndex(TcpRaftRpcResult {
                ok: arm.ok.map(ReadIndexResponse::from),
                error: arm.error,
            })),
            ResponseKind::Batch(arm) => {
                let mut responses = Vec::with_capacity(arm.responses.len());
                for response in arm.responses {
                    responses.push(Self::try_from(response)?);
                }
                Ok(Self::Batch(responses))
            }
        }
    }
}

impl From<&TcpRaftTransportEnvelope> for EnvelopeProto {
    fn from(value: &TcpRaftTransportEnvelope) -> Self {
        Self {
            cluster_id: value.cluster_id,
            user_payload: value.user_payload.clone(),
            request: Some(RpcRequestProto::from(&value.request)),
        }
    }
}

impl TryFrom<EnvelopeProto> for TcpRaftTransportEnvelope {
    type Error = RaftError;

    fn try_from(proto: EnvelopeProto) -> Result<Self, Self::Error> {
        Ok(Self {
            cluster_id: proto.cluster_id,
            user_payload: proto.user_payload,
            request: TcpRaftTransportRequest::try_from(
                proto.request.ok_or_else(|| missing("its request"))?,
            )?,
        })
    }
}

// ---------------------------------------------------------------------------
// Framing.

fn frame(body: Vec<u8>) -> Result<Vec<u8>, RaftError> {
    let length = u32::try_from(body.len())
        .map_err(|_| RaftError::Transport("a framed RPC exceeds a frame length".to_string()))?;
    let mut out = Vec::with_capacity(RPC_MAGIC.len() + RPC_FRAME_BYTES + body.len());
    out.extend_from_slice(RPC_MAGIC);
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&FrameDigest::Crc32c.of(&body).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// The body of a framed RPC, checked against its length and its digest.
fn unframe(bytes: &[u8]) -> Result<&[u8], RaftError> {
    if !is_framed_rpc(bytes) {
        return Err(RaftError::Transport(
            "a framed RPC does not open with its magic".to_string(),
        ));
    }
    let rest = &bytes[RPC_MAGIC.len()..];
    if rest.len() < RPC_FRAME_BYTES {
        return Err(RaftError::Transport(
            "a framed RPC is shorter than its frame".to_string(),
        ));
    }
    let length = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
    let digest = u32::from_le_bytes([rest[4], rest[5], rest[6], rest[7]]);
    let end = RPC_FRAME_BYTES
        .checked_add(length)
        .ok_or_else(|| RaftError::Transport("a framed RPC's length overflows".to_string()))?;
    if rest.len() < end {
        return Err(RaftError::Transport(format!(
            "a framed RPC declares {length} bytes and carries {}",
            rest.len() - RPC_FRAME_BYTES
        )));
    }
    let body = &rest[RPC_FRAME_BYTES..end];
    if FrameDigest::Crc32c.of(body) != digest {
        return Err(RaftError::Transport(
            "a framed RPC fails its frame digest".to_string(),
        ));
    }
    Ok(body)
}

/// Whether these bytes open a framed RPC rather than a JSON line.
///
/// A JSON object starts with `{` and this frame starts with `M`, so one byte
/// settles it -- which is what lets a receiver take either shape while a
/// cluster upgrades.
pub(crate) fn is_framed_rpc(bytes: &[u8]) -> bool {
    bytes.starts_with(RPC_MAGIC)
}

/// How many bytes a framed RPC occupies, read from its own frame.
pub(crate) fn framed_rpc_len(bytes: &[u8]) -> Option<usize> {
    if !is_framed_rpc(bytes) || bytes.len() < RPC_MAGIC.len() + RPC_FRAME_BYTES {
        return None;
    }
    let rest = &bytes[RPC_MAGIC.len()..];
    let length = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
    Some(RPC_MAGIC.len() + RPC_FRAME_BYTES + length)
}

/// One request envelope, framed.
pub fn matrixraft_encode_rpc_envelope(
    envelope: &TcpRaftTransportEnvelope,
) -> Result<Vec<u8>, RaftError> {
    frame(prost::Message::encode_to_vec(&EnvelopeProto::from(
        envelope,
    )))
}

/// The envelope a framed request carries.
pub fn matrixraft_decode_rpc_envelope(bytes: &[u8]) -> Result<TcpRaftTransportEnvelope, RaftError> {
    let body = unframe(bytes)?;
    let proto = <EnvelopeProto as prost::Message>::decode(body)
        .map_err(|err| RaftError::Transport(format!("failed to decode a framed RPC: {err}")))?;
    TcpRaftTransportEnvelope::try_from(proto)
}

/// One response, framed.
pub fn matrixraft_encode_rpc_response(
    response: &TcpRaftTransportResponse,
) -> Result<Vec<u8>, RaftError> {
    frame(prost::Message::encode_to_vec(&RpcResponseProto::from(
        response,
    )))
}

/// The response a framed reply carries.
pub fn matrixraft_decode_rpc_response(bytes: &[u8]) -> Result<TcpRaftTransportResponse, RaftError> {
    let body = unframe(bytes)?;
    let proto = <RpcResponseProto as prost::Message>::decode(body).map_err(|err| {
        RaftError::Transport(format!("failed to decode a framed RPC response: {err}"))
    })?;
    TcpRaftTransportResponse::try_from(proto)
}
