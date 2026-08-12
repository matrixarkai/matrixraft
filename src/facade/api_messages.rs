// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// public API request/response/message/admin command contracts.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftNodeOptions {
    pub group_id: RustRaftGroupId,
    pub node_id: RustRaftNodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    pub wal_dir: String,
    pub snapshot_dir: String,
    pub role: RustRaftReplicaRole,
    pub config: RustRaftConfig,
    #[serde(default)]
    pub peers: Vec<RustRaftPeer>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProposeOptions {
    pub expected_term: Option<RustRaftTerm>,
    pub is_command: bool,
    #[serde(default)]
    pub is_membership_change: bool,
}

impl Default for RustRaftProposeOptions {
    fn default() -> Self {
        Self {
            expected_term: None,
            is_command: true,
            is_membership_change: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftApplyRequest {
    pub group_id: RustRaftGroupId,
    pub log_id: RustRaftLogId,
    pub payload: RustRaftPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftApplyResponse {
    pub applied_index: RustRaftLogIndex,
    pub response: RustRaftPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftGenericApplyRequest<G = RustRaftGroupId, P = RustRaftPayload> {
    pub group_id: G,
    pub log_id: RustRaftLogId,
    pub payload: P,
}

pub type RaftApplyRequest<G = RustRaftGroupId, P = EntryPayload> =
    RustRaftGenericApplyRequest<G, P>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftGenericApplyResponse<P = RustRaftPayload> {
    pub applied_index: RustRaftLogIndex,
    pub response: P,
}

pub type RaftApplyResponse<P = EntryPayload> = RustRaftGenericApplyResponse<P>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReferenceRaftParitySurface {
    pub node_lifecycle: Vec<String>,
    pub transport_api: Vec<String>,
    pub write_api: Vec<String>,
    pub read_api: Vec<String>,
    pub membership_api: Vec<String>,
    pub durability_api: Vec<String>,
    pub observability_api: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPeerStatus {
    pub node_id: RustRaftNodeId,
    pub matched: RustRaftLogIndex,
    pub next_index: RustRaftLogIndex,
    pub learner: bool,
    pub healthy: bool,
    pub lag: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftStatusSnapshot {
    pub group_id: RustRaftGroupId,
    pub node_id: RustRaftNodeId,
    pub role: RustRaftRole,
    pub term: RustRaftTerm,
    pub leader_id: Option<RustRaftNodeId>,
    pub commit_index: RustRaftLogIndex,
    pub applied_index: RustRaftLogIndex,
    pub last_log_index: RustRaftLogIndex,
    pub last_snapshot_index: RustRaftLogIndex,
    pub peers: Vec<RustRaftPeerStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendEntriesRequest {
    pub group_id: RustRaftGroupId,
    pub term: RustRaftTerm,
    pub leader_id: RustRaftNodeId,
    pub prev_log_id: Option<RustRaftLogId>,
    pub entries: Vec<RustRaftLogEntry>,
    pub leader_commit: RustRaftLogIndex,
    #[serde(default)]
    pub lease_epoch: u64,
}

pub type AppendEntriesRequest = RustRaftAppendEntriesRequest;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendEntriesResponse {
    pub term: RustRaftTerm,
    pub success: bool,
    pub match_index: RustRaftLogIndex,
    pub rejection_hint: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub rejected_index: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub require_snapshot: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub snapshot_state: RustRaftSnapshotState,
    #[serde(default)]
    pub lease_confirmation_epoch: u64,
    #[serde(default)]
    pub lease_duration_ms: u64,
}

pub type AppendEntriesResponse = RustRaftAppendEntriesResponse;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftSnapshotState {
    #[default]
    None,
    Creating,
    Receiving,
    Received,
    NotReady,
}

pub type SnapshotState = RustRaftSnapshotState;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftConfState {
    #[default]
    Voter,
    Learner,
    Witness,
}

impl From<RustRaftReplicaRole> for MatrixRaftConfState {
    fn from(role: RustRaftReplicaRole) -> Self {
        match role {
            RustRaftReplicaRole::Voter => Self::Voter,
            RustRaftReplicaRole::Learner => Self::Learner,
            RustRaftReplicaRole::Witness => Self::Witness,
        }
    }
}

impl From<MatrixRaftConfState> for RustRaftReplicaRole {
    fn from(state: MatrixRaftConfState) -> Self {
        match state {
            MatrixRaftConfState::Voter => Self::Voter,
            MatrixRaftConfState::Learner => Self::Learner,
            MatrixRaftConfState::Witness => Self::Witness,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMemberId {
    pub id: RustRaftNodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    pub is_from_options: bool,
    #[serde(default)]
    pub conf_state: MatrixRaftConfState,
    #[serde(default)]
    pub auto_promote: bool,
}

impl MatrixRaftMemberId {
    pub fn to_peer(&self) -> RustRaftPeer {
        RustRaftPeer {
            node_id: self.id,
            raft_addr: self.raft_addr.clone(),
            snapshot_addr: self.snapshot_addr.clone(),
            role: self.conf_state.into(),
            auto_promote: self.auto_promote,
        }
    }
}

impl From<&RustRaftPeer> for MatrixRaftMemberId {
    fn from(peer: &RustRaftPeer) -> Self {
        Self {
            id: peer.node_id,
            raft_addr: peer.raft_addr.clone(),
            snapshot_addr: peer.snapshot_addr.clone(),
            is_from_options: true,
            conf_state: peer.role.into(),
            auto_promote: peer.auto_promote,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotDesc {
    #[serde(default)]
    pub snapshot_id: Option<RustRaftSnapshotId>,
    pub index: RustRaftLogIndex,
    pub term: RustRaftTerm,
    #[serde(default)]
    pub members: Vec<MatrixRaftMemberId>,
    #[serde(default)]
    pub checksum_type: Option<String>,
    #[serde(default)]
    pub checksum: Option<i32>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub local: bool,
    #[serde(default = "default_matrixraft_snapshot_version")]
    pub version: i32,
}

fn default_matrixraft_snapshot_version() -> i32 {
    1
}

impl MatrixRaftSnapshotDesc {
    pub fn from_snapshot_meta(meta: &RustRaftSnapshotMeta) -> Self {
        Self {
            snapshot_id: Some(meta.snapshot_id.clone()),
            index: meta.last_log_id.index,
            term: meta.last_log_id.term,
            members: meta.members.iter().map(MatrixRaftMemberId::from).collect(),
            checksum_type: None,
            checksum: None,
            url: None,
            local: false,
            version: default_matrixraft_snapshot_version(),
        }
    }

    pub fn to_snapshot_meta(&self, snapshot_id: impl Into<String>) -> RustRaftSnapshotMeta {
        let members: Vec<_> = self
            .members
            .iter()
            .map(MatrixRaftMemberId::to_peer)
            .collect();
        RustRaftSnapshotMeta {
            snapshot_id: snapshot_id.into(),
            last_log_id: RustRaftLogId {
                term: self.term,
                index: self.index,
            },
            membership: members.iter().map(|peer| peer.node_id).collect(),
            members,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftInitialState {
    pub index: RustRaftLogIndex,
    pub term: RustRaftTerm,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftHardState {
    pub current_term: RustRaftTerm,
    pub voted_for: Option<RustRaftNodeId>,
}

impl From<&RustRaftHardState> for MatrixRaftHardState {
    fn from(state: &RustRaftHardState) -> Self {
        Self {
            current_term: state.current_term,
            voted_for: state.voted_for,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMembers {
    pub members: Vec<MatrixRaftMemberId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMetadata {
    pub current_term: RustRaftTerm,
    pub voted_for: Option<RustRaftNodeId>,
    pub committed_index: RustRaftLogIndex,
    #[serde(default)]
    pub local_id: Option<MatrixRaftMemberId>,
    #[serde(default)]
    pub members: Vec<MatrixRaftMemberId>,
    #[serde(default)]
    pub initial_state: Option<MatrixRaftInitialState>,
    #[serde(default = "default_matrixraft_metadata_version")]
    pub version: i32,
    #[serde(default)]
    pub conf_state: MatrixRaftConfState,
    #[serde(default)]
    pub auto_promote: bool,
}

fn default_matrixraft_metadata_version() -> i32 {
    2
}

impl MatrixRaftMetadata {
    pub fn from_hard_state_and_membership(
        hard_state: &RustRaftHardState,
        membership: &RaftMembership,
        peers: &[RustRaftPeer],
    ) -> Self {
        let committed_index = hard_state
            .committed
            .as_ref()
            .map(|log_id| log_id.index)
            .unwrap_or_default();
        let members = peers
            .iter()
            .filter(|peer| {
                membership.voters.contains(&peer.node_id)
                    || membership.learners.contains(&peer.node_id)
                    || membership.witnesses.contains(&peer.node_id)
            })
            .map(MatrixRaftMemberId::from)
            .collect();
        Self {
            current_term: hard_state.current_term,
            voted_for: hard_state.voted_for,
            committed_index,
            local_id: None,
            members,
            initial_state: hard_state.committed.as_ref().map(|log_id| MatrixRaftInitialState {
                index: log_id.index,
                term: log_id.term,
            }),
            version: default_matrixraft_metadata_version(),
            conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftMessageType {
    Propose,
    MembershipOperation,
    ConfigChange,
    VoteRequest,
    VoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    ReadIndexRequest,
    CatchUpPeer,
    PromotePeer,
    AutoPromoteLearner,
    SnapshotProgress,
    SnapshotFinish,
    TimeoutNow,
    Snapshot,
    PreVote,
    PreVoteRequest,
    PreVoteResponse,
    AdminCommand,
    NetworkError,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftAdminCommandType {
    Election,
    Synced,
    Applied,
    Replicated,
    ApplyTaskInflight,
    TransferLeader,
    CompleteLeaderTransfer,
    AbortLeaderTransfer,
    StepDown,
    PartitionPeer,
    HealPeer,
    SetNodeHealthy,
    FireFatalEvent,
    ReceiveOutOfOrderAppend,
    ExpirePeerReorderQueue,
    ProhibitsElection,
    IgnoreWitness,
    Resign,
    TriggerSnapshot,
    SnapshotReady,
    SnapshotApplied,
    SetLeaderLeaseValid,
    ReceiveLeaderLeaseConfirmation,
    TickLeaderLease,
    ReceiveFollowerLease,
    TickFollowerLease,
    BeginSnapshotSend,
    RecordSnapshotChunkSent,
    AcknowledgeSnapshotChunk,
    RetrySnapshotChunk,
    CancelSnapshotSend,
    BeginSnapshotInstall,
    ReceiveSnapshotChunk,
    RollbackSnapshotInstall,
    CompactLogsThrough,
    CompactLogsWithStorageFence,
    CheckpointSnapshot,
    WitnessQuorum,
    ReleaseMemory,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftAdminStatus {
    pub success: bool,
    pub tips: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftAdminCommand {
    pub command_type: MatrixRaftAdminCommandType,
    #[serde(default)]
    pub request_id: Option<u64>,
    #[serde(default)]
    pub node_id: Option<RustRaftNodeId>,
    #[serde(default)]
    pub transferee_id: Option<RustRaftNodeId>,
    #[serde(default)]
    pub forced_campaign: bool,
    #[serde(default)]
    pub status: Option<MatrixRaftAdminStatus>,
    #[serde(default)]
    pub snapshot_state: Option<RustRaftSnapshotState>,
    #[serde(default)]
    pub snapshot_id: Option<RustRaftSnapshotId>,
    #[serde(default)]
    pub applied_index: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub log_index: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub first_index: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub last_index: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub prohibits_election: Option<bool>,
    #[serde(default)]
    pub apply_task_rejected: bool,
    #[serde(default)]
    pub stabled_config_change_index: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub ignore_witness: Option<bool>,
    #[serde(default)]
    pub snapshot_peer_id: Option<RustRaftNodeId>,
    #[serde(default)]
    pub snapshot_index: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub snapshot_total_chunks: Option<u64>,
    #[serde(default)]
    pub snapshot_bytes: Option<u64>,
    #[serde(default)]
    pub snapshot_done: bool,
    #[serde(default)]
    pub storage_fence: Option<RustRaftStorageApplyFence>,
    #[serde(default)]
    pub acknowledgements: Vec<RustRaftNodeId>,
    #[serde(default)]
    pub lease_valid: Option<bool>,
    #[serde(default)]
    pub lease_epoch: Option<u64>,
    #[serde(default)]
    pub lease_duration_ms: Option<u64>,
    #[serde(default)]
    pub elapsed_ms: Option<u64>,
    #[serde(default)]
    pub healthy: Option<bool>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub entry: Option<MatrixRaftEntry>,
}

impl MatrixRaftAdminCommand {
    pub fn election(forced: bool) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::Election,
            request_id: None,
            node_id: None,
            transferee_id: None,
            forced_campaign: forced,
            status: None,
            snapshot_state: None,
            snapshot_id: None,
            applied_index: None,
            log_index: None,
            first_index: None,
            last_index: None,
            prohibits_election: None,
            apply_task_rejected: false,
            stabled_config_change_index: None,
            ignore_witness: None,
            snapshot_peer_id: None,
            snapshot_index: None,
            snapshot_total_chunks: None,
            snapshot_bytes: None,
            snapshot_done: false,
            storage_fence: None,
            acknowledgements: Vec::new(),
            lease_valid: None,
            lease_epoch: None,
            lease_duration_ms: None,
            elapsed_ms: None,
            healthy: None,
            reason: None,
            entry: None,
        }
    }

    pub fn campaign(candidate_id: RustRaftNodeId, forced: bool) -> Self {
        Self {
            node_id: Some(candidate_id),
            ..Self::election(forced)
        }
    }

    pub fn transfer_leader(transferee_id: RustRaftNodeId) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::TransferLeader,
            transferee_id: Some(transferee_id),
            ..Self::election(false)
        }
    }

    pub fn complete_leader_transfer() -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::CompleteLeaderTransfer,
            ..Self::election(false)
        }
    }

    pub fn abort_leader_transfer(reason: impl Into<String>) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::AbortLeaderTransfer,
            reason: Some(reason.into()),
            ..Self::election(false)
        }
    }

    pub fn step_down(transferee_id: Option<RustRaftNodeId>) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::StepDown,
            transferee_id,
            ..Self::election(false)
        }
    }

    pub fn partition_peer(peer_id: RustRaftNodeId) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::PartitionPeer,
            snapshot_peer_id: Some(peer_id),
            ..Self::election(false)
        }
    }

    pub fn heal_peer(peer_id: RustRaftNodeId) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::HealPeer,
            snapshot_peer_id: Some(peer_id),
            ..Self::election(false)
        }
    }

    pub fn set_node_healthy(node_id: RustRaftNodeId, healthy: bool) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::SetNodeHealthy,
            node_id: Some(node_id),
            healthy: Some(healthy),
            ..Self::election(false)
        }
    }

    pub fn fire_fatal_event(node_id: RustRaftNodeId, reason: impl Into<String>) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::FireFatalEvent,
            node_id: Some(node_id),
            reason: Some(reason.into()),
            ..Self::election(false)
        }
    }

    pub fn receive_out_of_order_append(peer_id: RustRaftNodeId, entry: MatrixRaftEntry) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::ReceiveOutOfOrderAppend,
            node_id: Some(peer_id),
            entry: Some(entry),
            ..Self::election(false)
        }
    }

    pub fn expire_peer_reorder_queue(peer_id: RustRaftNodeId) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::ExpirePeerReorderQueue,
            node_id: Some(peer_id),
            ..Self::election(false)
        }
    }

    pub fn prohibits_election(prohibits: bool) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::ProhibitsElection,
            prohibits_election: Some(prohibits),
            ..Self::election(false)
        }
    }

    pub fn ignore_witness(ignore: bool) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::IgnoreWitness,
            ignore_witness: Some(ignore),
            ..Self::election(false)
        }
    }

    pub fn trigger_snapshot() -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::TriggerSnapshot,
            ..Self::election(false)
        }
    }

    pub fn snapshot_ready(snapshot_id: impl Into<String>, success: bool) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::SnapshotReady,
            snapshot_id: Some(snapshot_id.into()),
            status: Some(MatrixRaftAdminStatus { success, tips: Vec::new() }),
            ..Self::election(false)
        }
    }

    pub fn snapshot_applied(snapshot_id: impl Into<String>) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::SnapshotApplied,
            snapshot_id: Some(snapshot_id.into()),
            status: Some(MatrixRaftAdminStatus { success: true, tips: Vec::new() }),
            ..Self::election(false)
        }
    }

    pub fn set_leader_lease_valid(valid: bool) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::SetLeaderLeaseValid,
            lease_valid: Some(valid),
            ..Self::election(false)
        }
    }

    pub fn receive_leader_lease_confirmation(
        node_id: RustRaftNodeId,
        confirmation_epoch: u64,
        duration_ms: Option<u64>,
    ) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::ReceiveLeaderLeaseConfirmation,
            node_id: Some(node_id),
            lease_epoch: Some(confirmation_epoch),
            lease_duration_ms: duration_ms,
            ..Self::election(false)
        }
    }

    pub fn tick_leader_lease(elapsed_ms: u64) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::TickLeaderLease,
            elapsed_ms: Some(elapsed_ms),
            ..Self::election(false)
        }
    }

    pub fn receive_follower_lease(epoch: u64) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::ReceiveFollowerLease,
            lease_epoch: Some(epoch),
            ..Self::election(false)
        }
    }

    pub fn tick_follower_lease(elapsed_ms: u64) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::TickFollowerLease,
            elapsed_ms: Some(elapsed_ms),
            ..Self::election(false)
        }
    }

    pub fn resign() -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::Resign,
            ..Self::election(false)
        }
    }

    pub fn begin_snapshot_send(
        peer_id: RustRaftNodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: RustRaftLogIndex,
        total_chunks: u64,
    ) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::BeginSnapshotSend,
            snapshot_peer_id: Some(peer_id),
            snapshot_id: Some(snapshot_id.into()),
            snapshot_index: Some(snapshot_index),
            snapshot_total_chunks: Some(total_chunks),
            ..Self::election(false)
        }
    }

    pub fn record_snapshot_chunk_sent(peer_id: RustRaftNodeId, bytes: u64) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::RecordSnapshotChunkSent,
            snapshot_peer_id: Some(peer_id),
            snapshot_bytes: Some(bytes),
            ..Self::election(false)
        }
    }

    pub fn acknowledge_snapshot_chunk(peer_id: RustRaftNodeId) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::AcknowledgeSnapshotChunk,
            snapshot_peer_id: Some(peer_id),
            ..Self::election(false)
        }
    }

    pub fn retry_snapshot_chunk(peer_id: RustRaftNodeId) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::RetrySnapshotChunk,
            snapshot_peer_id: Some(peer_id),
            ..Self::election(false)
        }
    }

    pub fn cancel_snapshot_send(peer_id: RustRaftNodeId) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::CancelSnapshotSend,
            snapshot_peer_id: Some(peer_id),
            ..Self::election(false)
        }
    }

    pub fn begin_snapshot_install(
        peer_id: RustRaftNodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: RustRaftLogIndex,
        total_chunks: u64,
    ) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::BeginSnapshotInstall,
            snapshot_peer_id: Some(peer_id),
            snapshot_id: Some(snapshot_id.into()),
            snapshot_index: Some(snapshot_index),
            snapshot_total_chunks: Some(total_chunks),
            ..Self::election(false)
        }
    }

    pub fn receive_snapshot_chunk(peer_id: RustRaftNodeId, bytes: u64, done: bool) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::ReceiveSnapshotChunk,
            snapshot_peer_id: Some(peer_id),
            snapshot_bytes: Some(bytes),
            snapshot_done: done,
            ..Self::election(false)
        }
    }

    pub fn rollback_snapshot_install(peer_id: RustRaftNodeId) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::RollbackSnapshotInstall,
            snapshot_peer_id: Some(peer_id),
            ..Self::election(false)
        }
    }

    pub fn synced(
        first_index: Option<RustRaftLogIndex>,
        last_index: Option<RustRaftLogIndex>,
        stabled_config_change_index: RustRaftLogIndex,
    ) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::Synced,
            first_index,
            last_index,
            stabled_config_change_index: Some(stabled_config_change_index),
            ..Self::election(false)
        }
    }

    pub fn applied(
        node_id: RustRaftNodeId,
        applied_index: RustRaftLogIndex,
        rejected: bool,
    ) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::Applied,
            node_id: Some(node_id),
            applied_index: Some(applied_index),
            apply_task_rejected: rejected,
            ..Self::election(false)
        }
    }

    pub fn apply_task_inflight(
        node_id: RustRaftNodeId,
        applied_index: RustRaftLogIndex,
    ) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::ApplyTaskInflight,
            node_id: Some(node_id),
            applied_index: Some(applied_index),
            ..Self::election(false)
        }
    }

    pub fn replicated(peer_id: RustRaftNodeId, success: bool) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::Replicated,
            node_id: Some(peer_id),
            status: Some(MatrixRaftAdminStatus { success, tips: Vec::new() }),
            ..Self::election(false)
        }
    }

    pub fn compact_logs_through(log_index: RustRaftLogIndex) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::CompactLogsThrough,
            log_index: Some(log_index),
            ..Self::election(false)
        }
    }

    pub fn compact_logs_with_storage_fence(
        log_index: RustRaftLogIndex,
        fence: RustRaftStorageApplyFence,
    ) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::CompactLogsWithStorageFence,
            log_index: Some(log_index),
            storage_fence: Some(fence),
            ..Self::election(false)
        }
    }

    pub fn checkpoint_snapshot(
        node_id: RustRaftNodeId,
        snapshot_id: impl Into<String>,
    ) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::CheckpointSnapshot,
            node_id: Some(node_id),
            snapshot_id: Some(snapshot_id.into()),
            ..Self::election(false)
        }
    }

    pub fn witness_quorum(acknowledgements: impl IntoIterator<Item = RustRaftNodeId>) -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::WitnessQuorum,
            acknowledgements: acknowledgements.into_iter().collect(),
            ..Self::election(false)
        }
    }

    pub fn release_memory() -> Self {
        Self {
            command_type: MatrixRaftAdminCommandType::ReleaseMemory,
            ..Self::election(false)
        }
    }

    pub fn require_node_id(&self, purpose: &str) -> Result<RustRaftNodeId, RaftError> {
        self.node_id.ok_or_else(|| {
            RaftError::InvalidRequest(format!("{purpose} admin command missing node id"))
        })
    }

    pub fn require_log_index(&self, purpose: &str) -> Result<RustRaftLogIndex, RaftError> {
        self.log_index.ok_or_else(|| {
            RaftError::InvalidRequest(format!("{purpose} admin command missing log index"))
        })
    }

    pub fn require_lease_epoch(&self, purpose: &str) -> Result<u64, RaftError> {
        self.lease_epoch.ok_or_else(|| {
            RaftError::InvalidRequest(format!("{purpose} admin command missing lease epoch"))
        })
    }

    pub fn require_elapsed_ms(&self, purpose: &str) -> Result<u64, RaftError> {
        self.elapsed_ms.ok_or_else(|| {
            RaftError::InvalidRequest(format!("{purpose} admin command missing elapsed ms"))
        })
    }

    pub fn require_snapshot_peer_id(&self) -> Result<RustRaftNodeId, RaftError> {
        self.snapshot_peer_id.ok_or_else(|| {
            RaftError::InvalidRequest("snapshot admin command missing peer id".to_string())
        })
    }

    pub fn require_snapshot_id(&self) -> Result<RustRaftSnapshotId, RaftError> {
        self.snapshot_id.clone().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot admin command missing snapshot id".to_string())
        })
    }

    pub fn require_snapshot_index(&self) -> Result<RustRaftLogIndex, RaftError> {
        self.snapshot_index.ok_or_else(|| {
            RaftError::InvalidRequest("snapshot admin command missing snapshot index".to_string())
        })
    }

    pub fn require_snapshot_total_chunks(&self) -> Result<u64, RaftError> {
        self.snapshot_total_chunks.ok_or_else(|| {
            RaftError::InvalidRequest("snapshot admin command missing total chunks".to_string())
        })
    }

    pub fn require_snapshot_bytes(&self) -> Result<u64, RaftError> {
        self.snapshot_bytes.ok_or_else(|| {
            RaftError::InvalidRequest("snapshot admin command missing byte count".to_string())
        })
    }

    pub fn to_wire_bytes(&self) -> Result<Vec<u8>, RaftError> {
        serde_json::to_vec(self).map_err(|err| {
            RaftError::Transport(format!("failed to encode MatrixRaft admin command: {err}"))
        })
    }

    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self, RaftError> {
        serde_json::from_slice(bytes).map_err(|err| {
            RaftError::Transport(format!("failed to decode MatrixRaft admin command: {err}"))
        })
    }

    pub fn wire_size(&self) -> Result<u64, RaftError> {
        Ok(self.to_wire_bytes()?.len() as u64)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftConfigChangeType {
    AddNode,
    RemoveNode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftConfigChange {
    #[serde(default)]
    pub request_id: Option<u64>,
    pub change_type: MatrixRaftConfigChangeType,
    pub member_id: RustRaftNodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    #[serde(default)]
    pub old_members: Vec<MatrixRaftMemberId>,
    #[serde(default)]
    pub conf_state: MatrixRaftConfState,
    #[serde(default)]
    pub auto_promote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftPropose {
    #[serde(default)]
    pub request_id: Option<u64>,
    pub data: RustRaftPayload,
    #[serde(default)]
    pub context: Vec<u8>,
    #[serde(default)]
    pub is_command: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftEntryType {
    Normal,
    ConfigChange,
    Meta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftEntry {
    pub entry_type: MatrixRaftEntryType,
    pub term: RustRaftTerm,
    pub index: RustRaftLogIndex,
    #[serde(default)]
    pub propose: Option<MatrixRaftPropose>,
    #[serde(default)]
    pub config_change: Option<MatrixRaftConfigChange>,
    #[serde(default)]
    pub memberships: Vec<MatrixRaftConfigChange>,
    #[serde(default)]
    pub request_id: u64,
    #[serde(default)]
    pub bytes_size: u64,
}

impl From<&RustRaftLogEntry> for MatrixRaftEntry {
    fn from(entry: &RustRaftLogEntry) -> Self {
        let bytes_size = entry.payload.len() as u64;
        Self {
            entry_type: if entry.is_command {
                MatrixRaftEntryType::Normal
            } else {
                MatrixRaftEntryType::Meta
            },
            term: entry.log_id.term,
            index: entry.log_id.index,
            propose: Some(MatrixRaftPropose {
                request_id: None,
                data: entry.payload.clone(),
                context: Vec::new(),
                is_command: entry.is_command,
            }),
            config_change: None,
            memberships: Vec::new(),
            request_id: 0,
            bytes_size,
        }
    }
}

impl MatrixRaftEntry {
    pub fn to_log_entry(&self) -> RustRaftLogEntry {
        RustRaftLogEntry {
            log_id: RustRaftLogId {
                term: self.term,
                index: self.index,
            },
            payload: self
                .propose
                .as_ref()
                .map(|propose| propose.data.clone())
                .or_else(|| {
                    self.config_change
                        .as_ref()
                        .map(|change| format!("{change:?}").into_bytes())
                })
                .unwrap_or_default(),
            is_command: matches!(self.entry_type, MatrixRaftEntryType::Normal),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftAppendEntriesRequest {
    pub prev_term: RustRaftTerm,
    pub prev_index: RustRaftLogIndex,
    #[serde(default)]
    pub entries: Vec<MatrixRaftEntry>,
}

impl From<&AppendEntriesRequest> for MatrixRaftAppendEntriesRequest {
    fn from(request: &AppendEntriesRequest) -> Self {
        Self {
            prev_term: request
                .prev_log_id
                .as_ref()
                .map(|log_id| log_id.term)
                .unwrap_or_default(),
            prev_index: request
                .prev_log_id
                .as_ref()
                .map(|log_id| log_id.index)
                .unwrap_or_default(),
            entries: request.entries.iter().map(MatrixRaftEntry::from).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftAppendEntriesResponse {
    pub received: bool,
    #[serde(default)]
    pub matched_index: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub rejected_hint: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub rejected_index: Option<RustRaftLogIndex>,
}

impl From<&AppendEntriesResponse> for MatrixRaftAppendEntriesResponse {
    fn from(response: &AppendEntriesResponse) -> Self {
        Self {
            received: response.success,
            matched_index: response.success.then_some(response.match_index),
            rejected_hint: response.rejection_hint,
            rejected_index: response.rejected_index,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftOldSnapshotFinishState {
    Received,
    Rejected,
    NotFromLeader,
    UnpackFailed,
    Staled,
    Error,
    ChecksumError,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftOldSnapshotFinish {
    pub finish_state: MatrixRaftOldSnapshotFinishState,
    pub snapshot_index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRequireSnapshot {
    pub required_index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotProgress {
    pub remote_receiving: bool,
    pub elapsed_since_last_receiving_ms: u64,
    pub send_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLeaseRequest {
    pub epoch_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLeaseResponse {
    pub max_met_epoch_id: i64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMessage {
    pub message_type: MatrixRaftMessageType,
    #[serde(default)]
    pub from: Option<RustRaftNodeId>,
    #[serde(default)]
    pub raft_addr: Option<String>,
    #[serde(default)]
    pub snapshot_addr: Option<String>,
    #[serde(default)]
    pub to: Option<RustRaftNodeId>,
    #[serde(default)]
    pub term: Option<RustRaftTerm>,
    #[serde(default)]
    pub committed_index: Option<RustRaftLogIndex>,
    #[serde(default)]
    pub vote_request: Option<VoteRequest>,
    #[serde(default)]
    pub vote_response: Option<VoteResponse>,
    #[serde(default)]
    pub config_change: Option<MatrixRaftConfigChange>,
    #[serde(default)]
    pub membership_operation: Option<RaftMembershipOperation>,
    #[serde(default)]
    pub propose: Option<MatrixRaftPropose>,
    #[serde(default)]
    pub entry: Option<MatrixRaftEntry>,
    #[serde(default)]
    pub append_entries_request: Option<MatrixRaftAppendEntriesRequest>,
    #[serde(default)]
    pub append_entries_response: Option<MatrixRaftAppendEntriesResponse>,
    #[serde(default)]
    pub install_snapshot_request: Option<InstallSnapshotRequest>,
    #[serde(default)]
    pub install_snapshot_response: Option<InstallSnapshotResponse>,
    #[serde(default)]
    pub read_index_request: Option<ReadIndexRequest>,
    #[serde(default)]
    pub read_index_response: Option<ReadIndexResponse>,
    #[serde(default)]
    pub old_snapshot_finish: Option<MatrixRaftOldSnapshotFinish>,
    #[serde(default)]
    pub timestamp: Option<u64>,
    #[serde(default)]
    pub snapshot_state: Option<RustRaftSnapshotState>,
    #[serde(default)]
    pub snapshot: Option<MatrixRaftSnapshotDesc>,
    #[serde(default)]
    pub snapshot_progress: Option<MatrixRaftSnapshotProgress>,
    #[serde(default)]
    pub require_snapshot: Option<MatrixRaftRequireSnapshot>,
    #[serde(default)]
    pub to_conf_state: MatrixRaftConfState,
    #[serde(default)]
    pub auto_promote: bool,
    #[serde(default)]
    pub lease_request: Option<MatrixRaftLeaseRequest>,
    #[serde(default)]
    pub lease_response: Option<MatrixRaftLeaseResponse>,
    #[serde(default)]
    pub bytes_size: u64,
    #[serde(default)]
    pub command: Option<MatrixRaftAdminCommand>,
}

impl MatrixRaftMessage {
    pub fn admin(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        command: MatrixRaftAdminCommand,
    ) -> Self {
        Self {
            message_type: MatrixRaftMessageType::AdminCommand,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: Some(command),
        }
    }

    pub fn vote(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        request: VoteRequest,
        pre_vote: bool,
    ) -> Self {
        Self {
            message_type: if pre_vote {
                MatrixRaftMessageType::PreVoteRequest
            } else {
                MatrixRaftMessageType::VoteRequest
            },
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: Some(request.term),
            committed_index: None,
            vote_request: Some(request),
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn vote_response(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        response: VoteResponse,
        pre_vote: bool,
    ) -> Self {
        Self {
            message_type: if pre_vote {
                MatrixRaftMessageType::PreVoteResponse
            } else {
                MatrixRaftMessageType::VoteResponse
            },
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: Some(response.term),
            committed_index: None,
            vote_request: None,
            vote_response: Some(response),
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn pre_vote(from: RustRaftNodeId, to: RustRaftNodeId) -> Self {
        Self {
            message_type: MatrixRaftMessageType::PreVote,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn timeout_now(from: RustRaftNodeId, to: RustRaftNodeId) -> Self {
        Self {
            message_type: MatrixRaftMessageType::TimeoutNow,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn append_entries(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        request: &AppendEntriesRequest,
    ) -> Self {
        Self {
            message_type: MatrixRaftMessageType::AppendEntriesRequest,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: Some(request.term),
            committed_index: Some(request.leader_commit),
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: Some(MatrixRaftAppendEntriesRequest::from(request)),
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: request
                .entries
                .iter()
                .map(|entry| entry.payload.len() as u64)
                .sum(),
            command: None,
        }
    }

    pub fn append_entries_lease_request(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        request: &AppendEntriesRequest,
        lease_request: MatrixRaftLeaseRequest,
    ) -> Self {
        let mut message = Self::append_entries(from, to, request);
        message.lease_request = Some(lease_request);
        message
    }

    pub fn append_entries_response(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        response: &AppendEntriesResponse,
    ) -> Self {
        Self {
            message_type: MatrixRaftMessageType::AppendEntriesResponse,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: Some(response.term),
            committed_index: Some(response.match_index),
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: Some(MatrixRaftAppendEntriesResponse::from(response)),
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: Some(response.snapshot_state),
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: response
                .require_snapshot
                .map(|required_index| MatrixRaftRequireSnapshot { required_index }),
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: (response.lease_confirmation_epoch > 0
                || response.lease_duration_ms > 0)
                .then_some(MatrixRaftLeaseResponse {
                    max_met_epoch_id: response.lease_confirmation_epoch as i64,
                    duration_ms: response.lease_duration_ms,
                }),
            bytes_size: 0,
            command: None,
        }
    }

    pub fn append_entries_lease_response(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        response: &AppendEntriesResponse,
        lease_response: MatrixRaftLeaseResponse,
    ) -> Self {
        let mut message = Self::append_entries_response(from, to, response);
        message.lease_response = Some(lease_response);
        message
    }

    pub fn install_snapshot_response(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        response: InstallSnapshotResponse,
    ) -> Self {
        Self {
            message_type: MatrixRaftMessageType::InstallSnapshotResponse,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: Some(response.term),
            committed_index: Some(response.committed_index),
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: Some(response),
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn install_snapshot(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        request: InstallSnapshotRequest,
    ) -> Self {
        Self {
            message_type: MatrixRaftMessageType::InstallSnapshotRequest,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: Some(request.term),
            committed_index: Some(request.chunk.meta.last_log_id.index),
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: Some(request),
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn snapshot_progress(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        progress: MatrixRaftSnapshotProgress,
    ) -> Self {
        Self {
            message_type: MatrixRaftMessageType::SnapshotProgress,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: Some(progress),
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn read_index(from: RustRaftNodeId, to: RustRaftNodeId, request: ReadIndexRequest) -> Self {
        Self {
            message_type: MatrixRaftMessageType::ReadIndexRequest,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: None,
            committed_index: Some(request.min_commit_index),
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: Some(request),
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn catch_up_peer(from: RustRaftNodeId, peer_id: RustRaftNodeId) -> Self {
        Self {
            message_type: MatrixRaftMessageType::CatchUpPeer,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(peer_id),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn promote_peer(from: RustRaftNodeId, peer_id: RustRaftNodeId) -> Self {
        Self {
            message_type: MatrixRaftMessageType::PromotePeer,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(peer_id),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn auto_promote_learner(from: RustRaftNodeId, learner_id: RustRaftNodeId) -> Self {
        Self {
            message_type: MatrixRaftMessageType::AutoPromoteLearner,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(learner_id),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: true,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn network_error(from: RustRaftNodeId, peer_id: RustRaftNodeId) -> Self {
        Self {
            message_type: MatrixRaftMessageType::NetworkError,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(peer_id),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn propose(from: RustRaftNodeId, to: RustRaftNodeId, propose: MatrixRaftPropose) -> Self {
        let bytes_size = propose.data.len() as u64;
        Self {
            message_type: MatrixRaftMessageType::Propose,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: None,
            propose: Some(propose),
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size,
            command: None,
        }
    }

    pub fn membership_operation(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        operation: RaftMembershipOperation,
    ) -> Self {
        Self {
            message_type: MatrixRaftMessageType::MembershipOperation,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: None,
            membership_operation: Some(operation),
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn config_change(
        from: RustRaftNodeId,
        to: RustRaftNodeId,
        config_change: MatrixRaftConfigChange,
    ) -> Self {
        Self {
            message_type: MatrixRaftMessageType::ConfigChange,
            from: Some(from),
            raft_addr: None,
            snapshot_addr: None,
            to: Some(to),
            term: None,
            committed_index: None,
            vote_request: None,
            vote_response: None,
            config_change: Some(config_change),
            membership_operation: None,
            propose: None,
            entry: None,
            append_entries_request: None,
            append_entries_response: None,
            install_snapshot_request: None,
            install_snapshot_response: None,
            read_index_request: None,
            read_index_response: None,
            old_snapshot_finish: None,
            timestamp: None,
            snapshot_state: None,
            snapshot: None,
            snapshot_progress: None,
            require_snapshot: None,
            to_conf_state: MatrixRaftConfState::Voter,
            auto_promote: false,
            lease_request: None,
            lease_response: None,
            bytes_size: 0,
            command: None,
        }
    }

    pub fn to_wire_bytes(&self) -> Result<Vec<u8>, RaftError> {
        serde_json::to_vec(self).map_err(|err| {
            RaftError::Transport(format!("failed to encode MatrixRaft message: {err}"))
        })
    }

    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self, RaftError> {
        serde_json::from_slice(bytes).map_err(|err| {
            RaftError::Transport(format!("failed to decode MatrixRaft message: {err}"))
        })
    }

    pub fn wire_size(&self) -> Result<u64, RaftError> {
        Ok(self.to_wire_bytes()?.len() as u64)
    }

    pub fn with_wire_size(mut self) -> Result<Self, RaftError> {
        for _ in 0..8 {
            let size = self.wire_size()?;
            if self.bytes_size == size {
                return Ok(self);
            }
            self.bytes_size = size;
        }
        self.bytes_size = self.wire_size()?;
        Ok(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftVoteRequest {
    pub group_id: RustRaftGroupId,
    pub term: RustRaftTerm,
    pub candidate_id: RustRaftNodeId,
    pub last_log_id: Option<RustRaftLogId>,
    pub pre_vote: bool,
    #[serde(default)]
    pub force: bool,
}

pub type VoteRequest = RustRaftVoteRequest;
pub type PreVoteRequest = RustRaftVoteRequest;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftVoteResponse {
    pub term: RustRaftTerm,
    pub vote_granted: bool,
    pub reason: String,
}

pub type VoteResponse = RustRaftVoteResponse;
pub type PreVoteResponse = RustRaftVoteResponse;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftTimeoutNowResponse {
    pub node_id: RustRaftNodeId,
    pub from: RustRaftNodeId,
    pub campaigned: bool,
    pub term: RustRaftTerm,
    pub leader_id: Option<RustRaftNodeId>,
    pub reason: String,
}

pub type TimeoutNowResponse = RustRaftTimeoutNowResponse;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RustRaftAdminCommand {
    Campaign {
        candidate_id: RustRaftNodeId,
        forced: bool,
    },
    TransferLeader {
        target: RustRaftNodeId,
    },
    CompleteLeaderTransfer,
    AbortLeaderTransfer {
        reason: String,
    },
    FireFatalEvent {
        node_id: RustRaftNodeId,
        reason: String,
    },
    StepDown {
        transferee: Option<RustRaftNodeId>,
    },
    Resign {
        reason: String,
    },
    TriggerSnapshot,
    SnapshotReady {
        snapshot_id: RustRaftSnapshotId,
        success: bool,
    },
    SnapshotApplied {
        snapshot_id: RustRaftSnapshotId,
    },
    BeginSnapshotSend {
        peer_id: RustRaftNodeId,
        snapshot_id: RustRaftSnapshotId,
        snapshot_index: RustRaftLogIndex,
        total_chunks: u64,
    },
    RecordSnapshotChunkSent {
        peer_id: RustRaftNodeId,
        bytes: u64,
    },
    AcknowledgeSnapshotChunk {
        peer_id: RustRaftNodeId,
    },
    RetrySnapshotChunk {
        peer_id: RustRaftNodeId,
    },
    CancelSnapshotSend {
        peer_id: RustRaftNodeId,
    },
    BeginSnapshotInstall {
        peer_id: RustRaftNodeId,
        snapshot_id: RustRaftSnapshotId,
        snapshot_index: RustRaftLogIndex,
        total_chunks: u64,
    },
    ReceiveSnapshotChunk {
        peer_id: RustRaftNodeId,
        bytes: u64,
        done: bool,
    },
    RollbackSnapshotInstall {
        peer_id: RustRaftNodeId,
    },
    ApplyResult {
        node_id: RustRaftNodeId,
        applied_index: RustRaftLogIndex,
        rejected: bool,
    },
    ApplyTaskInflight {
        node_id: RustRaftNodeId,
        applied_index: RustRaftLogIndex,
    },
    StabledResult {
        first_index: Option<RustRaftLogIndex>,
        last_index: Option<RustRaftLogIndex>,
        stabled_membership_change_index: RustRaftLogIndex,
    },
    Replicated {
        peer_id: RustRaftNodeId,
        success: bool,
    },
    CompactLogsThrough {
        log_index: RustRaftLogIndex,
    },
    CompactLogsWithStorageFence {
        log_index: RustRaftLogIndex,
        fence: RustRaftStorageApplyFence,
    },
    CheckpointSnapshot {
        target: RustRaftNodeId,
        snapshot_id: RustRaftSnapshotId,
    },
    WitnessQuorum {
        acknowledgements: Vec<RustRaftNodeId>,
    },
    PartitionPeer {
        peer_id: RustRaftNodeId,
    },
    HealPeer {
        peer_id: RustRaftNodeId,
    },
    ReceiveOutOfOrderAppend {
        peer_id: RustRaftNodeId,
        entry: RustRaftLogEntry,
    },
    ExpirePeerReorderQueue {
        peer_id: RustRaftNodeId,
    },
    SetNodeHealthy {
        node_id: RustRaftNodeId,
        healthy: bool,
    },
    SetLeaderLeaseValid {
        valid: bool,
    },
    ReceiveLeaderLeaseConfirmation {
        node_id: RustRaftNodeId,
        confirmation_epoch: u64,
        duration_ms: Option<u64>,
    },
    TickLeaderLease {
        elapsed_ms: u64,
    },
    ReceiveFollowerLease {
        epoch: u64,
    },
    TickFollowerLease {
        elapsed_ms: u64,
    },
    ProhibitsElection {
        prohibits: bool,
    },
    IgnoreWitness {
        ignore: bool,
    },
    ReleaseMemory,
}

pub type RaftAdminCommand = RustRaftAdminCommand;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RustRaftMessage {
    Admin {
        command: RustRaftAdminCommand,
    },
    Propose {
        payload: RustRaftPayload,
        options: RustRaftProposeOptions,
    },
    Membership {
        operation: RaftMembershipOperation,
    },
    AutoPromoteLearner {
        learner_id: RustRaftNodeId,
    },
    CatchUpPeer {
        peer_id: RustRaftNodeId,
    },
    PreVote {
        candidate_id: RustRaftNodeId,
    },
    AppendEntries {
        target: RustRaftNodeId,
        request: AppendEntriesRequest,
    },
    AppendEntriesResponse {
        local_node_id: RustRaftNodeId,
        peer_id: RustRaftNodeId,
        response: AppendEntriesResponse,
    },
    Vote {
        target: RustRaftNodeId,
        request: VoteRequest,
    },
    VoteResponse {
        local_node_id: RustRaftNodeId,
        peer_id: Option<RustRaftNodeId>,
        response: VoteResponse,
        pre_vote: bool,
    },
    InstallSnapshot {
        target: RustRaftNodeId,
        request: InstallSnapshotRequest,
    },
    InstallSnapshotResponse {
        local_node_id: RustRaftNodeId,
        peer_id: RustRaftNodeId,
        response: InstallSnapshotResponse,
    },
    NetworkError {
        peer_id: RustRaftNodeId,
    },
    SnapshotFinish {
        peer_id: RustRaftNodeId,
        accepted: bool,
        committed_index: RustRaftLogIndex,
    },
    SnapshotProgress {
        peer_id: RustRaftNodeId,
        remote_receiving: bool,
        elapsed_since_last_receiving_ms: u64,
        send_timeout_ms: u64,
    },
    ReadIndex {
        request: ReadIndexRequest,
    },
    TimeoutNow {
        from: RustRaftNodeId,
        target: RustRaftNodeId,
    },
}

pub type RaftMessage = RustRaftMessage;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
// Value-type step-result contract: boxing the largest variant would change the
// public enum's shape for every consumer, so the size trade-off is accepted here.
#[allow(clippy::large_enum_variant)]
pub enum RustRaftStepResult {
    Handled,
    Proposed(RustRaftLogId),
    Membership(RaftMembershipExecutionReport),
    AutoPromoteLearner(RaftLearnerAutoPromoteReport),
    CatchUpPeer(RaftLearnerCatchUpLoopReport),
    PreVote(VoteResponse),
    SnapshotTriggered(RustRaftSnapshotMeta),
    ReleasedMemory(bool),
    CompactedLogs(u64),
    FencedCompaction(RaftWalCompactionReport),
    CheckpointedSnapshot(RaftSnapshot),
    LeaderTransferCompleted(bool),
    LeaderTransferAborted(bool),
    FatalEvent(Option<RustRaftNodeId>),
    LeaderResigned(bool),
    LeaderLeaseConfirmed(bool),
    LeaderLeaseExpired(bool),
    FollowerLeaseReceived(bool),
    FollowerLeaseExpired(bool),
    StepDown(Option<RustRaftNodeId>),
    WitnessQuorum(RaftWitnessQuorumReport),
    AppendEntries(AppendEntriesResponse),
    Vote(VoteResponse),
    InstallSnapshot(InstallSnapshotResponse),
    ReadIndex(ReadIndexResponse),
    TimeoutNow(TimeoutNowResponse),
}

pub type RaftStepResult = RustRaftStepResult;

