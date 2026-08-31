// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// MatrixRaft-compatible public facade.
// This layer keeps MatrixRaft's native API intact while exposing the function
// names and option shapes expected by MatrixRaft-style embedders.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftNodeId {
    pub peer_id: NodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
}

impl From<&Peer> for MatrixRaftNodeId {
    fn from(peer: &Peer) -> Self {
        Self {
            peer_id: peer.node_id,
            raft_addr: peer.raft_addr.clone(),
            snapshot_addr: peer.snapshot_addr.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[derive(Default)]
pub struct MatrixRaftProposeOptions {
    pub with_term: Option<Term>,
    pub is_command: bool,
}


impl From<MatrixRaftProposeOptions> for ProposeOptions {
    fn from(options: MatrixRaftProposeOptions) -> Self {
        Self {
            expected_term: options.with_term,
            is_command: options.is_command,
            is_membership_change: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftReadIndexMode {
    LeaseRead,
    QuorumRead,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftReadIndexOptions {
    pub min_commit_index: LogIndex,
    pub mode: MatrixRaftReadIndexMode,
}

impl MatrixRaftReadIndexOptions {
    pub fn lease_read(min_commit_index: LogIndex) -> Self {
        Self {
            min_commit_index,
            mode: MatrixRaftReadIndexMode::LeaseRead,
        }
    }

    pub fn quorum_read(min_commit_index: LogIndex) -> Self {
        Self {
            min_commit_index,
            mode: MatrixRaftReadIndexMode::QuorumRead,
        }
    }

    pub fn allow_lease_read(self) -> bool {
        self.mode == MatrixRaftReadIndexMode::LeaseRead
    }

    pub fn into_request(
        self,
        group_id: GroupId,
        requester_id: NodeId,
    ) -> ReadIndexRequest {
        ReadIndexRequest {
            group_id,
            requester_id,
            min_commit_index: self.min_commit_index,
            allow_lease_read: self.allow_lease_read(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftBoundedStaleReadOptions {
    pub min_commit_index: LogIndex,
    pub max_stale_index_lag: LogIndex,
}

impl MatrixRaftBoundedStaleReadOptions {
    pub fn new(
        min_commit_index: LogIndex,
        max_stale_index_lag: LogIndex,
    ) -> Self {
        Self {
            min_commit_index,
            max_stale_index_lag,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftAttribute {
    ProhibitsElection,
    IgnoreWitness,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftStatus {
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub role: StateRole,
    pub term: Term,
    pub leader_id: Option<NodeId>,
    pub leader_lease_valid: bool,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub last_log_index: LogIndex,
    pub membership: Membership,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLocalStatus {
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub state: NodeRuntimeState,
    pub restart_count: u64,
    pub worker_running: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftFatalEvent {
    pub node_id: Option<NodeId>,
    pub reason: String,
    pub raw_id: String,
}

impl From<Blocker> for MatrixRaftFatalEvent {
    fn from(blocker: Blocker) -> Self {
        let mut parts = blocker.id.splitn(3, ':');
        let event_type = parts.next();
        let node_id = parts
            .next()
            .and_then(|node| node.parse::<NodeId>().ok());
        let reason = parts.next().unwrap_or(&blocker.id).to_string();
        if event_type == Some("fatal_event") {
            Self {
                node_id,
                reason,
                raw_id: blocker.id,
            }
        } else {
            Self {
                node_id: None,
                reason: blocker.detail,
                raw_id: blocker.id,
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftPromoteReport {
    pub learner_id: NodeId,
    pub catch_up: LearnerCatchUpLoopReport,
    pub membership: MembershipExecutionReport,
    pub promoted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRemoveReport {
    pub removed_id: NodeId,
    #[serde(default)]
    pub removed_node: Option<MatrixRaftNodeId>,
    #[serde(default)]
    pub removed_conf_state: Option<MatrixRaftConfState>,
    pub membership: MembershipExecutionReport,
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftTransferLeaderReport {
    pub transferee_id: NodeId,
    #[serde(default)]
    pub transferee_node: Option<MatrixRaftNodeId>,
    #[serde(default)]
    pub state: Option<LeaderTransferState>,
    /// True only when leadership actually moved. `state` cannot substitute for
    /// this: it is `None` both for an ignored request and for a completed one.
    pub transferred: bool,
    /// Which of the three things the request did. `transferred` is
    /// `outcome == Transferred`; this says which of the two non-transfers it
    /// was when it is false.
    #[serde(default = "default_leader_transfer_outcome")]
    pub outcome: crate::LeaderTransferOutcome,
}

/// Older payloads predate `outcome` and only ever carried `transferred: true`,
/// which by then meant "the call returned Ok". `Ignored` is the honest default
/// for a field that was not recorded.
fn default_leader_transfer_outcome() -> crate::LeaderTransferOutcome {
    crate::LeaderTransferOutcome::Ignored
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftStepDownReport {
    #[serde(default)]
    pub requested_transferee_id: Option<NodeId>,
    #[serde(default)]
    pub transferee_id: Option<NodeId>,
    #[serde(default)]
    pub transferee_node: Option<MatrixRaftNodeId>,
    #[serde(default)]
    pub state: Option<LeaderTransferState>,
    pub stepped_down: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftResignReport {
    pub reason: String,
    #[serde(default)]
    pub leader_before: Option<NodeId>,
    #[serde(default)]
    pub leader_after: Option<NodeId>,
    pub resigned: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftAsyncOperation {
    Propose,
    AddNode,
    AddLearner,
    AddWitness,
    Promote,
    RemoveNode,
    ReadIndex,
    Campaign,
    ForcedCampaign,
    TransferLeader,
    TimeoutNow,
    StepDown,
    ResignLeader,
    AsyncSnapshot,
    AutoPromoteLearner,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftAsyncResultStatus {
    Ok,
    Error,
    TimedOut,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftAsyncResult {
    pub operation: MatrixRaftAsyncOperation,
    pub ok: bool,
    pub timed_out: bool,
    pub timeout_ms: u64,
    #[serde(default)]
    pub node_id: Option<NodeId>,
    #[serde(default)]
    pub request_id: Option<u64>,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub log_id: Option<LogId>,
    #[serde(default)]
    pub read_index: Option<ReadIndexResponse>,
    #[serde(default)]
    pub membership: Option<MembershipExecutionReport>,
    #[serde(default)]
    pub snapshot: Option<SnapshotMetadata>,
    #[serde(default)]
    pub auto_promote: Option<LearnerAutoPromoteReport>,
    #[serde(default)]
    pub remove: Option<MatrixRaftRemoveReport>,
    #[serde(default)]
    pub transfer_leader: Option<MatrixRaftTransferLeaderReport>,
    #[serde(default)]
    pub timeout_now: Option<TimeoutNowResponse>,
    #[serde(default)]
    pub step_down: Option<MatrixRaftStepDownReport>,
    #[serde(default)]
    pub resign: Option<MatrixRaftResignReport>,
}

impl MatrixRaftAsyncResult {
    pub fn ok(operation: MatrixRaftAsyncOperation, timeout_ms: u64) -> Self {
        Self {
            operation,
            ok: true,
            timed_out: false,
            timeout_ms,
            node_id: None,
            request_id: None,
            deadline_ms: None,
            error: None,
            log_id: None,
            read_index: None,
            membership: None,
            snapshot: None,
            auto_promote: None,
            remove: None,
            transfer_leader: None,
            timeout_now: None,
            step_down: None,
            resign: None,
        }
    }

    pub fn error(
        operation: MatrixRaftAsyncOperation,
        timeout_ms: u64,
        error: impl ToString,
    ) -> Self {
        Self {
            operation,
            ok: false,
            timed_out: false,
            timeout_ms,
            node_id: None,
            request_id: None,
            deadline_ms: None,
            error: Some(error.to_string()),
            log_id: None,
            read_index: None,
            membership: None,
            snapshot: None,
            auto_promote: None,
            remove: None,
            transfer_leader: None,
            timeout_now: None,
            step_down: None,
            resign: None,
        }
    }

    pub fn timeout(operation: MatrixRaftAsyncOperation, timeout_ms: u64) -> Self {
        Self {
            operation,
            ok: false,
            timed_out: true,
            timeout_ms,
            node_id: None,
            request_id: None,
            deadline_ms: None,
            error: Some("matrixraft operation timed out".to_string()),
            log_id: None,
            read_index: None,
            membership: None,
            snapshot: None,
            auto_promote: None,
            remove: None,
            transfer_leader: None,
            timeout_now: None,
            step_down: None,
            resign: None,
        }
    }

    pub fn with_timer_task(mut self, task: &TimerTask, operation: MatrixRaftAsyncOperation) -> Self {
        self.operation = operation;
        self.node_id = Some(task.node_id);
        self.request_id = Some(task.request_id);
        self.deadline_ms = (task.deadline_ms != 0).then_some(task.deadline_ms);
        self.timeout_ms = task.deadline_ms.saturating_sub(task.start_at_ms);
        self
    }

    pub fn status(&self) -> MatrixRaftAsyncResultStatus {
        if self.timed_out {
            MatrixRaftAsyncResultStatus::TimedOut
        } else if self.ok {
            MatrixRaftAsyncResultStatus::Ok
        } else {
            MatrixRaftAsyncResultStatus::Error
        }
    }

    pub fn is_ok(&self) -> bool {
        self.status() == MatrixRaftAsyncResultStatus::Ok
    }

    pub fn is_error(&self) -> bool {
        self.status() == MatrixRaftAsyncResultStatus::Error
    }

    pub fn is_timed_out(&self) -> bool {
        self.status() == MatrixRaftAsyncResultStatus::TimedOut
    }

    pub fn has_node_id(&self) -> bool {
        self.node_id.is_some()
    }

    pub fn has_request_id(&self) -> bool {
        self.request_id.is_some()
    }

    pub fn has_deadline(&self) -> bool {
        self.deadline_ms.is_some()
    }

    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    pub fn has_log_id(&self) -> bool {
        self.log_id.is_some()
    }

    pub fn read_index_presence(&self) -> bool {
        self.read_index.is_some()
    }

    pub fn membership_presence(&self) -> bool {
        self.membership.is_some()
    }

    pub fn snapshot_presence(&self) -> bool {
        self.snapshot.is_some()
    }

    pub fn auto_promote_presence(&self) -> bool {
        self.auto_promote.is_some()
    }

    pub fn remove_presence(&self) -> bool {
        self.remove.is_some()
    }

    pub fn transfer_leader_presence(&self) -> bool {
        self.transfer_leader.is_some()
    }

    pub fn timeout_now_presence(&self) -> bool {
        self.timeout_now.is_some()
    }

    pub fn step_down_presence(&self) -> bool {
        self.step_down.is_some()
    }

    pub fn resign_presence(&self) -> bool {
        self.resign.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftAsyncGroupSummary {
    pub group_id: GroupId,
    pub result_count: usize,
    pub ok_count: usize,
    pub error_count: usize,
    pub timed_out_count: usize,
    pub node_ids: Vec<Option<NodeId>>,
    pub ok_node_ids: Vec<Option<NodeId>>,
    pub error_node_ids: Vec<Option<NodeId>>,
    pub timed_out_node_ids: Vec<Option<NodeId>>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub ok_route_keys: Vec<MatrixRaftRouteKey>,
    pub error_route_keys: Vec<MatrixRaftRouteKey>,
    pub timed_out_route_keys: Vec<MatrixRaftRouteKey>,
    pub operations: Vec<MatrixRaftAsyncOperation>,
    pub ok_operations: Vec<MatrixRaftAsyncOperation>,
    pub error_operations: Vec<MatrixRaftAsyncOperation>,
    pub timed_out_operations: Vec<MatrixRaftAsyncOperation>,
    pub counts_by_operation: Vec<(MatrixRaftAsyncOperation, usize, usize, usize, usize)>,
    pub request_ids: Vec<Option<u64>>,
    pub ok_request_ids: Vec<Option<u64>>,
    pub error_request_ids: Vec<Option<u64>>,
    pub timed_out_request_ids: Vec<Option<u64>>,
    pub deadline_ms: Vec<Option<u64>>,
    pub timeout_ms: Vec<u64>,
    pub proposed_log_ids: Vec<Option<LogId>>,
    pub read_index_present: Vec<bool>,
    pub request_ids_by_route_key: Vec<(MatrixRaftRouteKey, Option<u64>)>,
    pub deadlines_by_route_key: Vec<(MatrixRaftRouteKey, Option<u64>)>,
    pub timeouts_by_route_key: Vec<(MatrixRaftRouteKey, u64)>,
    pub log_ids_by_route_key: Vec<(MatrixRaftRouteKey, Option<LogId>)>,
    pub read_index_presence_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    #[serde(default)]
    pub read_index_responses_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)>,
    #[serde(default)]
    pub read_indices_by_route_key: Vec<(MatrixRaftRouteKey, Option<LogIndex>)>,
    #[serde(default)]
    pub read_index_safe_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub read_index_lease_read_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub read_index_reasons_by_route_key: Vec<(MatrixRaftRouteKey, Option<String>)>,
    #[serde(default)]
    pub membership_presence_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    #[serde(default)]
    pub snapshot_presence_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    #[serde(default)]
    pub auto_promote_presence_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    #[serde(default)]
    pub remove_presence_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    #[serde(default)]
    pub transfer_leader_presence_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    #[serde(default)]
    pub timeout_now_presence_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    #[serde(default)]
    pub step_down_presence_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    #[serde(default)]
    pub resign_presence_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    #[serde(default)]
    pub removed_ids_by_route_key: Vec<(MatrixRaftRouteKey, Option<NodeId>)>,
    #[serde(default)]
    pub removed_conf_states_by_route_key: Vec<(MatrixRaftRouteKey, Option<MatrixRaftConfState>)>,
    #[serde(default)]
    pub removed_values_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub remove_membership_success_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub membership_success_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub membership_reasons_by_route_key: Vec<(MatrixRaftRouteKey, Option<String>)>,
    #[serde(default)]
    pub snapshot_ids_by_route_key: Vec<(MatrixRaftRouteKey, Option<SnapshotId>)>,
    #[serde(default)]
    pub snapshot_indices_by_route_key: Vec<(MatrixRaftRouteKey, Option<LogIndex>)>,
    #[serde(default)]
    pub auto_promote_learner_ids_by_route_key: Vec<(MatrixRaftRouteKey, Option<NodeId>)>,
    #[serde(default)]
    pub auto_promote_enabled_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub auto_promote_promoted_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub transfer_leader_transferee_ids_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<NodeId>)>,
    #[serde(default)]
    pub transfer_leader_transferred_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub timeout_now_responses_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)>,
    #[serde(default)]
    pub timeout_now_node_ids_by_route_key: Vec<(MatrixRaftRouteKey, Option<NodeId>)>,
    #[serde(default)]
    pub timeout_now_from_ids_by_route_key: Vec<(MatrixRaftRouteKey, Option<NodeId>)>,
    #[serde(default)]
    pub timeout_now_campaigned_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub timeout_now_terms_by_route_key: Vec<(MatrixRaftRouteKey, Option<Term>)>,
    #[serde(default)]
    pub timeout_now_leader_ids_by_route_key: Vec<(MatrixRaftRouteKey, Option<NodeId>)>,
    #[serde(default)]
    pub timeout_now_reasons_by_route_key: Vec<(MatrixRaftRouteKey, Option<String>)>,
    #[serde(default)]
    pub step_down_requested_transferee_ids_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<NodeId>)>,
    #[serde(default)]
    pub step_down_transferee_ids_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<NodeId>)>,
    #[serde(default)]
    pub step_down_stepped_down_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub resign_reasons_by_route_key: Vec<(MatrixRaftRouteKey, Option<String>)>,
    #[serde(default)]
    pub resign_resigned_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub operations_by_route_key: Vec<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)>,
    pub statuses_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    pub errors_by_route_key: Vec<(MatrixRaftRouteKey, Option<String>)>,
}

impl MatrixRaftAsyncGroupSummary {
    pub fn from_results(
        group_id: GroupId,
        results: &[(MatrixRaftRouteKey, MatrixRaftAsyncResult)],
    ) -> Self {
        let mut node_ids = Vec::with_capacity(results.len());
        let mut ok_node_ids = Vec::new();
        let mut error_node_ids = Vec::new();
        let mut timed_out_node_ids = Vec::new();
        let mut route_keys = Vec::with_capacity(results.len());
        let mut ok_route_keys = Vec::new();
        let mut error_route_keys = Vec::new();
        let mut timed_out_route_keys = Vec::new();
        let mut operations = Vec::new();
        let mut ok_operations = Vec::new();
        let mut error_operations = Vec::new();
        let mut timed_out_operations = Vec::new();
        let mut counts_by_operation =
            Vec::<(MatrixRaftAsyncOperation, usize, usize, usize, usize)>::new();
        let mut request_ids = Vec::with_capacity(results.len());
        let mut ok_request_ids = Vec::new();
        let mut error_request_ids = Vec::new();
        let mut timed_out_request_ids = Vec::new();
        let mut deadline_ms = Vec::with_capacity(results.len());
        let mut timeout_ms = Vec::with_capacity(results.len());
        let mut proposed_log_ids = Vec::with_capacity(results.len());
        let mut read_index_present = Vec::with_capacity(results.len());
        let mut request_ids_by_route_key = Vec::with_capacity(results.len());
        let mut deadlines_by_route_key = Vec::with_capacity(results.len());
        let mut timeouts_by_route_key = Vec::with_capacity(results.len());
        let mut log_ids_by_route_key = Vec::with_capacity(results.len());
        let mut read_index_presence_by_route_key = Vec::with_capacity(results.len());
        let mut read_index_responses_by_route_key = Vec::with_capacity(results.len());
        let mut read_indices_by_route_key = Vec::with_capacity(results.len());
        let mut read_index_safe_by_route_key = Vec::with_capacity(results.len());
        let mut read_index_lease_read_by_route_key = Vec::with_capacity(results.len());
        let mut read_index_reasons_by_route_key = Vec::with_capacity(results.len());
        let mut membership_presence_by_route_key = Vec::with_capacity(results.len());
        let mut snapshot_presence_by_route_key = Vec::with_capacity(results.len());
        let mut auto_promote_presence_by_route_key = Vec::with_capacity(results.len());
        let mut remove_presence_by_route_key = Vec::with_capacity(results.len());
        let mut transfer_leader_presence_by_route_key = Vec::with_capacity(results.len());
        let mut timeout_now_presence_by_route_key = Vec::with_capacity(results.len());
        let mut step_down_presence_by_route_key = Vec::with_capacity(results.len());
        let mut resign_presence_by_route_key = Vec::with_capacity(results.len());
        let mut removed_ids_by_route_key = Vec::with_capacity(results.len());
        let mut removed_conf_states_by_route_key = Vec::with_capacity(results.len());
        let mut removed_values_by_route_key = Vec::with_capacity(results.len());
        let mut remove_membership_success_by_route_key = Vec::with_capacity(results.len());
        let mut membership_success_by_route_key = Vec::with_capacity(results.len());
        let mut membership_reasons_by_route_key = Vec::with_capacity(results.len());
        let mut snapshot_ids_by_route_key = Vec::with_capacity(results.len());
        let mut snapshot_indices_by_route_key = Vec::with_capacity(results.len());
        let mut auto_promote_learner_ids_by_route_key = Vec::with_capacity(results.len());
        let mut auto_promote_enabled_by_route_key = Vec::with_capacity(results.len());
        let mut auto_promote_promoted_by_route_key = Vec::with_capacity(results.len());
        let mut transfer_leader_transferee_ids_by_route_key = Vec::with_capacity(results.len());
        let mut transfer_leader_transferred_by_route_key = Vec::with_capacity(results.len());
        let mut timeout_now_responses_by_route_key = Vec::with_capacity(results.len());
        let mut timeout_now_node_ids_by_route_key = Vec::with_capacity(results.len());
        let mut timeout_now_from_ids_by_route_key = Vec::with_capacity(results.len());
        let mut timeout_now_campaigned_by_route_key = Vec::with_capacity(results.len());
        let mut timeout_now_terms_by_route_key = Vec::with_capacity(results.len());
        let mut timeout_now_leader_ids_by_route_key = Vec::with_capacity(results.len());
        let mut timeout_now_reasons_by_route_key = Vec::with_capacity(results.len());
        let mut step_down_requested_transferee_ids_by_route_key = Vec::with_capacity(results.len());
        let mut step_down_transferee_ids_by_route_key = Vec::with_capacity(results.len());
        let mut step_down_stepped_down_by_route_key = Vec::with_capacity(results.len());
        let mut resign_reasons_by_route_key = Vec::with_capacity(results.len());
        let mut resign_resigned_by_route_key = Vec::with_capacity(results.len());
        let mut operations_by_route_key = Vec::with_capacity(results.len());
        let mut statuses_by_route_key = Vec::with_capacity(results.len());
        let mut errors_by_route_key = Vec::with_capacity(results.len());

        for (key, result) in results {
            node_ids.push(result.node_id);
            route_keys.push(*key);
            request_ids.push(result.request_id);
            deadline_ms.push(result.deadline_ms);
            timeout_ms.push(result.timeout_ms);
            proposed_log_ids.push(result.log_id.clone());
            read_index_present.push(result.read_index.is_some());
            if result.ok {
                ok_node_ids.push(result.node_id);
                ok_route_keys.push(*key);
                ok_request_ids.push(result.request_id);
                if !ok_operations.contains(&result.operation) {
                    ok_operations.push(result.operation);
                }
            } else {
                error_node_ids.push(result.node_id);
                error_route_keys.push(*key);
                error_request_ids.push(result.request_id);
                if !error_operations.contains(&result.operation) {
                    error_operations.push(result.operation);
                }
            }
            if result.timed_out {
                timed_out_node_ids.push(result.node_id);
                timed_out_route_keys.push(*key);
                timed_out_request_ids.push(result.request_id);
                if !timed_out_operations.contains(&result.operation) {
                    timed_out_operations.push(result.operation);
                }
            }
            if !operations.contains(&result.operation) {
                operations.push(result.operation);
            }
            if let Some((_, total, ok, error, timed_out)) = counts_by_operation
                .iter_mut()
                .find(|(operation, _, _, _, _)| *operation == result.operation)
            {
                *total += 1;
                if result.ok {
                    *ok += 1;
                } else {
                    *error += 1;
                }
                if result.timed_out {
                    *timed_out += 1;
                }
            } else {
                counts_by_operation.push((
                    result.operation,
                    1,
                    usize::from(result.ok),
                    usize::from(!result.ok),
                    usize::from(result.timed_out),
                ));
            }
            request_ids_by_route_key.push((*key, result.request_id));
            deadlines_by_route_key.push((*key, result.deadline_ms));
            timeouts_by_route_key.push((*key, result.timeout_ms));
            log_ids_by_route_key.push((*key, result.log_id.clone()));
            read_index_presence_by_route_key.push((*key, result.read_index.is_some()));
            read_index_responses_by_route_key.push((*key, result.read_index.clone()));
            read_indices_by_route_key.push((
                *key,
                result.read_index.as_ref().map(|response| response.read_index),
            ));
            read_index_safe_by_route_key.push((
                *key,
                result.read_index.as_ref().map(|response| response.safe),
            ));
            read_index_lease_read_by_route_key.push((
                *key,
                result
                    .read_index
                    .as_ref()
                    .map(|response| response.lease_read),
            ));
            read_index_reasons_by_route_key.push((
                *key,
                result
                    .read_index
                    .as_ref()
                    .map(|response| response.reason.clone()),
            ));
            membership_presence_by_route_key.push((*key, result.membership.is_some()));
            snapshot_presence_by_route_key.push((*key, result.snapshot.is_some()));
            auto_promote_presence_by_route_key.push((*key, result.auto_promote.is_some()));
            remove_presence_by_route_key.push((*key, result.remove.is_some()));
            transfer_leader_presence_by_route_key.push((*key, result.transfer_leader.is_some()));
            timeout_now_presence_by_route_key.push((*key, result.timeout_now.is_some()));
            step_down_presence_by_route_key.push((*key, result.step_down.is_some()));
            resign_presence_by_route_key.push((*key, result.resign.is_some()));
            removed_ids_by_route_key.push((*key, result.remove.as_ref().map(|report| report.removed_id)));
            removed_conf_states_by_route_key.push((
                *key,
                result
                    .remove
                    .as_ref()
                    .and_then(|report| report.removed_conf_state),
            ));
            removed_values_by_route_key.push((*key, result.remove.as_ref().map(|report| report.removed)));
            remove_membership_success_by_route_key.push((
                *key,
                result
                    .remove
                    .as_ref()
                    .map(|report| report.membership.success),
            ));
            membership_success_by_route_key.push((
                *key,
                result.membership.as_ref().map(|report| report.success),
            ));
            membership_reasons_by_route_key.push((
                *key,
                result
                    .membership
                    .as_ref()
                    .map(|report| report.reason.clone()),
            ));
            snapshot_ids_by_route_key.push((
                *key,
                result
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.snapshot_id.clone()),
            ));
            snapshot_indices_by_route_key.push((
                *key,
                result
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.last_log_id.index),
            ));
            auto_promote_learner_ids_by_route_key.push((
                *key,
                result
                    .auto_promote
                    .as_ref()
                    .map(|report| report.learner_id),
            ));
            auto_promote_enabled_by_route_key.push((
                *key,
                result
                    .auto_promote
                    .as_ref()
                    .map(|report| report.auto_promote),
            ));
            auto_promote_promoted_by_route_key.push((
                *key,
                result
                    .auto_promote
                    .as_ref()
                    .map(|report| report.promoted),
            ));
            transfer_leader_transferee_ids_by_route_key.push((
                *key,
                result
                    .transfer_leader
                    .as_ref()
                    .map(|report| report.transferee_id),
            ));
            transfer_leader_transferred_by_route_key.push((
                *key,
                result
                    .transfer_leader
                    .as_ref()
                    .map(|report| report.transferred),
            ));
            timeout_now_responses_by_route_key.push((*key, result.timeout_now.clone()));
            timeout_now_node_ids_by_route_key.push((
                *key,
                result
                    .timeout_now
                    .as_ref()
                    .map(|response| response.node_id),
            ));
            timeout_now_from_ids_by_route_key.push((
                *key,
                result.timeout_now.as_ref().map(|response| response.from),
            ));
            timeout_now_campaigned_by_route_key.push((
                *key,
                result
                    .timeout_now
                    .as_ref()
                    .map(|response| response.campaigned),
            ));
            timeout_now_terms_by_route_key.push((
                *key,
                result.timeout_now.as_ref().map(|response| response.term),
            ));
            timeout_now_leader_ids_by_route_key.push((
                *key,
                result
                    .timeout_now
                    .as_ref()
                    .and_then(|response| response.leader_id),
            ));
            timeout_now_reasons_by_route_key.push((
                *key,
                result
                    .timeout_now
                    .as_ref()
                    .map(|response| response.reason.clone()),
            ));
            step_down_requested_transferee_ids_by_route_key.push((
                *key,
                result
                    .step_down
                    .as_ref()
                    .and_then(|report| report.requested_transferee_id),
            ));
            step_down_transferee_ids_by_route_key.push((
                *key,
                result
                    .step_down
                    .as_ref()
                    .and_then(|report| report.transferee_id),
            ));
            step_down_stepped_down_by_route_key.push((
                *key,
                result.step_down.as_ref().map(|report| report.stepped_down),
            ));
            resign_reasons_by_route_key.push((
                *key,
                result.resign.as_ref().map(|report| report.reason.clone()),
            ));
            resign_resigned_by_route_key.push((
                *key,
                result.resign.as_ref().map(|report| report.resigned),
            ));
            operations_by_route_key.push((*key, result.operation));
            statuses_by_route_key.push((*key, result.ok));
            errors_by_route_key.push((*key, result.error.clone()));
        }

        let ok_count = ok_route_keys.len();
        let timed_out_count = timed_out_route_keys.len();
        Self {
            group_id,
            result_count: results.len(),
            ok_count,
            error_count: results.len().saturating_sub(ok_count),
            timed_out_count,
            node_ids,
            ok_node_ids,
            error_node_ids,
            timed_out_node_ids,
            route_keys,
            ok_route_keys,
            error_route_keys,
            timed_out_route_keys,
            operations,
            ok_operations,
            error_operations,
            timed_out_operations,
            counts_by_operation,
            request_ids,
            ok_request_ids,
            error_request_ids,
            timed_out_request_ids,
            deadline_ms,
            timeout_ms,
            proposed_log_ids,
            read_index_present,
            request_ids_by_route_key,
            deadlines_by_route_key,
            timeouts_by_route_key,
            log_ids_by_route_key,
            read_index_presence_by_route_key,
            read_index_responses_by_route_key,
            read_indices_by_route_key,
            read_index_safe_by_route_key,
            read_index_lease_read_by_route_key,
            read_index_reasons_by_route_key,
            membership_presence_by_route_key,
            snapshot_presence_by_route_key,
            auto_promote_presence_by_route_key,
            remove_presence_by_route_key,
            transfer_leader_presence_by_route_key,
            timeout_now_presence_by_route_key,
            step_down_presence_by_route_key,
            resign_presence_by_route_key,
            removed_ids_by_route_key,
            removed_conf_states_by_route_key,
            removed_values_by_route_key,
            remove_membership_success_by_route_key,
            membership_success_by_route_key,
            membership_reasons_by_route_key,
            snapshot_ids_by_route_key,
            snapshot_indices_by_route_key,
            auto_promote_learner_ids_by_route_key,
            auto_promote_enabled_by_route_key,
            auto_promote_promoted_by_route_key,
            transfer_leader_transferee_ids_by_route_key,
            transfer_leader_transferred_by_route_key,
            timeout_now_responses_by_route_key,
            timeout_now_node_ids_by_route_key,
            timeout_now_from_ids_by_route_key,
            timeout_now_campaigned_by_route_key,
            timeout_now_terms_by_route_key,
            timeout_now_leader_ids_by_route_key,
            timeout_now_reasons_by_route_key,
            step_down_requested_transferee_ids_by_route_key,
            step_down_transferee_ids_by_route_key,
            step_down_stepped_down_by_route_key,
            resign_reasons_by_route_key,
            resign_resigned_by_route_key,
            operations_by_route_key,
            statuses_by_route_key,
            errors_by_route_key,
        }
    }

    pub fn from_grouped_results(
        groups: &[(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)],
    ) -> Vec<Self> {
        groups
            .iter()
            .map(|(group_id, results)| Self::from_results(*group_id, results))
            .collect()
    }

    pub fn is_ok(&self) -> bool {
        self.error_count == 0 && self.ok_count == self.result_count
    }

    pub fn result_counts_by_status(&self) -> (usize, usize, usize) {
        (self.ok_count, self.error_count, self.timed_out_count)
    }

    pub fn route_key_counts_by_status(&self) -> (usize, usize, usize) {
        (
            self.ok_route_keys.len(),
            self.error_route_keys.len(),
            self.timed_out_route_keys.len(),
        )
    }

    pub fn status_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResultStatus)> {
        self.route_keys
            .iter()
            .map(|route_key| {
                let timed_out = self.timed_out_route_keys.contains(route_key);
                let ok = self
                    .statuses_by_route_key
                    .iter()
                    .find(|(key, _)| key == route_key)
                    .is_some_and(|(_, ok)| *ok);
                let status = if timed_out {
                    MatrixRaftAsyncResultStatus::TimedOut
                } else if ok {
                    MatrixRaftAsyncResultStatus::Ok
                } else {
                    MatrixRaftAsyncResultStatus::Error
                };
                (*route_key, status)
            })
            .collect()
    }

    pub fn ok_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.statuses_by_route_key.clone()
    }

    pub fn error_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.errors_by_route_key
            .iter()
            .map(|(key, error)| (*key, error.is_some()))
            .collect()
    }

    pub fn timed_out_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.route_keys
            .iter()
            .map(|route_key| (*route_key, self.timed_out_route_keys.contains(route_key)))
            .collect()
    }

    pub fn request_ids_by_status(
        &self,
    ) -> (
        Vec<Option<u64>>,
        Vec<Option<u64>>,
        Vec<Option<u64>>,
    ) {
        (
            self.ok_request_ids.clone(),
            self.error_request_ids.clone(),
            self.timed_out_request_ids.clone(),
        )
    }

    pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.route_keys
            .iter()
            .zip(self.node_ids.iter())
            .map(|(route_key, node_id)| (*route_key, *node_id))
            .collect()
    }

    pub fn ok_node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.ok_route_keys
            .iter()
            .zip(self.ok_node_ids.iter())
            .map(|(route_key, node_id)| (*route_key, *node_id))
            .collect()
    }

    pub fn error_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.error_route_keys
            .iter()
            .zip(self.error_node_ids.iter())
            .map(|(route_key, node_id)| (*route_key, *node_id))
            .collect()
    }

    pub fn timed_out_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.timed_out_route_keys
            .iter()
            .zip(self.timed_out_node_ids.iter())
            .map(|(route_key, node_id)| (*route_key, *node_id))
            .collect()
    }

    pub fn ok_operations_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)> {
        self.status_operations_by_route_key(true, false)
    }

    pub fn error_operations_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)> {
        self.status_operations_by_route_key(false, false)
    }

    pub fn timed_out_operations_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)> {
        self.status_operations_by_route_key(false, true)
    }

    fn status_operations_by_route_key(
        &self,
        ok: bool,
        timed_out: bool,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftAsyncOperation)> {
        self.route_keys
            .iter()
            .enumerate()
            .filter_map(|(index, route_key)| {
                let status_ok = self
                    .statuses_by_route_key
                    .get(index)
                    .is_some_and(|(_, status_ok)| *status_ok);
                let status_timed_out = self.timed_out_route_keys.contains(route_key);
                (status_ok == ok && status_timed_out == timed_out)
                    .then(|| self.operations_by_route_key.get(index).map(|(_, operation)| (*route_key, *operation)))
                    .flatten()
            })
            .collect()
    }

    fn route_key_has_status(
        &self,
        route_key: &MatrixRaftRouteKey,
        status: MatrixRaftAsyncResultStatus,
    ) -> bool {
        let timed_out = self.timed_out_route_keys.contains(route_key);
        let ok = self
            .statuses_by_route_key
            .iter()
            .find(|(key, _)| key == route_key)
            .is_some_and(|(_, ok)| *ok);
        match status {
            MatrixRaftAsyncResultStatus::Ok => ok && !timed_out,
            MatrixRaftAsyncResultStatus::Error => !ok && !timed_out,
            MatrixRaftAsyncResultStatus::TimedOut => timed_out,
        }
    }

    fn status_values_by_route_key<T: Clone>(
        &self,
        values: &[(MatrixRaftRouteKey, T)],
        status: MatrixRaftAsyncResultStatus,
    ) -> Vec<(MatrixRaftRouteKey, T)> {
        values
            .iter()
            .filter(|(route_key, _)| self.route_key_has_status(route_key, status))
            .cloned()
            .collect()
    }

    pub fn log_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.log_ids_by_route_key
            .iter()
            .map(|(key, log_id)| (*key, log_id.is_some()))
            .collect()
    }

    pub fn request_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.request_ids_by_route_key
            .iter()
            .map(|(key, request_id)| (*key, request_id.is_some()))
            .collect()
    }

    pub fn deadline_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.deadlines_by_route_key
            .iter()
            .map(|(key, deadline_ms)| (*key, deadline_ms.is_some()))
            .collect()
    }

    pub fn read_index_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.read_index_responses_by_route_key.clone()
    }

    pub fn ok_read_index_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.status_values_by_route_key(
            &self.read_index_responses_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_read_index_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.status_values_by_route_key(
            &self.read_index_responses_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_read_index_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.status_values_by_route_key(
            &self.read_index_responses_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn read_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.read_indices_by_route_key.clone()
    }

    pub fn ok_read_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.status_values_by_route_key(
            &self.read_indices_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_read_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.status_values_by_route_key(
            &self.read_indices_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_read_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.status_values_by_route_key(
            &self.read_indices_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn read_index_safe_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.read_index_safe_by_route_key.clone()
    }

    pub fn ok_read_index_safe_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.read_index_safe_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_read_index_safe_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.read_index_safe_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_read_index_safe_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.read_index_safe_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn read_index_lease_read_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.read_index_lease_read_by_route_key.clone()
    }

    pub fn ok_read_index_lease_read_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.read_index_lease_read_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_read_index_lease_read_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.read_index_lease_read_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_read_index_lease_read_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.read_index_lease_read_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn read_index_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.read_index_reasons_by_route_key.clone()
    }

    pub fn ok_read_index_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.status_values_by_route_key(
            &self.read_index_reasons_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_read_index_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.status_values_by_route_key(
            &self.read_index_reasons_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_read_index_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.status_values_by_route_key(
            &self.read_index_reasons_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn membership_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.membership_presence_by_route_key.clone()
    }

    pub fn snapshot_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.snapshot_presence_by_route_key.clone()
    }

    pub fn auto_promote_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.auto_promote_presence_by_route_key.clone()
    }

    pub fn remove_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.remove_presence_by_route_key.clone()
    }

    pub fn removed_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.removed_ids_by_route_key.clone()
    }

    pub fn removed_conf_states_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftConfState>)> {
        self.removed_conf_states_by_route_key.clone()
    }

    pub fn removed_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.removed_values_by_route_key.clone()
    }

    pub fn remove_membership_success_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.remove_membership_success_by_route_key.clone()
    }

    pub fn membership_success_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.membership_success_by_route_key.clone()
    }

    pub fn membership_reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.membership_reasons_by_route_key.clone()
    }

    pub fn snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.snapshot_ids_by_route_key.clone()
    }

    pub fn snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.snapshot_indices_by_route_key.clone()
    }

    pub fn auto_promote_learner_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.auto_promote_learner_ids_by_route_key.clone()
    }

    pub fn auto_promote_enabled_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.auto_promote_enabled_by_route_key.clone()
    }

    pub fn auto_promote_promoted_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.auto_promote_promoted_by_route_key.clone()
    }

    pub fn transfer_leader_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.transfer_leader_transferee_ids_by_route_key.clone()
    }

    pub fn ok_transfer_leader_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.transfer_leader_transferee_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_transfer_leader_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.transfer_leader_transferee_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_transfer_leader_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.transfer_leader_transferee_ids_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn transfer_leader_transferred_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.transfer_leader_transferred_by_route_key.clone()
    }

    pub fn ok_transfer_leader_transferred_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.transfer_leader_transferred_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_transfer_leader_transferred_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.transfer_leader_transferred_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_transfer_leader_transferred_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.transfer_leader_transferred_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn step_down_requested_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.step_down_requested_transferee_ids_by_route_key.clone()
    }

    pub fn ok_step_down_requested_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.step_down_requested_transferee_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_step_down_requested_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.step_down_requested_transferee_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_step_down_requested_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.step_down_requested_transferee_ids_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn step_down_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.step_down_transferee_ids_by_route_key.clone()
    }

    pub fn ok_step_down_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.step_down_transferee_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_step_down_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.step_down_transferee_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_step_down_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.step_down_transferee_ids_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn step_down_stepped_down_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.step_down_stepped_down_by_route_key.clone()
    }

    pub fn ok_step_down_stepped_down_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.step_down_stepped_down_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_step_down_stepped_down_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.step_down_stepped_down_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_step_down_stepped_down_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.step_down_stepped_down_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn resign_reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.resign_reasons_by_route_key.clone()
    }

    pub fn ok_resign_reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.status_values_by_route_key(
            &self.resign_reasons_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_resign_reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.status_values_by_route_key(
            &self.resign_reasons_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_resign_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.status_values_by_route_key(
            &self.resign_reasons_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn resign_resigned_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.resign_resigned_by_route_key.clone()
    }

    pub fn ok_resign_resigned_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.resign_resigned_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_resign_resigned_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.resign_resigned_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_resign_resigned_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.resign_resigned_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn transfer_leader_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.transfer_leader_presence_by_route_key.clone()
    }

    pub fn ok_transfer_leader_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.status_values_by_route_key(
            &self.transfer_leader_presence_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_transfer_leader_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.status_values_by_route_key(
            &self.transfer_leader_presence_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_transfer_leader_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.status_values_by_route_key(
            &self.transfer_leader_presence_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn timeout_now_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.timeout_now_presence_by_route_key.clone()
    }

    pub fn timeout_now_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)> {
        self.timeout_now_responses_by_route_key.clone()
    }

    pub fn ok_timeout_now_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)> {
        self.status_values_by_route_key(
            &self.timeout_now_responses_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_timeout_now_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)> {
        self.status_values_by_route_key(
            &self.timeout_now_responses_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_timeout_now_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)> {
        self.status_values_by_route_key(
            &self.timeout_now_responses_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn timeout_now_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.timeout_now_node_ids_by_route_key.clone()
    }

    pub fn ok_timeout_now_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.timeout_now_node_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_timeout_now_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.timeout_now_node_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_timeout_now_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.timeout_now_node_ids_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn timeout_now_from_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.timeout_now_from_ids_by_route_key.clone()
    }

    pub fn ok_timeout_now_from_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.timeout_now_from_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_timeout_now_from_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.timeout_now_from_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_timeout_now_from_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.timeout_now_from_ids_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn timeout_now_campaigned_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.timeout_now_campaigned_by_route_key.clone()
    }

    pub fn ok_timeout_now_campaigned_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.timeout_now_campaigned_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_timeout_now_campaigned_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.timeout_now_campaigned_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_timeout_now_campaigned_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.status_values_by_route_key(
            &self.timeout_now_campaigned_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn timeout_now_terms_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<Term>)> {
        self.timeout_now_terms_by_route_key.clone()
    }

    pub fn ok_timeout_now_terms_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<Term>)> {
        self.status_values_by_route_key(
            &self.timeout_now_terms_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_timeout_now_terms_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<Term>)> {
        self.status_values_by_route_key(
            &self.timeout_now_terms_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_timeout_now_terms_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<Term>)> {
        self.status_values_by_route_key(
            &self.timeout_now_terms_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn timeout_now_leader_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.timeout_now_leader_ids_by_route_key.clone()
    }

    pub fn ok_timeout_now_leader_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.timeout_now_leader_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_timeout_now_leader_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.timeout_now_leader_ids_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_timeout_now_leader_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.status_values_by_route_key(
            &self.timeout_now_leader_ids_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn timeout_now_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.timeout_now_reasons_by_route_key.clone()
    }

    pub fn ok_timeout_now_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.status_values_by_route_key(
            &self.timeout_now_reasons_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_timeout_now_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.status_values_by_route_key(
            &self.timeout_now_reasons_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_timeout_now_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.status_values_by_route_key(
            &self.timeout_now_reasons_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn step_down_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.step_down_presence_by_route_key.clone()
    }

    pub fn ok_step_down_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.status_values_by_route_key(
            &self.step_down_presence_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_step_down_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.status_values_by_route_key(
            &self.step_down_presence_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_step_down_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.status_values_by_route_key(
            &self.step_down_presence_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn resign_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.resign_presence_by_route_key.clone()
    }

    pub fn ok_resign_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.status_values_by_route_key(
            &self.resign_presence_by_route_key,
            MatrixRaftAsyncResultStatus::Ok,
        )
    }

    pub fn error_resign_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.status_values_by_route_key(
            &self.resign_presence_by_route_key,
            MatrixRaftAsyncResultStatus::Error,
        )
    }

    pub fn timed_out_resign_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.status_values_by_route_key(
            &self.resign_presence_by_route_key,
            MatrixRaftAsyncResultStatus::TimedOut,
        )
    }

    pub fn callback_timing_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>, u64)> {
        self.deadlines_by_route_key
            .iter()
            .zip(self.timeouts_by_route_key.iter())
            .map(|((deadline_key, deadline_ms), (timeout_key, timeout_ms))| {
                debug_assert_eq!(deadline_key, timeout_key);
                (*deadline_key, *deadline_ms, *timeout_ms)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftScheduledCallback {
    pub operation: MatrixRaftAsyncOperation,
    pub task: TimerTask,
}

impl MatrixRaftScheduledCallback {
    pub fn timeout_result(&self) -> MatrixRaftAsyncResult {
        MatrixRaftAsyncResult::timeout(
            self.operation,
            self.task.deadline_ms.saturating_sub(self.task.start_at_ms),
        )
        .with_timer_task(&self.task, self.operation)
    }

    pub fn completed_result(&self) -> MatrixRaftAsyncResult {
        MatrixRaftAsyncResult::ok(
            self.operation,
            self.task.deadline_ms.saturating_sub(self.task.start_at_ms),
        )
        .with_timer_task(&self.task, self.operation)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftCallbackScheduler {
    timer: RequestTimer,
    operations: BTreeMap<(NodeId, u64), MatrixRaftAsyncOperation>,
}

impl MatrixRaftCallbackScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn schedule(
        &mut self,
        node_id: NodeId,
        request_id: u64,
        operation: MatrixRaftAsyncOperation,
        start_at_ms: u64,
        timeout_ms: u64,
    ) -> Option<MatrixRaftScheduledCallback> {
        let deadline_ms = if timeout_ms == 0 {
            0
        } else {
            start_at_ms.saturating_add(timeout_ms)
        };
        let previous_operation = self.operations.insert((node_id, request_id), operation);
        let previous_task = self
            .timer
            .watch(node_id, request_id, deadline_ms, start_at_ms);
        previous_task.map(|task| MatrixRaftScheduledCallback {
            operation: previous_operation.unwrap_or(operation),
            task,
        })
    }

    pub fn complete(
        &mut self,
        node_id: NodeId,
        request_id: u64,
    ) -> Option<MatrixRaftScheduledCallback> {
        let task = self.timer.notify(node_id, request_id)?;
        let operation = self
            .operations
            .remove(&(node_id, request_id))
            .unwrap_or(MatrixRaftAsyncOperation::Propose);
        Some(MatrixRaftScheduledCallback { operation, task })
    }

    pub fn cancel(
        &mut self,
        node_id: NodeId,
        request_id: u64,
    ) -> Option<MatrixRaftScheduledCallback> {
        let task = self.timer.cancel(node_id, request_id)?;
        let operation = self
            .operations
            .remove(&(node_id, request_id))
            .unwrap_or(MatrixRaftAsyncOperation::Propose);
        Some(MatrixRaftScheduledCallback { operation, task })
    }

    pub fn lapsed(&mut self, now_ms: u64, limit: usize) -> Vec<MatrixRaftAsyncResult> {
        self.timer
            .lapsed(now_ms, limit)
            .into_iter()
            .map(|task| {
                let operation = self
                    .operations
                    .remove(&(task.node_id, task.request_id))
                    .unwrap_or(MatrixRaftAsyncOperation::Propose);
                MatrixRaftScheduledCallback { operation, task }.timeout_result()
            })
            .collect()
    }

    pub fn next_timeout_ms(&self, now_ms: u64) -> u64 {
        self.timer.next_timeout_ms(now_ms)
    }

    pub fn len(&self) -> usize {
        self.timer.len()
    }

    pub fn timed_len(&self) -> usize {
        self.timer.timed_len()
    }

    pub fn is_empty(&self) -> bool {
        self.timer.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotStatus {
    pub snapshot_id: Option<SnapshotId>,
    pub sending: bool,
    pub downloading: bool,
    pub total_chunks: u64,
    pub sent_chunks: u64,
    pub received_chunks: u64,
    pub retry_count: u64,
    pub throttled_ticks: u64,
    pub rate_limited_ticks: u64,
    pub rolled_back: u64,
    pub completed: bool,
    pub installed_index: LogIndex,
}

impl From<SnapshotLifecycleStatus> for MatrixRaftSnapshotStatus {
    fn from(status: SnapshotLifecycleStatus) -> Self {
        Self {
            snapshot_id: status.snapshot_id,
            sending: status.sending,
            downloading: status.installing,
            total_chunks: status.total_chunks,
            sent_chunks: status.sent_chunks,
            received_chunks: status.received_chunks,
            retry_count: status.retry_count,
            throttled_ticks: status.throttled_ticks,
            rate_limited_ticks: status.rate_limited_ticks,
            rolled_back: status.rolled_back,
            completed: status.completed,
            installed_index: status.installed_index,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotDownloadResult {
    pub accepted: bool,
    pub response: InstallSnapshotResponse,
    #[serde(default)]
    pub installed_snapshot: Option<RaftSnapshot>,
    #[serde(default)]
    pub finish: Option<MatrixRaftOldSnapshotFinish>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotCancelResult {
    pub canceled: bool,
    pub status_before: MatrixRaftSnapshotStatus,
    pub status_after: MatrixRaftSnapshotStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotPeerReport {
    pub peer_id: NodeId,
    pub status: PeerProgress,
    #[serde(default)]
    pub peer_healthy: Option<bool>,
    #[serde(default)]
    pub peer_lag: Option<LogIndex>,
}

impl MatrixRaftOldSnapshotFinish {
    pub fn received(snapshot_index: LogIndex) -> Self {
        Self {
            finish_state: MatrixRaftOldSnapshotFinishState::Received,
            snapshot_index,
        }
    }

    pub fn rejected(snapshot_index: LogIndex) -> Self {
        Self {
            finish_state: MatrixRaftOldSnapshotFinishState::Rejected,
            snapshot_index,
        }
    }

    pub fn staled(snapshot_index: LogIndex) -> Self {
        Self {
            finish_state: MatrixRaftOldSnapshotFinishState::Staled,
            snapshot_index,
        }
    }

    pub fn from_install_snapshot_response(
        response: &InstallSnapshotResponse,
        snapshot_index: LogIndex,
    ) -> Self {
        if response.accepted {
            Self::received(snapshot_index)
        } else if response.reason.contains("stale") || response.reason.contains("older") {
            Self::staled(snapshot_index)
        } else {
            Self::rejected(snapshot_index)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotSender {
    lifecycle: SnapshotLifecycle,
}

impl MatrixRaftSnapshotSender {
    pub fn new(config: SnapshotLifecycleConfig) -> Result<Self, RaftError> {
        Ok(Self {
            lifecycle: SnapshotLifecycle::new(config)?,
        })
    }

    pub fn begin_send(
        &mut self,
        snapshot: &RaftSnapshot,
        term: Term,
        leader_id: NodeId,
    ) -> Result<(), RaftError> {
        self.lifecycle.begin_send(snapshot, term, leader_id)
    }

    pub fn send(
        &mut self,
        snapshot: &RaftSnapshot,
        term: Term,
        leader_id: NodeId,
    ) -> Result<(), RaftError> {
        self.begin_send(snapshot, term, leader_id)
    }

    pub fn poll_send_requests(&mut self) -> Result<Vec<InstallSnapshotRequest>, RaftError> {
        self.lifecycle.poll_send_requests()
    }

    pub fn poll_send_requests_with_limiter(
        &mut self,
        limiter: &mut impl RateLimiter,
    ) -> Result<Vec<InstallSnapshotRequest>, RaftError> {
        self.lifecycle.poll_send_requests_with_limiter(limiter)
    }

    pub fn record_send_response(
        &mut self,
        response: &InstallSnapshotResponse,
    ) -> Result<Option<MatrixRaftOldSnapshotFinish>, RaftError> {
        self.lifecycle.record_send_response(response)?;
        if response.accepted && response.committed_index == 0 {
            return Ok(None);
        }
        Ok(Some(
            MatrixRaftOldSnapshotFinish::from_install_snapshot_response(
                response,
                response.committed_index,
            ),
        ))
    }

    pub fn record_send_timeout(&mut self) -> Result<(), RaftError> {
        self.lifecycle.record_send_timeout()
    }

    pub fn cancel(&mut self) -> MatrixRaftSnapshotCancelResult {
        let status_before = self.status();
        let canceled = self.lifecycle.cancel_send();
        let status_after = self.status();
        MatrixRaftSnapshotCancelResult {
            canceled,
            status_before,
            status_after,
        }
    }

    pub fn status(&self) -> MatrixRaftSnapshotStatus {
        self.lifecycle.status().into()
    }
}

impl Default for MatrixRaftSnapshotSender {
    fn default() -> Self {
        Self::new(SnapshotLifecycleConfig::default())
            .expect("default MatrixRaft snapshot sender config is valid")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotDownloader {
    lifecycle: SnapshotLifecycle,
}

impl MatrixRaftSnapshotDownloader {
    pub fn new(config: SnapshotLifecycleConfig) -> Result<Self, RaftError> {
        Ok(Self {
            lifecycle: SnapshotLifecycle::new(config)?,
        })
    }

    pub fn download(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> Result<MatrixRaftSnapshotDownloadResult, RaftError> {
        let snapshot_index = request.chunk.meta.last_log_id.index;
        let next_offset = request
            .chunk
            .offset
            .saturating_add(request.chunk.data.len() as u64);
        let installed_snapshot = self.lifecycle.install_request(request)?;
        let accepted = true;
        let response = InstallSnapshotResponse {
            term: installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.meta.last_log_id.term)
                .unwrap_or_default(),
            accepted,
            next_offset,
            committed_index: installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.meta.last_log_id.index)
                .unwrap_or_default(),
            reason: if installed_snapshot.is_some() {
                "snapshot downloaded".to_string()
            } else {
                "snapshot chunk accepted".to_string()
            },
        };
        let finish = installed_snapshot
            .is_some()
            .then(|| MatrixRaftOldSnapshotFinish::received(snapshot_index));
        Ok(MatrixRaftSnapshotDownloadResult {
            accepted,
            response,
            installed_snapshot,
            finish,
        })
    }

    pub fn rollback(&mut self) {
        self.lifecycle.rollback_install();
    }

    pub fn cancel(&mut self) {
        self.rollback();
    }

    pub fn status(&self) -> MatrixRaftSnapshotStatus {
        self.lifecycle.status().into()
    }
}

impl Default for MatrixRaftSnapshotDownloader {
    fn default() -> Self {
        Self::new(SnapshotLifecycleConfig::default())
            .expect("default MatrixRaft snapshot downloader config is valid")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotCreator {
    pub store_id: u64,
    pub worker_num: usize,
}

impl MatrixRaftSnapshotCreator {
    pub fn new(store_id: u64, worker_num: usize) -> Self {
        Self {
            store_id,
            worker_num,
        }
    }

    pub fn checkpoint(
        &self,
        snapshot: &RaftSnapshot,
        chunk_size: u64,
    ) -> Result<Vec<SnapshotChunk>, RaftError> {
        SnapshotLifecycle::checkpoint(snapshot, chunk_size)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotLoader {
    pub store_id: u64,
    pub worker_num: usize,
}

impl MatrixRaftSnapshotLoader {
    pub fn new(store_id: u64, worker_num: usize) -> Self {
        Self {
            store_id,
            worker_num,
        }
    }

    pub fn install_chunk(
        &self,
        state: &mut SnapshotInstallState,
        chunk: SnapshotChunk,
    ) -> Result<(), RaftError> {
        state.install_chunk(chunk)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftGroupThreadPool {
    Worker,
    Flusher,
    Applier,
    Reader,
    Executor,
    Snapshotter,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRateLimiterConfig {
    pub bytes_limit_per_sec: u64,
    pub check_cycle_sec: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftNodeCreator {
    pub store_id: u64,
    pub applier_num: usize,
    pub apply_max_batch_count: usize,
    pub snapshot_loader_num: usize,
    pub snapshot_downloader_num: usize,
    pub snapshot_creator_num: usize,
    pub snapshot_sender_num: usize,
    pub snapshot_send_rate_limiter: Option<MatrixRaftRateLimiterConfig>,
    pub snapshot_download_rate_limiter: Option<MatrixRaftRateLimiterConfig>,
    pub flexible_apply: bool,
    pub heartbeat_merge: bool,
    pub merge_heartbeat_interval_milli: u64,
    pub has_store_fsm: bool,
    pub has_group_storage: bool,
}

impl Default for MatrixRaftNodeCreator {
    fn default() -> Self {
        Self {
            store_id: 0,
            applier_num: 1,
            apply_max_batch_count: 1,
            snapshot_loader_num: 1,
            snapshot_downloader_num: 1,
            snapshot_creator_num: 1,
            snapshot_sender_num: 1,
            snapshot_send_rate_limiter: None,
            snapshot_download_rate_limiter: None,
            flexible_apply: false,
            heartbeat_merge: false,
            merge_heartbeat_interval_milli: 0,
            has_store_fsm: false,
            has_group_storage: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[derive(Default)]
pub struct MatrixRaftNodeCreatorBuilder {
    creator: MatrixRaftNodeCreator,
}


impl MatrixRaftNodeCreatorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn store_id(mut self, id: u64) -> Self {
        self.creator.store_id = id;
        self
    }

    pub fn applier_num(mut self, num: usize) -> Self {
        self.creator.applier_num = num.max(1);
        self
    }

    pub fn apply_max_batch_count(mut self, count: usize) -> Self {
        self.creator.apply_max_batch_count = count.max(1);
        self
    }

    pub fn snapshot_loader_num(mut self, num: usize) -> Self {
        self.creator.snapshot_loader_num = num.max(1);
        self
    }

    pub fn snapshot_downloader_num(mut self, num: usize) -> Self {
        self.creator.snapshot_downloader_num = num.max(1);
        self
    }

    pub fn snapshot_creator_num(mut self, num: usize) -> Self {
        self.creator.snapshot_creator_num = num.max(1);
        self
    }

    pub fn snapshot_sender_num(mut self, num: usize) -> Self {
        self.creator.snapshot_sender_num = num.max(1);
        self
    }

    pub fn snapshot_send_rate_limiter(mut self, limiter: MatrixRaftRateLimiterConfig) -> Self {
        self.creator.snapshot_send_rate_limiter = Some(limiter);
        self
    }

    pub fn snapshot_download_rate_limiter(mut self, limiter: MatrixRaftRateLimiterConfig) -> Self {
        self.creator.snapshot_download_rate_limiter = Some(limiter);
        self
    }

    pub fn enable_flexible_apply(self) -> Self {
        self.flexible_apply(true)
    }

    pub fn flexible_apply(mut self, enable: bool) -> Self {
        self.creator.flexible_apply = enable;
        self
    }

    pub fn enable_heartbeat_merge(self) -> Self {
        self.heartbeat_merge(true)
    }

    pub fn heartbeat_merge(mut self, enable: bool) -> Self {
        self.creator.heartbeat_merge = enable;
        self
    }

    pub fn merge_heartbeat_interval_milli(mut self, interval_ms: u64) -> Self {
        self.creator.merge_heartbeat_interval_milli = interval_ms;
        self
    }

    pub fn fsm(mut self) -> Self {
        self.creator.has_store_fsm = true;
        self
    }

    pub fn group_storage(mut self) -> Self {
        self.creator.has_group_storage = true;
        self
    }

    pub fn build(self) -> MatrixRaftNodeCreator {
        self.creator
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftTransportOptions {
    pub cluster_id: u64,
    pub timeout_ms: u32,
    pub num_connection_group: usize,
    pub dynamic_address_map: bool,
    pub address_resolver_bound: bool,
    pub user_payload_callback_bound: bool,
}

impl Default for MatrixRaftTransportOptions {
    fn default() -> Self {
        Self {
            cluster_id: 0,
            timeout_ms: 250,
            num_connection_group: 1,
            dynamic_address_map: false,
            address_resolver_bound: false,
            user_payload_callback_bound: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[derive(Default)]
pub struct MatrixRaftTransportBuilder {
    options: MatrixRaftTransportOptions,
}


impl MatrixRaftTransportBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_dynamic_address_map(mut self) -> Self {
        self.options.dynamic_address_map = true;
        self
    }

    pub fn set_cluster_id(mut self, cluster_id: u64) -> Self {
        self.options.cluster_id = cluster_id;
        self
    }

    pub fn set_timeout_ms(mut self, timeout_ms: u32) -> Self {
        self.options.timeout_ms = timeout_ms.max(1);
        self
    }

    pub fn set_num_connection_group(mut self, num: usize) -> Self {
        self.options.num_connection_group = num.max(1);
        self
    }

    pub fn bind_address_resolver(mut self) -> Self {
        self.options.address_resolver_bound = true;
        self
    }

    pub fn set_get_user_payload_callback(mut self) -> Self {
        self.options.user_payload_callback_bound = true;
        self
    }

    pub fn build(self) -> Result<MatrixRaftTransportOptions, RaftError> {
        if !self.options.address_resolver_bound {
            return Err(RaftError::InvalidRequest(
                "matrixraft transport builder requires an address resolver".to_string(),
            ));
        }
        Ok(self.options)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftGroupContext {
    pub tick_interval_ms: u64,
    pub max_messages_each_poll: u64,
    pub max_queue_depth: u64,
    pub worker_num: usize,
    pub reader_num: usize,
    pub executor_num: usize,
    pub applier_num: usize,
    pub snapshot_loader_num: usize,
    pub snapshot_downloader_num: usize,
    pub snapshot_sender_num: usize,
    pub snapshot_creator_num: usize,
    pub apply_max_batch_count: usize,
    pub driver_batch_bytes: usize,
    pub flexible_apply: bool,
    pub heartbeat_merge: bool,
    pub watched_address_resolver: bool,
    pub transport: Option<MatrixRaftTransportOptions>,
    pub node_creators: Vec<MatrixRaftNodeCreator>,
    pub snapshot_send_rate_limiter: Option<MatrixRaftRateLimiterConfig>,
    pub snapshot_download_rate_limiter: Option<MatrixRaftRateLimiterConfig>,
}

impl Default for MatrixRaftGroupContext {
    fn default() -> Self {
        Self {
            tick_interval_ms: 100,
            max_messages_each_poll: 128,
            max_queue_depth: 1024,
            worker_num: 1,
            reader_num: 1,
            executor_num: 1,
            applier_num: 1,
            snapshot_loader_num: 1,
            snapshot_downloader_num: 1,
            snapshot_sender_num: 1,
            snapshot_creator_num: 1,
            apply_max_batch_count: 1,
            driver_batch_bytes: 64 * 1024,
            flexible_apply: false,
            heartbeat_merge: false,
            watched_address_resolver: false,
            transport: None,
            node_creators: Vec::new(),
            snapshot_send_rate_limiter: None,
            snapshot_download_rate_limiter: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[derive(Default)]
pub struct MatrixRaftGroupContextBuilder {
    context: MatrixRaftGroupContext,
}


impl MatrixRaftGroupContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tick_interval(mut self, ms: u64) -> Self {
        self.context.tick_interval_ms = ms.max(1);
        self
    }

    pub fn transport(mut self, transport: MatrixRaftTransportOptions) -> Self {
        self.context.transport = Some(transport);
        self
    }

    pub fn max_messages_each_poll(mut self, value: u64) -> Self {
        self.context.max_messages_each_poll = value.max(1);
        self
    }

    pub fn max_queue_depth(mut self, value: u64) -> Self {
        self.context.max_queue_depth = value.max(1);
        self
    }

    pub fn add_raft_node_creator(mut self, creator: MatrixRaftNodeCreator) -> Self {
        self.context.node_creators.push(creator);
        self
    }

    pub fn worker_num(mut self, num: usize) -> Self {
        self.context.worker_num = num.max(1);
        self
    }

    pub fn reader_num(mut self, num: usize) -> Self {
        self.context.reader_num = num.max(1);
        self
    }

    pub fn executor_num(mut self, num: usize) -> Self {
        self.context.executor_num = num.max(1);
        self
    }

    pub fn applier_num(mut self, num: usize) -> Self {
        self.context.applier_num = num.max(1);
        self
    }

    pub fn snapshot_loader_num(mut self, num: usize) -> Self {
        self.context.snapshot_loader_num = num.max(1);
        self
    }

    pub fn snapshot_downloader_num(mut self, num: usize) -> Self {
        self.context.snapshot_downloader_num = num.max(1);
        self
    }

    pub fn snapshot_sender_num(mut self, num: usize) -> Self {
        self.context.snapshot_sender_num = num.max(1);
        self
    }

    pub fn snapshot_creator_num(mut self, num: usize) -> Self {
        self.context.snapshot_creator_num = num.max(1);
        self
    }

    pub fn apply_max_batch_count(mut self, count: usize) -> Self {
        self.context.apply_max_batch_count = count.max(1);
        self
    }

    pub fn driver_batch_bytes(mut self, bytes: usize) -> Self {
        self.context.driver_batch_bytes = bytes.max(1);
        self
    }

    pub fn watch_address_resolver(mut self) -> Self {
        self.context.watched_address_resolver = true;
        self
    }

    pub fn snapshot_download_rate_limiter(mut self, limiter: MatrixRaftRateLimiterConfig) -> Self {
        self.context.snapshot_download_rate_limiter = Some(limiter);
        self
    }

    pub fn snapshot_send_rate_limiter(mut self, limiter: MatrixRaftRateLimiterConfig) -> Self {
        self.context.snapshot_send_rate_limiter = Some(limiter);
        self
    }

    pub fn enable_flexible_apply(mut self) -> Self {
        self.context.flexible_apply = true;
        self
    }

    pub fn enable_heartbeat_merge(mut self) -> Self {
        self.context.heartbeat_merge = true;
        self
    }

    pub fn build(self) -> Result<MatrixRaftGroupContext, RaftError> {
        if self.context.transport.is_none() {
            return Err(RaftError::InvalidRequest(
                "matrixraft group context builder requires transport".to_string(),
            ));
        }
        Ok(self.context)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRuntimeWiring {
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub creator_index: Option<usize>,
    pub store_id: u64,
    pub worker_num: usize,
    pub reader_num: usize,
    pub executor_num: usize,
    pub applier_num: usize,
    pub apply_max_batch_count: usize,
    pub snapshot_loader_num: usize,
    pub snapshot_downloader_num: usize,
    pub snapshot_creator_num: usize,
    pub snapshot_sender_num: usize,
    pub flexible_apply: bool,
    pub heartbeat_merge: bool,
    pub merge_heartbeat_interval_milli: u64,
    pub watched_address_resolver: bool,
    pub transport: MatrixRaftTransportOptions,
    pub snapshot_send_rate_limiter: Option<MatrixRaftRateLimiterConfig>,
    pub snapshot_download_rate_limiter: Option<MatrixRaftRateLimiterConfig>,
    pub has_store_fsm: bool,
    pub has_group_storage: bool,
}

impl MatrixRaftRuntimeWiring {
    fn from_context_and_creator(
        context: &MatrixRaftGroupContext,
        options: &MatrixRaftOptions,
        creator_index: Option<usize>,
        creator: Option<&MatrixRaftNodeCreator>,
    ) -> Result<Self, RaftError> {
        let transport = context.transport.clone().ok_or_else(|| {
            RaftError::InvalidRequest(
                "matrixraft runtime wiring requires group transport".to_string(),
            )
        })?;
        Ok(Self {
            group_id: options.group_id,
            node_id: options.peer_id,
            creator_index,
            store_id: creator.map_or(options.peer_id, |creator| creator.store_id),
            worker_num: context.worker_num,
            reader_num: context.reader_num,
            executor_num: context.executor_num,
            applier_num: creator.map_or(context.applier_num, |creator| creator.applier_num),
            apply_max_batch_count: creator.map_or(context.apply_max_batch_count, |creator| {
                creator.apply_max_batch_count
            }),
            snapshot_loader_num: creator.map_or(context.snapshot_loader_num, |creator| {
                creator.snapshot_loader_num
            }),
            snapshot_downloader_num: creator.map_or(context.snapshot_downloader_num, |creator| {
                creator.snapshot_downloader_num
            }),
            snapshot_creator_num: creator.map_or(context.snapshot_creator_num, |creator| {
                creator.snapshot_creator_num
            }),
            snapshot_sender_num: creator.map_or(context.snapshot_sender_num, |creator| {
                creator.snapshot_sender_num
            }),
            flexible_apply: context.flexible_apply
                || creator.is_some_and(|creator| creator.flexible_apply),
            heartbeat_merge: context.heartbeat_merge
                || creator.is_some_and(|creator| creator.heartbeat_merge),
            merge_heartbeat_interval_milli: creator
                .map_or(0, |creator| creator.merge_heartbeat_interval_milli),
            watched_address_resolver: context.watched_address_resolver,
            transport,
            snapshot_send_rate_limiter: creator
                .and_then(|creator| creator.snapshot_send_rate_limiter.clone())
                .or_else(|| context.snapshot_send_rate_limiter.clone()),
            snapshot_download_rate_limiter: creator
                .and_then(|creator| creator.snapshot_download_rate_limiter.clone())
                .or_else(|| context.snapshot_download_rate_limiter.clone()),
            has_store_fsm: creator.is_some_and(|creator| creator.has_store_fsm),
            has_group_storage: creator.is_some_and(|creator| creator.has_group_storage),
        })
    }
}

fn matrixraft_default_true() -> bool {
    true
}

fn matrixraft_default_reorder_timeout_us() -> u64 {
    3_000
}

fn matrixraft_default_reorder_window_size() -> u64 {
    128
}

fn matrixraft_default_max_disk_replicate_log_num() -> u64 {
    64
}

fn matrixraft_default_max_inflights_apply_task() -> u64 {
    5
}

fn matrixraft_default_max_inflights_replicate() -> u64 {
    128
}

fn matrixraft_default_send_snapshot_timeout_ms() -> u64 {
    60_000
}

fn matrixraft_default_max_applied_log_bytes() -> u64 {
    u64::MAX
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftReorderQueueOptions {
    pub enable_reorder_queue: bool,
    pub reorder_timeout_us: u64,
    pub reorder_window_size: u64,
}

impl Default for MatrixRaftReorderQueueOptions {
    fn default() -> Self {
        Self {
            enable_reorder_queue: true,
            reorder_timeout_us: matrixraft_default_reorder_timeout_us(),
            reorder_window_size: matrixraft_default_reorder_window_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftInflightOptions {
    pub max_inflights_apply_task: u64,
    pub max_inflights_replicate: u64,
}

impl Default for MatrixRaftInflightOptions {
    fn default() -> Self {
        Self {
            max_inflights_apply_task: matrixraft_default_max_inflights_apply_task(),
            max_inflights_replicate: matrixraft_default_max_inflights_replicate(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotRecycleOptions {
    pub can_trigger_snapshot: bool,
    pub max_applied_log_bytes: u64,
    pub send_snapshot_timeout_ms: u64,
}

impl Default for MatrixRaftSnapshotRecycleOptions {
    fn default() -> Self {
        Self {
            can_trigger_snapshot: true,
            max_applied_log_bytes: matrixraft_default_max_applied_log_bytes(),
            send_snapshot_timeout_ms: matrixraft_default_send_snapshot_timeout_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftOptions {
    pub group_id: GroupId,
    pub peer_id: NodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    pub wal_dir: String,
    pub snapshot_dir: String,
    pub peers: Vec<Peer>,
    pub role: ReplicaRole,
    pub wal_sync: bool,
    pub election_cycle_tick: u64,
    pub transfer_timeout_tick: u64,
    pub offline_timeout_tick: u64,
    pub tick_interval_ms: u64,
    pub lease_duration_ms: u64,
    pub last_lease_duration_ms: u64,
    pub assume_lease_when_start: bool,
    pub max_memory_replicate_log_bytes: u64,
    #[serde(default = "matrixraft_default_max_disk_replicate_log_num")]
    pub max_disk_replicate_log_num: u64,
    pub max_cache_memory_bytes: u64,
    pub max_apply_batch_bytes: u64,
    #[serde(default = "matrixraft_default_true")]
    pub enable_reorder_queue: bool,
    #[serde(default = "matrixraft_default_reorder_timeout_us")]
    pub reorder_timeout_us: u64,
    #[serde(default = "matrixraft_default_reorder_window_size")]
    pub reorder_window_size: u64,
    #[serde(default = "matrixraft_default_max_inflights_apply_task")]
    pub max_inflights_apply_task: u64,
    #[serde(default = "matrixraft_default_max_inflights_replicate")]
    pub max_inflights_replicate: u64,
    pub enable_pre_vote: bool,
    pub max_segment_bytes: u64,
    pub min_keep_segment_num: u64,
    #[serde(default = "matrixraft_default_true")]
    pub can_trigger_snapshot: bool,
    #[serde(default = "matrixraft_default_max_applied_log_bytes")]
    pub max_applied_log_bytes: u64,
    #[serde(default = "matrixraft_default_send_snapshot_timeout_ms")]
    pub send_snapshot_timeout_ms: u64,
}

impl MatrixRaftOptions {
    pub fn to_raft_config(&self) -> Config {
        let election_timeout_ms = self
            .election_cycle_tick
            .max(1)
            .saturating_mul(self.tick_interval_ms.max(1));
        let leader_lease_ms = if self.lease_duration_ms == 0 {
            election_timeout_ms.saturating_sub(1).max(1)
        } else {
            self.lease_duration_ms.min(election_timeout_ms.saturating_sub(1).max(1))
        };
        Config {
            election_timeout_ms,
            heartbeat_interval_ms: self.tick_interval_ms.max(1),
            leader_lease_ms,
            last_follower_lease_ms: self.last_lease_duration_ms,
            max_payload_bytes: self.max_memory_replicate_log_bytes.max(1),
            max_log_buffer_bytes: self.max_cache_memory_bytes.max(1),
            snapshot_threshold_entries: self.max_apply_batch_bytes.max(1),
            max_segment_bytes: self.max_segment_bytes.max(1),
            min_keep_segment_num: self.min_keep_segment_num.max(1),
            enable_pre_vote: self.enable_pre_vote,
            enable_lease_read: true,
            assume_lease_when_start: self.assume_lease_when_start,
        }
    }

    pub fn to_node_options(&self) -> NodeOptions {
        NodeOptions {
            group_id: self.group_id,
            node_id: self.peer_id,
            raft_addr: self.raft_addr.clone(),
            snapshot_addr: self.snapshot_addr.clone(),
            wal_dir: self.wal_dir.clone(),
            snapshot_dir: self.snapshot_dir.clone(),
            role: self.role,
            config: self.to_raft_config(),
            peers: self.peers.clone(),
        }
    }

    pub fn reorder_queue_options(&self) -> MatrixRaftReorderQueueOptions {
        MatrixRaftReorderQueueOptions {
            enable_reorder_queue: self.enable_reorder_queue,
            reorder_timeout_us: self.reorder_timeout_us,
            reorder_window_size: self.reorder_window_size,
        }
    }

    pub fn inflight_options(&self) -> MatrixRaftInflightOptions {
        MatrixRaftInflightOptions {
            max_inflights_apply_task: self.max_inflights_apply_task,
            max_inflights_replicate: self.max_inflights_replicate,
        }
    }

    pub fn snapshot_recycle_options(&self) -> MatrixRaftSnapshotRecycleOptions {
        MatrixRaftSnapshotRecycleOptions {
            can_trigger_snapshot: self.can_trigger_snapshot,
            max_applied_log_bytes: self.max_applied_log_bytes,
            send_snapshot_timeout_ms: self.send_snapshot_timeout_ms,
        }
    }

    pub fn to_pipeline_limits(&self) -> PipelineLimits {
        PipelineLimits {
            max_inflights_replicate: self.max_inflights_replicate.max(1),
            max_memory_replicate_log_bytes: self.max_memory_replicate_log_bytes.max(1),
            max_inflights_apply_task: self.max_inflights_apply_task.max(1),
            max_apply_batch_bytes: self.max_apply_batch_bytes.max(1),
            enable_reorder_queue: self.enable_reorder_queue,
            reorder_window_size: self.reorder_window_size.max(1),
            reorder_timeout_us: self.reorder_timeout_us,
        }
    }

    pub fn create_node(&self, start_index: LogIndex) -> Result<MatrixRaftNode, RaftError> {
        MatrixRaftNode::create(self.to_node_options(), start_index)
    }
}

#[derive(Debug)]
pub struct MatrixRaftNode {
    runtime: NodeRuntime,
    peers: BTreeMap<NodeId, Peer>,
    start_index: LogIndex,
    recover_fsm_from_snapshot: bool,
    callback_scheduler: std::rc::Rc<std::cell::RefCell<MatrixRaftCallbackScheduler>>,
    next_callback_request_id: std::cell::Cell<u64>,
}

impl MatrixRaftNode {
    pub fn create(
        options: NodeOptions,
        start_index: LogIndex,
    ) -> Result<Self, RaftError> {
        let mut peers: BTreeMap<_, _> = options
            .peers
            .iter()
            .cloned()
            .map(|peer| (peer.node_id, peer))
            .collect();
        peers.entry(options.node_id).or_insert_with(|| Peer {
            node_id: options.node_id,
            raft_addr: options.raft_addr.clone(),
            snapshot_addr: options.snapshot_addr.clone(),
            role: options.role,
            auto_promote: false,
        });
        Ok(Self {
            runtime: NodeRuntime::create(options)?,
            peers,
            start_index,
            recover_fsm_from_snapshot: false,
            callback_scheduler: std::rc::Rc::new(std::cell::RefCell::new(
                MatrixRaftCallbackScheduler::new(),
            )),
            next_callback_request_id: std::cell::Cell::new(1),
        })
    }

    pub fn start(&mut self, start_index: LogIndex) -> Result<(), RaftError> {
        self.start_index = start_index;
        self.runtime.start()
    }

    pub fn restart(&mut self, recover_fsm_from_snapshot: bool) -> Result<(), RaftError> {
        self.recover_fsm_from_snapshot = recover_fsm_from_snapshot;
        self.runtime.restart()
    }

    pub fn stop(&mut self) -> Result<(), RaftError> {
        self.runtime.stop()
    }

    pub fn shutdown(&mut self) -> Result<(), RaftError> {
        self.runtime.shutdown()
    }

    pub fn in_lease(&self, term: Option<Term>) -> Result<bool, RaftError> {
        let status = self.get_status()?;
        Ok(status.role == StateRole::Leader
            && status.leader_lease_valid
            && term.is_none_or(|term| term == status.term))
    }

    pub fn group_id(&self) -> GroupId {
        self.runtime.group_id()
    }

    pub fn node_id(&self) -> NodeId {
        self.runtime.node_id()
    }

    pub fn callback_scheduler(&self) -> MatrixRaftCallbackScheduler {
        self.callback_scheduler.borrow().clone()
    }

    pub fn callback_scheduler_len(&self) -> usize {
        self.callback_scheduler.borrow().len()
    }

    pub fn callback_scheduler_next_timeout_ms(&self, now_ms: u64) -> u64 {
        self.callback_scheduler.borrow().next_timeout_ms(now_ms)
    }

    pub fn drain_lapsed_callbacks(&self, now_ms: u64, limit: usize) -> Vec<MatrixRaftAsyncResult> {
        self.callback_scheduler.borrow_mut().lapsed(now_ms, limit)
    }

    pub fn cancel_callback(&self, request_id: u64) -> Option<MatrixRaftAsyncResult> {
        self.callback_scheduler
            .borrow_mut()
            .cancel(self.node_id(), request_id)
            .map(|scheduled| scheduled.completed_result())
    }

    pub fn propose(&self, data: Payload) -> Result<LogId, RaftError> {
        self.propose_with_options(MatrixRaftProposeOptions::default(), data)
    }

    pub fn propose_with_options(
        &self,
        options: MatrixRaftProposeOptions,
        data: Payload,
    ) -> Result<LogId, RaftError> {
        if options.is_command && self.local_replica_role() == Some(ReplicaRole::Witness) {
            return Err(RaftError::InvalidRequest(
                "matrixraft witness node ignores normal command proposals".to_string(),
            ));
        }
        self.runtime.propose_with_options(data, options.into())
    }

    pub fn propose_callback<F>(
        &self,
        data: Payload,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        self.propose_with_options_callback(
            MatrixRaftProposeOptions::default(),
            data,
            callback,
            timeout_ms,
        )
    }

    pub fn propose_with_options_callback<F>(
        &self,
        options: MatrixRaftProposeOptions,
        data: Payload,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::Propose,
            timeout_ms,
            callback,
            |result, log_id| {
                result.log_id = Some(log_id);
            },
            || self.propose_with_options(options, data),
        )
    }

    pub fn add_node(
        &mut self,
        node_id: MatrixRaftNodeId,
    ) -> Result<MembershipExecutionReport, RaftError> {
        let peer = self.peer_from_node_id(node_id, ReplicaRole::Voter, false);
        let report = self.execute_membership(MembershipOperation::AddNode(peer.clone()))?;
        if report.success {
            self.peers.insert(peer.node_id, peer);
        }
        Ok(report)
    }

    pub fn add_node_callback<F>(
        &mut self,
        node_id: MatrixRaftNodeId,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::AddNode,
            timeout_ms,
            callback,
            |result, report: MembershipExecutionReport| {
                result.ok = report.success;
                result.membership = Some(report);
            },
            || self.add_node(node_id),
        )
    }

    pub fn add_learner(
        &mut self,
        node_id: MatrixRaftNodeId,
        auto_promote: bool,
    ) -> Result<MembershipExecutionReport, RaftError> {
        let peer = self.peer_from_node_id(node_id, ReplicaRole::Learner, auto_promote);
        let report = self.execute_membership(MembershipOperation::AddLearner(peer.clone()))?;
        if report.success {
            self.peers.insert(peer.node_id, peer);
        }
        Ok(report)
    }

    pub fn add_learner_callback<F>(
        &mut self,
        node_id: MatrixRaftNodeId,
        auto_promote: bool,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::AddLearner,
            timeout_ms,
            callback,
            |result, report: MembershipExecutionReport| {
                result.ok = report.success;
                result.membership = Some(report);
            },
            || self.add_learner(node_id, auto_promote),
        )
    }

    pub fn add_witness(
        &mut self,
        node_id: MatrixRaftNodeId,
    ) -> Result<MembershipExecutionReport, RaftError> {
        let peer = self.peer_from_node_id(node_id, ReplicaRole::Witness, false);
        let report = self.execute_membership(MembershipOperation::AddWitness(peer.clone()))?;
        if report.success {
            self.peers.insert(peer.node_id, peer);
        }
        Ok(report)
    }

    pub fn add_witness_callback<F>(
        &mut self,
        node_id: MatrixRaftNodeId,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::AddWitness,
            timeout_ms,
            callback,
            |result, report: MembershipExecutionReport| {
                result.ok = report.success;
                result.membership = Some(report);
            },
            || self.add_witness(node_id),
        )
    }

    pub fn promote(
        &mut self,
        node_id: NodeId,
    ) -> Result<MembershipExecutionReport, RaftError> {
        let report = self.promote_after_catch_up(node_id)?.membership;
        Ok(report)
    }

    pub fn catch_up_peer(
        &self,
        peer_id: NodeId,
    ) -> Result<LearnerCatchUpLoopReport, RaftError> {
        self.runtime.catch_up_peer(peer_id)
    }

    pub fn promote_after_catch_up(
        &mut self,
        node_id: NodeId,
    ) -> Result<MatrixRaftPromoteReport, RaftError> {
        let catch_up = self.catch_up_peer(node_id)?;
        if !catch_up.caught_up {
            return Err(RaftError::InvalidRequest(format!(
                "matrixraft learner {} cannot be promoted before catch-up: {}",
                node_id, catch_up.reason
            )));
        }
        let report = self.execute_membership(MembershipOperation::Promote(node_id))?;
        if report.success {
            if let Some(peer) = self.peers.get_mut(&node_id) {
                peer.role = ReplicaRole::Voter;
                peer.auto_promote = false;
            }
        }
        Ok(MatrixRaftPromoteReport {
            learner_id: node_id,
            catch_up,
            promoted: report.success,
            membership: report,
        })
    }

    pub fn auto_promote_learner(
        &mut self,
        node_id: NodeId,
    ) -> Result<LearnerAutoPromoteReport, RaftError> {
        let report = self.runtime.auto_promote_learner(node_id)?;
        if report.promoted {
            if let Some(peer) = self.peers.get_mut(&node_id) {
                peer.role = ReplicaRole::Voter;
                peer.auto_promote = false;
            }
        }
        Ok(report)
    }

    pub fn auto_promote_learner_callback<F>(
        &mut self,
        node_id: NodeId,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::AutoPromoteLearner,
            timeout_ms,
            callback,
            |result, report: LearnerAutoPromoteReport| {
                result.ok = report.promoted;
                result.auto_promote = Some(report);
            },
            || self.auto_promote_learner(node_id),
        )
    }

    pub fn promote_callback<F>(
        &mut self,
        node_id: MatrixRaftNodeId,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::Promote,
            timeout_ms,
            callback,
            |result, report: MembershipExecutionReport| {
                result.ok = report.success;
                result.membership = Some(report);
            },
            || self.promote(node_id.peer_id),
        )
    }

    pub fn remove_node(
        &mut self,
        node_id: NodeId,
    ) -> Result<MembershipExecutionReport, RaftError> {
        Ok(self.remove_node_with_report(node_id)?.membership)
    }

    pub fn remove_node_with_report(
        &mut self,
        node_id: NodeId,
    ) -> Result<MatrixRaftRemoveReport, RaftError> {
        let removed_node = self.peers.get(&node_id).map(MatrixRaftNodeId::from);
        let report = self.execute_membership(MembershipOperation::Remove(node_id))?;
        let removed_conf_state = if report.before.voters.contains(&node_id) {
            Some(MatrixRaftConfState::Voter)
        } else if report.before.learners.contains(&node_id) {
            Some(MatrixRaftConfState::Learner)
        } else if report.before.witnesses.contains(&node_id) {
            Some(MatrixRaftConfState::Witness)
        } else {
            None
        };
        if report.success {
            self.peers.remove(&node_id);
        }
        Ok(MatrixRaftRemoveReport {
            removed_id: node_id,
            removed_node,
            removed_conf_state,
            removed: report.success,
            membership: report,
        })
    }

    pub fn remove_node_callback<F>(
        &mut self,
        node_id: MatrixRaftNodeId,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::RemoveNode,
            timeout_ms,
            callback,
            |result, report: MatrixRaftRemoveReport| {
                result.ok = report.removed;
                result.membership = Some(report.membership.clone());
                result.remove = Some(report);
            },
            || self.remove_node_with_report(node_id.peer_id),
        )
    }

    pub fn read_index(
        &self,
        min_commit_index: LogIndex,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.read_index_with_options(MatrixRaftReadIndexOptions::lease_read(min_commit_index))
    }

    pub fn lease_read_index(
        &self,
        min_commit_index: LogIndex,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.read_index_with_options(MatrixRaftReadIndexOptions::lease_read(min_commit_index))
    }

    pub fn quorum_read_index(
        &self,
        min_commit_index: LogIndex,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.read_index_with_options(MatrixRaftReadIndexOptions::quorum_read(min_commit_index))
    }

    pub fn read_index_with_options(
        &self,
        options: MatrixRaftReadIndexOptions,
    ) -> Result<ReadIndexResponse, RaftError> {
        let request = options.into_request(self.group_id(), self.node_id());
        self.runtime.read_index_request(request)
    }

    pub fn bounded_stale_read_index(
        &self,
        min_commit_index: LogIndex,
        max_stale_index_lag: LogIndex,
    ) -> Result<ReadPathReport, RaftError> {
        self.bounded_stale_read_index_with_options(MatrixRaftBoundedStaleReadOptions::new(
            min_commit_index,
            max_stale_index_lag,
        ))
    }

    pub fn bounded_stale_read_index_with_options(
        &self,
        options: MatrixRaftBoundedStaleReadOptions,
    ) -> Result<ReadPathReport, RaftError> {
        self.runtime
            .bounded_stale_read_index(options.min_commit_index, options.max_stale_index_lag)
    }

    pub fn read_index_callback<F>(&self, callback: F, timeout_ms: u64) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        let min_commit_index = self
            .get_status()
            .map(|status| status.commit_index)
            .unwrap_or_default();
        self.read_index_with_options_callback(
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
            callback,
            timeout_ms,
        )
    }

    pub fn read_index_with_min_callback<F>(
        &self,
        min_commit_index: LogIndex,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        self.read_index_with_options_callback(
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
            callback,
            timeout_ms,
        )
    }

    pub fn lease_read_index_callback<F>(
        &self,
        min_commit_index: LogIndex,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        self.read_index_with_options_callback(
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
            callback,
            timeout_ms,
        )
    }

    pub fn quorum_read_index_callback<F>(
        &self,
        min_commit_index: LogIndex,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        self.read_index_with_options_callback(
            MatrixRaftReadIndexOptions::quorum_read(min_commit_index),
            callback,
            timeout_ms,
        )
    }

    pub fn read_index_with_options_callback<F>(
        &self,
        options: MatrixRaftReadIndexOptions,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::ReadIndex,
            timeout_ms,
            callback,
            |result, response: ReadIndexResponse| {
                result.ok = response.safe;
                result.read_index = Some(response);
            },
            || self.read_index_with_options(options),
        )
    }

    pub fn campaign(&self) -> Result<(), RaftError> {
        self.runtime.campaign(false)
    }

    pub fn campaign_callback<F>(&self, callback: F, timeout_ms: u64) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::Campaign,
            timeout_ms,
            callback,
            |_, ()| {},
            || self.campaign(),
        )
    }

    pub fn forced_campaign(&self) -> Result<(), RaftError> {
        self.runtime.campaign(true)
    }

    pub fn forced_campaign_callback<F>(&self, callback: F, timeout_ms: u64) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::ForcedCampaign,
            timeout_ms,
            callback,
            |_, ()| {},
            || self.forced_campaign(),
        )
    }

    pub fn transfer_leader(&self, transferee: NodeId) -> Result<(), RaftError> {
        if let Some(peer) = self.peers.get(&transferee) {
            if !peer.role.can_be_leader() {
                return Err(RaftError::InvalidRequest(format!(
                    "matrixraft leader transfer target must be voter, got {:?}",
                    peer.role
                )));
            }
        }
        self.runtime.transfer_leader(transferee)
    }

    pub fn transfer_leader_with_report(
        &self,
        transferee: NodeId,
    ) -> Result<MatrixRaftTransferLeaderReport, RaftError> {
        let transferee_node = self.peers.get(&transferee).map(MatrixRaftNodeId::from);
        if let Some(peer) = self.peers.get(&transferee) {
            if !peer.role.can_be_leader() {
                return Err(RaftError::InvalidRequest(format!(
                    "matrixraft leader transfer target must be voter, got {:?}",
                    peer.role
                )));
            }
        }
        // `transferred` reports whether leadership moved, not whether the call
        // returned Ok. An unknown or ineligible transferee is ignored and a
        // lagging one is queued; both of those are Ok and neither is a
        // transfer. The runtime classifies the outcome in the same step that
        // performs it, so this cannot be invalidated by anything happening in
        // between.
        let outcome = self.runtime.transfer_leader_outcome(transferee)?;
        Ok(MatrixRaftTransferLeaderReport {
            transferee_id: transferee,
            transferee_node,
            state: self.runtime.leader_transfer_state()?,
            transferred: outcome.is_transferred(),
            outcome,
        })
    }

    pub fn transfer_leader_callback<F>(
        &self,
        transferee: NodeId,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::TransferLeader,
            timeout_ms,
            callback,
            |result, report: MatrixRaftTransferLeaderReport| {
                result.ok = report.transferred;
                result.transfer_leader = Some(report);
            },
            || self.transfer_leader_with_report(transferee),
        )
    }

    pub fn timeout_now(
        &self,
        from: NodeId,
        target: NodeId,
    ) -> Result<TimeoutNowResponse, RaftError> {
        self.runtime.timeout_now(from, target)
    }

    pub fn timeout_now_callback<F>(
        &self,
        from: NodeId,
        target: NodeId,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::TimeoutNow,
            timeout_ms,
            callback,
            |result, response: TimeoutNowResponse| {
                result.ok = response.campaigned;
                result.timeout_now = Some(response);
            },
            || self.timeout_now(from, target),
        )
    }

    pub fn step_down(
        &self,
        transferee: Option<NodeId>,
    ) -> Result<MatrixRaftStepDownReport, RaftError> {
        let selected = self.runtime.step_down(transferee)?;
        let transferee_node = selected.and_then(|node_id| self.peers.get(&node_id).map(MatrixRaftNodeId::from));
        Ok(MatrixRaftStepDownReport {
            requested_transferee_id: transferee,
            transferee_id: selected,
            transferee_node,
            state: self.runtime.leader_transfer_state()?,
            stepped_down: selected.is_some(),
        })
    }

    pub fn step_down_callback<F>(
        &self,
        transferee: Option<NodeId>,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::StepDown,
            timeout_ms,
            callback,
            |result, report: MatrixRaftStepDownReport| {
                result.ok = report.stepped_down;
                result.step_down = Some(report);
            },
            || self.step_down(transferee),
        )
    }

    pub fn resign_leader(&self, reason: impl Into<String>) -> Result<MatrixRaftResignReport, RaftError> {
        let reason = reason.into();
        let leader_before = self.leader()?;
        let resigned = self.runtime.resign_leader(reason.clone())?;
        let leader_after = self.leader()?;
        Ok(MatrixRaftResignReport {
            reason,
            leader_before,
            leader_after,
            resigned,
        })
    }

    pub fn resign_leader_callback<F>(
        &self,
        reason: impl Into<String>,
        callback: F,
        timeout_ms: u64,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        let reason = reason.into();
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::ResignLeader,
            timeout_ms,
            callback,
            |result, report: MatrixRaftResignReport| {
                result.ok = report.resigned;
                result.resign = Some(report);
            },
            || self.resign_leader(reason),
        )
    }

    pub fn async_snapshot(&self) -> Result<SnapshotMetadata, RaftError> {
        self.runtime.trigger_snapshot()
    }

    pub fn async_snapshot_ready(
        &self,
        snapshot_id: impl AsRef<str>,
        success: bool,
    ) -> Result<(), RaftError> {
        self.runtime
            .mark_snapshot_ready(snapshot_id.as_ref(), success)
    }

    pub fn async_snapshot_applied(&self, snapshot_id: impl AsRef<str>) -> Result<(), RaftError> {
        self.runtime.complete_snapshot_trigger(snapshot_id.as_ref())
    }

    fn snapshot_peer_report(
        &self,
        peer_id: NodeId,
    ) -> Result<MatrixRaftSnapshotPeerReport, RaftError> {
        let runtime_status = self.runtime.status()?;
        let peer_status = runtime_status
            .cluster_status
            .as_ref()
            .and_then(|cluster| {
                cluster
                    .nodes
                    .iter()
                    .find(|node| node.node_id == self.runtime.node_id())
            })
            .and_then(|local| local.peers.iter().find(|peer| peer.node_id == peer_id));
        Ok(MatrixRaftSnapshotPeerReport {
            peer_id,
            status: self.runtime.peer_pipeline_status(peer_id)?,
            peer_healthy: peer_status.map(|peer| peer.healthy),
            peer_lag: peer_status.map(|peer| peer.lag),
        })
    }

    pub fn begin_snapshot_send_to(
        &self,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<MatrixRaftSnapshotPeerReport, RaftError> {
        self.runtime.begin_snapshot_send_to(
            peer_id,
            snapshot_id,
            snapshot_index,
            total_chunks,
        )?;
        self.snapshot_peer_report(peer_id)
    }

    pub fn record_snapshot_chunk_sent_to(
        &self,
        peer_id: NodeId,
        bytes: u64,
    ) -> Result<MatrixRaftSnapshotPeerReport, RaftError> {
        self.runtime.record_snapshot_chunk_sent_to(peer_id, bytes)?;
        self.snapshot_peer_report(peer_id)
    }

    pub fn acknowledge_snapshot_chunk_to(
        &self,
        peer_id: NodeId,
    ) -> Result<MatrixRaftSnapshotPeerReport, RaftError> {
        self.runtime.acknowledge_snapshot_chunk_to(peer_id)?;
        self.snapshot_peer_report(peer_id)
    }

    pub fn retry_snapshot_chunk_to(
        &self,
        peer_id: NodeId,
    ) -> Result<MatrixRaftSnapshotPeerReport, RaftError> {
        self.runtime.retry_snapshot_chunk_to(peer_id)?;
        self.snapshot_peer_report(peer_id)
    }

    pub fn cancel_snapshot_send_to(
        &self,
        peer_id: NodeId,
    ) -> Result<MatrixRaftSnapshotPeerReport, RaftError> {
        self.runtime.cancel_snapshot_send_to(peer_id)?;
        self.snapshot_peer_report(peer_id)
    }

    pub fn begin_snapshot_install_from(
        &self,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<MatrixRaftSnapshotPeerReport, RaftError> {
        self.runtime.begin_snapshot_install_from(
            peer_id,
            snapshot_id,
            snapshot_index,
            total_chunks,
        )?;
        self.snapshot_peer_report(peer_id)
    }

    pub fn receive_snapshot_chunk_from(
        &self,
        peer_id: NodeId,
        bytes: u64,
        done: bool,
    ) -> Result<MatrixRaftSnapshotPeerReport, RaftError> {
        self.runtime
            .receive_snapshot_chunk_from(peer_id, bytes, done)?;
        self.snapshot_peer_report(peer_id)
    }

    pub fn rollback_snapshot_install_from(
        &self,
        peer_id: NodeId,
    ) -> Result<MatrixRaftSnapshotPeerReport, RaftError> {
        self.runtime.rollback_snapshot_install_from(peer_id)?;
        self.snapshot_peer_report(peer_id)
    }

    pub fn install_snapshot_to(
        &self,
        target: NodeId,
        snapshot: RaftSnapshot,
        fence: ApplySnapshotFence,
    ) -> Result<(), RaftError> {
        self.runtime.install_snapshot_to(target, snapshot, fence)
    }

    pub fn async_snapshot_callback<F>(&self, callback: F) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        Self::finish_callback(self.callback_scheduler.clone(), self.node_id(), self.next_callback_request_id(),
            MatrixRaftAsyncOperation::AsyncSnapshot,
            0,
            callback,
            |result, snapshot| {
                result.snapshot = Some(snapshot);
            },
            || self.async_snapshot(),
        )
    }

    pub fn resolve_address(&self, peer_id: NodeId) -> Result<MatrixRaftNodeId, RaftError> {
        self.peers
            .get(&peer_id)
            .map(MatrixRaftNodeId::from)
            .ok_or(RaftError::NodeNotFound(peer_id))
    }

    pub fn leader(&self) -> Result<Option<NodeId>, RaftError> {
        Ok(self.get_status()?.leader_id)
    }

    pub fn leader_node(&self) -> Result<Option<MatrixRaftNodeId>, RaftError> {
        self.leader()?
            .map(|leader_id| self.resolve_address(leader_id))
            .transpose()
    }

    pub fn get_status(&self) -> Result<MatrixRaftStatus, RaftError> {
        let status = self.runtime.status()?;
        let cluster = status.cluster_status.ok_or_else(|| {
            RaftError::InvalidRequest("raft node runtime has no cluster status".to_string())
        })?;
        let local = cluster
            .nodes
            .iter()
            .find(|node| node.node_id == self.runtime.node_id())
            .ok_or(RaftError::NodeNotFound(self.runtime.node_id()))?;
        let membership = self.membership_from_cluster_status(&cluster);
        Ok(MatrixRaftStatus {
            node_id: local.node_id,
            group_id: local.group_id,
            role: local.role,
            term: local.term,
            leader_id: local.leader_id,
            leader_lease_valid: status.timer_status.leader_lease_valid,
            commit_index: local.commit_index,
            applied_index: local.applied_index,
            last_log_index: local.last_log_index,
            membership,
        })
    }

    pub fn get_local_status(&self) -> Result<MatrixRaftLocalStatus, RaftError> {
        let status = self.runtime.status()?;
        Ok(MatrixRaftLocalStatus {
            node_id: status.node_id,
            group_id: status.group_id,
            state: status.state,
            restart_count: status.restart_count,
            worker_running: status.worker_running,
        })
    }

    pub fn get_node_id(&self) -> MatrixRaftNodeId {
        self.peers
            .get(&self.runtime.node_id())
            .map(MatrixRaftNodeId::from)
            .unwrap_or(MatrixRaftNodeId {
                peer_id: self.runtime.node_id(),
                raft_addr: String::new(),
                snapshot_addr: String::new(),
            })
    }

    pub fn get_membership(&self) -> Result<Vec<MatrixRaftNodeId>, RaftError> {
        Ok(self
            .get_membership_members()?
            .into_iter()
            .map(|member| MatrixRaftNodeId {
                peer_id: member.id,
                raft_addr: member.raft_addr,
                snapshot_addr: member.snapshot_addr,
            })
            .collect())
    }

    pub fn get_membership_members(&self) -> Result<Vec<MatrixRaftMemberId>, RaftError> {
        let membership = self.get_status()?.membership;
        Ok(membership
            .voters
            .into_iter()
            .map(|peer_id| (peer_id, MatrixRaftConfState::Voter))
            .chain(
                membership
                    .learners
                    .into_iter()
                    .map(|peer_id| (peer_id, MatrixRaftConfState::Learner)),
            )
            .chain(
                membership
                    .witnesses
                    .into_iter()
                    .map(|peer_id| (peer_id, MatrixRaftConfState::Witness)),
            )
            .filter_map(|(peer_id, conf_state)| {
                self.peers.get(&peer_id).map(|peer| MatrixRaftMemberId {
                    id: peer.node_id,
                    raft_addr: peer.raft_addr.clone(),
                    snapshot_addr: peer.snapshot_addr.clone(),
                    is_from_options: true,
                    conf_state,
                    auto_promote: conf_state == MatrixRaftConfState::Learner && peer.auto_promote,
                })
            })
            .collect())
    }

    pub fn sync_fsm_runtime<F>(
        &self,
        binding: &mut MatrixRaftFsmRuntimeBinding<F>,
    ) -> Result<MatrixRaftFsmRuntimeHookReport, RaftError>
    where
        F: MatrixRaftFsm,
    {
        let status = self.get_status()?;
        let membership = self.get_membership()?;
        binding.observe_status(&status, membership)
    }

    pub fn alter_attribute(
        &self,
        attribute: MatrixRaftAttribute,
        value: bool,
    ) -> Result<(), RaftError> {
        match attribute {
            MatrixRaftAttribute::ProhibitsElection => self.runtime.set_prohibits_election(value),
            MatrixRaftAttribute::IgnoreWitness => self.runtime.set_ignore_witness(value),
        }
    }

    pub fn set_leader_lease_valid(&self, valid: bool) -> Result<(), RaftError> {
        self.runtime.set_leader_lease_valid(valid)
    }

    /// Advances the leader-lease clock by `elapsed_ms`, reporting whether the
    /// lease has now expired. The multi-raft server has had
    /// `tick_leader_lease_on_node` for a while; this is the single-node
    /// equivalent.
    pub fn tick_leader_lease(&self, elapsed_ms: u64) -> Result<bool, RaftError> {
        self.runtime.tick_leader_lease(elapsed_ms)
    }

    /// Advances the follower-lease clock by `elapsed_ms`, reporting whether the
    /// lease has now expired.
    ///
    /// Without this there is no way to expire a *follower* lease through this
    /// facade, which matters because a node inside one refuses to campaign
    /// (`InvalidRequest("follower is still in leader lease")`).
    pub fn tick_follower_lease(&self, elapsed_ms: u64) -> Result<bool, RaftError> {
        self.runtime.tick_follower_lease(elapsed_ms)
    }

    pub fn fire_fatal_event(
        &self,
        node_id: NodeId,
        reason: impl Into<String>,
    ) -> Result<Option<NodeId>, RaftError> {
        self.runtime.fire_fatal_event(node_id, reason)
    }

    pub fn get_fatal_blockers(&self) -> Result<Vec<Blocker>, RaftError> {
        Ok(self
            .runtime
            .status()?
            .fatal_blocker_report
            .blockers)
    }

    pub fn get_fatal_events(&self) -> Result<Vec<MatrixRaftFatalEvent>, RaftError> {
        Ok(self
            .get_fatal_blockers()?
            .into_iter()
            .map(MatrixRaftFatalEvent::from)
            .collect())
    }

    pub fn start_index(&self) -> LogIndex {
        self.start_index
    }

    pub fn recover_fsm_from_snapshot(&self) -> bool {
        self.recover_fsm_from_snapshot
    }

    pub fn into_runtime(self) -> NodeRuntime {
        self.runtime
    }

    fn execute_membership(
        &self,
        operation: MembershipOperation,
    ) -> Result<MembershipExecutionReport, RaftError> {
        self.runtime.execute_membership_operation(operation)
    }

    fn apply_successful_membership_operation(&mut self, operation: MembershipOperation) {
        match operation {
            MembershipOperation::AddNode(mut peer)
            | MembershipOperation::AddVoter(mut peer) => {
                peer.role = ReplicaRole::Voter;
                peer.auto_promote = false;
                self.peers.insert(peer.node_id, peer);
            }
            MembershipOperation::AddLearner(mut peer) => {
                peer.role = ReplicaRole::Learner;
                self.peers.insert(peer.node_id, peer);
            }
            MembershipOperation::AddWitness(mut peer) => {
                peer.role = ReplicaRole::Witness;
                peer.auto_promote = false;
                self.peers.insert(peer.node_id, peer);
            }
            MembershipOperation::Promote(node_id) => {
                if let Some(peer) = self.peers.get_mut(&node_id) {
                    peer.role = ReplicaRole::Voter;
                    peer.auto_promote = false;
                }
            }
            MembershipOperation::Remove(node_id) => {
                self.peers.remove(&node_id);
            }
            MembershipOperation::TransferLeader(_) => {}
        }
    }

    fn execute_membership_operation(
        &mut self,
        operation: MembershipOperation,
    ) -> Result<MembershipExecutionReport, RaftError> {
        let report = self.execute_membership(operation.clone())?;
        if report.success {
            self.apply_successful_membership_operation(operation);
        }
        Ok(report)
    }

    fn execute_membership_workflow_with_rollback<I>(
        &mut self,
        operations: I,
    ) -> Result<Vec<MembershipExecutionReport>, RaftError>
    where
        I: IntoIterator<Item = MembershipOperation>,
    {
        let reports = self
            .runtime
            .execute_membership_workflow_with_rollback(operations)?;
        for report in &reports {
            if report.success {
                self.apply_successful_membership_operation(report.operation.clone());
            }
        }
        Ok(reports)
    }

    fn finish_callback<T, F, M, O>(
        callback_scheduler: std::rc::Rc<std::cell::RefCell<MatrixRaftCallbackScheduler>>,
        node_id: NodeId,
        request_id: u64,
        operation: MatrixRaftAsyncOperation,
        timeout_ms: u64,
        callback: F,
        mut map_success: M,
        operation_fn: O,
    ) -> MatrixRaftAsyncResult
    where
        F: FnOnce(MatrixRaftAsyncResult),
        M: FnMut(&mut MatrixRaftAsyncResult, T),
        O: FnOnce() -> Result<T, RaftError>,
    {
        let start_at_ms = Self::current_epoch_millis();
        callback_scheduler.borrow_mut().schedule(
            node_id,
            request_id,
            operation,
            start_at_ms,
            timeout_ms,
        );
        let started = Instant::now();
        let mut result = match operation_fn() {
            Ok(value) => {
                let mut result = MatrixRaftAsyncResult::ok(operation, timeout_ms);
                map_success(&mut result, value);
                result
            }
            Err(error) => MatrixRaftAsyncResult::error(operation, timeout_ms, error),
        };
        if timeout_ms > 0 && started.elapsed() > Duration::from_millis(timeout_ms) {
            let deadline_ms = start_at_ms.saturating_add(timeout_ms);
            result = callback_scheduler
                .borrow_mut()
                .lapsed(deadline_ms, usize::MAX)
                .into_iter()
                .find(|result| result.node_id == Some(node_id) && result.request_id == Some(request_id))
                .unwrap_or_else(|| MatrixRaftAsyncResult::timeout(operation, timeout_ms));
        } else if let Some(completed) = callback_scheduler.borrow_mut().complete(node_id, request_id)
        {
            result = result.with_timer_task(&completed.task, completed.operation);
        }
        callback(result.clone());
        result
    }

    fn next_callback_request_id(&self) -> u64 {
        let request_id = self.next_callback_request_id.get();
        self.next_callback_request_id
            .set(request_id.saturating_add(1).max(1));
        request_id
    }

    fn current_epoch_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or_default()
    }

    fn peer_from_node_id(
        &self,
        node_id: MatrixRaftNodeId,
        role: ReplicaRole,
        auto_promote: bool,
    ) -> Peer {
        Peer {
            node_id: node_id.peer_id,
            raft_addr: node_id.raft_addr,
            snapshot_addr: node_id.snapshot_addr,
            role,
            auto_promote,
        }
    }

    fn local_replica_role(&self) -> Option<ReplicaRole> {
        self.peers.get(&self.runtime.node_id()).map(|peer| peer.role)
    }

    fn membership_from_cluster_status(&self, cluster: &ClusterStatusReport) -> Membership {
        let mut membership = Membership {
            group_id: cluster.group_id,
            voters: Vec::new(),
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 0,
        };
        for node in &cluster.nodes {
            let role = self
                .peers
                .get(&node.node_id)
                .map(|peer| peer.role)
                .unwrap_or_else(|| {
                    if node.role == StateRole::Learner {
                        ReplicaRole::Learner
                    } else {
                        ReplicaRole::Voter
                    }
                });
            match role {
                ReplicaRole::Voter => membership.voters.push(node.node_id),
                ReplicaRole::Learner => membership.learners.push(node.node_id),
                ReplicaRole::Witness => membership.witnesses.push(node.node_id),
            }
        }
        membership
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MatrixRaftRouteKey {
    pub group_id: GroupId,
    pub node_id: NodeId,
}

impl MatrixRaftRouteKey {
    pub fn new(group_id: GroupId, node_id: NodeId) -> Self {
        Self { group_id, node_id }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftGroupTopology {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub runtime_wiring_count: usize,
    pub snapshot_route_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftTopology {
    pub group_count: usize,
    pub node_count: usize,
    pub runtime_wiring_count: usize,
    pub snapshot_route_count: usize,
    pub groups: Vec<MatrixRaftGroupTopology>,
}

impl MatrixRaftTopology {
    pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_group(&self) -> Vec<(GroupId, Vec<NodeId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect()
    }

    pub fn counts_by_group(&self) -> Vec<(GroupId, usize, usize, usize)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group.node_count,
                    group.runtime_wiring_count,
                    group.snapshot_route_count,
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftCreateNodePlan {
    pub key: MatrixRaftRouteKey,
    pub start_index: LogIndex,
    pub runtime_wiring: MatrixRaftRuntimeWiring,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftCreateGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub start_indices: Vec<LogIndex>,
    pub node_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftCreateBatchPlan {
    pub creator_index: Option<usize>,
    pub node_count: usize,
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub groups: Vec<MatrixRaftCreateGroupPlan>,
    pub nodes: Vec<MatrixRaftCreateNodePlan>,
}

impl MatrixRaftCreateBatchPlan {
    pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_group(&self) -> Vec<(GroupId, Vec<NodeId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect()
    }

    pub fn start_indices_by_group(&self) -> Vec<(GroupId, Vec<LogIndex>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.start_indices.clone()))
            .collect()
    }

    pub fn node_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_count))
            .collect()
    }

    pub fn route_key_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.len()))
            .collect()
    }

    pub fn fanout_counts_by_group(&self) -> Vec<(GroupId, usize, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_count, group.route_keys.len()))
            .collect()
    }

    pub fn creator_indices_by_group(&self) -> Vec<(GroupId, Vec<Option<usize>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.nodes
                        .iter()
                        .filter(|node| node.key.group_id == group.group_id)
                        .map(|node| node.runtime_wiring.creator_index)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn creator_index_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.creator_indices_by_group())
    }

    pub fn store_ids_by_group(&self) -> Vec<(GroupId, Vec<u64>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.nodes
                        .iter()
                        .filter(|node| node.key.group_id == group.group_id)
                        .map(|node| node.runtime_wiring.store_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn flexible_apply_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.nodes
                        .iter()
                        .filter(|node| node.key.group_id == group.group_id)
                        .map(|node| node.runtime_wiring.flexible_apply)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn heartbeat_merge_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.nodes
                        .iter()
                        .filter(|node| node.key.group_id == group.group_id)
                        .map(|node| node.runtime_wiring.heartbeat_merge)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.nodes
            .iter()
            .map(|node| (node.key, node.key.node_id))
            .collect()
    }

    pub fn runtime_wiring_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.nodes.iter().map(|node| (node.key, true)).collect()
    }

    pub fn creator_indices_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<usize>)> {
        self.nodes
            .iter()
            .map(|node| (node.key, node.runtime_wiring.creator_index))
            .collect()
    }

    pub fn creator_index_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.creator_indices_by_route_key())
    }

    pub fn store_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, u64)> {
        self.nodes
            .iter()
            .map(|node| (node.key, node.runtime_wiring.store_id))
            .collect()
    }

    pub fn flexible_apply_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.nodes
            .iter()
            .map(|node| (node.key, node.runtime_wiring.flexible_apply))
            .collect()
    }

    pub fn heartbeat_merge_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.nodes
            .iter()
            .map(|node| (node.key, node.runtime_wiring.heartbeat_merge))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftCreateNodeResult {
    pub key: MatrixRaftRouteKey,
    pub start_index: LogIndex,
    pub ok: bool,
    #[serde(default)]
    pub runtime_wiring: Option<MatrixRaftRuntimeWiring>,
    #[serde(default)]
    pub error: Option<String>,
}

impl MatrixRaftCreateNodeResult {
    pub fn ok(
        key: MatrixRaftRouteKey,
        start_index: LogIndex,
        runtime_wiring: MatrixRaftRuntimeWiring,
    ) -> Self {
        Self {
            key,
            start_index,
            ok: true,
            runtime_wiring: Some(runtime_wiring),
            error: None,
        }
    }

    pub fn error(key: MatrixRaftRouteKey, start_index: LogIndex, error: RaftError) -> Self {
        Self {
            key,
            start_index,
            ok: false,
            runtime_wiring: None,
            error: Some(error.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.ok && self.error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftCreateGroupResult {
    pub group_id: GroupId,
    pub node_count: usize,
    pub ok_count: usize,
    pub error_count: usize,
    pub results: Vec<MatrixRaftCreateNodeResult>,
}

impl MatrixRaftCreateGroupResult {
    pub fn is_ok(&self) -> bool {
        self.error_count == 0 && self.ok_count == self.node_count
    }

    pub fn results_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftCreateNodeResult)> {
        self.results
            .iter()
            .map(|result| (result.key, result.clone()))
            .collect()
    }

    pub fn ok_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftCreateNodeResult)> {
        self.results
            .iter()
            .filter(|result| result.is_ok())
            .map(|result| (result.key, result.clone()))
            .collect()
    }

    pub fn error_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftCreateNodeResult)> {
        self.results
            .iter()
            .filter(|result| !result.is_ok())
            .map(|result| (result.key, result.clone()))
            .collect()
    }

    pub fn start_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.results
            .iter()
            .map(|result| (result.key, result.start_index))
            .collect()
    }

    pub fn runtime_wiring_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.results
            .iter()
            .map(|result| (result.key, result.runtime_wiring.is_some()))
            .collect()
    }

    pub fn creator_indices_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<usize>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .runtime_wiring
                        .as_ref()
                        .and_then(|wiring| wiring.creator_index),
                )
            })
            .collect()
    }

    pub fn creator_index_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.creator_indices_by_route_key())
    }

    pub fn store_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result.runtime_wiring.as_ref().map(|wiring| wiring.store_id),
                )
            })
            .collect()
    }

    pub fn store_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.store_ids_by_route_key())
    }

    pub fn flexible_apply_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .runtime_wiring
                        .as_ref()
                        .map(|wiring| wiring.flexible_apply),
                )
            })
            .collect()
    }

    pub fn flexible_apply_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.flexible_apply_by_route_key())
    }

    pub fn heartbeat_merge_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .runtime_wiring
                        .as_ref()
                        .map(|wiring| wiring.heartbeat_merge),
                )
            })
            .collect()
    }

    pub fn heartbeat_merge_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.heartbeat_merge_by_route_key())
    }
}

macro_rules! impl_matrixraft_group_result_route_accessors {
    ($group_result:ty) => {
        impl $group_result {
            pub fn route_keys(&self) -> Vec<MatrixRaftRouteKey> {
                self.results.iter().map(|result| result.key).collect()
            }

            pub fn ok_route_keys(&self) -> Vec<MatrixRaftRouteKey> {
                self.results
                    .iter()
                    .filter(|result| result.is_ok())
                    .map(|result| result.key)
                    .collect()
            }

            pub fn error_route_keys(&self) -> Vec<MatrixRaftRouteKey> {
                self.results
                    .iter()
                    .filter(|result| !result.is_ok())
                    .map(|result| result.key)
                    .collect()
            }

            pub fn node_ids(&self) -> Vec<NodeId> {
                self.results
                    .iter()
                    .map(|result| result.key.node_id)
                    .collect()
            }

            pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
                self.results
                    .iter()
                    .map(|result| (result.key, result.key.node_id))
                    .collect()
            }

            pub fn ok_node_ids(&self) -> Vec<NodeId> {
                self.results
                    .iter()
                    .filter(|result| result.is_ok())
                    .map(|result| result.key.node_id)
                    .collect()
            }

            pub fn ok_node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
                self.results
                    .iter()
                    .filter(|result| result.is_ok())
                    .map(|result| (result.key, result.key.node_id))
                    .collect()
            }

            pub fn error_node_ids(&self) -> Vec<NodeId> {
                self.results
                    .iter()
                    .filter(|result| !result.is_ok())
                    .map(|result| result.key.node_id)
                    .collect()
            }

            pub fn error_node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
                self.results
                    .iter()
                    .filter(|result| !result.is_ok())
                    .map(|result| (result.key, result.key.node_id))
                    .collect()
            }

            pub fn statuses_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
                self.results
                    .iter()
                    .map(|result| (result.key, result.is_ok()))
                    .collect()
            }

            pub fn errors_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
                self.results
                    .iter()
                    .map(|result| (result.key, result.error.clone()))
                    .collect()
            }
        }
    };
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftUnregisterGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub runtime_wiring_count: usize,
    pub snapshot_route_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftUnregisterBatchPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub runtime_wiring_count: usize,
    pub snapshot_route_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub groups: Vec<MatrixRaftUnregisterGroupPlan>,
}

impl MatrixRaftUnregisterBatchPlan {
    pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_group(&self) -> Vec<(GroupId, Vec<NodeId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect()
    }

    pub fn route_key_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.len()))
            .collect()
    }

    pub fn counts_by_group(&self) -> Vec<(GroupId, usize, usize, usize)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group.node_count,
                    group.runtime_wiring_count,
                    group.snapshot_route_count,
                )
            })
            .collect()
    }

    pub fn unregister_counts_by_group(
        &self,
    ) -> Vec<(GroupId, usize, usize, usize, usize)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group.node_count,
                    group.route_keys.len(),
                    group.runtime_wiring_count,
                    group.snapshot_route_count,
                )
            })
            .collect()
    }

    pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, route_key.node_id))
            })
            .collect()
    }

    pub fn runtime_wiring_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group.runtime_wiring_count > 0
                            && group.runtime_wiring_count >= group.route_keys.len(),
                    )
                })
            })
            .collect()
    }

    pub fn snapshot_route_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group.snapshot_route_count > 0
                            && group.snapshot_route_count >= group.route_keys.len(),
                    )
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftUnregisterGroupResult {
    pub group_id: GroupId,
    pub ok: bool,
    pub node_count: usize,
    pub runtime_wiring_count: usize,
    pub snapshot_route_count: usize,
    pub removed_node_ids: Vec<NodeId>,
    pub removed_route_keys: Vec<MatrixRaftRouteKey>,
    #[serde(default)]
    pub error: Option<String>,
}

impl MatrixRaftUnregisterGroupResult {
    pub fn ok(plan: MatrixRaftUnregisterGroupPlan) -> Self {
        Self {
            group_id: plan.group_id,
            ok: true,
            node_count: plan.node_count,
            runtime_wiring_count: plan.runtime_wiring_count,
            snapshot_route_count: plan.snapshot_route_count,
            removed_node_ids: plan.node_ids,
            removed_route_keys: plan.route_keys,
            error: None,
        }
    }

    pub fn error(group_id: GroupId, error: RaftError) -> Self {
        Self {
            group_id,
            ok: false,
            node_count: 0,
            runtime_wiring_count: 0,
            snapshot_route_count: 0,
            removed_node_ids: Vec::new(),
            removed_route_keys: Vec::new(),
            error: Some(error.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.ok && self.error.is_none()
    }

    pub fn result_by_group_id(&self) -> (GroupId, MatrixRaftUnregisterGroupResult) {
        (self.group_id, self.clone())
    }

    pub fn ok_result_by_group_id(&self) -> Option<(GroupId, MatrixRaftUnregisterGroupResult)> {
        self.is_ok().then(|| (self.group_id, self.clone()))
    }

    pub fn error_result_by_group_id(
        &self,
    ) -> Option<(GroupId, MatrixRaftUnregisterGroupResult)> {
        (!self.is_ok()).then(|| (self.group_id, self.clone()))
    }

    pub fn results_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftUnregisterGroupResult)> {
        self.removed_route_keys
            .iter()
            .map(|key| (*key, self.clone()))
            .collect()
    }

    pub fn ok_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftUnregisterGroupResult)> {
        self.removed_route_keys
            .iter()
            .filter(|_| self.is_ok())
            .map(|key| (*key, self.clone()))
            .collect()
    }

    pub fn error_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftUnregisterGroupResult)> {
        self.removed_route_keys
            .iter()
            .filter(|_| !self.is_ok())
            .map(|key| (*key, self.clone()))
            .collect()
    }

    pub fn route_keys(&self) -> Vec<MatrixRaftRouteKey> {
        self.removed_route_keys.clone()
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.removed_node_ids.clone()
    }

    pub fn removal_counts(&self) -> (usize, usize, usize, usize) {
        (
            self.node_count,
            self.removed_route_keys.len(),
            self.runtime_wiring_count,
            self.snapshot_route_count,
        )
    }

    pub fn removed_node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.removed_route_keys
            .iter()
            .map(|key| (*key, key.node_id))
            .collect()
    }

    pub fn runtime_wiring_removed_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.removed_route_keys
            .iter()
            .map(|key| {
                (
                    *key,
                    self.runtime_wiring_count > 0 && self.runtime_wiring_count >= self.removed_route_keys.len(),
                )
            })
            .collect()
    }

    pub fn snapshot_routes_removed_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.removed_route_keys
            .iter()
            .map(|key| {
                (
                    *key,
                    self.snapshot_route_count > 0 && self.snapshot_route_count >= self.removed_route_keys.len(),
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftLifecycleAction {
    Start,
    Stop,
    Restart,
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLifecycleGroupPlan {
    pub action: MatrixRaftLifecycleAction,
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub start_index: Option<LogIndex>,
    pub recover_fsm_from_snapshot: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLifecycleBatchPlan {
    pub action: MatrixRaftLifecycleAction,
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub groups: Vec<MatrixRaftLifecycleGroupPlan>,
    pub start_index: Option<LogIndex>,
    pub recover_fsm_from_snapshot: Option<bool>,
}

impl MatrixRaftLifecycleBatchPlan {
    pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_group(&self) -> Vec<(GroupId, Vec<NodeId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect()
    }

    pub fn node_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_count))
            .collect()
    }

    pub fn route_key_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.len()))
            .collect()
    }

    pub fn fanout_counts_by_group(&self) -> Vec<(GroupId, usize, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_count, group.route_keys.len()))
            .collect()
    }

    pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, route_key.node_id))
            })
            .collect()
    }

    pub fn actions_by_group(&self) -> Vec<(GroupId, MatrixRaftLifecycleAction)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.action))
            .collect()
    }

    pub fn actions_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftLifecycleAction)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.action))
            })
            .collect()
    }

    pub fn start_indices_by_group(&self) -> Vec<(GroupId, Option<LogIndex>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.start_index))
            .collect()
    }

    pub fn start_index_presence_by_group(&self) -> Vec<(GroupId, bool)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.start_index.is_some()))
            .collect()
    }

    pub fn start_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.start_index))
            })
            .collect()
    }

    pub fn start_index_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.start_index.is_some()))
            })
            .collect()
    }

    pub fn recover_fsm_from_snapshot_by_group(&self) -> Vec<(GroupId, Option<bool>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.recover_fsm_from_snapshot))
            .collect()
    }

    pub fn recover_fsm_from_snapshot_presence_by_group(&self) -> Vec<(GroupId, bool)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.recover_fsm_from_snapshot.is_some()))
            .collect()
    }

    pub fn recover_fsm_from_snapshot_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.recover_fsm_from_snapshot))
            })
            .collect()
    }

    pub fn recover_fsm_from_snapshot_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.recover_fsm_from_snapshot.is_some()))
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLifecycleNodeResult {
    pub key: MatrixRaftRouteKey,
    pub action: MatrixRaftLifecycleAction,
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
}

impl MatrixRaftLifecycleNodeResult {
    pub fn ok(key: MatrixRaftRouteKey, action: MatrixRaftLifecycleAction) -> Self {
        Self {
            key,
            action,
            ok: true,
            error: None,
        }
    }

    pub fn error(key: MatrixRaftRouteKey, action: MatrixRaftLifecycleAction, error: RaftError) -> Self {
        Self {
            key,
            action,
            ok: false,
            error: Some(error.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.ok && self.error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLifecycleGroupResult {
    pub group_id: GroupId,
    pub action: MatrixRaftLifecycleAction,
    pub node_count: usize,
    pub ok_count: usize,
    pub error_count: usize,
    pub results: Vec<MatrixRaftLifecycleNodeResult>,
}

impl MatrixRaftLifecycleGroupResult {
    pub fn is_ok(&self) -> bool {
        self.error_count == 0 && self.ok_count == self.node_count
    }

    pub fn actions_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftLifecycleAction)> {
        self.results
            .iter()
            .map(|result| (result.key, result.action))
            .collect()
    }

    pub fn ok_actions_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftLifecycleAction)> {
        self.results
            .iter()
            .filter(|result| result.is_ok())
            .map(|result| (result.key, result.action))
            .collect()
    }

    pub fn error_actions_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftLifecycleAction)> {
        self.results
            .iter()
            .filter(|result| !result.is_ok())
            .map(|result| (result.key, result.action))
            .collect()
    }

    pub fn results_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftLifecycleNodeResult)> {
        self.results
            .iter()
            .map(|result| (result.key, result.clone()))
            .collect()
    }

    pub fn ok_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftLifecycleNodeResult)> {
        self.results
            .iter()
            .filter(|result| result.is_ok())
            .map(|result| (result.key, result.clone()))
            .collect()
    }

    pub fn error_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftLifecycleNodeResult)> {
        self.results
            .iter()
            .filter(|result| !result.is_ok())
            .map(|result| (result.key, result.clone()))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftFsmRuntimeSyncNodeResult {
    pub key: MatrixRaftRouteKey,
    pub ok: bool,
    #[serde(default)]
    pub report: Option<MatrixRaftFsmRuntimeHookReport>,
    #[serde(default)]
    pub error: Option<String>,
}

impl MatrixRaftFsmRuntimeSyncNodeResult {
    pub fn ok(key: MatrixRaftRouteKey, report: MatrixRaftFsmRuntimeHookReport) -> Self {
        Self {
            key,
            ok: true,
            report: Some(report),
            error: None,
        }
    }

    pub fn error(key: MatrixRaftRouteKey, error: RaftError) -> Self {
        Self {
            key,
            ok: false,
            report: None,
            error: Some(error.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.ok && self.error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftFsmRuntimeSyncGroupResult {
    pub group_id: GroupId,
    pub node_count: usize,
    pub ok_count: usize,
    pub error_count: usize,
    pub results: Vec<MatrixRaftFsmRuntimeSyncNodeResult>,
}

impl MatrixRaftFsmRuntimeSyncGroupResult {
    pub fn is_ok(&self) -> bool {
        self.error_count == 0 && self.ok_count == self.node_count
    }

    pub fn reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftFsmRuntimeHookReport>)> {
        self.results
            .iter()
            .map(|result| (result.key, result.report.clone()))
            .collect()
    }

    pub fn report_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.results
            .iter()
            .map(|result| (result.key, result.report.is_some()))
            .collect()
    }

    pub fn ok_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftFsmRuntimeHookReport)> {
        self.results
            .iter()
            .filter(|result| result.is_ok())
            .filter_map(|result| result.report.clone().map(|report| (result.key, report)))
            .collect()
    }

    pub fn error_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftFsmRuntimeHookReport>)> {
        self.results
            .iter()
            .filter(|result| !result.is_ok())
            .map(|result| (result.key, result.report.clone()))
            .collect()
    }

    pub fn opened_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| (result.key, result.report.as_ref().map(|report| report.opened)))
            .collect()
    }

    pub fn opened_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.opened_by_route_key())
    }

    pub fn closed_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| (result.key, result.report.as_ref().map(|report| report.closed)))
            .collect()
    }

    pub fn closed_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.closed_by_route_key())
    }

    pub fn leader_started_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result.report.as_ref().map(|report| report.leader_started),
                )
            })
            .collect()
    }

    pub fn leader_started_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.leader_started_by_route_key())
    }

    pub fn following_started_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .report
                        .as_ref()
                        .map(|report| report.following_started),
                )
            })
            .collect()
    }

    pub fn following_started_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.following_started_by_route_key())
    }

    pub fn terms_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<Term>)> {
        self.results
            .iter()
            .map(|result| (result.key, result.report.as_ref().map(|report| report.term)))
            .collect()
    }

    pub fn term_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.terms_by_route_key())
    }

    pub fn leader_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result.report.as_ref().and_then(|report| report.leader_id),
                )
            })
            .collect()
    }

    pub fn leader_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.leader_ids_by_route_key())
    }

    pub fn roles_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<StateRole>)> {
        self.results
            .iter()
            .map(|result| (result.key, result.report.as_ref().map(|report| report.role)))
            .collect()
    }

    pub fn role_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.roles_by_route_key())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftNodeSnapshotCompletionResult {
    pub key: MatrixRaftRouteKey,
    pub snapshot_id: String,
    pub operation: String,
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
}

impl MatrixRaftNodeSnapshotCompletionResult {
    pub fn ok(key: MatrixRaftRouteKey, snapshot_id: String, operation: impl Into<String>) -> Self {
        Self {
            key,
            snapshot_id,
            operation: operation.into(),
            ok: true,
            error: None,
        }
    }

    pub fn error(
        key: MatrixRaftRouteKey,
        snapshot_id: String,
        operation: impl Into<String>,
        error: RaftError,
    ) -> Self {
        Self {
            key,
            snapshot_id,
            operation: operation.into(),
            ok: false,
            error: Some(error.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.ok && self.error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotInstallFanoutGroupPlan {
    pub group_id: GroupId,
    pub target: NodeId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub snapshot: RaftSnapshot,
    pub fence: ApplySnapshotFence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotInstallFanoutPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub groups: Vec<MatrixRaftSnapshotInstallFanoutGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotInstallNodeResult {
    pub key: MatrixRaftRouteKey,
    pub target: NodeId,
    pub snapshot_id: SnapshotId,
    pub snapshot_index: LogIndex,
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
}

impl MatrixRaftSnapshotInstallNodeResult {
    pub fn ok(key: MatrixRaftRouteKey, target: NodeId, snapshot: &RaftSnapshot) -> Self {
        Self {
            key,
            target,
            snapshot_id: snapshot.meta.snapshot_id.clone(),
            snapshot_index: snapshot.meta.last_log_id.index,
            ok: true,
            error: None,
        }
    }

    pub fn error(
        key: MatrixRaftRouteKey,
        target: NodeId,
        snapshot: &RaftSnapshot,
        error: RaftError,
    ) -> Self {
        Self {
            key,
            target,
            snapshot_id: snapshot.meta.snapshot_id.clone(),
            snapshot_index: snapshot.meta.last_log_id.index,
            ok: false,
            error: Some(error.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.ok && self.error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotInstallGroupResult {
    pub group_id: GroupId,
    pub target: NodeId,
    pub node_count: usize,
    pub ok_count: usize,
    pub error_count: usize,
    pub results: Vec<MatrixRaftSnapshotInstallNodeResult>,
}

impl MatrixRaftSnapshotInstallGroupResult {
    pub fn is_ok(&self) -> bool {
        self.error_count == 0 && self.ok_count == self.node_count
    }

    pub fn results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftSnapshotInstallNodeResult)> {
        self.results
            .iter()
            .map(|result| (result.key, result.clone()))
            .collect()
    }

    pub fn ok_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftSnapshotInstallNodeResult)> {
        self.results
            .iter()
            .filter(|result| result.is_ok())
            .map(|result| (result.key, result.clone()))
            .collect()
    }

    pub fn error_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftSnapshotInstallNodeResult)> {
        self.results
            .iter()
            .filter(|result| !result.is_ok())
            .map(|result| (result.key, result.clone()))
            .collect()
    }

    pub fn targets_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.results
            .iter()
            .map(|result| (result.key, result.target))
            .collect()
    }

    pub fn snapshot_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, SnapshotId)> {
        self.results
            .iter()
            .map(|result| (result.key, result.snapshot_id.clone()))
            .collect()
    }

    pub fn snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.results
            .iter()
            .map(|result| (result.key, result.snapshot_index))
            .collect()
    }

    pub fn ok_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, SnapshotId)> {
        self.results
            .iter()
            .filter(|result| result.is_ok())
            .map(|result| (result.key, result.snapshot_id.clone()))
            .collect()
    }

    pub fn ok_snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.results
            .iter()
            .filter(|result| result.is_ok())
            .map(|result| (result.key, result.snapshot_index))
            .collect()
    }

    pub fn error_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, SnapshotId)> {
        self.results
            .iter()
            .filter(|result| !result.is_ok())
            .map(|result| (result.key, result.snapshot_id.clone()))
            .collect()
    }

    pub fn error_snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.results
            .iter()
            .filter(|result| !result.is_ok())
            .map(|result| (result.key, result.snapshot_index))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMembershipFanoutGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub operation: MembershipOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMembershipFanoutPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub operation: MembershipOperation,
    pub groups: Vec<MatrixRaftMembershipFanoutGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMembershipWorkflowFanoutGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub operation_count: usize,
    pub operations: Vec<MembershipOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMembershipWorkflowFanoutPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub operation_count: usize,
    pub operations: Vec<MembershipOperation>,
    pub groups: Vec<MatrixRaftMembershipWorkflowFanoutGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMembershipWorkflowNodeResult {
    pub key: MatrixRaftRouteKey,
    #[serde(default)]
    pub reports: Option<Vec<MembershipExecutionReport>>,
    #[serde(default)]
    pub error: Option<String>,
}

impl MatrixRaftMembershipWorkflowNodeResult {
    pub fn ok(key: MatrixRaftRouteKey, reports: Vec<MembershipExecutionReport>) -> Self {
        Self {
            key,
            reports: Some(reports),
            error: None,
        }
    }

    pub fn error(key: MatrixRaftRouteKey, error: RaftError) -> Self {
        Self {
            key,
            reports: None,
            error: Some(error.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.reports.is_some() && self.error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMembershipWorkflowGroupResult {
    pub group_id: GroupId,
    pub node_count: usize,
    pub ok_count: usize,
    pub error_count: usize,
    pub results: Vec<MatrixRaftMembershipWorkflowNodeResult>,
}

impl MatrixRaftMembershipWorkflowGroupResult {
    pub fn is_ok(&self) -> bool {
        self.error_count == 0 && self.ok_count == self.node_count
    }

    pub fn reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<Vec<MembershipExecutionReport>>)> {
        self.results
            .iter()
            .map(|result| (result.key, result.reports.clone()))
            .collect()
    }

    pub fn report_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.results
            .iter()
            .map(|result| (result.key, result.reports.is_some()))
            .collect()
    }

    pub fn ok_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Vec<MembershipExecutionReport>)> {
        self.results
            .iter()
            .filter(|result| result.is_ok())
            .map(|result| {
                (
                    result.key,
                    result.reports.clone().unwrap_or_default(),
                )
            })
            .collect()
    }

    pub fn error_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<Vec<MembershipExecutionReport>>)> {
        self.results
            .iter()
            .filter(|result| !result.is_ok())
            .map(|result| (result.key, result.reports.clone()))
            .collect()
    }

    pub fn report_counts_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, usize)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result.reports.as_ref().map_or(0, Vec::len),
                )
            })
            .collect()
    }

    pub fn operations_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Vec<MembershipOperation>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .reports
                        .as_ref()
                        .map(|reports| {
                            reports
                                .iter()
                                .map(|report| report.operation.clone())
                                .collect()
                        })
                        .unwrap_or_default(),
                )
            })
            .collect()
    }

    pub fn operation_member_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Vec<NodeId>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .reports
                        .as_ref()
                        .map(|reports| {
                            reports
                                .iter()
                                .map(|report| matrixraft_membership_operation_node_id(&report.operation))
                                .collect()
                        })
                        .unwrap_or_default(),
                )
            })
            .collect()
    }

    pub fn success_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Vec<bool>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .reports
                        .as_ref()
                        .map(|reports| reports.iter().map(|report| report.success).collect())
                        .unwrap_or_default(),
                )
            })
            .collect()
    }

    pub fn validation_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Vec<bool>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .reports
                        .as_ref()
                        .map(|reports| {
                            reports
                                .iter()
                                .map(|report| report.validation_passed)
                                .collect()
                        })
                        .unwrap_or_default(),
                )
            })
            .collect()
    }

    pub fn rollback_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Vec<bool>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .reports
                        .as_ref()
                        .map(|reports| reports.iter().map(|report| report.rolled_back).collect())
                        .unwrap_or_default(),
                )
            })
            .collect()
    }

    pub fn reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Vec<String>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .reports
                        .as_ref()
                        .map(|reports| {
                            reports
                                .iter()
                                .map(|report| report.reason.clone())
                                .collect()
                        })
                        .unwrap_or_default(),
                )
            })
            .collect()
    }
}

fn matrixraft_membership_operation_node_id(operation: &MembershipOperation) -> NodeId {
    match operation {
        MembershipOperation::AddNode(peer)
        | MembershipOperation::AddVoter(peer)
        | MembershipOperation::AddLearner(peer)
        | MembershipOperation::AddWitness(peer) => peer.node_id,
        MembershipOperation::Promote(node_id)
        | MembershipOperation::Remove(node_id)
        | MembershipOperation::TransferLeader(node_id) => *node_id,
    }
}

fn matrixraft_membership_operation_type(operation: &MembershipOperation) -> &'static str {
    match operation {
        MembershipOperation::AddNode(_) => "add_node",
        MembershipOperation::AddVoter(_) => "add_voter",
        MembershipOperation::AddLearner(_) => "add_learner",
        MembershipOperation::AddWitness(_) => "add_witness",
        MembershipOperation::Promote(_) => "promote",
        MembershipOperation::Remove(_) => "remove",
        MembershipOperation::TransferLeader(_) => "transfer_leader",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftConfigChangeFanoutGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub change: MatrixRaftConfigChange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftConfigChangeFanoutPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub change: MatrixRaftConfigChange,
    pub groups: Vec<MatrixRaftConfigChangeFanoutGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftProposeFanoutGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub options: MatrixRaftProposeOptions,
    pub payload_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftProposeFanoutPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub options: MatrixRaftProposeOptions,
    pub payload_bytes: usize,
    pub groups: Vec<MatrixRaftProposeFanoutGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftReadIndexFanoutGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub options: MatrixRaftReadIndexOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftReadIndexFanoutPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub options: MatrixRaftReadIndexOptions,
    pub groups: Vec<MatrixRaftReadIndexFanoutGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftReadIndexNodeResult {
    pub key: MatrixRaftRouteKey,
    #[serde(default)]
    pub read_index: Option<ReadIndexResponse>,
    #[serde(default)]
    pub error: Option<String>,
}

impl MatrixRaftReadIndexNodeResult {
    pub fn ok(key: MatrixRaftRouteKey, read_index: ReadIndexResponse) -> Self {
        Self {
            key,
            read_index: Some(read_index),
            error: None,
        }
    }

    pub fn error(key: MatrixRaftRouteKey, error: RaftError) -> Self {
        Self {
            key,
            read_index: None,
            error: Some(error.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.read_index.is_some() && self.error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftReadIndexGroupResult {
    pub group_id: GroupId,
    pub node_count: usize,
    pub ok_count: usize,
    pub error_count: usize,
    pub results: Vec<MatrixRaftReadIndexNodeResult>,
}

impl MatrixRaftReadIndexGroupResult {
    pub fn is_ok(&self) -> bool {
        self.error_count == 0 && self.ok_count == self.node_count
    }

    pub fn responses_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.results
            .iter()
            .map(|result| (result.key, result.read_index.clone()))
            .collect()
    }

    pub fn response_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.results
            .iter()
            .map(|result| (result.key, result.read_index.is_some()))
            .collect()
    }

    pub fn read_indices_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result.read_index.as_ref().map(|read| read.read_index),
                )
            })
            .collect()
    }

    pub fn read_index_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.read_indices_by_route_key())
    }

    pub fn safe_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| (result.key, result.read_index.as_ref().map(|read| read.safe)))
            .collect()
    }

    pub fn safe_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.safe_values_by_route_key())
    }

    pub fn lease_read_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result.read_index.as_ref().map(|read| read.lease_read),
                )
            })
            .collect()
    }

    pub fn lease_read_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.lease_read_values_by_route_key())
    }

    pub fn reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result.read_index.as_ref().map(|read| read.reason.clone()),
                )
            })
            .collect()
    }

    pub fn reason_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.reasons_by_route_key())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftBoundedStaleReadFanoutGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub options: MatrixRaftBoundedStaleReadOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftBoundedStaleReadFanoutPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub options: MatrixRaftBoundedStaleReadOptions,
    pub groups: Vec<MatrixRaftBoundedStaleReadFanoutGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftBoundedStaleReadNodeResult {
    pub key: MatrixRaftRouteKey,
    #[serde(default)]
    pub report: Option<ReadPathReport>,
    #[serde(default)]
    pub error: Option<String>,
}

impl MatrixRaftBoundedStaleReadNodeResult {
    pub fn ok(key: MatrixRaftRouteKey, report: ReadPathReport) -> Self {
        Self {
            key,
            report: Some(report),
            error: None,
        }
    }

    pub fn error(key: MatrixRaftRouteKey, error: RaftError) -> Self {
        Self {
            key,
            report: None,
            error: Some(error.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.report.is_some() && self.error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftBoundedStaleReadGroupResult {
    pub group_id: GroupId,
    pub node_count: usize,
    pub ok_count: usize,
    pub error_count: usize,
    pub results: Vec<MatrixRaftBoundedStaleReadNodeResult>,
}

impl MatrixRaftBoundedStaleReadGroupResult {
    pub fn is_ok(&self) -> bool {
        self.error_count == 0 && self.ok_count == self.node_count
    }

    pub fn reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadPathReport>)> {
        self.results
            .iter()
            .map(|result| (result.key, result.report.clone()))
            .collect()
    }

    pub fn report_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.results
            .iter()
            .map(|result| (result.key, result.report.is_some()))
            .collect()
    }

    pub fn bounded_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .report
                        .as_ref()
                        .is_some_and(|report| report.bounded_stale.is_some()),
                )
            })
            .collect()
    }

    pub fn read_indices_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result.report.as_ref().map(|report| report.read_index),
                )
            })
            .collect()
    }

    pub fn read_index_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.read_indices_by_route_key())
    }

    pub fn safe_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| (result.key, result.report.as_ref().map(|report| report.safe)))
            .collect()
    }

    pub fn safe_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.safe_values_by_route_key())
    }

    pub fn lease_read_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result.report.as_ref().map(|report| report.lease_read),
                )
            })
            .collect()
    }

    pub fn lease_read_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.lease_read_values_by_route_key())
    }

    pub fn reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result.report.as_ref().map(|report| report.reason.clone()),
                )
            })
            .collect()
    }

    pub fn reason_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.reasons_by_route_key())
    }

    pub fn bounded_allowed_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .report
                        .as_ref()
                        .and_then(|report| report.bounded_stale.as_ref())
                        .map(|bounded| bounded.allowed),
                )
            })
            .collect()
    }

    pub fn bounded_allowed_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.bounded_allowed_by_route_key())
    }

    pub fn bounded_lags_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.results
            .iter()
            .map(|result| {
                (
                    result.key,
                    result
                        .report
                        .as_ref()
                        .and_then(|report| report.bounded_stale.as_ref())
                        .map(|bounded| bounded.lag),
                )
            })
            .collect()
    }

    pub fn bounded_lag_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.bounded_lags_by_route_key())
    }
}

impl_matrixraft_group_result_route_accessors!(MatrixRaftCreateGroupResult);
impl_matrixraft_group_result_route_accessors!(MatrixRaftLifecycleGroupResult);
impl_matrixraft_group_result_route_accessors!(MatrixRaftFsmRuntimeSyncGroupResult);
impl_matrixraft_group_result_route_accessors!(MatrixRaftSnapshotInstallGroupResult);
impl_matrixraft_group_result_route_accessors!(MatrixRaftMembershipWorkflowGroupResult);
impl_matrixraft_group_result_route_accessors!(MatrixRaftReadIndexGroupResult);
impl_matrixraft_group_result_route_accessors!(MatrixRaftBoundedStaleReadGroupResult);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotPublishGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub existing_route_count: usize,
    #[serde(default)]
    pub existing_route_keys: Vec<MatrixRaftRouteKey>,
    pub snapshot: MatrixRaftSnapshotDesc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotPublishPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub existing_route_count: usize,
    pub groups: Vec<MatrixRaftSnapshotPublishGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotFinishGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub active_route_count: usize,
    #[serde(default)]
    pub active_route_keys: Vec<MatrixRaftRouteKey>,
    pub finish: MatrixRaftOldSnapshotFinish,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSnapshotFinishPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub active_route_count: usize,
    pub finish: MatrixRaftOldSnapshotFinish,
    pub groups: Vec<MatrixRaftSnapshotFinishGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMessageFanoutGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub message_type: MatrixRaftMessageType,
    pub message: MatrixRaftMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftMessageFanoutPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub message_type: MatrixRaftMessageType,
    pub groups: Vec<MatrixRaftMessageFanoutGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftHeartbeatMergeGroupPlan {
    pub group_id: GroupId,
    pub from: NodeId,
    pub to: NodeId,
    pub route_key: MatrixRaftRouteKey,
    pub raft_addr: String,
    pub message_type: MatrixRaftMessageType,
    pub message: MatrixRaftMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftHeartbeatMergeBatchPlan {
    pub raft_addr: String,
    pub message_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub messages: Vec<MatrixRaftMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftHeartbeatMergePlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub message_count: usize,
    pub batch_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub message_type: MatrixRaftMessageType,
    pub groups: Vec<MatrixRaftHeartbeatMergeGroupPlan>,
    pub batches: Vec<MatrixRaftHeartbeatMergeBatchPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftAdminCommandFanoutGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub command_type: MatrixRaftAdminCommandType,
    pub command: MatrixRaftAdminCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftAdminCommandFanoutPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub command_type: MatrixRaftAdminCommandType,
    pub groups: Vec<MatrixRaftAdminCommandFanoutGroupPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftAdminCommandBatchPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub command_types: Vec<MatrixRaftAdminCommandType>,
    pub groups: Vec<MatrixRaftAdminCommandFanoutGroupPlan>,
}

macro_rules! impl_matrixraft_group_node_route_accessors {
    ($plan:ty) => {
        impl $plan {
            pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.route_keys.clone()))
                    .collect()
            }

            pub fn node_ids_by_group(&self) -> Vec<(GroupId, Vec<NodeId>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.node_ids.clone()))
                    .collect()
            }

            pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, route_key.node_id))
                    })
                    .collect()
            }

            pub fn node_counts_by_group(&self) -> Vec<(GroupId, usize)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.node_count))
                    .collect()
            }

            pub fn route_key_counts_by_group(&self) -> Vec<(GroupId, usize)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.route_keys.len()))
                    .collect()
            }

            pub fn fanout_counts_by_group(&self) -> Vec<(GroupId, usize, usize)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.node_count, group.route_keys.len()))
                    .collect()
            }
        }
    };
}

impl_matrixraft_group_node_route_accessors!(MatrixRaftSnapshotInstallFanoutPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftMembershipFanoutPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftMembershipWorkflowFanoutPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftConfigChangeFanoutPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftProposeFanoutPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftReadIndexFanoutPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftBoundedStaleReadFanoutPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftSnapshotPublishPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftSnapshotFinishPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftMessageFanoutPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftAdminCommandFanoutPlan);
impl_matrixraft_group_node_route_accessors!(MatrixRaftAdminCommandBatchPlan);

impl MatrixRaftSnapshotInstallFanoutPlan {
    pub fn targets_by_group(&self) -> Vec<(GroupId, NodeId)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.target))
            .collect()
    }

    pub fn snapshots_by_group(&self) -> Vec<(GroupId, RaftSnapshot)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.snapshot.clone()))
            .collect()
    }

    pub fn snapshots_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, RaftSnapshot)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.snapshot.clone()))
            })
            .collect()
    }

    pub fn snapshot_ids_by_group(&self) -> Vec<(GroupId, SnapshotId)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.snapshot.meta.snapshot_id.clone()))
            .collect()
    }

    pub fn snapshot_indices_by_group(&self) -> Vec<(GroupId, LogIndex)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.snapshot.meta.last_log_id.index))
            .collect()
    }

    pub fn fences_by_group(&self) -> Vec<(GroupId, ApplySnapshotFence)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.fence.clone()))
            .collect()
    }

    pub fn fences_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, ApplySnapshotFence)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.fence.clone()))
            })
            .collect()
    }

    pub fn fence_applied_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.fence.applied_index))
            })
            .collect()
    }

    pub fn fence_commit_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.fence.commit_index))
            })
            .collect()
    }

    pub fn fence_installed_snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (*route_key, group.fence.installed_snapshot_index)
                })
            })
            .collect()
    }

    pub fn fence_first_retained_log_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (*route_key, group.fence.first_retained_log_index)
                })
            })
            .collect()
    }
}

impl MatrixRaftMembershipFanoutPlan {
    pub fn operations_by_group(&self) -> Vec<(GroupId, MembershipOperation)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.operation.clone()))
            .collect()
    }

    pub fn operations_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MembershipOperation)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.operation.clone()))
            })
            .collect()
    }

    pub fn operation_types_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, String)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        matrixraft_membership_operation_type(&group.operation).to_string(),
                    )
                })
            })
            .collect()
    }

    pub fn operation_member_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        matrixraft_membership_operation_node_id(&group.operation),
                    )
                })
            })
            .collect()
    }
}

impl MatrixRaftMembershipWorkflowFanoutPlan {
    pub fn operation_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.operation_count))
            .collect()
    }

    pub fn operation_counts_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, usize)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.operation_count))
            })
            .collect()
    }

    pub fn operations_by_group(&self) -> Vec<(GroupId, Vec<MembershipOperation>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.operations.clone()))
            .collect()
    }

    pub fn operations_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Vec<MembershipOperation>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.operations.clone()))
            })
            .collect()
    }

    pub fn operation_types_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Vec<String>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group
                            .operations
                            .iter()
                            .map(|operation| {
                                matrixraft_membership_operation_type(operation).to_string()
                            })
                            .collect(),
                    )
                })
            })
            .collect()
    }

    pub fn operation_member_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Vec<NodeId>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group
                            .operations
                            .iter()
                            .map(matrixraft_membership_operation_node_id)
                            .collect(),
                    )
                })
            })
            .collect()
    }
}

impl MatrixRaftConfigChangeFanoutPlan {
    pub fn changes_by_group(&self) -> Vec<(GroupId, MatrixRaftConfigChange)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.change.clone()))
            .collect()
    }

    pub fn changes_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftConfigChange)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.change.clone()))
            })
            .collect()
    }

    pub fn change_types_by_group(&self) -> Vec<(GroupId, MatrixRaftConfigChangeType)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.change.change_type))
            .collect()
    }

    pub fn change_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftConfigChangeType)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.change.change_type))
            })
            .collect()
    }

    pub fn member_ids_by_group(&self) -> Vec<(GroupId, NodeId)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.change.member_id))
            .collect()
    }

    pub fn member_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.change.member_id))
            })
            .collect()
    }

    pub fn request_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.change.request_id))
            })
            .collect()
    }

    pub fn request_ids_by_group(&self) -> Vec<(GroupId, Option<u64>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.change.request_id))
            .collect()
    }

    pub fn conf_states_by_group(&self) -> Vec<(GroupId, MatrixRaftConfState)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.change.conf_state))
            .collect()
    }

    pub fn conf_states_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftConfState)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.change.conf_state))
            })
            .collect()
    }

    pub fn auto_promote_values_by_group(&self) -> Vec<(GroupId, bool)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.change.auto_promote))
            .collect()
    }

    pub fn auto_promote_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.change.auto_promote))
            })
            .collect()
    }
}

impl MatrixRaftProposeFanoutPlan {
    pub fn options_by_group(&self) -> Vec<(GroupId, MatrixRaftProposeOptions)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.options.clone()))
            .collect()
    }

    pub fn options_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftProposeOptions)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.options.clone()))
            })
            .collect()
    }

    pub fn terms_by_group(&self) -> Vec<(GroupId, Option<Term>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.options.with_term))
            .collect()
    }

    pub fn terms_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<Term>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.options.with_term))
            })
            .collect()
    }

    pub fn command_values_by_group(&self) -> Vec<(GroupId, bool)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.options.is_command))
            .collect()
    }

    pub fn command_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.options.is_command))
            })
            .collect()
    }

    pub fn payload_bytes_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.payload_bytes))
            .collect()
    }

    pub fn payload_bytes_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, usize)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.payload_bytes))
            })
            .collect()
    }
}

impl MatrixRaftReadIndexFanoutPlan {
    pub fn options_by_group(&self) -> Vec<(GroupId, MatrixRaftReadIndexOptions)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.options))
            .collect()
    }

    pub fn options_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftReadIndexOptions)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.options))
            })
            .collect()
    }

    pub fn min_commit_indices_by_group(
        &self,
    ) -> Vec<(GroupId, LogIndex)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.options.min_commit_index))
            .collect()
    }

    pub fn min_commit_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.options.min_commit_index))
            })
            .collect()
    }

    pub fn modes_by_group(&self) -> Vec<(GroupId, MatrixRaftReadIndexMode)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.options.mode))
            .collect()
    }

    pub fn modes_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftReadIndexMode)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.options.mode))
            })
            .collect()
    }
}

impl MatrixRaftBoundedStaleReadFanoutPlan {
    pub fn options_by_group(&self) -> Vec<(GroupId, MatrixRaftBoundedStaleReadOptions)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.options))
            .collect()
    }

    pub fn options_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftBoundedStaleReadOptions)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.options))
            })
            .collect()
    }

    pub fn min_commit_indices_by_group(
        &self,
    ) -> Vec<(GroupId, LogIndex)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.options.min_commit_index))
            .collect()
    }

    pub fn min_commit_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.options.min_commit_index))
            })
            .collect()
    }

    pub fn max_stale_index_lags_by_group(
        &self,
    ) -> Vec<(GroupId, LogIndex)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.options.max_stale_index_lag))
            .collect()
    }

    pub fn max_stale_index_lags_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.options.max_stale_index_lag))
            })
            .collect()
    }
}

impl MatrixRaftSnapshotPublishPlan {
    pub fn existing_route_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.existing_route_count))
            .collect()
    }

    pub fn existing_route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.existing_route_keys.clone()))
            .collect()
    }

    pub fn existing_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (*route_key, group.existing_route_keys.contains(route_key))
                })
            })
            .collect()
    }

    pub fn snapshots_by_group(&self) -> Vec<(GroupId, MatrixRaftSnapshotDesc)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.snapshot.clone()))
            .collect()
    }

    pub fn snapshots_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftSnapshotDesc)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.snapshot.clone()))
            })
            .collect()
    }

    pub fn snapshot_ids_by_group(&self) -> Vec<(GroupId, Option<SnapshotId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.snapshot.snapshot_id.clone()))
            .collect()
    }

    pub fn snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.snapshot.snapshot_id.clone()))
            })
            .collect()
    }

    pub fn snapshot_indices_by_group(&self) -> Vec<(GroupId, LogIndex)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.snapshot.index))
            .collect()
    }

    pub fn snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.snapshot.index))
            })
            .collect()
    }

    pub fn snapshot_terms_by_group(&self) -> Vec<(GroupId, Term)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.snapshot.term))
            .collect()
    }

    pub fn snapshot_terms_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Term)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.snapshot.term))
            })
            .collect()
    }

    pub fn snapshot_member_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.snapshot.members.len()))
            .collect()
    }

    pub fn snapshot_member_counts_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, usize)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.snapshot.members.len()))
            })
            .collect()
    }
}

impl MatrixRaftSnapshotFinishPlan {
    pub fn active_route_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.active_route_count))
            .collect()
    }

    pub fn active_route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.active_route_keys.clone()))
            .collect()
    }

    pub fn active_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.active_route_keys.contains(route_key)))
            })
            .collect()
    }

    pub fn finishes_by_group(&self) -> Vec<(GroupId, MatrixRaftOldSnapshotFinish)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.finish.clone()))
            .collect()
    }

    pub fn finishes_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftOldSnapshotFinish)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.finish.clone()))
            })
            .collect()
    }

    pub fn finish_states_by_group(
        &self,
    ) -> Vec<(GroupId, MatrixRaftOldSnapshotFinishState)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.finish.finish_state))
            .collect()
    }

    pub fn finish_states_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftOldSnapshotFinishState)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.finish.finish_state))
            })
            .collect()
    }

    pub fn snapshot_indices_by_group(&self) -> Vec<(GroupId, LogIndex)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.finish.snapshot_index))
            .collect()
    }

    pub fn snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, LogIndex)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.finish.snapshot_index))
            })
            .collect()
    }
}

impl MatrixRaftMessageFanoutPlan {
    pub fn messages_by_group(&self) -> Vec<(GroupId, MatrixRaftMessage)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message.clone()))
            .collect()
    }

    pub fn messages_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessage)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.message.clone()))
            })
            .collect()
    }

    pub fn message_types_by_group(&self) -> Vec<(GroupId, MatrixRaftMessageType)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message_type))
            .collect()
    }

    pub fn message_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessageType)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.message_type))
            })
            .collect()
    }

    pub fn sender_receiver_by_group(
        &self,
    ) -> Vec<(GroupId, Option<NodeId>, Option<NodeId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message.from, group.message.to))
            .collect()
    }

    pub fn sender_receiver_by_route_key(
        &self,
    ) -> Vec<(
        MatrixRaftRouteKey,
        (Option<NodeId>, Option<NodeId>),
    )> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, (group.message.from, group.message.to)))
            })
            .collect()
    }

    pub fn terms_by_group(&self) -> Vec<(GroupId, Option<Term>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message.term))
            .collect()
    }

    pub fn terms_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<Term>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.message.term))
            })
            .collect()
    }

    pub fn committed_indices_by_group(&self) -> Vec<(GroupId, Option<LogIndex>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message.committed_index))
            .collect()
    }

    pub fn committed_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.message.committed_index))
            })
            .collect()
    }

    pub fn message_bytes_by_group(&self) -> Vec<(GroupId, u64)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message.bytes_size))
            .collect()
    }

    pub fn message_bytes_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, u64)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.message.bytes_size))
            })
            .collect()
    }

    pub fn propose_request_ids_by_group(&self) -> Vec<(GroupId, Option<u64>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group
                        .message
                        .propose
                        .as_ref()
                        .and_then(|propose| propose.request_id),
                )
            })
            .collect()
    }

    pub fn propose_request_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group
                            .message
                            .propose
                            .as_ref()
                            .and_then(|propose| propose.request_id),
                    )
                })
            })
            .collect()
    }

    pub fn snapshot_ids_by_group(&self) -> Vec<(GroupId, Option<SnapshotId>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.meta.snapshot_id.clone()),
                )
            })
            .collect()
    }

    pub fn snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group
                            .message
                            .install_snapshot_request
                            .as_ref()
                            .map(|request| request.chunk.meta.snapshot_id.clone()),
                    )
                })
            })
            .collect()
    }

    pub fn snapshot_chunk_offsets_by_group(&self) -> Vec<(GroupId, Option<u64>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.offset),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_offsets_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group
                            .message
                            .install_snapshot_request
                            .as_ref()
                            .map(|request| request.chunk.offset),
                    )
                })
            })
            .collect()
    }

    pub fn snapshot_chunk_done_by_group(&self) -> Vec<(GroupId, Option<bool>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.done),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_done_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group
                            .message
                            .install_snapshot_request
                            .as_ref()
                            .map(|request| request.chunk.done),
                    )
                })
            })
            .collect()
    }

    pub fn snapshot_chunk_payload_bytes_by_group(&self) -> Vec<(GroupId, Option<usize>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.data.len()),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_payload_bytes_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<usize>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group
                            .message
                            .install_snapshot_request
                            .as_ref()
                            .map(|request| request.chunk.data.len()),
                    )
                })
            })
            .collect()
    }
}

macro_rules! impl_matrixraft_admin_command_metadata_accessors {
    ($plan:ty) => {
        impl $plan {
            pub fn commands_by_group(&self) -> Vec<(GroupId, MatrixRaftAdminCommand)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.clone()))
                    .collect()
            }

            pub fn commands_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, MatrixRaftAdminCommand)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.clone()))
                    })
                    .collect()
            }

            pub fn command_types_by_group(
                &self,
            ) -> Vec<(GroupId, MatrixRaftAdminCommandType)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command_type))
                    .collect()
            }

            pub fn command_types_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, MatrixRaftAdminCommandType)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command_type))
                    })
                    .collect()
            }

            pub fn request_ids_by_group(&self) -> Vec<(GroupId, Option<u64>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.request_id))
                    .collect()
            }

            pub fn request_id_presence_by_group(&self) -> Vec<(GroupId, bool)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.request_id.is_some()))
                    .collect()
            }

            pub fn request_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.request_id))
                    })
                    .collect()
            }

            pub fn request_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
                matrixraft_presence_by_route_key(self.request_ids_by_route_key())
            }

            pub fn command_node_ids_by_group(
                &self,
            ) -> Vec<(GroupId, Option<NodeId>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.node_id))
                    .collect()
            }

            pub fn command_node_ids_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.node_id))
                    })
                    .collect()
            }

            pub fn transferee_ids_by_group(
                &self,
            ) -> Vec<(GroupId, Option<NodeId>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.transferee_id))
                    .collect()
            }

            pub fn transferee_id_presence_by_group(&self) -> Vec<(GroupId, bool)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.transferee_id.is_some()))
                    .collect()
            }

            pub fn transferee_ids_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.transferee_id))
                    })
                    .collect()
            }

            pub fn transferee_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
                matrixraft_presence_by_route_key(self.transferee_ids_by_route_key())
            }

            pub fn forced_campaigns_by_group(&self) -> Vec<(GroupId, bool)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.forced_campaign))
                    .collect()
            }

            pub fn forced_campaigns_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.forced_campaign))
                    })
                    .collect()
            }

            pub fn node_healthy_values_by_group(
                &self,
            ) -> Vec<(GroupId, Option<bool>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.healthy))
                    .collect()
            }

            pub fn node_healthy_presence_by_group(&self) -> Vec<(GroupId, bool)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.healthy.is_some()))
                    .collect()
            }

            pub fn node_healthy_values_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.healthy))
                    })
                    .collect()
            }

            pub fn node_healthy_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
                matrixraft_presence_by_route_key(self.node_healthy_values_by_route_key())
            }

            pub fn lease_valid_values_by_group(&self) -> Vec<(GroupId, Option<bool>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.lease_valid))
                    .collect()
            }

            pub fn lease_valid_presence_by_group(&self) -> Vec<(GroupId, bool)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.lease_valid.is_some()))
                    .collect()
            }

            pub fn lease_valid_values_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.lease_valid))
                    })
                    .collect()
            }

            pub fn lease_valid_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
                matrixraft_presence_by_route_key(self.lease_valid_values_by_route_key())
            }

            pub fn snapshot_ids_by_group(
                &self,
            ) -> Vec<(GroupId, Option<SnapshotId>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.snapshot_id.clone()))
                    .collect()
            }

            pub fn snapshot_id_presence_by_group(&self) -> Vec<(GroupId, bool)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.snapshot_id.is_some()))
                    .collect()
            }

            pub fn snapshot_ids_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.snapshot_id.clone()))
                    })
                    .collect()
            }

            pub fn snapshot_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
                matrixraft_presence_by_route_key(self.snapshot_ids_by_route_key())
            }

            pub fn snapshot_peer_ids_by_group(
                &self,
            ) -> Vec<(GroupId, Option<NodeId>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.snapshot_peer_id))
                    .collect()
            }

            pub fn snapshot_peer_id_presence_by_group(&self) -> Vec<(GroupId, bool)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.snapshot_peer_id.is_some()))
                    .collect()
            }

            pub fn snapshot_peer_ids_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.snapshot_peer_id))
                    })
                    .collect()
            }

            pub fn snapshot_peer_id_presence_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, bool)> {
                matrixraft_presence_by_route_key(self.snapshot_peer_ids_by_route_key())
            }

            pub fn snapshot_indices_by_group(
                &self,
            ) -> Vec<(GroupId, Option<LogIndex>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.snapshot_index))
                    .collect()
            }

            pub fn snapshot_index_presence_by_group(&self) -> Vec<(GroupId, bool)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.snapshot_index.is_some()))
                    .collect()
            }

            pub fn snapshot_indices_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.snapshot_index))
                    })
                    .collect()
            }

            pub fn snapshot_index_presence_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, bool)> {
                matrixraft_presence_by_route_key(self.snapshot_indices_by_route_key())
            }

            pub fn log_indices_by_group(
                &self,
            ) -> Vec<(GroupId, Option<LogIndex>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.log_index))
                    .collect()
            }

            pub fn log_index_presence_by_group(&self) -> Vec<(GroupId, bool)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.log_index.is_some()))
                    .collect()
            }

            pub fn log_indices_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.log_index))
                    })
                    .collect()
            }

            pub fn log_index_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
                matrixraft_presence_by_route_key(self.log_indices_by_route_key())
            }

            pub fn storage_fences_by_group(
                &self,
            ) -> Vec<(GroupId, Option<StorageApplyFence>)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.storage_fence.clone()))
                    .collect()
            }

            pub fn storage_fences_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, Option<StorageApplyFence>)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.storage_fence.clone()))
                    })
                    .collect()
            }

            pub fn storage_fence_presence_by_group(&self) -> Vec<(GroupId, bool)> {
                self.groups
                    .iter()
                    .map(|group| (group.group_id, group.command.storage_fence.is_some()))
                    .collect()
            }

            pub fn storage_fence_presence_by_route_key(
                &self,
            ) -> Vec<(MatrixRaftRouteKey, bool)> {
                self.groups
                    .iter()
                    .flat_map(|group| {
                        group
                            .route_keys
                            .iter()
                            .map(|route_key| (*route_key, group.command.storage_fence.is_some()))
                    })
                    .collect()
            }
        }
    };
}

impl_matrixraft_admin_command_metadata_accessors!(MatrixRaftAdminCommandFanoutPlan);
impl_matrixraft_admin_command_metadata_accessors!(MatrixRaftAdminCommandBatchPlan);

impl MatrixRaftHeartbeatMergePlan {
    pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, vec![group.route_key]))
            .collect()
    }

    pub fn raft_addrs_by_group(&self) -> Vec<(GroupId, String)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.raft_addr.clone()))
            .collect()
    }

    pub fn sender_receiver_by_group(
        &self,
    ) -> Vec<(GroupId, NodeId, NodeId)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.from, group.to))
            .collect()
    }

    pub fn message_types_by_group(&self) -> Vec<(GroupId, MatrixRaftMessageType)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message_type))
            .collect()
    }

    pub fn messages_by_group(&self) -> Vec<(GroupId, MatrixRaftMessage)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message.clone()))
            .collect()
    }

    pub fn terms_by_group(&self) -> Vec<(GroupId, Option<Term>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message.term))
            .collect()
    }

    pub fn committed_indices_by_group(&self) -> Vec<(GroupId, Option<LogIndex>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message.committed_index))
            .collect()
    }

    pub fn message_bytes_by_group(&self) -> Vec<(GroupId, u64)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message.bytes_size))
            .collect()
    }

    pub fn raft_addrs_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, String)> {
        self.groups
            .iter()
            .map(|group| (group.route_key, group.raft_addr.clone()))
            .collect()
    }

    pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.groups
            .iter()
            .map(|group| (group.route_key, group.route_key.node_id))
            .collect()
    }

    pub fn sender_receiver_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, NodeId, NodeId)> {
        self.groups
            .iter()
            .map(|group| (group.route_key, group.from, group.to))
            .collect()
    }

    pub fn message_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessageType)> {
        self.groups
            .iter()
            .map(|group| (group.route_key, group.message_type))
            .collect()
    }

    pub fn messages_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessage)> {
        self.groups
            .iter()
            .map(|group| (group.route_key, group.message.clone()))
            .collect()
    }

    pub fn terms_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<Term>)> {
        self.groups
            .iter()
            .map(|group| (group.route_key, group.message.term))
            .collect()
    }

    pub fn committed_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.groups
            .iter()
            .map(|group| (group.route_key, group.message.committed_index))
            .collect()
    }

    pub fn message_bytes_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, u64)> {
        self.groups
            .iter()
            .map(|group| (group.route_key, group.message.bytes_size))
            .collect()
    }

    pub fn route_keys_by_raft_addr(&self) -> Vec<(String, Vec<MatrixRaftRouteKey>)> {
        self.batches
            .iter()
            .map(|batch| (batch.raft_addr.clone(), batch.route_keys.clone()))
            .collect()
    }

    pub fn messages_by_raft_addr(&self) -> Vec<(String, Vec<MatrixRaftMessage>)> {
        self.batches
            .iter()
            .map(|batch| (batch.raft_addr.clone(), batch.messages.clone()))
            .collect()
    }

    pub fn message_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, 1))
            .collect()
    }

    pub fn route_key_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, 1))
            .collect()
    }

    pub fn fanout_counts_by_group(&self) -> Vec<(GroupId, usize, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, 1, 1))
            .collect()
    }

    pub fn message_counts_by_raft_addr(&self) -> Vec<(String, usize)> {
        self.batches
            .iter()
            .map(|batch| (batch.raft_addr.clone(), batch.message_count))
            .collect()
    }

    pub fn route_key_counts_by_raft_addr(&self) -> Vec<(String, usize)> {
        self.batches
            .iter()
            .map(|batch| (batch.raft_addr.clone(), batch.route_keys.len()))
            .collect()
    }

    pub fn batch_fanout_counts_by_raft_addr(&self) -> Vec<(String, usize, usize)> {
        self.batches
            .iter()
            .map(|batch| {
                (
                    batch.raft_addr.clone(),
                    batch.message_count,
                    batch.route_keys.len(),
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRoutedAdminCommand {
    pub group_id: GroupId,
    pub runtime_node_id: NodeId,
    pub command: MatrixRaftAdminCommand,
}

impl MatrixRaftRoutedAdminCommand {
    pub fn new(
        group_id: GroupId,
        runtime_node_id: NodeId,
        command: MatrixRaftAdminCommand,
    ) -> Self {
        Self {
            group_id,
            runtime_node_id,
            command,
        }
    }

    pub fn route_key(&self) -> MatrixRaftRouteKey {
        MatrixRaftRouteKey::new(self.group_id, self.runtime_node_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftPriorityRoutedAdminCommand {
    pub priority: MailPriority,
    pub routed: MatrixRaftRoutedAdminCommand,
}

impl MatrixRaftPriorityRoutedAdminCommand {
    pub fn new(priority: MailPriority, routed: MatrixRaftRoutedAdminCommand) -> Self {
        Self { priority, routed }
    }

    pub fn route_key(&self) -> MatrixRaftRouteKey {
        self.routed.route_key()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRoutedAdminCommandBatchGroupPlan {
    pub group_id: GroupId,
    pub command_count: usize,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub command_types: Vec<MatrixRaftAdminCommandType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRoutedAdminCommandBatchPlan {
    pub command_count: usize,
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub command_types: Vec<MatrixRaftAdminCommandType>,
    pub groups: Vec<MatrixRaftRoutedAdminCommandBatchGroupPlan>,
    pub commands: Vec<MatrixRaftRoutedAdminCommand>,
}

impl MatrixRaftRoutedAdminCommandBatchPlan {
    pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_group(&self) -> Vec<(GroupId, Vec<NodeId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect()
    }

    pub fn command_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.command_count))
            .collect()
    }

    pub fn route_key_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.len()))
            .collect()
    }

    pub fn command_fanout_counts_by_group(&self) -> Vec<(GroupId, usize, usize)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group.command_count,
                    group.route_keys.len(),
                )
            })
            .collect()
    }

    pub fn command_types_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<MatrixRaftAdminCommandType>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.command_types.clone()))
            .collect()
    }

    pub fn commands_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftAdminCommand>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn commands_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftAdminCommand)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.clone()))
            .collect()
    }

    pub fn command_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftAdminCommandType)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.command_type))
            .collect()
    }

    pub fn command_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.node_id))
            .collect()
    }

    pub fn request_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.request_id))
            .collect()
    }

    pub fn snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.snapshot_id.clone()))
            .collect()
    }

    pub fn snapshot_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_ids_by_route_key())
    }

    pub fn snapshot_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.snapshot_peer_id))
            .collect()
    }

    pub fn snapshot_peer_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_peer_ids_by_route_key())
    }

    pub fn snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.snapshot_index))
            .collect()
    }

    pub fn snapshot_index_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_indices_by_route_key())
    }

    pub fn transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.transferee_id))
            .collect()
    }

    pub fn transferee_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.transferee_ids_by_route_key())
    }

    pub fn forced_campaigns_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.forced_campaign))
            .collect()
    }

    pub fn log_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.log_index))
            .collect()
    }

    pub fn log_index_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.log_indices_by_route_key())
    }

    pub fn storage_fence_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.storage_fence.is_some()))
            .collect()
    }

    pub fn node_healthy_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.healthy))
            .collect()
    }

    pub fn node_healthy_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.node_healthy_values_by_route_key())
    }

    pub fn lease_valid_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.command.lease_valid))
            .collect()
    }

    pub fn lease_valid_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.lease_valid_values_by_route_key())
    }

    pub fn command_node_ids_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<NodeId>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.node_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn request_ids_by_group(&self) -> Vec<(GroupId, Vec<Option<u64>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.request_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn request_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.request_ids_by_group())
    }

    pub fn snapshot_ids_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<SnapshotId>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.snapshot_id.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_ids_by_group())
    }

    pub fn snapshot_peer_ids_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<NodeId>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.snapshot_peer_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_peer_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_peer_ids_by_group())
    }

    pub fn snapshot_indices_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<LogIndex>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.snapshot_index)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_index_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_indices_by_group())
    }

    pub fn transferee_ids_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<NodeId>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.transferee_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn transferee_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.transferee_ids_by_group())
    }

    pub fn forced_campaigns_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.forced_campaign)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn log_indices_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<LogIndex>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.log_index)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn log_index_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.log_indices_by_group())
    }

    pub fn storage_fences_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<StorageApplyFence>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.storage_fence.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn storage_fence_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.storage_fence.is_some())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn node_healthy_values_by_group(&self) -> Vec<(GroupId, Vec<Option<bool>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.healthy)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn node_healthy_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.node_healthy_values_by_group())
    }

    pub fn lease_valid_values_by_group(&self) -> Vec<(GroupId, Vec<Option<bool>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.group_id == group.group_id)
                        .map(|command| command.command.lease_valid)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn lease_valid_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.lease_valid_values_by_group())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftPriorityAdminCommandGroupPlan {
    pub priority: MailPriority,
    pub command_count: usize,
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_ids: Vec<NodeId>,
    pub command_types: Vec<MatrixRaftAdminCommandType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftPriorityAdminCommandBatchPlan {
    pub command_count: usize,
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub command_types: Vec<MatrixRaftAdminCommandType>,
    pub priority_groups: Vec<MatrixRaftPriorityAdminCommandGroupPlan>,
    pub groups: Vec<MatrixRaftRoutedAdminCommandBatchGroupPlan>,
    pub commands: Vec<MatrixRaftPriorityRoutedAdminCommand>,
}

impl MatrixRaftPriorityAdminCommandBatchPlan {
    pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_group(&self) -> Vec<(GroupId, Vec<NodeId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect()
    }

    pub fn command_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.command_count))
            .collect()
    }

    pub fn route_key_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.len()))
            .collect()
    }

    pub fn command_fanout_counts_by_group(&self) -> Vec<(GroupId, usize, usize)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group.command_count,
                    group.route_keys.len(),
                )
            })
            .collect()
    }

    pub fn command_types_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<MatrixRaftAdminCommandType>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.command_types.clone()))
            .collect()
    }

    pub fn priorities_by_group(&self) -> Vec<(GroupId, Vec<MailPriority>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.priority)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn commands_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftAdminCommand>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn priorities_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MailPriority)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.priority))
            .collect()
    }

    pub fn commands_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftAdminCommand)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.clone()))
            .collect()
    }

    pub fn command_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftAdminCommandType)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.command_type))
            .collect()
    }

    pub fn command_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.node_id))
            .collect()
    }

    pub fn request_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.request_id))
            .collect()
    }

    pub fn snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.snapshot_id.clone()))
            .collect()
    }

    pub fn snapshot_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_ids_by_route_key())
    }

    pub fn snapshot_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.snapshot_peer_id))
            .collect()
    }

    pub fn snapshot_peer_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_peer_ids_by_route_key())
    }

    pub fn snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.snapshot_index))
            .collect()
    }

    pub fn snapshot_index_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_indices_by_route_key())
    }

    pub fn transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.transferee_id))
            .collect()
    }

    pub fn transferee_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.transferee_ids_by_route_key())
    }

    pub fn forced_campaigns_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.forced_campaign))
            .collect()
    }

    pub fn log_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.log_index))
            .collect()
    }

    pub fn log_index_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.log_indices_by_route_key())
    }

    pub fn storage_fence_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.commands
            .iter()
            .map(|command| {
                (
                    command.route_key(),
                    command.routed.command.storage_fence.is_some(),
                )
            })
            .collect()
    }

    pub fn node_healthy_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.healthy))
            .collect()
    }

    pub fn node_healthy_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.node_healthy_values_by_route_key())
    }

    pub fn lease_valid_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.commands
            .iter()
            .map(|command| (command.route_key(), command.routed.command.lease_valid))
            .collect()
    }

    pub fn lease_valid_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.lease_valid_values_by_route_key())
    }

    pub fn route_keys_by_priority(&self) -> Vec<(MailPriority, Vec<MatrixRaftRouteKey>)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_priority(&self) -> Vec<(MailPriority, Vec<NodeId>)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.node_ids.clone()))
            .collect()
    }

    pub fn group_ids_by_priority(&self) -> Vec<(MailPriority, Vec<GroupId>)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.group_ids.clone()))
            .collect()
    }

    pub fn command_types_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<MatrixRaftAdminCommandType>)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.command_types.clone()))
            .collect()
    }

    pub fn commands_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<MatrixRaftAdminCommand>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn command_counts_by_priority(&self) -> Vec<(MailPriority, usize)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.command_count))
            .collect()
    }

    pub fn route_key_counts_by_priority(&self) -> Vec<(MailPriority, usize)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.route_keys.len()))
            .collect()
    }

    pub fn command_fanout_counts_by_priority(
        &self,
    ) -> Vec<(MailPriority, usize, usize)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    group.command_count,
                    group.route_keys.len(),
                )
            })
            .collect()
    }

    pub fn command_node_ids_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<NodeId>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.node_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn request_ids_by_group(&self) -> Vec<(GroupId, Vec<Option<u64>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.request_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn request_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.request_ids_by_group())
    }

    pub fn snapshot_ids_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<SnapshotId>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.snapshot_id.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_ids_by_group())
    }

    pub fn snapshot_peer_ids_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<NodeId>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.snapshot_peer_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_peer_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_peer_ids_by_group())
    }

    pub fn snapshot_indices_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<LogIndex>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.snapshot_index)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_index_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_indices_by_group())
    }

    pub fn transferee_ids_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<NodeId>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.transferee_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn transferee_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.transferee_ids_by_group())
    }

    pub fn forced_campaigns_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.forced_campaign)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn log_indices_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<LogIndex>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.log_index)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn log_index_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.log_indices_by_group())
    }

    pub fn storage_fences_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<StorageApplyFence>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.storage_fence.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn storage_fence_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.storage_fence.is_some())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn node_healthy_values_by_group(&self) -> Vec<(GroupId, Vec<Option<bool>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.healthy)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn node_healthy_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.node_healthy_values_by_group())
    }

    pub fn lease_valid_values_by_group(&self) -> Vec<(GroupId, Vec<Option<bool>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.commands
                        .iter()
                        .filter(|command| command.routed.group_id == group.group_id)
                        .map(|command| command.routed.command.lease_valid)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn lease_valid_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.lease_valid_values_by_group())
    }

    pub fn command_node_ids_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<NodeId>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.node_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn request_ids_by_priority(&self) -> Vec<(MailPriority, Vec<Option<u64>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.request_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn request_id_presence_by_priority(&self) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.request_ids_by_priority())
    }

    pub fn snapshot_ids_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<SnapshotId>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.snapshot_id.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_id_presence_by_priority(&self) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.snapshot_ids_by_priority())
    }

    pub fn snapshot_peer_ids_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<NodeId>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.snapshot_peer_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_peer_id_presence_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.snapshot_peer_ids_by_priority())
    }

    pub fn snapshot_indices_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<LogIndex>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.snapshot_index)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_index_presence_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.snapshot_indices_by_priority())
    }

    pub fn transferee_ids_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<NodeId>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.transferee_id)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn transferee_id_presence_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.transferee_ids_by_priority())
    }

    pub fn forced_campaigns_by_priority(&self) -> Vec<(MailPriority, Vec<bool>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.forced_campaign)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn log_indices_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<LogIndex>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.log_index)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn log_index_presence_by_priority(&self) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.log_indices_by_priority())
    }

    pub fn storage_fences_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<StorageApplyFence>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.storage_fence.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn storage_fence_presence_by_priority(&self) -> Vec<(MailPriority, Vec<bool>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.storage_fence.is_some())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn node_healthy_values_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<bool>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.healthy)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn node_healthy_presence_by_priority(&self) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.node_healthy_values_by_priority())
    }

    pub fn lease_valid_values_by_priority(&self) -> Vec<(MailPriority, Vec<Option<bool>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.commands
                        .iter()
                        .filter(|command| command.priority == group.priority)
                        .map(|command| command.routed.command.lease_valid)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn lease_valid_presence_by_priority(&self) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.lease_valid_values_by_priority())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftQueryFanoutGroupPlan {
    pub group_id: GroupId,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_count: usize,
    pub operation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftQueryFanoutPlan {
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub operation: String,
    pub groups: Vec<MatrixRaftQueryFanoutGroupPlan>,
}

impl MatrixRaftQueryFanoutPlan {
    pub fn operation_name(&self) -> String {
        self.operation
            .split(':')
            .next()
            .unwrap_or(self.operation.as_str())
            .to_string()
    }

    pub fn operation_arguments(&self) -> Vec<String> {
        self.operation
            .split(':')
            .skip(1)
            .map(ToString::to_string)
            .collect()
    }

    pub fn operation_argument_count(&self) -> usize {
        self.operation.split(':').skip(1).count()
    }

    pub fn fanout_counts_by_operation(&self) -> Vec<(String, usize, usize, usize)> {
        vec![(
            self.operation_name(),
            self.group_count,
            self.node_count,
            self.route_keys.len(),
        )]
    }

    pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_group(&self) -> Vec<(GroupId, Vec<NodeId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect()
    }

    pub fn node_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_count))
            .collect()
    }

    pub fn route_key_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.len()))
            .collect()
    }

    pub fn fanout_counts_by_group(&self) -> Vec<(GroupId, usize, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_count, group.route_keys.len()))
            .collect()
    }

    pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, route_key.node_id))
            })
            .collect()
    }

    pub fn operations_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, String)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .route_keys
                    .iter()
                    .map(|route_key| (*route_key, group.operation.clone()))
            })
            .collect()
    }

    pub fn operation_names_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, String)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group
                            .operation
                            .split(':')
                            .next()
                            .unwrap_or(group.operation.as_str())
                            .to_string(),
                    )
                })
            })
            .collect()
    }

    pub fn operation_arguments_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Vec<String>)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (
                        *route_key,
                        group
                            .operation
                            .split(':')
                            .skip(1)
                            .map(ToString::to_string)
                            .collect(),
                    )
                })
            })
            .collect()
    }

    pub fn operation_argument_counts_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, usize)> {
        self.groups
            .iter()
            .flat_map(|group| {
                group.route_keys.iter().map(|route_key| {
                    (*route_key, group.operation.split(':').skip(1).count())
                })
            })
            .collect()
    }

    pub fn operations_by_group(&self) -> Vec<(GroupId, String)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.operation.clone()))
            .collect()
    }

    pub fn operation_names_by_group(&self) -> Vec<(GroupId, String)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group
                        .operation
                        .split(':')
                        .next()
                        .unwrap_or(group.operation.as_str())
                        .to_string(),
                )
            })
            .collect()
    }

    pub fn operation_arguments_by_group(&self) -> Vec<(GroupId, Vec<String>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group
                        .operation
                        .split(':')
                        .skip(1)
                        .map(ToString::to_string)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn operation_argument_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.operation.split(':').skip(1).count()))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRoutedMessage {
    pub group_id: GroupId,
    pub runtime_node_id: NodeId,
    pub message: MatrixRaftMessage,
}

impl MatrixRaftRoutedMessage {
    pub fn new(
        group_id: GroupId,
        runtime_node_id: NodeId,
        message: MatrixRaftMessage,
    ) -> Self {
        Self {
            group_id,
            runtime_node_id,
            message,
        }
    }

    pub fn route_key(&self) -> MatrixRaftRouteKey {
        MatrixRaftRouteKey::new(self.group_id, self.runtime_node_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRouteBatchGroupPlan {
    pub group_id: GroupId,
    pub message_count: usize,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub message_types: Vec<MatrixRaftMessageType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRouteBatchPlan {
    pub message_count: usize,
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub message_types: Vec<MatrixRaftMessageType>,
    pub groups: Vec<MatrixRaftRouteBatchGroupPlan>,
    pub messages: Vec<MatrixRaftRoutedMessage>,
}

fn matrixraft_presence_by_route_key<T>(
    values: Vec<(MatrixRaftRouteKey, Option<T>)>,
) -> Vec<(MatrixRaftRouteKey, bool)> {
    values
        .into_iter()
        .map(|(key, value)| (key, value.is_some()))
        .collect()
}

fn matrixraft_presence_by_group<T>(
    values: Vec<(GroupId, Vec<Option<T>>)>,
) -> Vec<(GroupId, Vec<bool>)> {
    values
        .into_iter()
        .map(|(group_id, values)| {
            (
                group_id,
                values.into_iter().map(|value| value.is_some()).collect(),
            )
        })
        .collect()
}

fn matrixraft_presence_by_priority<T>(
    values: Vec<(MailPriority, Vec<Option<T>>)>,
) -> Vec<(MailPriority, Vec<bool>)> {
    values
        .into_iter()
        .map(|(priority, values)| {
            (
                priority,
                values.into_iter().map(|value| value.is_some()).collect(),
            )
        })
        .collect()
}

impl MatrixRaftRouteBatchPlan {
    pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_group(&self) -> Vec<(GroupId, Vec<NodeId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect()
    }

    pub fn message_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message_count))
            .collect()
    }

    pub fn route_key_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.len()))
            .collect()
    }

    pub fn fanout_counts_by_group(&self) -> Vec<(GroupId, usize, usize)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group.message_count,
                    group.route_keys.len(),
                )
            })
            .collect()
    }

    pub fn message_types_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<MatrixRaftMessageType>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message_types.clone()))
            .collect()
    }

    pub fn messages_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftMessage>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.group_id == group.group_id)
                        .map(|message| message.message.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.runtime_node_id))
            .collect()
    }

    pub fn messages_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessage)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.message.clone()))
            .collect()
    }

    pub fn message_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessageType)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.message.message_type))
            .collect()
    }

    pub fn sender_receiver_by_route_key(
        &self,
    ) -> Vec<(
        MatrixRaftRouteKey,
        (Option<NodeId>, Option<NodeId>),
    )> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    (message.message.from, message.message.to),
                )
            })
            .collect()
    }

    pub fn terms_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<Term>)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.message.term))
            .collect()
    }

    pub fn committed_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.message.committed_index))
            .collect()
    }

    pub fn message_bytes_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, u64)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.message.bytes_size))
            .collect()
    }

    pub fn propose_request_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    message
                        .message
                        .propose
                        .as_ref()
                        .and_then(|propose| propose.request_id),
                )
            })
            .collect()
    }

    pub fn propose_request_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.propose_request_ids_by_route_key())
    }

    pub fn snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    message
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.meta.snapshot_id.clone()),
                )
            })
            .collect()
    }

    pub fn snapshot_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_ids_by_route_key())
    }

    pub fn snapshot_chunk_offsets_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    message
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.offset),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_offset_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_chunk_offsets_by_route_key())
    }

    pub fn snapshot_chunk_done_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    message
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.done),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_done_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_chunk_done_by_route_key())
    }

    pub fn snapshot_chunk_payload_bytes_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<usize>)> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    message
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.data.len()),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_payload_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_chunk_payload_bytes_by_route_key())
    }

    pub fn sender_receiver_by_group(
        &self,
    ) -> Vec<(
        GroupId,
        Vec<(Option<NodeId>, Option<NodeId>)>,
    )> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.group_id == group.group_id)
                        .map(|message| (message.message.from, message.message.to))
                        .collect(),
                )
            })
            .collect()
    }

    pub fn terms_by_group(&self) -> Vec<(GroupId, Vec<Option<Term>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.group_id == group.group_id)
                        .map(|message| message.message.term)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn committed_indices_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<LogIndex>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.group_id == group.group_id)
                        .map(|message| message.message.committed_index)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn message_bytes_by_group(&self) -> Vec<(GroupId, Vec<u64>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.group_id == group.group_id)
                        .map(|message| message.message.bytes_size)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn propose_request_ids_by_group(&self) -> Vec<(GroupId, Vec<Option<u64>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.group_id == group.group_id)
                        .map(|message| {
                            message
                                .message
                                .propose
                                .as_ref()
                                .and_then(|propose| propose.request_id)
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn propose_request_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.propose_request_ids_by_group())
    }

    pub fn snapshot_ids_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<SnapshotId>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.group_id == group.group_id)
                        .map(|message| {
                            message
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.meta.snapshot_id.clone())
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_ids_by_group())
    }

    pub fn snapshot_chunk_offsets_by_group(&self) -> Vec<(GroupId, Vec<Option<u64>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.group_id == group.group_id)
                        .map(|message| {
                            message
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.offset)
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_offset_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_chunk_offsets_by_group())
    }

    pub fn snapshot_chunk_done_by_group(&self) -> Vec<(GroupId, Vec<Option<bool>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.group_id == group.group_id)
                        .map(|message| {
                            message
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.done)
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_done_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_chunk_done_by_group())
    }

    pub fn snapshot_chunk_payload_bytes_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<usize>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.group_id == group.group_id)
                        .map(|message| {
                            message
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.data.len())
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_payload_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_chunk_payload_bytes_by_group())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftPriorityRoutedMessage {
    pub priority: MailPriority,
    pub routed: MatrixRaftRoutedMessage,
}

impl MatrixRaftPriorityRoutedMessage {
    pub fn new(priority: MailPriority, routed: MatrixRaftRoutedMessage) -> Self {
        Self { priority, routed }
    }

    pub fn route_key(&self) -> MatrixRaftRouteKey {
        self.routed.route_key()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftPriorityRouteGroupPlan {
    pub priority: MailPriority,
    pub message_count: usize,
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub node_ids: Vec<NodeId>,
    pub message_types: Vec<MatrixRaftMessageType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftPriorityRouteBatchPlan {
    pub message_count: usize,
    pub group_count: usize,
    pub group_ids: Vec<GroupId>,
    pub node_count: usize,
    pub node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub message_types: Vec<MatrixRaftMessageType>,
    pub priority_groups: Vec<MatrixRaftPriorityRouteGroupPlan>,
    pub groups: Vec<MatrixRaftRouteBatchGroupPlan>,
    pub messages: Vec<MatrixRaftPriorityRoutedMessage>,
}

impl MatrixRaftPriorityRouteBatchPlan {
    pub fn route_keys_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftRouteKey>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_group(&self) -> Vec<(GroupId, Vec<NodeId>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.node_ids.clone()))
            .collect()
    }

    pub fn message_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message_count))
            .collect()
    }

    pub fn route_key_counts_by_group(&self) -> Vec<(GroupId, usize)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.route_keys.len()))
            .collect()
    }

    pub fn fanout_counts_by_group(&self) -> Vec<(GroupId, usize, usize)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    group.message_count,
                    group.route_keys.len(),
                )
            })
            .collect()
    }

    pub fn message_types_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<MatrixRaftMessageType>)> {
        self.groups
            .iter()
            .map(|group| (group.group_id, group.message_types.clone()))
            .collect()
    }

    pub fn priorities_by_group(&self) -> Vec<(GroupId, Vec<MailPriority>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| message.priority)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn messages_by_group(&self) -> Vec<(GroupId, Vec<MatrixRaftMessage>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| message.routed.message.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn priorities_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MailPriority)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.priority))
            .collect()
    }

    pub fn messages_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessage)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.routed.message.clone()))
            .collect()
    }

    pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.routed.runtime_node_id))
            .collect()
    }

    pub fn message_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessageType)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.routed.message.message_type))
            .collect()
    }

    pub fn sender_receiver_by_route_key(
        &self,
    ) -> Vec<(
        MatrixRaftRouteKey,
        (Option<NodeId>, Option<NodeId>),
    )> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    (message.routed.message.from, message.routed.message.to),
                )
            })
            .collect()
    }

    pub fn terms_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<Term>)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.routed.message.term))
            .collect()
    }

    pub fn committed_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.routed.message.committed_index))
            .collect()
    }

    pub fn message_bytes_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, u64)> {
        self.messages
            .iter()
            .map(|message| (message.route_key(), message.routed.message.bytes_size))
            .collect()
    }

    pub fn propose_request_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    message
                        .routed
                        .message
                        .propose
                        .as_ref()
                        .and_then(|propose| propose.request_id),
                )
            })
            .collect()
    }

    pub fn propose_request_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.propose_request_ids_by_route_key())
    }

    pub fn snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    message
                        .routed
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.meta.snapshot_id.clone()),
                )
            })
            .collect()
    }

    pub fn snapshot_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_ids_by_route_key())
    }

    pub fn snapshot_chunk_offsets_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    message
                        .routed
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.offset),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_offset_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_chunk_offsets_by_route_key())
    }

    pub fn snapshot_chunk_done_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    message
                        .routed
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.done),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_done_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_chunk_done_by_route_key())
    }

    pub fn snapshot_chunk_payload_bytes_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<usize>)> {
        self.messages
            .iter()
            .map(|message| {
                (
                    message.route_key(),
                    message
                        .routed
                        .message
                        .install_snapshot_request
                        .as_ref()
                        .map(|request| request.chunk.data.len()),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_payload_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        matrixraft_presence_by_route_key(self.snapshot_chunk_payload_bytes_by_route_key())
    }

    pub fn sender_receiver_by_group(
        &self,
    ) -> Vec<(
        GroupId,
        Vec<(Option<NodeId>, Option<NodeId>)>,
    )> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| (message.routed.message.from, message.routed.message.to))
                        .collect(),
                )
            })
            .collect()
    }

    pub fn terms_by_group(&self) -> Vec<(GroupId, Vec<Option<Term>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| message.routed.message.term)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn committed_indices_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<LogIndex>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| message.routed.message.committed_index)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn message_bytes_by_group(&self) -> Vec<(GroupId, Vec<u64>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| message.routed.message.bytes_size)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn propose_request_ids_by_group(&self) -> Vec<(GroupId, Vec<Option<u64>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| {
                            message
                                .routed
                                .message
                                .propose
                                .as_ref()
                                .and_then(|propose| propose.request_id)
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn propose_request_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.propose_request_ids_by_group())
    }

    pub fn snapshot_ids_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<SnapshotId>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| {
                            message
                                .routed
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.meta.snapshot_id.clone())
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_id_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_ids_by_group())
    }

    pub fn snapshot_chunk_offsets_by_group(&self) -> Vec<(GroupId, Vec<Option<u64>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| {
                            message
                                .routed
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.offset)
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_offset_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_chunk_offsets_by_group())
    }

    pub fn snapshot_chunk_done_by_group(&self) -> Vec<(GroupId, Vec<Option<bool>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| {
                            message
                                .routed
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.done)
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_done_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_chunk_done_by_group())
    }

    pub fn snapshot_chunk_payload_bytes_by_group(
        &self,
    ) -> Vec<(GroupId, Vec<Option<usize>>)> {
        self.groups
            .iter()
            .map(|group| {
                (
                    group.group_id,
                    self.messages
                        .iter()
                        .filter(|message| message.routed.group_id == group.group_id)
                        .map(|message| {
                            message
                                .routed
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.data.len())
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_payload_presence_by_group(&self) -> Vec<(GroupId, Vec<bool>)> {
        matrixraft_presence_by_group(self.snapshot_chunk_payload_bytes_by_group())
    }

    pub fn route_keys_by_priority(&self) -> Vec<(MailPriority, Vec<MatrixRaftRouteKey>)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.route_keys.clone()))
            .collect()
    }

    pub fn node_ids_by_priority(&self) -> Vec<(MailPriority, Vec<NodeId>)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.node_ids.clone()))
            .collect()
    }

    pub fn group_ids_by_priority(&self) -> Vec<(MailPriority, Vec<GroupId>)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.group_ids.clone()))
            .collect()
    }

    pub fn message_types_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<MatrixRaftMessageType>)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.message_types.clone()))
            .collect()
    }

    pub fn messages_by_priority(&self) -> Vec<(MailPriority, Vec<MatrixRaftMessage>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.messages
                        .iter()
                        .filter(|message| message.priority == group.priority)
                        .map(|message| message.routed.message.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn sender_receiver_by_priority(
        &self,
    ) -> Vec<(
        MailPriority,
        Vec<(Option<NodeId>, Option<NodeId>)>,
    )> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.messages
                        .iter()
                        .filter(|message| message.priority == group.priority)
                        .map(|message| (message.routed.message.from, message.routed.message.to))
                        .collect(),
                )
            })
            .collect()
    }

    pub fn terms_by_priority(&self) -> Vec<(MailPriority, Vec<Option<Term>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.messages
                        .iter()
                        .filter(|message| message.priority == group.priority)
                        .map(|message| message.routed.message.term)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn committed_indices_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<LogIndex>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.messages
                        .iter()
                        .filter(|message| message.priority == group.priority)
                        .map(|message| message.routed.message.committed_index)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn message_counts_by_priority(&self) -> Vec<(MailPriority, usize)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.message_count))
            .collect()
    }

    pub fn route_key_counts_by_priority(&self) -> Vec<(MailPriority, usize)> {
        self.priority_groups
            .iter()
            .map(|group| (group.priority, group.route_keys.len()))
            .collect()
    }

    pub fn fanout_counts_by_priority(&self) -> Vec<(MailPriority, usize, usize)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    group.message_count,
                    group.route_keys.len(),
                )
            })
            .collect()
    }

    pub fn message_bytes_by_priority(&self) -> Vec<(MailPriority, Vec<u64>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.messages
                        .iter()
                        .filter(|message| message.priority == group.priority)
                        .map(|message| message.routed.message.bytes_size)
                        .collect(),
                )
            })
            .collect()
    }

    pub fn propose_request_ids_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<u64>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.messages
                        .iter()
                        .filter(|message| message.priority == group.priority)
                        .map(|message| {
                            message
                                .routed
                                .message
                                .propose
                                .as_ref()
                                .and_then(|propose| propose.request_id)
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn propose_request_id_presence_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.propose_request_ids_by_priority())
    }

    pub fn snapshot_ids_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<SnapshotId>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.messages
                        .iter()
                        .filter(|message| message.priority == group.priority)
                        .map(|message| {
                            message
                                .routed
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.meta.snapshot_id.clone())
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_id_presence_by_priority(&self) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.snapshot_ids_by_priority())
    }

    pub fn snapshot_chunk_offsets_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<u64>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.messages
                        .iter()
                        .filter(|message| message.priority == group.priority)
                        .map(|message| {
                            message
                                .routed
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.offset)
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_offset_presence_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.snapshot_chunk_offsets_by_priority())
    }

    pub fn snapshot_chunk_done_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<bool>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.messages
                        .iter()
                        .filter(|message| message.priority == group.priority)
                        .map(|message| {
                            message
                                .routed
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.done)
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_done_presence_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.snapshot_chunk_done_by_priority())
    }

    pub fn snapshot_chunk_payload_bytes_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<Option<usize>>)> {
        self.priority_groups
            .iter()
            .map(|group| {
                (
                    group.priority,
                    self.messages
                        .iter()
                        .filter(|message| message.priority == group.priority)
                        .map(|message| {
                            message
                                .routed
                                .message
                                .install_snapshot_request
                                .as_ref()
                                .map(|request| request.chunk.data.len())
                        })
                        .collect(),
                )
            })
            .collect()
    }

    pub fn snapshot_chunk_payload_presence_by_priority(
        &self,
    ) -> Vec<(MailPriority, Vec<bool>)> {
        matrixraft_presence_by_priority(self.snapshot_chunk_payload_bytes_by_priority())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftBatchRouteResult {
    pub group_id: GroupId,
    pub runtime_node_id: NodeId,
    pub message_type: MatrixRaftMessageType,
    #[serde(default)]
    pub result: Option<MatrixRaftRouteResult>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftBatchRouteResultStatus {
    Ok,
    Error,
}

impl MatrixRaftBatchRouteResult {
    fn from_routed_result(routed: &MatrixRaftRoutedMessage, result: MatrixRaftRouteResult) -> Self {
        Self {
            group_id: routed.group_id,
            runtime_node_id: routed.runtime_node_id,
            message_type: routed.message.message_type,
            result: Some(result),
            error: None,
        }
    }

    fn from_routed_error(routed: &MatrixRaftRoutedMessage, error: RaftError) -> Self {
        Self {
            group_id: routed.group_id,
            runtime_node_id: routed.runtime_node_id,
            message_type: routed.message.message_type,
            result: None,
            error: Some(error.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.result.is_some() && self.error.is_none()
    }

    pub fn status(&self) -> MatrixRaftBatchRouteResultStatus {
        if self.is_ok() {
            MatrixRaftBatchRouteResultStatus::Ok
        } else {
            MatrixRaftBatchRouteResultStatus::Error
        }
    }

    pub fn is_error(&self) -> bool {
        self.status() == MatrixRaftBatchRouteResultStatus::Error
    }

    pub fn route_key(&self) -> MatrixRaftRouteKey {
        MatrixRaftRouteKey::new(self.group_id, self.runtime_node_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftBatchRouteGroupSummary {
    pub group_id: GroupId,
    pub result_count: usize,
    pub ok_count: usize,
    pub error_count: usize,
    pub node_ids: Vec<NodeId>,
    pub ok_node_ids: Vec<NodeId>,
    pub error_node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub ok_route_keys: Vec<MatrixRaftRouteKey>,
    pub error_route_keys: Vec<MatrixRaftRouteKey>,
    pub message_types: Vec<MatrixRaftMessageType>,
    pub ok_message_types: Vec<MatrixRaftMessageType>,
    pub error_message_types: Vec<MatrixRaftMessageType>,
    pub counts_by_message_type: Vec<(MatrixRaftMessageType, usize, usize, usize)>,
    #[serde(default)]
    pub results_by_route_key: Vec<(MatrixRaftRouteKey, MatrixRaftBatchRouteResult)>,
    #[serde(default)]
    pub ok_results_by_route_key: Vec<(MatrixRaftRouteKey, MatrixRaftBatchRouteResult)>,
    #[serde(default)]
    pub error_results_by_route_key: Vec<(MatrixRaftRouteKey, MatrixRaftBatchRouteResult)>,
    pub statuses_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    pub errors_by_route_key: Vec<(MatrixRaftRouteKey, Option<String>)>,
    #[serde(default)]
    pub proposed_log_ids_by_route_key: Vec<(MatrixRaftRouteKey, Option<LogId>)>,
    #[serde(default)]
    pub read_index_responses_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)>,
    #[serde(default)]
    pub append_entries_responses_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<MatrixRaftAppendEntriesResponse>)>,
    #[serde(default)]
    pub install_snapshot_responses_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<InstallSnapshotResponse>)>,
    #[serde(default)]
    pub vote_responses_by_route_key: Vec<(MatrixRaftRouteKey, Option<VoteResponse>)>,
    #[serde(default)]
    pub timeout_now_responses_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)>,
    #[serde(default)]
    pub snapshots_by_route_key: Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotDesc>)>,
    #[serde(default)]
    pub snapshot_peer_reports_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotPeerReport>)>,
    #[serde(default)]
    pub apply_results_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<MatrixRaftApplyResultReport>)>,
    #[serde(default)]
    pub synced_reports_by_route_key: Vec<(MatrixRaftRouteKey, Option<MatrixRaftSyncedReport>)>,
    #[serde(default)]
    pub replicated_reports_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<MatrixRaftReplicatedReport>)>,
    #[serde(default)]
    pub compacted_logs_by_route_key: Vec<(MatrixRaftRouteKey, Option<u64>)>,
    #[serde(default)]
    pub fenced_compactions_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<WalCompactionReport>)>,
    #[serde(default)]
    pub checkpoints_by_route_key: Vec<(MatrixRaftRouteKey, Option<RaftSnapshot>)>,
    #[serde(default)]
    pub witness_quorums_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<WitnessQuorumReport>)>,
    #[serde(default)]
    pub released_memory_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub leader_lease_valid_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub leader_lease_confirmed_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub leader_lease_expired_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub follower_lease_received_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub follower_lease_expired_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub node_healthy_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub reorder_queue_dropped_by_route_key: Vec<(MatrixRaftRouteKey, Option<u64>)>,
    #[serde(default)]
    pub fatal_event_transfer_targets_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<NodeId>)>,
}

impl MatrixRaftBatchRouteGroupSummary {
    pub fn from_results(group_id: GroupId, results: &[MatrixRaftBatchRouteResult]) -> Self {
        let mut node_ids = Vec::with_capacity(results.len());
        let mut ok_node_ids = Vec::new();
        let mut error_node_ids = Vec::new();
        let mut route_keys = Vec::with_capacity(results.len());
        let mut ok_route_keys = Vec::new();
        let mut error_route_keys = Vec::new();
        let mut message_types = Vec::new();
        let mut ok_message_types = Vec::new();
        let mut error_message_types = Vec::new();
        let mut counts_by_message_type = Vec::<(MatrixRaftMessageType, usize, usize, usize)>::new();
        let mut results_by_route_key = Vec::with_capacity(results.len());
        let mut ok_results_by_route_key = Vec::new();
        let mut error_results_by_route_key = Vec::new();
        let mut statuses_by_route_key = Vec::with_capacity(results.len());
        let mut errors_by_route_key = Vec::with_capacity(results.len());
        let mut proposed_log_ids_by_route_key = Vec::with_capacity(results.len());
        let mut read_index_responses_by_route_key = Vec::with_capacity(results.len());
        let mut append_entries_responses_by_route_key = Vec::with_capacity(results.len());
        let mut install_snapshot_responses_by_route_key = Vec::with_capacity(results.len());
        let mut vote_responses_by_route_key = Vec::with_capacity(results.len());
        let mut timeout_now_responses_by_route_key = Vec::with_capacity(results.len());
        let mut snapshots_by_route_key = Vec::with_capacity(results.len());
        let mut snapshot_peer_reports_by_route_key = Vec::with_capacity(results.len());
        let mut apply_results_by_route_key = Vec::with_capacity(results.len());
        let mut synced_reports_by_route_key = Vec::with_capacity(results.len());
        let mut replicated_reports_by_route_key = Vec::with_capacity(results.len());
        let mut compacted_logs_by_route_key = Vec::with_capacity(results.len());
        let mut fenced_compactions_by_route_key = Vec::with_capacity(results.len());
        let mut checkpoints_by_route_key = Vec::with_capacity(results.len());
        let mut witness_quorums_by_route_key = Vec::with_capacity(results.len());
        let mut released_memory_by_route_key = Vec::with_capacity(results.len());
        let mut leader_lease_valid_by_route_key = Vec::with_capacity(results.len());
        let mut leader_lease_confirmed_by_route_key = Vec::with_capacity(results.len());
        let mut leader_lease_expired_by_route_key = Vec::with_capacity(results.len());
        let mut follower_lease_received_by_route_key = Vec::with_capacity(results.len());
        let mut follower_lease_expired_by_route_key = Vec::with_capacity(results.len());
        let mut node_healthy_by_route_key = Vec::with_capacity(results.len());
        let mut reorder_queue_dropped_by_route_key = Vec::with_capacity(results.len());
        let mut fatal_event_transfer_targets_by_route_key = Vec::with_capacity(results.len());

        for result in results {
            let key = result.route_key();
            node_ids.push(result.runtime_node_id);
            route_keys.push(key);
            results_by_route_key.push((key, result.clone()));
            if result.is_ok() {
                ok_node_ids.push(result.runtime_node_id);
                ok_route_keys.push(key);
                ok_results_by_route_key.push((key, result.clone()));
                if !ok_message_types.contains(&result.message_type) {
                    ok_message_types.push(result.message_type);
                }
            } else {
                error_node_ids.push(result.runtime_node_id);
                error_route_keys.push(key);
                error_results_by_route_key.push((key, result.clone()));
                if !error_message_types.contains(&result.message_type) {
                    error_message_types.push(result.message_type);
                }
            }
            if !message_types.contains(&result.message_type) {
                message_types.push(result.message_type);
            }
            if let Some((_, total, ok, error)) = counts_by_message_type
                .iter_mut()
                .find(|(message_type, _, _, _)| *message_type == result.message_type)
            {
                *total += 1;
                if result.is_ok() {
                    *ok += 1;
                } else {
                    *error += 1;
                }
            } else {
                counts_by_message_type.push((
                    result.message_type,
                    1,
                    usize::from(result.is_ok()),
                    usize::from(!result.is_ok()),
                ));
            }
            statuses_by_route_key.push((key, result.is_ok()));
            errors_by_route_key.push((key, result.error.clone()));
            let route_result = result.result.as_ref();
            proposed_log_ids_by_route_key
                .push((key, route_result.and_then(|result| result.proposed_log_id.clone())));
            read_index_responses_by_route_key.push((
                key,
                route_result.and_then(|result| result.read_index_response.clone()),
            ));
            append_entries_responses_by_route_key.push((
                key,
                route_result.and_then(|result| result.append_entries_response.clone()),
            ));
            install_snapshot_responses_by_route_key.push((
                key,
                route_result.and_then(|result| result.install_snapshot_response.clone()),
            ));
            vote_responses_by_route_key
                .push((key, route_result.and_then(|result| result.vote_response.clone())));
            timeout_now_responses_by_route_key.push((
                key,
                route_result.and_then(|result| result.timeout_now_response.clone()),
            ));
            snapshots_by_route_key.push((
                key,
                route_result.and_then(|result| result.snapshot.clone()),
            ));
            snapshot_peer_reports_by_route_key.push((
                key,
                route_result.and_then(|result| result.snapshot_peer_report.clone()),
            ));
            apply_results_by_route_key.push((
                key,
                route_result.and_then(|result| result.apply_result.clone()),
            ));
            synced_reports_by_route_key
                .push((key, route_result.and_then(|result| result.synced.clone())));
            replicated_reports_by_route_key.push((
                key,
                route_result.and_then(|result| result.replicated.clone()),
            ));
            compacted_logs_by_route_key
                .push((key, route_result.and_then(|result| result.compacted_logs)));
            fenced_compactions_by_route_key.push((
                key,
                route_result.and_then(|result| result.fenced_compaction.clone()),
            ));
            checkpoints_by_route_key.push((
                key,
                route_result.and_then(|result| result.checkpoint.clone()),
            ));
            witness_quorums_by_route_key.push((
                key,
                route_result.and_then(|result| result.witness_quorum.clone()),
            ));
            released_memory_by_route_key
                .push((key, route_result.and_then(|result| result.released_memory)));
            leader_lease_valid_by_route_key
                .push((key, route_result.and_then(|result| result.leader_lease_valid)));
            leader_lease_confirmed_by_route_key.push((
                key,
                route_result.and_then(|result| result.leader_lease_confirmed),
            ));
            leader_lease_expired_by_route_key.push((
                key,
                route_result.and_then(|result| result.leader_lease_expired),
            ));
            follower_lease_received_by_route_key.push((
                key,
                route_result.and_then(|result| result.follower_lease_received),
            ));
            follower_lease_expired_by_route_key.push((
                key,
                route_result.and_then(|result| result.follower_lease_expired),
            ));
            node_healthy_by_route_key
                .push((key, route_result.and_then(|result| result.node_healthy)));
            reorder_queue_dropped_by_route_key
                .push((key, route_result.and_then(|result| result.reorder_queue_dropped)));
            fatal_event_transfer_targets_by_route_key.push((
                key,
                route_result.and_then(|result| result.fatal_event_transfer_target),
            ));
        }

        let ok_count = ok_route_keys.len();
        Self {
            group_id,
            result_count: results.len(),
            ok_count,
            error_count: results.len().saturating_sub(ok_count),
            node_ids,
            ok_node_ids,
            error_node_ids,
            route_keys,
            ok_route_keys,
            error_route_keys,
            message_types,
            ok_message_types,
            error_message_types,
            counts_by_message_type,
            results_by_route_key,
            ok_results_by_route_key,
            error_results_by_route_key,
            statuses_by_route_key,
            errors_by_route_key,
            proposed_log_ids_by_route_key,
            read_index_responses_by_route_key,
            append_entries_responses_by_route_key,
            install_snapshot_responses_by_route_key,
            vote_responses_by_route_key,
            timeout_now_responses_by_route_key,
            snapshots_by_route_key,
            snapshot_peer_reports_by_route_key,
            apply_results_by_route_key,
            synced_reports_by_route_key,
            replicated_reports_by_route_key,
            compacted_logs_by_route_key,
            fenced_compactions_by_route_key,
            checkpoints_by_route_key,
            witness_quorums_by_route_key,
            released_memory_by_route_key,
            leader_lease_valid_by_route_key,
            leader_lease_confirmed_by_route_key,
            leader_lease_expired_by_route_key,
            follower_lease_received_by_route_key,
            follower_lease_expired_by_route_key,
            node_healthy_by_route_key,
            reorder_queue_dropped_by_route_key,
            fatal_event_transfer_targets_by_route_key,
        }
    }

    pub fn from_grouped_results(
        groups: &[(GroupId, Vec<MatrixRaftBatchRouteResult>)],
    ) -> Vec<Self> {
        groups
            .iter()
            .map(|(group_id, results)| Self::from_results(*group_id, results))
            .collect()
    }

    pub fn is_ok(&self) -> bool {
        self.error_count == 0 && self.ok_count == self.result_count
    }

    pub fn result_counts_by_status(&self) -> (usize, usize) {
        (self.ok_count, self.error_count)
    }

    pub fn route_key_counts_by_status(&self) -> (usize, usize) {
        (self.ok_route_keys.len(), self.error_route_keys.len())
    }

    pub fn status_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftBatchRouteResultStatus)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.status()))
            .collect()
    }

    pub fn ok_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.statuses_by_route_key.clone()
    }

    pub fn error_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.errors_by_route_key
            .iter()
            .map(|(key, error)| (*key, error.is_some()))
            .collect()
    }

    pub fn details_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .map(|route_result| route_result.detail.clone()),
                )
            })
            .collect()
    }

    pub fn ok_details_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .map(|route_result| route_result.detail.clone()),
                )
            })
            .collect()
    }

    pub fn error_details_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .map(|route_result| route_result.detail.clone())
                        .or_else(|| result.error.clone()),
                )
            })
            .collect()
    }

    pub fn message_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessageType)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.message_type))
            .collect()
    }

    pub fn ok_message_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessageType)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.message_type))
            .collect()
    }

    pub fn error_message_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessageType)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.message_type))
            .collect()
    }

    pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.route_keys
            .iter()
            .map(|route_key| (*route_key, route_key.node_id))
            .collect()
    }

    pub fn ok_node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.ok_route_keys
            .iter()
            .map(|route_key| (*route_key, route_key.node_id))
            .collect()
    }

    pub fn error_node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.error_route_keys
            .iter()
            .map(|route_key| (*route_key, route_key.node_id))
            .collect()
    }

    pub fn proposed_log_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.proposed_log_ids_by_route_key
            .iter()
            .map(|(key, log_id)| (*key, log_id.is_some()))
            .collect()
    }

    pub fn proposed_log_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogId>)> {
        self.proposed_log_ids_by_route_key.clone()
    }

    pub fn ok_proposed_log_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogId>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.proposed_log_id.clone()),
                )
            })
            .collect()
    }

    pub fn error_proposed_log_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogId>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.proposed_log_id.clone()),
                )
            })
            .collect()
    }

    pub fn ok_proposed_log_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_proposed_log_ids_by_route_key()
            .into_iter()
            .map(|(key, log_id)| (key, log_id.is_some()))
            .collect()
    }

    pub fn error_proposed_log_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_proposed_log_ids_by_route_key()
            .into_iter()
            .map(|(key, log_id)| (key, log_id.is_some()))
            .collect()
    }

    pub fn read_index_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.read_index_responses_by_route_key.clone()
    }

    pub fn ok_read_index_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.read_index_response.clone()),
                )
            })
            .collect()
    }

    pub fn error_read_index_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.read_index_response.clone()),
                )
            })
            .collect()
    }

    pub fn ok_read_index_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_read_index_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn error_read_index_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_read_index_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn read_index_response_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.read_index_responses_by_route_key
            .iter()
            .map(|(key, response)| (*key, response.is_some()))
            .collect()
    }

    pub fn append_entries_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftAppendEntriesResponse>)> {
        self.append_entries_responses_by_route_key.clone()
    }

    pub fn ok_append_entries_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftAppendEntriesResponse>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.append_entries_response.clone()),
                )
            })
            .collect()
    }

    pub fn error_append_entries_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftAppendEntriesResponse>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.append_entries_response.clone()),
                )
            })
            .collect()
    }

    pub fn ok_append_entries_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_append_entries_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn error_append_entries_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_append_entries_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn append_entries_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.append_entries_responses_by_route_key
            .iter()
            .map(|(key, response)| (*key, response.is_some()))
            .collect()
    }

    pub fn install_snapshot_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<InstallSnapshotResponse>)> {
        self.install_snapshot_responses_by_route_key.clone()
    }

    pub fn ok_install_snapshot_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<InstallSnapshotResponse>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.install_snapshot_response.clone()),
                )
            })
            .collect()
    }

    pub fn error_install_snapshot_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<InstallSnapshotResponse>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.install_snapshot_response.clone()),
                )
            })
            .collect()
    }

    pub fn ok_install_snapshot_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_install_snapshot_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn error_install_snapshot_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_install_snapshot_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn install_snapshot_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.install_snapshot_responses_by_route_key
            .iter()
            .map(|(key, response)| (*key, response.is_some()))
            .collect()
    }

    pub fn vote_responses_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<VoteResponse>)> {
        self.vote_responses_by_route_key.clone()
    }

    pub fn ok_vote_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<VoteResponse>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.vote_response.clone()),
                )
            })
            .collect()
    }

    pub fn error_vote_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<VoteResponse>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.vote_response.clone()),
                )
            })
            .collect()
    }

    pub fn ok_vote_response_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_vote_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn error_vote_response_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_vote_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn vote_response_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.vote_responses_by_route_key
            .iter()
            .map(|(key, response)| (*key, response.is_some()))
            .collect()
    }

    pub fn timeout_now_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)> {
        self.timeout_now_responses_by_route_key.clone()
    }

    pub fn ok_timeout_now_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.timeout_now_response.clone()),
                )
            })
            .collect()
    }

    pub fn error_timeout_now_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.timeout_now_response.clone()),
                )
            })
            .collect()
    }

    pub fn ok_timeout_now_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_timeout_now_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn error_timeout_now_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_timeout_now_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn timeout_now_response_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.timeout_now_responses_by_route_key
            .iter()
            .map(|(key, response)| (*key, response.is_some()))
            .collect()
    }

    pub fn campaign_candidate_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.campaign_candidate_id),
                )
            })
            .collect()
    }

    pub fn ok_campaign_candidate_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.campaign_candidate_id),
                )
            })
            .collect()
    }

    pub fn error_campaign_candidate_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.campaign_candidate_id),
                )
            })
            .collect()
    }

    pub fn campaign_forced_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.campaign_forced),
                )
            })
            .collect()
    }

    pub fn ok_campaign_forced_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.campaign_forced),
                )
            })
            .collect()
    }

    pub fn error_campaign_forced_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.campaign_forced),
                )
            })
            .collect()
    }

    pub fn transfer_leader_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.transfer_leader.as_ref())
                        .map(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn ok_transfer_leader_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.transfer_leader.as_ref())
                        .map(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn error_transfer_leader_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.transfer_leader.as_ref())
                        .map(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn transfer_leader_transferred_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.transfer_leader.as_ref())
                        .map(|report| report.transferred),
                )
            })
            .collect()
    }

    pub fn ok_transfer_leader_transferred_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.transfer_leader.as_ref())
                        .map(|report| report.transferred),
                )
            })
            .collect()
    }

    pub fn error_transfer_leader_transferred_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.transfer_leader.as_ref())
                        .map(|report| report.transferred),
                )
            })
            .collect()
    }

    pub fn leader_transfer_completed_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.leader_transfer_completed),
                )
            })
            .collect()
    }

    pub fn ok_leader_transfer_completed_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.leader_transfer_completed),
                )
            })
            .collect()
    }

    pub fn error_leader_transfer_completed_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.leader_transfer_completed),
                )
            })
            .collect()
    }

    pub fn leader_transfer_aborted_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.leader_transfer_aborted),
                )
            })
            .collect()
    }

    pub fn ok_leader_transfer_aborted_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.leader_transfer_aborted),
                )
            })
            .collect()
    }

    pub fn error_leader_transfer_aborted_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.leader_transfer_aborted),
                )
            })
            .collect()
    }

    pub fn step_down_requested_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.step_down.as_ref())
                        .and_then(|report| report.requested_transferee_id),
                )
            })
            .collect()
    }

    pub fn ok_step_down_requested_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.step_down.as_ref())
                        .and_then(|report| report.requested_transferee_id),
                )
            })
            .collect()
    }

    pub fn error_step_down_requested_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.step_down.as_ref())
                        .and_then(|report| report.requested_transferee_id),
                )
            })
            .collect()
    }

    pub fn step_down_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.step_down.as_ref())
                        .and_then(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn ok_step_down_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.step_down.as_ref())
                        .and_then(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn error_step_down_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.step_down.as_ref())
                        .and_then(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn step_down_stepped_down_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.step_down.as_ref())
                        .map(|report| report.stepped_down),
                )
            })
            .collect()
    }

    pub fn ok_step_down_stepped_down_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.step_down.as_ref())
                        .map(|report| report.stepped_down),
                )
            })
            .collect()
    }

    pub fn error_step_down_stepped_down_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.step_down.as_ref())
                        .map(|report| report.stepped_down),
                )
            })
            .collect()
    }

    pub fn resign_reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.resign.as_ref())
                        .map(|report| report.reason.clone()),
                )
            })
            .collect()
    }

    pub fn ok_resign_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.resign.as_ref())
                        .map(|report| report.reason.clone()),
                )
            })
            .collect()
    }

    pub fn error_resign_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.resign.as_ref())
                        .map(|report| report.reason.clone()),
                )
            })
            .collect()
    }

    pub fn resign_resigned_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.resign.as_ref())
                        .map(|report| report.resigned),
                )
            })
            .collect()
    }

    pub fn ok_resign_resigned_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.resign.as_ref())
                        .map(|report| report.resigned),
                )
            })
            .collect()
    }

    pub fn error_resign_resigned_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.resign.as_ref())
                        .map(|report| report.resigned),
                )
            })
            .collect()
    }

    pub fn membership_success_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.membership.as_ref())
                        .map(|report| report.success),
                )
            })
            .collect()
    }

    pub fn membership_reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.membership.as_ref())
                        .map(|report| report.reason.clone()),
                )
            })
            .collect()
    }

    pub fn catch_up_learner_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.catch_up.as_ref())
                        .map(|report| report.learner_id),
                )
            })
            .collect()
    }

    pub fn catch_up_caught_up_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.catch_up.as_ref())
                        .map(|report| report.caught_up),
                )
            })
            .collect()
    }

    pub fn promote_learner_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.promote.as_ref())
                        .map(|report| report.learner_id),
                )
            })
            .collect()
    }

    pub fn promote_promoted_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.promote.as_ref())
                        .map(|report| report.promoted),
                )
            })
            .collect()
    }

    pub fn promote_membership_success_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.promote.as_ref())
                        .map(|report| report.membership.success),
                )
            })
            .collect()
    }

    pub fn auto_promote_learner_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.auto_promote.as_ref())
                        .map(|report| report.learner_id),
                )
            })
            .collect()
    }

    pub fn auto_promote_enabled_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.auto_promote.as_ref())
                        .map(|report| report.auto_promote),
                )
            })
            .collect()
    }

    pub fn auto_promote_promoted_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|result| result.auto_promote.as_ref())
                        .map(|report| report.promoted),
                )
            })
            .collect()
    }

    pub fn snapshot_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.snapshots_by_route_key
            .iter()
            .map(|(key, snapshot)| (*key, snapshot.is_some()))
            .collect()
    }

    pub fn snapshots_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotDesc>)> {
        self.snapshots_by_route_key.clone()
    }

    pub fn ok_snapshots_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotDesc>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.snapshot.clone()),
                )
            })
            .collect()
    }

    pub fn error_snapshots_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotDesc>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.snapshot.clone()),
                )
            })
            .collect()
    }

    pub fn ok_snapshot_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.is_some()))
            .collect()
    }

    pub fn error_snapshot_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.is_some()))
            .collect()
    }

    pub fn snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.snapshots_by_route_key
            .iter()
            .map(|(key, snapshot)| (*key, snapshot.as_ref().and_then(|snapshot| snapshot.snapshot_id.clone())))
            .collect()
    }

    pub fn ok_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.ok_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.and_then(|snapshot| snapshot.snapshot_id)))
            .collect()
    }

    pub fn error_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.error_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.and_then(|snapshot| snapshot.snapshot_id)))
            .collect()
    }

    pub fn snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.snapshots_by_route_key
            .iter()
            .map(|(key, snapshot)| (*key, snapshot.as_ref().map(|snapshot| snapshot.index)))
            .collect()
    }

    pub fn ok_snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.ok_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.map(|snapshot| snapshot.index)))
            .collect()
    }

    pub fn error_snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.error_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.map(|snapshot| snapshot.index)))
            .collect()
    }

    pub fn snapshot_peer_report_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.snapshot_peer_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.is_some()))
            .collect()
    }

    pub fn snapshot_peer_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotPeerReport>)> {
        self.snapshot_peer_reports_by_route_key.clone()
    }

    pub fn ok_snapshot_peer_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotPeerReport>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.snapshot_peer_report.clone()),
                )
            })
            .collect()
    }

    pub fn error_snapshot_peer_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotPeerReport>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.snapshot_peer_report.clone()),
                )
            })
            .collect()
    }

    pub fn ok_snapshot_peer_report_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_snapshot_peer_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn error_snapshot_peer_report_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_snapshot_peer_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn snapshot_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.snapshot_peer_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.peer_id)))
            .collect()
    }

    pub fn ok_snapshot_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.ok_snapshot_peer_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.peer_id)))
            .collect()
    }

    pub fn error_snapshot_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.error_snapshot_peer_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.peer_id)))
            .collect()
    }

    pub fn apply_result_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.apply_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.is_some()))
            .collect()
    }

    pub fn apply_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftApplyResultReport>)> {
        self.apply_results_by_route_key.clone()
    }

    pub fn ok_apply_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftApplyResultReport>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.apply_result.clone()),
                )
            })
            .collect()
    }

    pub fn error_apply_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftApplyResultReport>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.apply_result.clone()),
                )
            })
            .collect()
    }

    pub fn ok_apply_result_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.is_some()))
            .collect()
    }

    pub fn error_apply_result_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.is_some()))
            .collect()
    }

    pub fn apply_result_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.apply_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.as_ref().map(|result| result.node_id)))
            .collect()
    }

    pub fn ok_apply_result_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.ok_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.node_id)))
            .collect()
    }

    pub fn error_apply_result_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.error_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.node_id)))
            .collect()
    }

    pub fn applied_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.apply_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.as_ref().map(|result| result.applied_index)))
            .collect()
    }

    pub fn ok_applied_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.ok_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.applied_index)))
            .collect()
    }

    pub fn error_applied_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.error_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.applied_index)))
            .collect()
    }

    pub fn apply_rejected_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.apply_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.as_ref().map(|result| result.rejected)))
            .collect()
    }

    pub fn ok_apply_rejected_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.rejected)))
            .collect()
    }

    pub fn error_apply_rejected_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.rejected)))
            .collect()
    }

    pub fn synced_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.synced_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.is_some()))
            .collect()
    }

    pub fn synced_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSyncedReport>)> {
        self.synced_reports_by_route_key.clone()
    }

    pub fn ok_synced_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSyncedReport>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.synced.clone()),
                )
            })
            .collect()
    }

    pub fn error_synced_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSyncedReport>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.synced.clone()),
                )
            })
            .collect()
    }

    pub fn ok_synced_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn error_synced_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn synced_first_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.synced_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().and_then(|report| report.first_index)))
            .collect()
    }

    pub fn ok_synced_first_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.ok_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.and_then(|report| report.first_index)))
            .collect()
    }

    pub fn error_synced_first_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.error_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.and_then(|report| report.first_index)))
            .collect()
    }

    pub fn synced_last_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.synced_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().and_then(|report| report.last_index)))
            .collect()
    }

    pub fn ok_synced_last_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.ok_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.and_then(|report| report.last_index)))
            .collect()
    }

    pub fn error_synced_last_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.error_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.and_then(|report| report.last_index)))
            .collect()
    }

    pub fn synced_stabled_config_change_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.synced_reports_by_route_key
            .iter()
            .map(|(key, report)| {
                (
                    *key,
                    report
                        .as_ref()
                        .map(|report| report.stabled_config_change_index),
                )
            })
            .collect()
    }

    pub fn ok_synced_stabled_config_change_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.ok_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.stabled_config_change_index)))
            .collect()
    }

    pub fn error_synced_stabled_config_change_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.error_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.stabled_config_change_index)))
            .collect()
    }

    pub fn replicated_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.replicated_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.is_some()))
            .collect()
    }

    pub fn replicated_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftReplicatedReport>)> {
        self.replicated_reports_by_route_key.clone()
    }

    pub fn ok_replicated_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftReplicatedReport>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.replicated.clone()),
                )
            })
            .collect()
    }

    pub fn error_replicated_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftReplicatedReport>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.replicated.clone()),
                )
            })
            .collect()
    }

    pub fn ok_replicated_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn error_replicated_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn replicated_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.replicated_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.peer_id)))
            .collect()
    }

    pub fn ok_replicated_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.ok_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.peer_id)))
            .collect()
    }

    pub fn error_replicated_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.error_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.peer_id)))
            .collect()
    }

    pub fn replicated_success_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.replicated_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.success)))
            .collect()
    }

    pub fn ok_replicated_success_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.success)))
            .collect()
    }

    pub fn error_replicated_success_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.success)))
            .collect()
    }

    pub fn compacted_logs_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.compacted_logs_by_route_key
            .iter()
            .map(|(key, compacted)| (*key, compacted.is_some()))
            .collect()
    }

    pub fn compacted_logs_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.compacted_logs_by_route_key.clone()
    }

    pub fn ok_compacted_logs_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.compacted_logs),
                )
            })
            .collect()
    }

    pub fn error_compacted_logs_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.compacted_logs),
                )
            })
            .collect()
    }

    pub fn ok_compacted_logs_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_compacted_logs_by_route_key()
            .into_iter()
            .map(|(key, compacted)| (key, compacted.is_some()))
            .collect()
    }

    pub fn error_compacted_logs_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_compacted_logs_by_route_key()
            .into_iter()
            .map(|(key, compacted)| (key, compacted.is_some()))
            .collect()
    }

    pub fn fenced_compaction_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.fenced_compactions_by_route_key
            .iter()
            .map(|(key, compaction)| (*key, compaction.is_some()))
            .collect()
    }

    pub fn fenced_compactions_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WalCompactionReport>)> {
        self.fenced_compactions_by_route_key.clone()
    }

    pub fn ok_fenced_compactions_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WalCompactionReport>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.fenced_compaction.clone()),
                )
            })
            .collect()
    }

    pub fn error_fenced_compactions_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WalCompactionReport>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.fenced_compaction.clone()),
                )
            })
            .collect()
    }

    pub fn ok_fenced_compaction_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_fenced_compactions_by_route_key()
            .into_iter()
            .map(|(key, compaction)| (key, compaction.is_some()))
            .collect()
    }

    pub fn error_fenced_compaction_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_fenced_compactions_by_route_key()
            .into_iter()
            .map(|(key, compaction)| (key, compaction.is_some()))
            .collect()
    }

    pub fn checkpoint_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.checkpoints_by_route_key
            .iter()
            .map(|(key, checkpoint)| (*key, checkpoint.is_some()))
            .collect()
    }

    pub fn checkpoints_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<RaftSnapshot>)> {
        self.checkpoints_by_route_key.clone()
    }

    pub fn ok_checkpoints_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<RaftSnapshot>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.checkpoint.clone()),
                )
            })
            .collect()
    }

    pub fn error_checkpoints_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<RaftSnapshot>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.checkpoint.clone()),
                )
            })
            .collect()
    }

    pub fn ok_checkpoint_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| (key, checkpoint.is_some()))
            .collect()
    }

    pub fn error_checkpoint_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| (key, checkpoint.is_some()))
            .collect()
    }

    pub fn checkpoint_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.checkpoints_by_route_key
            .iter()
            .map(|(key, checkpoint)| {
                (
                    *key,
                    checkpoint
                        .as_ref()
                        .map(|checkpoint| checkpoint.meta.snapshot_id.clone()),
                )
            })
            .collect()
    }

    pub fn ok_checkpoint_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.ok_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| {
                (
                    key,
                    checkpoint.map(|checkpoint| checkpoint.meta.snapshot_id),
                )
            })
            .collect()
    }

    pub fn error_checkpoint_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.error_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| {
                (
                    key,
                    checkpoint.map(|checkpoint| checkpoint.meta.snapshot_id),
                )
            })
            .collect()
    }

    pub fn checkpoint_last_log_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.checkpoints_by_route_key
            .iter()
            .map(|(key, checkpoint)| {
                (
                    *key,
                    checkpoint
                        .as_ref()
                        .map(|checkpoint| checkpoint.meta.last_log_id.index),
                )
            })
            .collect()
    }

    pub fn ok_checkpoint_last_log_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.ok_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| {
                (
                    key,
                    checkpoint.map(|checkpoint| checkpoint.meta.last_log_id.index),
                )
            })
            .collect()
    }

    pub fn error_checkpoint_last_log_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.error_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| {
                (
                    key,
                    checkpoint.map(|checkpoint| checkpoint.meta.last_log_id.index),
                )
            })
            .collect()
    }

    pub fn witness_quorum_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.witness_quorums_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.is_some()))
            .collect()
    }

    pub fn witness_quorums_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WitnessQuorumReport>)> {
        self.witness_quorums_by_route_key.clone()
    }

    pub fn ok_witness_quorums_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WitnessQuorumReport>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.witness_quorum.clone()),
                )
            })
            .collect()
    }

    pub fn error_witness_quorums_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WitnessQuorumReport>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.witness_quorum.clone()),
                )
            })
            .collect()
    }

    pub fn ok_witness_quorum_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn error_witness_quorum_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn witness_quorum_required_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.witness_quorums_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.required)))
            .collect()
    }

    pub fn ok_witness_quorum_required_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.ok_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.required)))
            .collect()
    }

    pub fn error_witness_quorum_required_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.error_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.required)))
            .collect()
    }

    pub fn witness_quorum_acknowledged_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.witness_quorums_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.acknowledged)))
            .collect()
    }

    pub fn ok_witness_quorum_acknowledged_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.ok_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.acknowledged)))
            .collect()
    }

    pub fn error_witness_quorum_acknowledged_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.error_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.acknowledged)))
            .collect()
    }

    pub fn witness_quorum_reached_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.witness_quorums_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.reached)))
            .collect()
    }

    pub fn ok_witness_quorum_reached_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.reached)))
            .collect()
    }

    pub fn error_witness_quorum_reached_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.reached)))
            .collect()
    }

    pub fn released_memory_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.released_memory_by_route_key
            .iter()
            .map(|(key, released)| (*key, released.is_some()))
            .collect()
    }

    pub fn released_memory_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.released_memory_by_route_key.clone()
    }

    pub fn ok_released_memory_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.released_memory),
                )
            })
            .collect()
    }

    pub fn error_released_memory_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.released_memory),
                )
            })
            .collect()
    }

    pub fn ok_released_memory_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_released_memory_values_by_route_key()
            .into_iter()
            .map(|(key, released)| (key, released.is_some()))
            .collect()
    }

    pub fn error_released_memory_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_released_memory_values_by_route_key()
            .into_iter()
            .map(|(key, released)| (key, released.is_some()))
            .collect()
    }

    pub fn leader_lease_valid_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.leader_lease_valid_by_route_key
            .iter()
            .map(|(key, valid)| (*key, valid.is_some()))
            .collect()
    }

    pub fn leader_lease_valid_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.leader_lease_valid_by_route_key.clone()
    }

    pub fn ok_leader_lease_valid_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.leader_lease_valid),
                )
            })
            .collect()
    }

    pub fn error_leader_lease_valid_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.leader_lease_valid),
                )
            })
            .collect()
    }

    pub fn ok_leader_lease_valid_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_leader_lease_valid_values_by_route_key()
            .into_iter()
            .map(|(key, valid)| (key, valid.is_some()))
            .collect()
    }

    pub fn error_leader_lease_valid_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_leader_lease_valid_values_by_route_key()
            .into_iter()
            .map(|(key, valid)| (key, valid.is_some()))
            .collect()
    }

    pub fn leader_lease_confirmed_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.leader_lease_confirmed_by_route_key
            .iter()
            .map(|(key, confirmed)| (*key, confirmed.is_some()))
            .collect()
    }

    pub fn leader_lease_confirmed_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.leader_lease_confirmed_by_route_key.clone()
    }

    pub fn ok_leader_lease_confirmed_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.leader_lease_confirmed),
                )
            })
            .collect()
    }

    pub fn error_leader_lease_confirmed_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.leader_lease_confirmed),
                )
            })
            .collect()
    }

    pub fn ok_leader_lease_confirmed_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_leader_lease_confirmed_values_by_route_key()
            .into_iter()
            .map(|(key, confirmed)| (key, confirmed.is_some()))
            .collect()
    }

    pub fn error_leader_lease_confirmed_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_leader_lease_confirmed_values_by_route_key()
            .into_iter()
            .map(|(key, confirmed)| (key, confirmed.is_some()))
            .collect()
    }

    pub fn leader_lease_expired_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.leader_lease_expired_by_route_key
            .iter()
            .map(|(key, expired)| (*key, expired.is_some()))
            .collect()
    }

    pub fn leader_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.leader_lease_expired_by_route_key.clone()
    }

    pub fn ok_leader_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.leader_lease_expired),
                )
            })
            .collect()
    }

    pub fn error_leader_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.leader_lease_expired),
                )
            })
            .collect()
    }

    pub fn ok_leader_lease_expired_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_leader_lease_expired_values_by_route_key()
            .into_iter()
            .map(|(key, expired)| (key, expired.is_some()))
            .collect()
    }

    pub fn error_leader_lease_expired_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_leader_lease_expired_values_by_route_key()
            .into_iter()
            .map(|(key, expired)| (key, expired.is_some()))
            .collect()
    }

    pub fn follower_lease_received_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.follower_lease_received_by_route_key
            .iter()
            .map(|(key, received)| (*key, received.is_some()))
            .collect()
    }

    pub fn follower_lease_received_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.follower_lease_received_by_route_key.clone()
    }

    pub fn ok_follower_lease_received_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.follower_lease_received),
                )
            })
            .collect()
    }

    pub fn error_follower_lease_received_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.follower_lease_received),
                )
            })
            .collect()
    }

    pub fn ok_follower_lease_received_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_follower_lease_received_values_by_route_key()
            .into_iter()
            .map(|(key, received)| (key, received.is_some()))
            .collect()
    }

    pub fn error_follower_lease_received_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_follower_lease_received_values_by_route_key()
            .into_iter()
            .map(|(key, received)| (key, received.is_some()))
            .collect()
    }

    pub fn follower_lease_expired_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.follower_lease_expired_by_route_key
            .iter()
            .map(|(key, expired)| (*key, expired.is_some()))
            .collect()
    }

    pub fn follower_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.follower_lease_expired_by_route_key.clone()
    }

    pub fn ok_follower_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.follower_lease_expired),
                )
            })
            .collect()
    }

    pub fn error_follower_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.follower_lease_expired),
                )
            })
            .collect()
    }

    pub fn ok_follower_lease_expired_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_follower_lease_expired_values_by_route_key()
            .into_iter()
            .map(|(key, expired)| (key, expired.is_some()))
            .collect()
    }

    pub fn error_follower_lease_expired_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_follower_lease_expired_values_by_route_key()
            .into_iter()
            .map(|(key, expired)| (key, expired.is_some()))
            .collect()
    }

    pub fn node_healthy_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.node_healthy_by_route_key
            .iter()
            .map(|(key, healthy)| (*key, healthy.is_some()))
            .collect()
    }

    pub fn node_healthy_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.node_healthy_by_route_key.clone()
    }

    pub fn ok_node_healthy_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.node_healthy),
                )
            })
            .collect()
    }

    pub fn error_node_healthy_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.node_healthy),
                )
            })
            .collect()
    }

    pub fn ok_node_healthy_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_node_healthy_values_by_route_key()
            .into_iter()
            .map(|(key, healthy)| (key, healthy.is_some()))
            .collect()
    }

    pub fn error_node_healthy_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_node_healthy_values_by_route_key()
            .into_iter()
            .map(|(key, healthy)| (key, healthy.is_some()))
            .collect()
    }

    pub fn reorder_queue_dropped_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.reorder_queue_dropped_by_route_key
            .iter()
            .map(|(key, dropped)| (*key, dropped.is_some()))
            .collect()
    }

    pub fn reorder_queue_dropped_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.reorder_queue_dropped_by_route_key.clone()
    }

    pub fn ok_reorder_queue_dropped_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.reorder_queue_dropped),
                )
            })
            .collect()
    }

    pub fn error_reorder_queue_dropped_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.reorder_queue_dropped),
                )
            })
            .collect()
    }

    pub fn ok_reorder_queue_dropped_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_reorder_queue_dropped_values_by_route_key()
            .into_iter()
            .map(|(key, dropped)| (key, dropped.is_some()))
            .collect()
    }

    pub fn error_reorder_queue_dropped_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_reorder_queue_dropped_values_by_route_key()
            .into_iter()
            .map(|(key, dropped)| (key, dropped.is_some()))
            .collect()
    }

    pub fn fatal_event_transfer_target_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.fatal_event_transfer_targets_by_route_key
            .iter()
            .map(|(key, target)| (*key, target.is_some()))
            .collect()
    }

    pub fn fatal_event_transfer_targets_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.fatal_event_transfer_targets_by_route_key.clone()
    }

    pub fn ok_fatal_event_transfer_targets_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.ok_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.fatal_event_transfer_target),
                )
            })
            .collect()
    }

    pub fn error_fatal_event_transfer_targets_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.error_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .result
                        .as_ref()
                        .and_then(|route_result| route_result.fatal_event_transfer_target),
                )
            })
            .collect()
    }

    pub fn ok_fatal_event_transfer_target_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.ok_fatal_event_transfer_targets_by_route_key()
            .into_iter()
            .map(|(key, target)| (key, target.is_some()))
            .collect()
    }

    pub fn error_fatal_event_transfer_target_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.error_fatal_event_transfer_targets_by_route_key()
            .into_iter()
            .map(|(key, target)| (key, target.is_some()))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftRouteResultKind {
    Delivered,
    SnapshotRegistered,
    SnapshotFinished,
    AcceptedMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftApplyResultReport {
    pub node_id: NodeId,
    pub applied_index: LogIndex,
    pub rejected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftSyncedReport {
    pub first_index: Option<LogIndex>,
    pub last_index: Option<LogIndex>,
    pub stabled_config_change_index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftReplicatedReport {
    pub peer_id: NodeId,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRouteResult {
    pub key: MatrixRaftRouteKey,
    pub message_type: MatrixRaftMessageType,
    pub kind: MatrixRaftRouteResultKind,
    pub handled: bool,
    pub detail: String,
    #[serde(default)]
    pub proposed_log_id: Option<LogId>,
    #[serde(default)]
    pub membership: Option<MembershipExecutionReport>,
    #[serde(default)]
    pub append_entries_response: Option<MatrixRaftAppendEntriesResponse>,
    #[serde(default)]
    pub install_snapshot_response: Option<InstallSnapshotResponse>,
    #[serde(default)]
    pub read_index_response: Option<ReadIndexResponse>,
    #[serde(default)]
    pub catch_up: Option<LearnerCatchUpLoopReport>,
    #[serde(default)]
    pub promote: Option<MatrixRaftPromoteReport>,
    #[serde(default)]
    pub auto_promote: Option<LearnerAutoPromoteReport>,
    #[serde(default)]
    pub vote_response: Option<VoteResponse>,
    #[serde(default)]
    pub campaign_candidate_id: Option<NodeId>,
    #[serde(default)]
    pub campaign_forced: Option<bool>,
    #[serde(default)]
    pub transfer_leader: Option<MatrixRaftTransferLeaderReport>,
    #[serde(default)]
    pub leader_transfer_completed: Option<bool>,
    #[serde(default)]
    pub leader_transfer_aborted: Option<bool>,
    #[serde(default)]
    pub step_down: Option<MatrixRaftStepDownReport>,
    #[serde(default)]
    pub resign: Option<MatrixRaftResignReport>,
    #[serde(default)]
    pub timeout_now_response: Option<TimeoutNowResponse>,
    #[serde(default)]
    pub snapshot: Option<MatrixRaftSnapshotDesc>,
    #[serde(default)]
    pub snapshot_peer_report: Option<MatrixRaftSnapshotPeerReport>,
    #[serde(default)]
    pub apply_result: Option<MatrixRaftApplyResultReport>,
    #[serde(default)]
    pub synced: Option<MatrixRaftSyncedReport>,
    #[serde(default)]
    pub replicated: Option<MatrixRaftReplicatedReport>,
    #[serde(default)]
    pub compacted_logs: Option<u64>,
    #[serde(default)]
    pub fenced_compaction: Option<WalCompactionReport>,
    #[serde(default)]
    pub checkpoint: Option<RaftSnapshot>,
    #[serde(default)]
    pub witness_quorum: Option<WitnessQuorumReport>,
    #[serde(default)]
    pub released_memory: Option<bool>,
    #[serde(default)]
    pub leader_lease_valid: Option<bool>,
    #[serde(default)]
    pub leader_lease_confirmed: Option<bool>,
    #[serde(default)]
    pub leader_lease_expired: Option<bool>,
    #[serde(default)]
    pub follower_lease_received: Option<bool>,
    #[serde(default)]
    pub follower_lease_expired: Option<bool>,
    #[serde(default)]
    pub node_healthy: Option<bool>,
    #[serde(default)]
    pub reorder_queue_dropped: Option<u64>,
    #[serde(default)]
    pub fatal_event_transfer_target: Option<NodeId>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftRouteResultStatus {
    Handled,
    Unhandled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRouteGroupSummary {
    pub group_id: GroupId,
    pub result_count: usize,
    pub handled_count: usize,
    pub unhandled_count: usize,
    pub node_ids: Vec<NodeId>,
    pub handled_node_ids: Vec<NodeId>,
    pub unhandled_node_ids: Vec<NodeId>,
    pub route_keys: Vec<MatrixRaftRouteKey>,
    pub handled_route_keys: Vec<MatrixRaftRouteKey>,
    pub unhandled_route_keys: Vec<MatrixRaftRouteKey>,
    pub message_types: Vec<MatrixRaftMessageType>,
    pub kinds: Vec<MatrixRaftRouteResultKind>,
    pub handled_message_types: Vec<MatrixRaftMessageType>,
    pub unhandled_message_types: Vec<MatrixRaftMessageType>,
    pub handled_kinds: Vec<MatrixRaftRouteResultKind>,
    pub unhandled_kinds: Vec<MatrixRaftRouteResultKind>,
    pub counts_by_message_type: Vec<(MatrixRaftMessageType, usize, usize, usize)>,
    pub counts_by_kind: Vec<(MatrixRaftRouteResultKind, usize, usize, usize)>,
    #[serde(default)]
    pub results_by_route_key: Vec<(MatrixRaftRouteKey, MatrixRaftRouteResult)>,
    #[serde(default)]
    pub handled_results_by_route_key: Vec<(MatrixRaftRouteKey, MatrixRaftRouteResult)>,
    #[serde(default)]
    pub unhandled_results_by_route_key: Vec<(MatrixRaftRouteKey, MatrixRaftRouteResult)>,
    pub handled_by_route_key: Vec<(MatrixRaftRouteKey, bool)>,
    #[serde(default)]
    pub proposed_log_ids_by_route_key: Vec<(MatrixRaftRouteKey, Option<LogId>)>,
    #[serde(default)]
    pub read_index_responses_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)>,
    #[serde(default)]
    pub append_entries_responses_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<MatrixRaftAppendEntriesResponse>)>,
    #[serde(default)]
    pub install_snapshot_responses_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<InstallSnapshotResponse>)>,
    #[serde(default)]
    pub vote_responses_by_route_key: Vec<(MatrixRaftRouteKey, Option<VoteResponse>)>,
    #[serde(default)]
    pub timeout_now_responses_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)>,
    #[serde(default)]
    pub snapshots_by_route_key: Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotDesc>)>,
    #[serde(default)]
    pub snapshot_peer_reports_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotPeerReport>)>,
    #[serde(default)]
    pub apply_results_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<MatrixRaftApplyResultReport>)>,
    #[serde(default)]
    pub synced_reports_by_route_key: Vec<(MatrixRaftRouteKey, Option<MatrixRaftSyncedReport>)>,
    #[serde(default)]
    pub replicated_reports_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<MatrixRaftReplicatedReport>)>,
    #[serde(default)]
    pub compacted_logs_by_route_key: Vec<(MatrixRaftRouteKey, Option<u64>)>,
    #[serde(default)]
    pub fenced_compactions_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<WalCompactionReport>)>,
    #[serde(default)]
    pub checkpoints_by_route_key: Vec<(MatrixRaftRouteKey, Option<RaftSnapshot>)>,
    #[serde(default)]
    pub witness_quorums_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<WitnessQuorumReport>)>,
    #[serde(default)]
    pub released_memory_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub leader_lease_valid_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub leader_lease_confirmed_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub leader_lease_expired_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub follower_lease_received_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub follower_lease_expired_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub node_healthy_by_route_key: Vec<(MatrixRaftRouteKey, Option<bool>)>,
    #[serde(default)]
    pub reorder_queue_dropped_by_route_key: Vec<(MatrixRaftRouteKey, Option<u64>)>,
    #[serde(default)]
    pub fatal_event_transfer_targets_by_route_key:
        Vec<(MatrixRaftRouteKey, Option<NodeId>)>,
}

impl MatrixRaftRouteGroupSummary {
    pub fn handled_step_down_requested_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .step_down
                        .as_ref()
                        .and_then(|report| report.requested_transferee_id),
                )
            })
            .collect()
    }

    pub fn unhandled_step_down_requested_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .step_down
                        .as_ref()
                        .and_then(|report| report.requested_transferee_id),
                )
            })
            .collect()
    }

    pub fn handled_step_down_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .step_down
                        .as_ref()
                        .and_then(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn unhandled_step_down_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .step_down
                        .as_ref()
                        .and_then(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn handled_step_down_stepped_down_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .step_down
                        .as_ref()
                        .map(|report| report.stepped_down),
                )
            })
            .collect()
    }

    pub fn unhandled_step_down_stepped_down_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .step_down
                        .as_ref()
                        .map(|report| report.stepped_down),
                )
            })
            .collect()
    }

    pub fn handled_resign_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result.resign.as_ref().map(|report| report.reason.clone()),
                )
            })
            .collect()
    }

    pub fn unhandled_resign_reasons_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result.resign.as_ref().map(|report| report.reason.clone()),
                )
            })
            .collect()
    }

    pub fn handled_snapshots_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotDesc>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.snapshot.clone()))
            .collect()
    }

    pub fn unhandled_snapshots_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotDesc>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.snapshot.clone()))
            .collect()
    }

    pub fn handled_snapshot_peer_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotPeerReport>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.snapshot_peer_report.clone()))
            .collect()
    }

    pub fn unhandled_snapshot_peer_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotPeerReport>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.snapshot_peer_report.clone()))
            .collect()
    }

    pub fn handled_apply_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftApplyResultReport>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.apply_result.clone()))
            .collect()
    }

    pub fn unhandled_apply_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftApplyResultReport>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.apply_result.clone()))
            .collect()
    }

    pub fn handled_synced_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSyncedReport>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.synced.clone()))
            .collect()
    }

    pub fn unhandled_synced_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSyncedReport>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.synced.clone()))
            .collect()
    }

    pub fn handled_replicated_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftReplicatedReport>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.replicated.clone()))
            .collect()
    }

    pub fn unhandled_replicated_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftReplicatedReport>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.replicated.clone()))
            .collect()
    }

    pub fn handled_compacted_logs_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.compacted_logs))
            .collect()
    }

    pub fn unhandled_compacted_logs_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.compacted_logs))
            .collect()
    }

    pub fn handled_fenced_compactions_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WalCompactionReport>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.fenced_compaction.clone()))
            .collect()
    }

    pub fn unhandled_fenced_compactions_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WalCompactionReport>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.fenced_compaction.clone()))
            .collect()
    }

    pub fn handled_checkpoints_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<RaftSnapshot>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.checkpoint.clone()))
            .collect()
    }

    pub fn unhandled_checkpoints_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<RaftSnapshot>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.checkpoint.clone()))
            .collect()
    }

    pub fn handled_witness_quorums_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WitnessQuorumReport>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.witness_quorum.clone()))
            .collect()
    }

    pub fn unhandled_witness_quorums_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WitnessQuorumReport>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.witness_quorum.clone()))
            .collect()
    }

    pub fn handled_snapshot_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.is_some()))
            .collect()
    }

    pub fn unhandled_snapshot_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.is_some()))
            .collect()
    }

    pub fn handled_snapshot_peer_report_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_snapshot_peer_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn unhandled_snapshot_peer_report_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_snapshot_peer_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn handled_apply_result_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.is_some()))
            .collect()
    }

    pub fn unhandled_apply_result_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.is_some()))
            .collect()
    }

    pub fn handled_synced_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn unhandled_synced_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn handled_replicated_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn unhandled_replicated_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn handled_compacted_logs_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_compacted_logs_by_route_key()
            .into_iter()
            .map(|(key, compacted)| (key, compacted.is_some()))
            .collect()
    }

    pub fn unhandled_compacted_logs_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_compacted_logs_by_route_key()
            .into_iter()
            .map(|(key, compacted)| (key, compacted.is_some()))
            .collect()
    }

    pub fn handled_fenced_compaction_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_fenced_compactions_by_route_key()
            .into_iter()
            .map(|(key, compaction)| (key, compaction.is_some()))
            .collect()
    }

    pub fn unhandled_fenced_compaction_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_fenced_compactions_by_route_key()
            .into_iter()
            .map(|(key, compaction)| (key, compaction.is_some()))
            .collect()
    }

    pub fn handled_checkpoint_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| (key, checkpoint.is_some()))
            .collect()
    }

    pub fn unhandled_checkpoint_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| (key, checkpoint.is_some()))
            .collect()
    }

    pub fn handled_witness_quorum_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn unhandled_witness_quorum_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.is_some()))
            .collect()
    }

    pub fn handled_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.handled_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.and_then(|snapshot| snapshot.snapshot_id)))
            .collect()
    }

    pub fn unhandled_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.unhandled_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.and_then(|snapshot| snapshot.snapshot_id)))
            .collect()
    }

    pub fn handled_snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.handled_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.map(|snapshot| snapshot.index)))
            .collect()
    }

    pub fn unhandled_snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.unhandled_snapshots_by_route_key()
            .into_iter()
            .map(|(key, snapshot)| (key, snapshot.map(|snapshot| snapshot.index)))
            .collect()
    }

    pub fn handled_snapshot_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.handled_snapshot_peer_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.peer_id)))
            .collect()
    }

    pub fn unhandled_snapshot_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.unhandled_snapshot_peer_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.peer_id)))
            .collect()
    }

    pub fn handled_apply_result_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.handled_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.node_id)))
            .collect()
    }

    pub fn unhandled_apply_result_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.unhandled_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.node_id)))
            .collect()
    }

    pub fn handled_applied_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.handled_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.applied_index)))
            .collect()
    }

    pub fn unhandled_applied_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.unhandled_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.applied_index)))
            .collect()
    }

    pub fn handled_apply_rejected_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.rejected)))
            .collect()
    }

    pub fn unhandled_apply_rejected_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_apply_results_by_route_key()
            .into_iter()
            .map(|(key, result)| (key, result.map(|result| result.rejected)))
            .collect()
    }

    pub fn handled_synced_first_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.handled_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.and_then(|report| report.first_index)))
            .collect()
    }

    pub fn unhandled_synced_first_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.unhandled_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.and_then(|report| report.first_index)))
            .collect()
    }

    pub fn handled_synced_last_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.handled_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.and_then(|report| report.last_index)))
            .collect()
    }

    pub fn unhandled_synced_last_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.unhandled_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.and_then(|report| report.last_index)))
            .collect()
    }

    pub fn handled_synced_stabled_config_change_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.handled_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.stabled_config_change_index)))
            .collect()
    }

    pub fn unhandled_synced_stabled_config_change_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.unhandled_synced_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.stabled_config_change_index)))
            .collect()
    }

    pub fn handled_replicated_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.handled_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.peer_id)))
            .collect()
    }

    pub fn unhandled_replicated_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.unhandled_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.peer_id)))
            .collect()
    }

    pub fn handled_replicated_success_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.success)))
            .collect()
    }

    pub fn unhandled_replicated_success_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_replicated_reports_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.success)))
            .collect()
    }

    pub fn handled_checkpoint_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.handled_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| (key, checkpoint.map(|checkpoint| checkpoint.meta.snapshot_id)))
            .collect()
    }

    pub fn unhandled_checkpoint_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.unhandled_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| (key, checkpoint.map(|checkpoint| checkpoint.meta.snapshot_id)))
            .collect()
    }

    pub fn handled_checkpoint_last_log_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.handled_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| {
                (
                    key,
                    checkpoint.map(|checkpoint| checkpoint.meta.last_log_id.index),
                )
            })
            .collect()
    }

    pub fn unhandled_checkpoint_last_log_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.unhandled_checkpoints_by_route_key()
            .into_iter()
            .map(|(key, checkpoint)| {
                (
                    key,
                    checkpoint.map(|checkpoint| checkpoint.meta.last_log_id.index),
                )
            })
            .collect()
    }

    pub fn handled_witness_quorum_required_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.handled_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.required)))
            .collect()
    }

    pub fn unhandled_witness_quorum_required_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.unhandled_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.required)))
            .collect()
    }

    pub fn handled_witness_quorum_acknowledged_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.handled_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.acknowledged)))
            .collect()
    }

    pub fn unhandled_witness_quorum_acknowledged_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.unhandled_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.acknowledged)))
            .collect()
    }

    pub fn handled_witness_quorum_reached_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.reached)))
            .collect()
    }

    pub fn unhandled_witness_quorum_reached_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_witness_quorums_by_route_key()
            .into_iter()
            .map(|(key, report)| (key, report.map(|report| report.reached)))
            .collect()
    }

    pub fn handled_released_memory_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.released_memory))
            .collect()
    }

    pub fn unhandled_released_memory_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.released_memory))
            .collect()
    }

    pub fn handled_released_memory_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_released_memory_values_by_route_key()
            .into_iter()
            .map(|(key, released)| (key, released.is_some()))
            .collect()
    }

    pub fn unhandled_released_memory_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_released_memory_values_by_route_key()
            .into_iter()
            .map(|(key, released)| (key, released.is_some()))
            .collect()
    }

    pub fn handled_leader_lease_valid_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_lease_valid))
            .collect()
    }

    pub fn unhandled_leader_lease_valid_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_lease_valid))
            .collect()
    }

    pub fn handled_leader_lease_valid_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_leader_lease_valid_values_by_route_key()
            .into_iter()
            .map(|(key, valid)| (key, valid.is_some()))
            .collect()
    }

    pub fn unhandled_leader_lease_valid_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_leader_lease_valid_values_by_route_key()
            .into_iter()
            .map(|(key, valid)| (key, valid.is_some()))
            .collect()
    }

    pub fn handled_leader_lease_confirmed_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_lease_confirmed))
            .collect()
    }

    pub fn unhandled_leader_lease_confirmed_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_lease_confirmed))
            .collect()
    }

    pub fn handled_leader_lease_confirmed_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_leader_lease_confirmed_values_by_route_key()
            .into_iter()
            .map(|(key, confirmed)| (key, confirmed.is_some()))
            .collect()
    }

    pub fn unhandled_leader_lease_confirmed_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_leader_lease_confirmed_values_by_route_key()
            .into_iter()
            .map(|(key, confirmed)| (key, confirmed.is_some()))
            .collect()
    }

    pub fn handled_leader_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_lease_expired))
            .collect()
    }

    pub fn unhandled_leader_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_lease_expired))
            .collect()
    }

    pub fn handled_leader_lease_expired_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_leader_lease_expired_values_by_route_key()
            .into_iter()
            .map(|(key, expired)| (key, expired.is_some()))
            .collect()
    }

    pub fn unhandled_leader_lease_expired_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_leader_lease_expired_values_by_route_key()
            .into_iter()
            .map(|(key, expired)| (key, expired.is_some()))
            .collect()
    }

    pub fn handled_follower_lease_received_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.follower_lease_received))
            .collect()
    }

    pub fn unhandled_follower_lease_received_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.follower_lease_received))
            .collect()
    }

    pub fn handled_follower_lease_received_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_follower_lease_received_values_by_route_key()
            .into_iter()
            .map(|(key, received)| (key, received.is_some()))
            .collect()
    }

    pub fn unhandled_follower_lease_received_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_follower_lease_received_values_by_route_key()
            .into_iter()
            .map(|(key, received)| (key, received.is_some()))
            .collect()
    }

    pub fn handled_follower_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.follower_lease_expired))
            .collect()
    }

    pub fn unhandled_follower_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.follower_lease_expired))
            .collect()
    }

    pub fn handled_follower_lease_expired_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_follower_lease_expired_values_by_route_key()
            .into_iter()
            .map(|(key, expired)| (key, expired.is_some()))
            .collect()
    }

    pub fn unhandled_follower_lease_expired_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_follower_lease_expired_values_by_route_key()
            .into_iter()
            .map(|(key, expired)| (key, expired.is_some()))
            .collect()
    }

    pub fn handled_node_healthy_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.node_healthy))
            .collect()
    }

    pub fn unhandled_node_healthy_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.node_healthy))
            .collect()
    }

    pub fn handled_node_healthy_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_node_healthy_values_by_route_key()
            .into_iter()
            .map(|(key, healthy)| (key, healthy.is_some()))
            .collect()
    }

    pub fn unhandled_node_healthy_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_node_healthy_values_by_route_key()
            .into_iter()
            .map(|(key, healthy)| (key, healthy.is_some()))
            .collect()
    }

    pub fn handled_reorder_queue_dropped_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.reorder_queue_dropped))
            .collect()
    }

    pub fn unhandled_reorder_queue_dropped_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.reorder_queue_dropped))
            .collect()
    }

    pub fn handled_reorder_queue_dropped_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_reorder_queue_dropped_values_by_route_key()
            .into_iter()
            .map(|(key, dropped)| (key, dropped.is_some()))
            .collect()
    }

    pub fn unhandled_reorder_queue_dropped_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_reorder_queue_dropped_values_by_route_key()
            .into_iter()
            .map(|(key, dropped)| (key, dropped.is_some()))
            .collect()
    }

    pub fn handled_fatal_event_transfer_targets_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.fatal_event_transfer_target))
            .collect()
    }

    pub fn unhandled_fatal_event_transfer_targets_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.fatal_event_transfer_target))
            .collect()
    }

    pub fn handled_fatal_event_transfer_target_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_fatal_event_transfer_targets_by_route_key()
            .into_iter()
            .map(|(key, target)| (key, target.is_some()))
            .collect()
    }

    pub fn unhandled_fatal_event_transfer_target_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_fatal_event_transfer_targets_by_route_key()
            .into_iter()
            .map(|(key, target)| (key, target.is_some()))
            .collect()
    }

    pub fn from_results(group_id: GroupId, results: &[MatrixRaftRouteResult]) -> Self {
        let mut node_ids = Vec::with_capacity(results.len());
        let mut handled_node_ids = Vec::new();
        let mut unhandled_node_ids = Vec::new();
        let mut route_keys = Vec::with_capacity(results.len());
        let mut handled_route_keys = Vec::new();
        let mut unhandled_route_keys = Vec::new();
        let mut message_types = Vec::new();
        let mut kinds = Vec::new();
        let mut handled_message_types = Vec::new();
        let mut unhandled_message_types = Vec::new();
        let mut handled_kinds = Vec::new();
        let mut unhandled_kinds = Vec::new();
        let mut counts_by_message_type = Vec::<(MatrixRaftMessageType, usize, usize, usize)>::new();
        let mut counts_by_kind = Vec::<(MatrixRaftRouteResultKind, usize, usize, usize)>::new();
        let mut results_by_route_key = Vec::with_capacity(results.len());
        let mut handled_results_by_route_key = Vec::new();
        let mut unhandled_results_by_route_key = Vec::new();
        let mut handled_by_route_key = Vec::with_capacity(results.len());
        let mut proposed_log_ids_by_route_key = Vec::with_capacity(results.len());
        let mut read_index_responses_by_route_key = Vec::with_capacity(results.len());
        let mut append_entries_responses_by_route_key = Vec::with_capacity(results.len());
        let mut install_snapshot_responses_by_route_key = Vec::with_capacity(results.len());
        let mut vote_responses_by_route_key = Vec::with_capacity(results.len());
        let mut timeout_now_responses_by_route_key = Vec::with_capacity(results.len());
        let mut snapshots_by_route_key = Vec::with_capacity(results.len());
        let mut snapshot_peer_reports_by_route_key = Vec::with_capacity(results.len());
        let mut apply_results_by_route_key = Vec::with_capacity(results.len());
        let mut synced_reports_by_route_key = Vec::with_capacity(results.len());
        let mut replicated_reports_by_route_key = Vec::with_capacity(results.len());
        let mut compacted_logs_by_route_key = Vec::with_capacity(results.len());
        let mut fenced_compactions_by_route_key = Vec::with_capacity(results.len());
        let mut checkpoints_by_route_key = Vec::with_capacity(results.len());
        let mut witness_quorums_by_route_key = Vec::with_capacity(results.len());
        let mut released_memory_by_route_key = Vec::with_capacity(results.len());
        let mut leader_lease_valid_by_route_key = Vec::with_capacity(results.len());
        let mut leader_lease_confirmed_by_route_key = Vec::with_capacity(results.len());
        let mut leader_lease_expired_by_route_key = Vec::with_capacity(results.len());
        let mut follower_lease_received_by_route_key = Vec::with_capacity(results.len());
        let mut follower_lease_expired_by_route_key = Vec::with_capacity(results.len());
        let mut node_healthy_by_route_key = Vec::with_capacity(results.len());
        let mut reorder_queue_dropped_by_route_key = Vec::with_capacity(results.len());
        let mut fatal_event_transfer_targets_by_route_key = Vec::with_capacity(results.len());

        for result in results {
            node_ids.push(result.key.node_id);
            route_keys.push(result.key);
            results_by_route_key.push((result.key, result.clone()));
            if result.handled {
                handled_node_ids.push(result.key.node_id);
                handled_route_keys.push(result.key);
                handled_results_by_route_key.push((result.key, result.clone()));
                if !handled_message_types.contains(&result.message_type) {
                    handled_message_types.push(result.message_type);
                }
                if !handled_kinds.contains(&result.kind) {
                    handled_kinds.push(result.kind);
                }
            } else {
                unhandled_node_ids.push(result.key.node_id);
                unhandled_route_keys.push(result.key);
                unhandled_results_by_route_key.push((result.key, result.clone()));
                if !unhandled_message_types.contains(&result.message_type) {
                    unhandled_message_types.push(result.message_type);
                }
                if !unhandled_kinds.contains(&result.kind) {
                    unhandled_kinds.push(result.kind);
                }
            }
            if !message_types.contains(&result.message_type) {
                message_types.push(result.message_type);
            }
            if !kinds.contains(&result.kind) {
                kinds.push(result.kind);
            }
            if let Some((_, total, handled, unhandled)) = counts_by_message_type
                .iter_mut()
                .find(|(message_type, _, _, _)| *message_type == result.message_type)
            {
                *total += 1;
                if result.handled {
                    *handled += 1;
                } else {
                    *unhandled += 1;
                }
            } else {
                counts_by_message_type.push((
                    result.message_type,
                    1,
                    usize::from(result.handled),
                    usize::from(!result.handled),
                ));
            }
            if let Some((_, total, handled, unhandled)) = counts_by_kind
                .iter_mut()
                .find(|(kind, _, _, _)| *kind == result.kind)
            {
                *total += 1;
                if result.handled {
                    *handled += 1;
                } else {
                    *unhandled += 1;
                }
            } else {
                counts_by_kind.push((
                    result.kind,
                    1,
                    usize::from(result.handled),
                    usize::from(!result.handled),
                ));
            }
            handled_by_route_key.push((result.key, result.handled));
            proposed_log_ids_by_route_key.push((result.key, result.proposed_log_id.clone()));
            read_index_responses_by_route_key
                .push((result.key, result.read_index_response.clone()));
            append_entries_responses_by_route_key
                .push((result.key, result.append_entries_response.clone()));
            install_snapshot_responses_by_route_key
                .push((result.key, result.install_snapshot_response.clone()));
            vote_responses_by_route_key.push((result.key, result.vote_response.clone()));
            timeout_now_responses_by_route_key
                .push((result.key, result.timeout_now_response.clone()));
            snapshots_by_route_key.push((result.key, result.snapshot.clone()));
            snapshot_peer_reports_by_route_key
                .push((result.key, result.snapshot_peer_report.clone()));
            apply_results_by_route_key.push((result.key, result.apply_result.clone()));
            synced_reports_by_route_key.push((result.key, result.synced.clone()));
            replicated_reports_by_route_key.push((result.key, result.replicated.clone()));
            compacted_logs_by_route_key.push((result.key, result.compacted_logs));
            fenced_compactions_by_route_key
                .push((result.key, result.fenced_compaction.clone()));
            checkpoints_by_route_key.push((result.key, result.checkpoint.clone()));
            witness_quorums_by_route_key.push((result.key, result.witness_quorum.clone()));
            released_memory_by_route_key.push((result.key, result.released_memory));
            leader_lease_valid_by_route_key.push((result.key, result.leader_lease_valid));
            leader_lease_confirmed_by_route_key
                .push((result.key, result.leader_lease_confirmed));
            leader_lease_expired_by_route_key.push((result.key, result.leader_lease_expired));
            follower_lease_received_by_route_key
                .push((result.key, result.follower_lease_received));
            follower_lease_expired_by_route_key
                .push((result.key, result.follower_lease_expired));
            node_healthy_by_route_key.push((result.key, result.node_healthy));
            reorder_queue_dropped_by_route_key.push((result.key, result.reorder_queue_dropped));
            fatal_event_transfer_targets_by_route_key
                .push((result.key, result.fatal_event_transfer_target));
        }

        let handled_count = handled_route_keys.len();
        Self {
            group_id,
            result_count: results.len(),
            handled_count,
            unhandled_count: results.len().saturating_sub(handled_count),
            node_ids,
            handled_node_ids,
            unhandled_node_ids,
            route_keys,
            handled_route_keys,
            unhandled_route_keys,
            message_types,
            kinds,
            handled_message_types,
            unhandled_message_types,
            handled_kinds,
            unhandled_kinds,
            counts_by_message_type,
            counts_by_kind,
            results_by_route_key,
            handled_results_by_route_key,
            unhandled_results_by_route_key,
            handled_by_route_key,
            proposed_log_ids_by_route_key,
            read_index_responses_by_route_key,
            append_entries_responses_by_route_key,
            install_snapshot_responses_by_route_key,
            vote_responses_by_route_key,
            timeout_now_responses_by_route_key,
            snapshots_by_route_key,
            snapshot_peer_reports_by_route_key,
            apply_results_by_route_key,
            synced_reports_by_route_key,
            replicated_reports_by_route_key,
            compacted_logs_by_route_key,
            fenced_compactions_by_route_key,
            checkpoints_by_route_key,
            witness_quorums_by_route_key,
            released_memory_by_route_key,
            leader_lease_valid_by_route_key,
            leader_lease_confirmed_by_route_key,
            leader_lease_expired_by_route_key,
            follower_lease_received_by_route_key,
            follower_lease_expired_by_route_key,
            node_healthy_by_route_key,
            reorder_queue_dropped_by_route_key,
            fatal_event_transfer_targets_by_route_key,
        }
    }

    pub fn from_grouped_results(
        groups: &[(GroupId, Vec<MatrixRaftRouteResult>)],
    ) -> Vec<Self> {
        groups
            .iter()
            .map(|(group_id, results)| Self::from_results(*group_id, results))
            .collect()
    }

    pub fn is_handled(&self) -> bool {
        self.unhandled_count == 0 && self.handled_count == self.result_count
    }

    pub fn result_counts_by_status(&self) -> (usize, usize) {
        (self.handled_count, self.unhandled_count)
    }

    pub fn route_key_counts_by_status(&self) -> (usize, usize) {
        (
            self.handled_route_keys.len(),
            self.unhandled_route_keys.len(),
        )
    }

    pub fn status_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftRouteResultStatus)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.status()))
            .collect()
    }

    pub fn handled_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_by_route_key.clone()
    }

    pub fn unhandled_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_by_route_key
            .iter()
            .map(|(key, handled)| (*key, !handled))
            .collect()
    }

    pub fn details_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, String)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.detail.clone()))
            .collect()
    }

    pub fn handled_details_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, String)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.detail.clone()))
            .collect()
    }

    pub fn unhandled_details_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, String)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.detail.clone()))
            .collect()
    }

    pub fn message_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessageType)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.message_type))
            .collect()
    }

    pub fn handled_message_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessageType)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.message_type))
            .collect()
    }

    pub fn unhandled_message_types_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftMessageType)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.message_type))
            .collect()
    }

    pub fn kinds_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, MatrixRaftRouteResultKind)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.kind))
            .collect()
    }

    pub fn handled_kinds_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftRouteResultKind)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.kind))
            .collect()
    }

    pub fn unhandled_kinds_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, MatrixRaftRouteResultKind)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.kind))
            .collect()
    }

    pub fn node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.route_keys
            .iter()
            .map(|route_key| (*route_key, route_key.node_id))
            .collect()
    }

    pub fn handled_node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.handled_route_keys
            .iter()
            .map(|route_key| (*route_key, route_key.node_id))
            .collect()
    }

    pub fn unhandled_node_ids_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, NodeId)> {
        self.unhandled_route_keys
            .iter()
            .map(|route_key| (*route_key, route_key.node_id))
            .collect()
    }

    pub fn proposed_log_id_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.proposed_log_ids_by_route_key
            .iter()
            .map(|(key, log_id)| (*key, log_id.is_some()))
            .collect()
    }

    pub fn proposed_log_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogId>)> {
        self.proposed_log_ids_by_route_key.clone()
    }

    pub fn handled_proposed_log_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogId>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.proposed_log_id.clone()))
            .collect()
    }

    pub fn unhandled_proposed_log_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogId>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.proposed_log_id.clone()))
            .collect()
    }

    pub fn handled_proposed_log_id_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_proposed_log_ids_by_route_key()
            .into_iter()
            .map(|(key, log_id)| (key, log_id.is_some()))
            .collect()
    }

    pub fn unhandled_proposed_log_id_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_proposed_log_ids_by_route_key()
            .into_iter()
            .map(|(key, log_id)| (key, log_id.is_some()))
            .collect()
    }

    pub fn read_index_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.read_index_responses_by_route_key.clone()
    }

    pub fn handled_read_index_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.read_index_response.clone()))
            .collect()
    }

    pub fn unhandled_read_index_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<ReadIndexResponse>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.read_index_response.clone()))
            .collect()
    }

    pub fn handled_read_index_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_read_index_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn unhandled_read_index_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_read_index_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn read_index_response_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.read_index_responses_by_route_key
            .iter()
            .map(|(key, response)| (*key, response.is_some()))
            .collect()
    }

    pub fn append_entries_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftAppendEntriesResponse>)> {
        self.append_entries_responses_by_route_key.clone()
    }

    pub fn handled_append_entries_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftAppendEntriesResponse>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.append_entries_response.clone()))
            .collect()
    }

    pub fn unhandled_append_entries_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftAppendEntriesResponse>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.append_entries_response.clone()))
            .collect()
    }

    pub fn handled_append_entries_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_append_entries_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn unhandled_append_entries_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_append_entries_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn append_entries_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.append_entries_responses_by_route_key
            .iter()
            .map(|(key, response)| (*key, response.is_some()))
            .collect()
    }

    pub fn install_snapshot_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<InstallSnapshotResponse>)> {
        self.install_snapshot_responses_by_route_key.clone()
    }

    pub fn handled_install_snapshot_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<InstallSnapshotResponse>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.install_snapshot_response.clone()))
            .collect()
    }

    pub fn unhandled_install_snapshot_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<InstallSnapshotResponse>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.install_snapshot_response.clone()))
            .collect()
    }

    pub fn handled_install_snapshot_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_install_snapshot_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn unhandled_install_snapshot_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_install_snapshot_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn install_snapshot_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.install_snapshot_responses_by_route_key
            .iter()
            .map(|(key, response)| (*key, response.is_some()))
            .collect()
    }

    pub fn vote_responses_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<VoteResponse>)> {
        self.vote_responses_by_route_key.clone()
    }

    pub fn handled_vote_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<VoteResponse>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.vote_response.clone()))
            .collect()
    }

    pub fn unhandled_vote_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<VoteResponse>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.vote_response.clone()))
            .collect()
    }

    pub fn handled_vote_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_vote_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn unhandled_vote_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_vote_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn vote_response_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.vote_responses_by_route_key
            .iter()
            .map(|(key, response)| (*key, response.is_some()))
            .collect()
    }

    pub fn timeout_now_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)> {
        self.timeout_now_responses_by_route_key.clone()
    }

    pub fn handled_timeout_now_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.timeout_now_response.clone()))
            .collect()
    }

    pub fn unhandled_timeout_now_responses_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<TimeoutNowResponse>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.timeout_now_response.clone()))
            .collect()
    }

    pub fn handled_timeout_now_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.handled_timeout_now_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn unhandled_timeout_now_response_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.unhandled_timeout_now_responses_by_route_key()
            .into_iter()
            .map(|(key, response)| (key, response.is_some()))
            .collect()
    }

    pub fn timeout_now_response_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.timeout_now_responses_by_route_key
            .iter()
            .map(|(key, response)| (*key, response.is_some()))
            .collect()
    }

    pub fn campaign_candidate_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.campaign_candidate_id))
            .collect()
    }

    pub fn handled_campaign_candidate_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.campaign_candidate_id))
            .collect()
    }

    pub fn unhandled_campaign_candidate_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.campaign_candidate_id))
            .collect()
    }

    pub fn campaign_forced_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.campaign_forced))
            .collect()
    }

    pub fn handled_campaign_forced_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.campaign_forced))
            .collect()
    }

    pub fn unhandled_campaign_forced_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.campaign_forced))
            .collect()
    }

    pub fn transfer_leader_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .transfer_leader
                        .as_ref()
                        .map(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn handled_transfer_leader_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .transfer_leader
                        .as_ref()
                        .map(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn unhandled_transfer_leader_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .transfer_leader
                        .as_ref()
                        .map(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn transfer_leader_transferred_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .transfer_leader
                        .as_ref()
                        .map(|report| report.transferred),
                )
            })
            .collect()
    }

    pub fn handled_transfer_leader_transferred_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .transfer_leader
                        .as_ref()
                        .map(|report| report.transferred),
                )
            })
            .collect()
    }

    pub fn unhandled_transfer_leader_transferred_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .transfer_leader
                        .as_ref()
                        .map(|report| report.transferred),
                )
            })
            .collect()
    }

    pub fn leader_transfer_completed_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_transfer_completed))
            .collect()
    }

    pub fn handled_leader_transfer_completed_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_transfer_completed))
            .collect()
    }

    pub fn unhandled_leader_transfer_completed_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_transfer_completed))
            .collect()
    }

    pub fn leader_transfer_aborted_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_transfer_aborted))
            .collect()
    }

    pub fn handled_leader_transfer_aborted_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_transfer_aborted))
            .collect()
    }

    pub fn unhandled_leader_transfer_aborted_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.leader_transfer_aborted))
            .collect()
    }

    pub fn step_down_requested_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .step_down
                        .as_ref()
                        .and_then(|report| report.requested_transferee_id),
                )
            })
            .collect()
    }

    pub fn step_down_transferee_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .step_down
                        .as_ref()
                        .and_then(|report| report.transferee_id),
                )
            })
            .collect()
    }

    pub fn step_down_stepped_down_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .step_down
                        .as_ref()
                        .map(|report| report.stepped_down),
                )
            })
            .collect()
    }

    pub fn resign_reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result.resign.as_ref().map(|report| report.reason.clone()),
                )
            })
            .collect()
    }

    pub fn resign_resigned_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.resign.as_ref().map(|report| report.resigned)))
            .collect()
    }

    pub fn handled_resign_resigned_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.handled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.resign.as_ref().map(|report| report.resigned)))
            .collect()
    }

    pub fn unhandled_resign_resigned_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.unhandled_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.resign.as_ref().map(|report| report.resigned)))
            .collect()
    }

    pub fn membership_success_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.membership.as_ref().map(|report| report.success)))
            .collect()
    }

    pub fn membership_reasons_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<String>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .membership
                        .as_ref()
                        .map(|report| report.reason.clone()),
                )
            })
            .collect()
    }

    pub fn catch_up_learner_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result.catch_up.as_ref().map(|report| report.learner_id),
                )
            })
            .collect()
    }

    pub fn catch_up_caught_up_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result.catch_up.as_ref().map(|report| report.caught_up),
                )
            })
            .collect()
    }

    pub fn promote_learner_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (*key, result.promote.as_ref().map(|report| report.learner_id))
            })
            .collect()
    }

    pub fn promote_promoted_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.promote.as_ref().map(|report| report.promoted)))
            .collect()
    }

    pub fn promote_membership_success_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .promote
                        .as_ref()
                        .map(|report| report.membership.success),
                )
            })
            .collect()
    }

    pub fn auto_promote_learner_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .auto_promote
                        .as_ref()
                        .map(|report| report.learner_id),
                )
            })
            .collect()
    }

    pub fn auto_promote_enabled_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .auto_promote
                        .as_ref()
                        .map(|report| report.auto_promote),
                )
            })
            .collect()
    }

    pub fn auto_promote_promoted_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.results_by_route_key
            .iter()
            .map(|(key, result)| {
                (
                    *key,
                    result
                        .auto_promote
                        .as_ref()
                        .map(|report| report.promoted),
                )
            })
            .collect()
    }

    pub fn snapshot_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.snapshots_by_route_key
            .iter()
            .map(|(key, snapshot)| (*key, snapshot.is_some()))
            .collect()
    }

    pub fn snapshots_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotDesc>)> {
        self.snapshots_by_route_key.clone()
    }

    pub fn snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.snapshots_by_route_key
            .iter()
            .map(|(key, snapshot)| (*key, snapshot.as_ref().and_then(|snapshot| snapshot.snapshot_id.clone())))
            .collect()
    }

    pub fn snapshot_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.snapshots_by_route_key
            .iter()
            .map(|(key, snapshot)| (*key, snapshot.as_ref().map(|snapshot| snapshot.index)))
            .collect()
    }

    pub fn snapshot_peer_report_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.snapshot_peer_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.is_some()))
            .collect()
    }

    pub fn snapshot_peer_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotPeerReport>)> {
        self.snapshot_peer_reports_by_route_key.clone()
    }

    pub fn snapshot_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.snapshot_peer_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.peer_id)))
            .collect()
    }

    pub fn apply_result_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.apply_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.is_some()))
            .collect()
    }

    pub fn apply_results_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftApplyResultReport>)> {
        self.apply_results_by_route_key.clone()
    }

    pub fn apply_result_node_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.apply_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.as_ref().map(|result| result.node_id)))
            .collect()
    }

    pub fn applied_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.apply_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.as_ref().map(|result| result.applied_index)))
            .collect()
    }

    pub fn apply_rejected_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.apply_results_by_route_key
            .iter()
            .map(|(key, result)| (*key, result.as_ref().map(|result| result.rejected)))
            .collect()
    }

    pub fn synced_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.synced_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.is_some()))
            .collect()
    }

    pub fn synced_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftSyncedReport>)> {
        self.synced_reports_by_route_key.clone()
    }

    pub fn synced_first_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.synced_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().and_then(|report| report.first_index)))
            .collect()
    }

    pub fn synced_last_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.synced_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().and_then(|report| report.last_index)))
            .collect()
    }

    pub fn synced_stabled_config_change_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.synced_reports_by_route_key
            .iter()
            .map(|(key, report)| {
                (
                    *key,
                    report
                        .as_ref()
                        .map(|report| report.stabled_config_change_index),
                )
            })
            .collect()
    }

    pub fn replicated_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.replicated_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.is_some()))
            .collect()
    }

    pub fn replicated_reports_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<MatrixRaftReplicatedReport>)> {
        self.replicated_reports_by_route_key.clone()
    }

    pub fn replicated_peer_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.replicated_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.peer_id)))
            .collect()
    }

    pub fn replicated_success_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.replicated_reports_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.success)))
            .collect()
    }

    pub fn compacted_logs_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.compacted_logs_by_route_key
            .iter()
            .map(|(key, compacted)| (*key, compacted.is_some()))
            .collect()
    }

    pub fn compacted_logs_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.compacted_logs_by_route_key.clone()
    }

    pub fn fenced_compaction_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.fenced_compactions_by_route_key
            .iter()
            .map(|(key, compaction)| (*key, compaction.is_some()))
            .collect()
    }

    pub fn fenced_compactions_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WalCompactionReport>)> {
        self.fenced_compactions_by_route_key.clone()
    }

    pub fn checkpoint_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.checkpoints_by_route_key
            .iter()
            .map(|(key, checkpoint)| (*key, checkpoint.is_some()))
            .collect()
    }

    pub fn checkpoints_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<RaftSnapshot>)> {
        self.checkpoints_by_route_key.clone()
    }

    pub fn checkpoint_snapshot_ids_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<SnapshotId>)> {
        self.checkpoints_by_route_key
            .iter()
            .map(|(key, checkpoint)| {
                (
                    *key,
                    checkpoint
                        .as_ref()
                        .map(|checkpoint| checkpoint.meta.snapshot_id.clone()),
                )
            })
            .collect()
    }

    pub fn checkpoint_last_log_indices_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<LogIndex>)> {
        self.checkpoints_by_route_key
            .iter()
            .map(|(key, checkpoint)| {
                (
                    *key,
                    checkpoint
                        .as_ref()
                        .map(|checkpoint| checkpoint.meta.last_log_id.index),
                )
            })
            .collect()
    }

    pub fn witness_quorum_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.witness_quorums_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.is_some()))
            .collect()
    }

    pub fn witness_quorums_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<WitnessQuorumReport>)> {
        self.witness_quorums_by_route_key.clone()
    }

    pub fn witness_quorum_required_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.witness_quorums_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.required)))
            .collect()
    }

    pub fn witness_quorum_acknowledged_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.witness_quorums_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.acknowledged)))
            .collect()
    }

    pub fn witness_quorum_reached_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.witness_quorums_by_route_key
            .iter()
            .map(|(key, report)| (*key, report.as_ref().map(|report| report.reached)))
            .collect()
    }

    pub fn released_memory_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.released_memory_by_route_key
            .iter()
            .map(|(key, released)| (*key, released.is_some()))
            .collect()
    }

    pub fn released_memory_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.released_memory_by_route_key.clone()
    }

    pub fn leader_lease_valid_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.leader_lease_valid_by_route_key
            .iter()
            .map(|(key, valid)| (*key, valid.is_some()))
            .collect()
    }

    pub fn leader_lease_valid_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.leader_lease_valid_by_route_key.clone()
    }

    pub fn leader_lease_confirmed_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.leader_lease_confirmed_by_route_key
            .iter()
            .map(|(key, confirmed)| (*key, confirmed.is_some()))
            .collect()
    }

    pub fn leader_lease_confirmed_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.leader_lease_confirmed_by_route_key.clone()
    }

    pub fn leader_lease_expired_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.leader_lease_expired_by_route_key
            .iter()
            .map(|(key, expired)| (*key, expired.is_some()))
            .collect()
    }

    pub fn leader_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.leader_lease_expired_by_route_key.clone()
    }

    pub fn follower_lease_received_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.follower_lease_received_by_route_key
            .iter()
            .map(|(key, received)| (*key, received.is_some()))
            .collect()
    }

    pub fn follower_lease_received_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.follower_lease_received_by_route_key.clone()
    }

    pub fn follower_lease_expired_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.follower_lease_expired_by_route_key
            .iter()
            .map(|(key, expired)| (*key, expired.is_some()))
            .collect()
    }

    pub fn follower_lease_expired_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.follower_lease_expired_by_route_key.clone()
    }

    pub fn node_healthy_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.node_healthy_by_route_key
            .iter()
            .map(|(key, healthy)| (*key, healthy.is_some()))
            .collect()
    }

    pub fn node_healthy_values_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, Option<bool>)> {
        self.node_healthy_by_route_key.clone()
    }

    pub fn reorder_queue_dropped_presence_by_route_key(&self) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.reorder_queue_dropped_by_route_key
            .iter()
            .map(|(key, dropped)| (*key, dropped.is_some()))
            .collect()
    }

    pub fn reorder_queue_dropped_values_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<u64>)> {
        self.reorder_queue_dropped_by_route_key.clone()
    }

    pub fn fatal_event_transfer_target_presence_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, bool)> {
        self.fatal_event_transfer_targets_by_route_key
            .iter()
            .map(|(key, target)| (*key, target.is_some()))
            .collect()
    }

    pub fn fatal_event_transfer_targets_by_route_key(
        &self,
    ) -> Vec<(MatrixRaftRouteKey, Option<NodeId>)> {
        self.fatal_event_transfer_targets_by_route_key.clone()
    }
}

impl MatrixRaftRouteResult {
    pub fn status(&self) -> MatrixRaftRouteResultStatus {
        if self.handled {
            MatrixRaftRouteResultStatus::Handled
        } else {
            MatrixRaftRouteResultStatus::Unhandled
        }
    }

    pub fn is_unhandled(&self) -> bool {
        self.status() == MatrixRaftRouteResultStatus::Unhandled
    }

    pub fn proposed_log_id_presence(&self) -> bool {
        self.proposed_log_id.is_some()
    }

    pub fn membership_presence(&self) -> bool {
        self.membership.is_some()
    }

    pub fn append_entries_response_presence(&self) -> bool {
        self.append_entries_response.is_some()
    }

    pub fn install_snapshot_response_presence(&self) -> bool {
        self.install_snapshot_response.is_some()
    }

    pub fn read_index_response_presence(&self) -> bool {
        self.read_index_response.is_some()
    }

    pub fn catch_up_presence(&self) -> bool {
        self.catch_up.is_some()
    }

    pub fn promote_presence(&self) -> bool {
        self.promote.is_some()
    }

    pub fn auto_promote_presence(&self) -> bool {
        self.auto_promote.is_some()
    }

    pub fn vote_response_presence(&self) -> bool {
        self.vote_response.is_some()
    }

    pub fn campaign_candidate_id_presence(&self) -> bool {
        self.campaign_candidate_id.is_some()
    }

    pub fn campaign_forced_presence(&self) -> bool {
        self.campaign_forced.is_some()
    }

    pub fn transfer_leader_presence(&self) -> bool {
        self.transfer_leader.is_some()
    }

    pub fn leader_transfer_completed_presence(&self) -> bool {
        self.leader_transfer_completed.is_some()
    }

    pub fn leader_transfer_aborted_presence(&self) -> bool {
        self.leader_transfer_aborted.is_some()
    }

    pub fn step_down_presence(&self) -> bool {
        self.step_down.is_some()
    }

    pub fn resign_presence(&self) -> bool {
        self.resign.is_some()
    }

    pub fn timeout_now_response_presence(&self) -> bool {
        self.timeout_now_response.is_some()
    }

    pub fn snapshot_presence(&self) -> bool {
        self.snapshot.is_some()
    }

    pub fn snapshot_peer_report_presence(&self) -> bool {
        self.snapshot_peer_report.is_some()
    }

    pub fn apply_result_presence(&self) -> bool {
        self.apply_result.is_some()
    }

    pub fn synced_presence(&self) -> bool {
        self.synced.is_some()
    }

    pub fn replicated_presence(&self) -> bool {
        self.replicated.is_some()
    }

    pub fn compacted_logs_presence(&self) -> bool {
        self.compacted_logs.is_some()
    }

    pub fn fenced_compaction_presence(&self) -> bool {
        self.fenced_compaction.is_some()
    }

    pub fn checkpoint_presence(&self) -> bool {
        self.checkpoint.is_some()
    }

    pub fn witness_quorum_presence(&self) -> bool {
        self.witness_quorum.is_some()
    }

    pub fn released_memory_presence(&self) -> bool {
        self.released_memory.is_some()
    }

    pub fn leader_lease_valid_presence(&self) -> bool {
        self.leader_lease_valid.is_some()
    }

    pub fn leader_lease_confirmed_presence(&self) -> bool {
        self.leader_lease_confirmed.is_some()
    }

    pub fn leader_lease_expired_presence(&self) -> bool {
        self.leader_lease_expired.is_some()
    }

    pub fn follower_lease_received_presence(&self) -> bool {
        self.follower_lease_received.is_some()
    }

    pub fn follower_lease_expired_presence(&self) -> bool {
        self.follower_lease_expired.is_some()
    }

    pub fn node_healthy_presence(&self) -> bool {
        self.node_healthy.is_some()
    }

    pub fn reorder_queue_dropped_presence(&self) -> bool {
        self.reorder_queue_dropped.is_some()
    }

    pub fn fatal_event_transfer_target_presence(&self) -> bool {
        self.fatal_event_transfer_target.is_some()
    }

    fn delivered(
        key: MatrixRaftRouteKey,
        message_type: MatrixRaftMessageType,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            key,
            message_type,
            kind: MatrixRaftRouteResultKind::Delivered,
            handled: true,
            detail: detail.into(),
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
        }
    }

    fn accepted_metadata(
        key: MatrixRaftRouteKey,
        message_type: MatrixRaftMessageType,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            key,
            message_type,
            kind: MatrixRaftRouteResultKind::AcceptedMetadata,
            handled: false,
            detail: detail.into(),
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
        }
    }
}

#[derive(Debug)]
pub struct MatrixRaftMultiRaftServer {
    context: MatrixRaftGroupContext,
    nodes: BTreeMap<MatrixRaftRouteKey, MatrixRaftNode>,
    snapshot_routes: BTreeMap<MatrixRaftRouteKey, MatrixRaftSnapshotDesc>,
    runtime_wiring: BTreeMap<MatrixRaftRouteKey, MatrixRaftRuntimeWiring>,
}

impl MatrixRaftMultiRaftServer {
    pub fn new(context: MatrixRaftGroupContext) -> Self {
        Self {
            context,
            nodes: BTreeMap::new(),
            snapshot_routes: BTreeMap::new(),
            runtime_wiring: BTreeMap::new(),
        }
    }

    pub fn context(&self) -> &MatrixRaftGroupContext {
        &self.context
    }

    pub fn register_node(&mut self, node: MatrixRaftNode) -> Result<(), RaftError> {
        let key = MatrixRaftRouteKey::new(node.group_id(), node.node_id());
        if self.nodes.contains_key(&key) {
            return Err(RaftError::InvalidRequest(format!(
                "matrixraft node {} in group {} is already registered",
                key.node_id, key.group_id
            )));
        }
        self.nodes.insert(key, node);
        Ok(())
    }

    pub fn create_node(
        &mut self,
        options: MatrixRaftOptions,
        start_index: LogIndex,
    ) -> Result<(), RaftError> {
        self.create_node_with_creator_index(options, start_index, 0)
    }

    pub fn create_nodes(
        &mut self,
        nodes: impl IntoIterator<Item = (MatrixRaftOptions, LogIndex)>,
    ) -> Result<Vec<MatrixRaftRouteKey>, RaftError> {
        self.create_nodes_with_creator_index(nodes, 0)
    }

    pub fn create_nodes_best_effort(
        &mut self,
        nodes: impl IntoIterator<Item = (MatrixRaftOptions, LogIndex)>,
    ) -> Result<Vec<MatrixRaftCreateGroupResult>, RaftError> {
        self.create_nodes_with_creator_index_best_effort(nodes, 0)
    }

    pub fn plan_create_nodes(
        &self,
        nodes: impl IntoIterator<Item = (MatrixRaftOptions, LogIndex)>,
    ) -> Result<MatrixRaftCreateBatchPlan, RaftError> {
        self.plan_create_nodes_with_creator_index(nodes, 0)
    }

    pub fn plan_create_nodes_with_creator_index(
        &self,
        nodes: impl IntoIterator<Item = (MatrixRaftOptions, LogIndex)>,
        creator_index: usize,
    ) -> Result<MatrixRaftCreateBatchPlan, RaftError> {
        let nodes: Vec<_> = nodes.into_iter().collect();
        self.create_batch_plan_from_nodes(&nodes, creator_index)
    }

    pub fn create_nodes_with_creator_index(
        &mut self,
        nodes: impl IntoIterator<Item = (MatrixRaftOptions, LogIndex)>,
        creator_index: usize,
    ) -> Result<Vec<MatrixRaftRouteKey>, RaftError> {
        let nodes: Vec<_> = nodes.into_iter().collect();
        let plan = self.create_batch_plan_from_nodes(&nodes, creator_index)?;

        for (options, start_index) in nodes {
            self.create_node_with_creator_index(options, start_index, creator_index)?;
        }
        Ok(plan.route_keys)
    }

    pub fn create_nodes_with_creator_index_best_effort(
        &mut self,
        nodes: impl IntoIterator<Item = (MatrixRaftOptions, LogIndex)>,
        creator_index: usize,
    ) -> Result<Vec<MatrixRaftCreateGroupResult>, RaftError> {
        if !self.context.node_creators.is_empty() {
            self.context.node_creators.get(creator_index).ok_or_else(|| {
                RaftError::InvalidRequest(format!(
                    "matrixraft node creator index {} is not registered",
                    creator_index
                ))
            })?;
        }
        let mut seen = BTreeSet::new();
        let mut groups = BTreeMap::<GroupId, Vec<MatrixRaftCreateNodeResult>>::new();

        for (options, start_index) in nodes {
            let key = MatrixRaftRouteKey::new(options.group_id, options.peer_id);
            let result = if !seen.insert(key) {
                MatrixRaftCreateNodeResult::error(
                    key,
                    start_index,
                    RaftError::InvalidRequest(format!(
                        "matrixraft node {} in group {} appears more than once in create batch",
                        key.node_id, key.group_id
                    )),
                )
            } else {
                match self.create_node_with_creator_index(options, start_index, creator_index) {
                    Ok(()) => MatrixRaftCreateNodeResult::ok(
                        key,
                        start_index,
                        self.runtime_wiring
                            .get(&key)
                            .expect("created node runtime wiring")
                            .clone(),
                    ),
                    Err(error) => MatrixRaftCreateNodeResult::error(key, start_index, error),
                }
            };
            groups.entry(key.group_id).or_default().push(result);
        }

        Ok(groups
            .into_iter()
            .map(|(group_id, results)| {
                let node_count = results.len();
                let ok_count = results.iter().filter(|result| result.is_ok()).count();
                let error_count = node_count.saturating_sub(ok_count);
                MatrixRaftCreateGroupResult {
                    group_id,
                    node_count,
                    ok_count,
                    error_count,
                    results,
                }
            })
            .collect())
    }

    pub fn create_node_with_creator_index(
        &mut self,
        options: MatrixRaftOptions,
        start_index: LogIndex,
        creator_index: usize,
    ) -> Result<(), RaftError> {
        let key = MatrixRaftRouteKey::new(options.group_id, options.peer_id);
        if self.nodes.contains_key(&key) {
            return Err(RaftError::InvalidRequest(format!(
                "matrixraft node {} in group {} is already registered",
                key.node_id, key.group_id
            )));
        }
        let creator = if self.context.node_creators.is_empty() {
            None
        } else {
            Some(self.context.node_creators.get(creator_index).ok_or_else(|| {
                RaftError::InvalidRequest(format!(
                    "matrixraft node creator index {} is not registered",
                    creator_index
                ))
            })?)
        };
        let bound_creator_index = creator.map(|_| creator_index);
        let wiring = MatrixRaftRuntimeWiring::from_context_and_creator(
            &self.context,
            &options,
            bound_creator_index,
            creator,
        )?;
        let node = options.create_node(start_index)?;
        self.nodes.insert(key, node);
        self.runtime_wiring.insert(key, wiring);
        Ok(())
    }

    pub fn unregister_node(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<MatrixRaftNode, RaftError> {
        let key = MatrixRaftRouteKey::new(group_id, node_id);
        self.snapshot_routes.remove(&key);
        self.runtime_wiring.remove(&key);
        self.nodes.remove(&key).ok_or(RaftError::NodeNotFound(node_id))
    }

    pub fn unregister_group(
        &mut self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftNode>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        let mut removed = Vec::with_capacity(keys.len());
        for key in keys {
            self.snapshot_routes.remove(&key);
            self.runtime_wiring.remove(&key);
            let node = self
                .nodes
                .remove(&key)
                .ok_or(RaftError::NodeNotFound(key.node_id))?;
            removed.push(node);
        }
        Ok(removed)
    }

    pub fn unregister_group_best_effort(
        &mut self,
        group_id: GroupId,
    ) -> MatrixRaftUnregisterGroupResult {
        match self.plan_unregister_group(group_id) {
            Ok(plan) => self.unregister_group_plan_best_effort(plan),
            Err(error) => MatrixRaftUnregisterGroupResult::error(group_id, error),
        }
    }

    pub fn plan_unregister_group(
        &self,
        group_id: GroupId,
    ) -> Result<MatrixRaftUnregisterGroupPlan, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        Ok(self.unregister_group_plan_from_keys(group_id, keys))
    }

    pub fn plan_unregister_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftUnregisterBatchPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.unregister_batch_plan_from_groups(&group_ids)
    }

    pub fn unregister_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftNode>)>, RaftError> {
        let group_ids: Vec<_> = group_ids.into_iter().collect();
        let plan = self.unregister_batch_plan_from_groups(&group_ids)?;

        let mut removed_groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut removed = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                self.snapshot_routes.remove(&key);
                self.runtime_wiring.remove(&key);
                let node = self
                    .nodes
                    .remove(&key)
                    .ok_or(RaftError::NodeNotFound(key.node_id))?;
                removed.push(node);
            }
            removed_groups.push((group.group_id, removed));
        }
        Ok(removed_groups)
    }

    pub fn unregister_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Vec<MatrixRaftUnregisterGroupResult> {
        let mut seen = BTreeSet::new();
        let mut results = Vec::new();
        for group_id in group_ids {
            if !seen.insert(group_id) {
                results.push(MatrixRaftUnregisterGroupResult::error(
                    group_id,
                    RaftError::InvalidRequest(format!(
                        "matrixraft group {} appears more than once in unregister batch",
                        group_id
                    )),
                ));
                continue;
            }
            results.push(self.unregister_group_best_effort(group_id));
        }
        results
    }

    fn unregister_group_plan_best_effort(
        &mut self,
        plan: MatrixRaftUnregisterGroupPlan,
    ) -> MatrixRaftUnregisterGroupResult {
        for key in &plan.route_keys {
            self.snapshot_routes.remove(key);
            self.runtime_wiring.remove(key);
            self.nodes.remove(key);
        }
        MatrixRaftUnregisterGroupResult::ok(plan)
    }

    pub fn has_node(&self, group_id: GroupId, node_id: NodeId) -> bool {
        self.nodes
            .contains_key(&MatrixRaftRouteKey::new(group_id, node_id))
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn group_count(&self) -> usize {
        self.nodes.keys().map(|key| key.group_id).collect::<BTreeSet<_>>().len()
    }

    pub fn group_ids(&self) -> Vec<GroupId> {
        self.nodes
            .keys()
            .map(|key| key.group_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn route_keys(&self) -> Vec<MatrixRaftRouteKey> {
        self.nodes.keys().copied().collect()
    }

    pub fn group_route_key_list(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftRouteKey>, RaftError> {
        self.group_route_keys(group_id)
    }

    pub fn route_keys_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteKey>)>, RaftError> {
        let plan = self.plan_query_for_groups(group_ids, "route_keys")?;
        Ok(plan
            .groups
            .into_iter()
            .map(|group| (group.group_id, group.route_keys))
            .collect())
    }

    pub fn plan_query_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        operation: impl Into<String>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.query_fanout_plan_from_groups(&group_ids, operation)
    }

    pub fn plan_route_keys_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "route_keys")
    }

    pub fn node_id_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<MatrixRaftNodeId, RaftError> {
        Ok(self.node(group_id, node_id)?.get_node_id())
    }

    pub fn node_ids_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftNodeId>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| Ok(self.node(key.group_id, key.node_id)?.get_node_id()))
            .collect()
    }

    pub fn node_ids_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftNodeId>)>, RaftError> {
        let plan = self.plan_node_ids_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let node_ids = group
                .route_keys
                .into_iter()
                .map(|key| {
                    Ok::<MatrixRaftNodeId, RaftError>(self
                        .nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .get_node_id())
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, node_ids));
        }
        Ok(groups)
    }

    pub fn plan_node_ids_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "node_ids")
    }

    pub fn group_topology(
        &self,
        group_id: GroupId,
    ) -> Result<MatrixRaftGroupTopology, RaftError> {
        let route_keys = self.group_route_keys(group_id)?;
        Ok(self.topology_for_route_keys(group_id, route_keys))
    }

    pub fn topologies_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<MatrixRaftGroupTopology>, RaftError> {
        let plan = self.plan_topologies_for_groups(group_ids)?;
        Ok(plan
            .groups
            .into_iter()
            .map(|group| self.topology_for_route_keys(group.group_id, group.route_keys))
            .collect())
    }

    pub fn plan_topologies_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "topologies")
    }

    pub fn topology(&self) -> MatrixRaftTopology {
        let groups = self
            .group_ids()
            .into_iter()
            .map(|group_id| {
                let route_keys = self
                    .nodes
                    .keys()
                    .filter(|key| key.group_id == group_id)
                    .copied()
                    .collect();
                self.topology_for_route_keys(group_id, route_keys)
            })
            .collect::<Vec<_>>();
        MatrixRaftTopology {
            group_count: groups.len(),
            node_count: self.node_count(),
            runtime_wiring_count: self.runtime_wiring_count(),
            snapshot_route_count: self.snapshot_route_count(),
            groups,
        }
    }

    pub fn runtime_wiring(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Option<&MatrixRaftRuntimeWiring> {
        self.runtime_wiring
            .get(&MatrixRaftRouteKey::new(group_id, node_id))
    }

    pub fn runtime_wiring_count(&self) -> usize {
        self.runtime_wiring.len()
    }

    fn group_route_keys(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftRouteKey>, RaftError> {
        let keys: Vec<_> = self
            .nodes
            .keys()
            .filter(|key| key.group_id == group_id)
            .copied()
            .collect();
        if keys.is_empty() {
            return Err(RaftError::InvalidRequest(format!(
                "matrixraft group {group_id} is not registered"
            )));
        }
        Ok(keys)
    }

    fn unregister_batch_plan_from_groups(
        &self,
        group_ids: &[GroupId],
    ) -> Result<MatrixRaftUnregisterBatchPlan, RaftError> {
        let mut seen = BTreeSet::new();
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            if !seen.insert(*group_id) {
                return Err(RaftError::InvalidRequest(format!(
                    "matrixraft group {group_id} appears more than once in unregister batch"
                )));
            }
            let keys = self.group_route_keys(*group_id)?;
            groups.push(self.unregister_group_plan_from_keys(*group_id, keys));
        }

        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        let runtime_wiring_count = groups
            .iter()
            .map(|group| group.runtime_wiring_count)
            .sum();
        let snapshot_route_count = groups
            .iter()
            .map(|group| group.snapshot_route_count)
            .sum();
        Ok(MatrixRaftUnregisterBatchPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            runtime_wiring_count,
            snapshot_route_count,
            route_keys,
            groups,
        })
    }

    fn unregister_group_plan_from_keys(
        &self,
        group_id: GroupId,
        route_keys: Vec<MatrixRaftRouteKey>,
    ) -> MatrixRaftUnregisterGroupPlan {
        let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
        let runtime_wiring_count = route_keys
            .iter()
            .filter(|key| self.runtime_wiring.contains_key(key))
            .count();
        let snapshot_route_count = route_keys
            .iter()
            .filter(|key| self.snapshot_routes.contains_key(key))
            .count();
        MatrixRaftUnregisterGroupPlan {
            group_id,
            node_count: route_keys.len(),
            route_keys,
            node_ids,
            runtime_wiring_count,
            snapshot_route_count,
        }
    }

    fn lifecycle_batch_plan_from_groups(
        &self,
        action: MatrixRaftLifecycleAction,
        group_ids: &[GroupId],
        start_index: Option<LogIndex>,
        recover_fsm_from_snapshot: Option<bool>,
    ) -> Result<MatrixRaftLifecycleBatchPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftLifecycleGroupPlan {
                action,
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                start_index,
                recover_fsm_from_snapshot,
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftLifecycleBatchPlan {
            action,
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            groups,
            start_index,
            recover_fsm_from_snapshot,
        })
    }

    fn membership_fanout_plan_from_groups(
        &self,
        group_ids: &[GroupId],
        operation: MembershipOperation,
    ) -> Result<MatrixRaftMembershipFanoutPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftMembershipFanoutGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                operation: operation.clone(),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftMembershipFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            operation,
            groups,
        })
    }

    fn membership_workflow_fanout_plan_from_groups(
        &self,
        group_ids: &[GroupId],
        operations: Vec<MembershipOperation>,
    ) -> Result<MatrixRaftMembershipWorkflowFanoutPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftMembershipWorkflowFanoutGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                operation_count: operations.len(),
                operations: operations.clone(),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftMembershipWorkflowFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            operation_count: operations.len(),
            operations,
            groups,
        })
    }

    fn config_change_fanout_plan_from_groups(
        &self,
        group_ids: &[GroupId],
        change: MatrixRaftConfigChange,
    ) -> Result<MatrixRaftConfigChangeFanoutPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftConfigChangeFanoutGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                change: change.clone(),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftConfigChangeFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            change,
            groups,
        })
    }

    fn propose_fanout_plan_from_groups(
        &self,
        group_ids: &[GroupId],
        options: MatrixRaftProposeOptions,
        data: &Payload,
    ) -> Result<MatrixRaftProposeFanoutPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftProposeFanoutGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                options: options.clone(),
                payload_bytes: data.len(),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftProposeFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            options,
            payload_bytes: data.len(),
            groups,
        })
    }

    fn read_index_fanout_plan_from_groups(
        &self,
        group_ids: &[GroupId],
        options: MatrixRaftReadIndexOptions,
    ) -> Result<MatrixRaftReadIndexFanoutPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftReadIndexFanoutGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                options,
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftReadIndexFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            options,
            groups,
        })
    }

    fn bounded_stale_read_fanout_plan_from_groups(
        &self,
        group_ids: &[GroupId],
        options: MatrixRaftBoundedStaleReadOptions,
    ) -> Result<MatrixRaftBoundedStaleReadFanoutPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftBoundedStaleReadFanoutGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                options,
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftBoundedStaleReadFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            options,
            groups,
        })
    }

    fn snapshot_install_plan_from_groups(
        &self,
        group_installs: &[(GroupId, NodeId, RaftSnapshot, ApplySnapshotFence)],
    ) -> Result<MatrixRaftSnapshotInstallFanoutPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_installs.len());
        for (group_id, target, snapshot, fence) in group_installs {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftSnapshotInstallFanoutGroupPlan {
                group_id: *group_id,
                target: *target,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                snapshot: snapshot.clone(),
                fence: fence.clone(),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftSnapshotInstallFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            groups,
        })
    }

    fn snapshot_publish_plan_from_groups(
        &self,
        group_snapshots: &[(GroupId, MatrixRaftSnapshotDesc)],
    ) -> Result<MatrixRaftSnapshotPublishPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_snapshots.len());
        for (group_id, snapshot) in group_snapshots {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            let existing_route_keys = route_keys
                .iter()
                .copied()
                .filter(|key| self.snapshot_routes.contains_key(key))
                .collect::<Vec<_>>();
            let existing_route_count = route_keys
                .iter()
                .filter(|key| self.snapshot_routes.contains_key(key))
                .count();
            groups.push(MatrixRaftSnapshotPublishGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                existing_route_count,
                existing_route_keys,
                snapshot: snapshot.clone(),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        let existing_route_count = groups.iter().map(|group| group.existing_route_count).sum();
        Ok(MatrixRaftSnapshotPublishPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            existing_route_count,
            groups,
        })
    }

    fn snapshot_finish_plan_from_groups(
        &self,
        group_ids: &[GroupId],
        finish: MatrixRaftOldSnapshotFinish,
    ) -> Result<MatrixRaftSnapshotFinishPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            let active_route_keys = route_keys
                .iter()
                .copied()
                .filter(|key| self.snapshot_routes.contains_key(key))
                .collect::<Vec<_>>();
            let active_route_count = route_keys
                .iter()
                .filter(|key| self.snapshot_routes.contains_key(key))
                .count();
            groups.push(MatrixRaftSnapshotFinishGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                active_route_count,
                active_route_keys,
                finish: finish.clone(),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        let active_route_count = groups.iter().map(|group| group.active_route_count).sum();
        Ok(MatrixRaftSnapshotFinishPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            active_route_count,
            finish,
            groups,
        })
    }

    fn message_fanout_plan_from_groups(
        &self,
        group_ids: &[GroupId],
        message: MatrixRaftMessage,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftMessageFanoutGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                message_type: message.message_type,
                message: message.clone(),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftMessageFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            message_type: message.message_type,
            groups,
        })
    }

    fn admin_command_fanout_plan_from_groups(
        &self,
        group_ids: &[GroupId],
        command: MatrixRaftAdminCommand,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftAdminCommandFanoutGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                command_type: command.command_type,
                command: command.clone(),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftAdminCommandFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            command_type: command.command_type,
            groups,
        })
    }

    fn admin_command_fanout_plan_from_group_commands(
        &self,
        command_type: MatrixRaftAdminCommandType,
        group_commands: impl IntoIterator<Item = (GroupId, MatrixRaftAdminCommand)>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        let mut groups = Vec::new();
        for (group_id, command) in group_commands {
            if command.command_type != command_type {
                return Err(RaftError::InvalidRequest(
                    "matrixraft admin command fanout requires one command type".to_string(),
                ));
            }
            let route_keys = self.group_route_keys(group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftAdminCommandFanoutGroupPlan {
                group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                command_type: command.command_type,
                command,
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftAdminCommandFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            command_type,
            groups,
        })
    }

    fn query_fanout_plan_from_groups(
        &self,
        group_ids: &[GroupId],
        operation: impl Into<String>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        let operation = operation.into();
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(*group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftQueryFanoutGroupPlan {
                group_id: *group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                operation: operation.clone(),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftQueryFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            operation,
            groups,
        })
    }

    fn create_batch_plan_from_nodes(
        &self,
        nodes: &[(MatrixRaftOptions, LogIndex)],
        creator_index: usize,
    ) -> Result<MatrixRaftCreateBatchPlan, RaftError> {
        let creator = if self.context.node_creators.is_empty() {
            None
        } else {
            Some(self.context.node_creators.get(creator_index).ok_or_else(|| {
                RaftError::InvalidRequest(format!(
                    "matrixraft node creator index {} is not registered",
                    creator_index
                ))
            })?)
        };
        let bound_creator_index = creator.map(|_| creator_index);
        let mut seen = BTreeSet::new();
        let mut node_plans = Vec::with_capacity(nodes.len());
        let mut group_plans = BTreeMap::<GroupId, MatrixRaftCreateGroupPlan>::new();

        for (options, start_index) in nodes {
            let key = MatrixRaftRouteKey::new(options.group_id, options.peer_id);
            if !seen.insert(key) {
                return Err(RaftError::InvalidRequest(format!(
                    "matrixraft node {} in group {} appears more than once in create batch",
                    key.node_id, key.group_id
                )));
            }
            if self.nodes.contains_key(&key) {
                return Err(RaftError::InvalidRequest(format!(
                    "matrixraft node {} in group {} is already registered",
                    key.node_id, key.group_id
                )));
            }
            let runtime_wiring = MatrixRaftRuntimeWiring::from_context_and_creator(
                &self.context,
                options,
                bound_creator_index,
                creator,
            )?;
            node_plans.push(MatrixRaftCreateNodePlan {
                key,
                start_index: *start_index,
                runtime_wiring,
            });
            let group = group_plans.entry(key.group_id).or_insert_with(|| {
                MatrixRaftCreateGroupPlan {
                    group_id: key.group_id,
                    node_ids: Vec::new(),
                    route_keys: Vec::new(),
                    start_indices: Vec::new(),
                    node_count: 0,
                }
            });
            group.node_ids.push(key.node_id);
            group.route_keys.push(key);
            group.start_indices.push(*start_index);
            group.node_count += 1;
        }

        let route_keys = node_plans.iter().map(|node| node.key).collect::<Vec<_>>();
        let groups = group_plans.into_values().collect::<Vec<_>>();
        let group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        Ok(MatrixRaftCreateBatchPlan {
            creator_index: bound_creator_index,
            node_count: node_plans.len(),
            group_count: groups.len(),
            group_ids,
            route_keys,
            groups,
            nodes: node_plans,
        })
    }

    fn topology_for_route_keys(
        &self,
        group_id: GroupId,
        route_keys: Vec<MatrixRaftRouteKey>,
    ) -> MatrixRaftGroupTopology {
        let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
        let runtime_wiring_count = route_keys
            .iter()
            .filter(|key| self.runtime_wiring.contains_key(key))
            .count();
        let snapshot_route_count = route_keys
            .iter()
            .filter(|key| self.snapshot_routes.contains_key(key))
            .count();
        MatrixRaftGroupTopology {
            group_id,
            node_count: route_keys.len(),
            route_keys,
            node_ids,
            runtime_wiring_count,
            snapshot_route_count,
        }
    }

    pub fn node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<&MatrixRaftNode, RaftError> {
        self.nodes
            .get(&MatrixRaftRouteKey::new(group_id, node_id))
            .ok_or(RaftError::NodeNotFound(node_id))
    }

    pub fn node_mut(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<&mut MatrixRaftNode, RaftError> {
        self.nodes
            .get_mut(&MatrixRaftRouteKey::new(group_id, node_id))
            .ok_or(RaftError::NodeNotFound(node_id))
    }

    /// Serve a linearizable read on `node_id` by forwarding to the group leader.
    ///
    /// A follower cannot certify a linearizable read from its local state — its own
    /// `read_index` reports `not_leader`. This obtains a quorum-confirmed ReadIndex
    /// from the group leader and then serves the read as `safe` only if the follower
    /// has applied up to that confirmed index. Otherwise it reports the read as not
    /// yet safe (`follower_apply_pending`) or unavailable (`leader_read_unavailable:*`).
    /// It never fakes linearizable safety.
    ///
    /// The leader itself (or a node with no distinct known leader) is served directly
    /// through its normal local read path.
    pub fn forwarded_read_index_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        options: MatrixRaftReadIndexOptions,
    ) -> Result<ReadIndexResponse, RaftError> {
        let leader_id = self.node(group_id, node_id)?.leader()?;
        match leader_id {
            Some(leader) if leader != node_id => {
                // Cross-node reads must be quorum-confirmed: a lease read is valid
                // only on the leader itself and must never authorize a follower read.
                let confirm = MatrixRaftReadIndexOptions::quorum_read(options.min_commit_index);
                let leader_read = self.node(group_id, leader)?.read_index_with_options(confirm)?;
                if !leader_read.safe {
                    return Ok(ReadIndexResponse {
                        safe: false,
                        read_index: leader_read.read_index,
                        lease_read: false,
                        reason: format!("leader_read_unavailable:{}", leader_read.reason),
                    });
                }
                // The follower may serve the read iff it has applied up to the
                // leader's quorum-confirmed read index.
                let applied = self.node(group_id, node_id)?.get_status()?.applied_index;
                if applied >= leader_read.read_index {
                    Ok(ReadIndexResponse {
                        safe: true,
                        read_index: leader_read.read_index,
                        lease_read: false,
                        reason: "follower_read_forwarded".to_string(),
                    })
                } else {
                    Ok(ReadIndexResponse {
                        safe: false,
                        read_index: leader_read.read_index,
                        lease_read: false,
                        reason: "follower_apply_pending".to_string(),
                    })
                }
            }
            _ => self.node(group_id, node_id)?.read_index_with_options(options),
        }
    }

    /// Fan a forwarded read across every node of a group. The leader is served
    /// directly; each follower is served via [`Self::forwarded_read_index_on_node`],
    /// so followers that have caught up return linearizable-safe reads while lagging
    /// followers honestly report `follower_apply_pending`.
    pub fn forwarded_read_index_for_group(
        &self,
        group_id: GroupId,
        options: MatrixRaftReadIndexOptions,
    ) -> Result<Vec<(MatrixRaftRouteKey, ReadIndexResponse)>, RaftError> {
        let mut route_keys: Vec<MatrixRaftRouteKey> = self
            .nodes
            .keys()
            .filter(|key| key.group_id == group_id)
            .cloned()
            .collect();
        if route_keys.is_empty() {
            return Err(RaftError::InvalidRequest(format!(
                "no nodes registered for group {group_id}"
            )));
        }
        route_keys.sort_by_key(|key| key.node_id);
        let mut results = Vec::with_capacity(route_keys.len());
        for key in route_keys {
            let response = self.forwarded_read_index_on_node(group_id, key.node_id, options)?;
            results.push((key, response));
        }
        Ok(results)
    }

    pub fn propose_to_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        data: Payload,
    ) -> Result<LogId, RaftError> {
        self.propose_to_node_with_options(
            group_id,
            node_id,
            MatrixRaftProposeOptions::default(),
            data,
        )
    }

    pub fn propose_to_node_with_options(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        options: MatrixRaftProposeOptions,
        data: Payload,
    ) -> Result<LogId, RaftError> {
        self.node(group_id, node_id)?.propose_with_options(options, data)
    }

    pub fn propose_to_group_nodes(
        &self,
        group_id: GroupId,
        data: Payload,
    ) -> Result<Vec<LogId>, RaftError> {
        self.propose_to_group_nodes_with_options(
            group_id,
            MatrixRaftProposeOptions::default(),
            data,
        )
    }

    pub fn propose_to_group_nodes_with_options(
        &self,
        group_id: GroupId,
        options: MatrixRaftProposeOptions,
        data: Payload,
    ) -> Result<Vec<LogId>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                self.node(key.group_id, key.node_id)?
                    .propose_with_options(options.clone(), data.clone())
            })
            .collect()
    }

    pub fn propose_to_group_nodes_best_effort(
        &self,
        group_id: GroupId,
        data: Payload,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.propose_to_group_nodes_with_options_best_effort(
            group_id,
            MatrixRaftProposeOptions::default(),
            data,
        )
    }

    pub fn propose_to_group_nodes_with_options_best_effort(
        &self,
        group_id: GroupId,
        options: MatrixRaftProposeOptions,
        data: Payload,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let routed = MatrixRaftRoutedMessage::new(
                key.group_id,
                key.node_id,
                MatrixRaftMessage::propose(
                    key.node_id,
                    key.node_id,
                    MatrixRaftPropose {
                        request_id: None,
                        data: data.clone(),
                        context: Vec::new(),
                        is_command: options.is_command,
                    },
                ),
            );
            let routed_result = match self
                .node(key.group_id, key.node_id)
                .and_then(|node| node.propose_with_options(options.clone(), data.clone()))
            {
                Ok(log_id) => {
                    let mut result = MatrixRaftRouteResult::delivered(
                        key,
                        MatrixRaftMessageType::Propose,
                        "propose delivered",
                    );
                    result.proposed_log_id = Some(log_id);
                    MatrixRaftBatchRouteResult::from_routed_result(&routed, result)
                }
                Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
            };
            results.push(routed_result);
        }
        Ok(results)
    }

    pub fn plan_propose_on_group(
        &self,
        group_id: GroupId,
        data: &Payload,
    ) -> Result<MatrixRaftProposeFanoutGroupPlan, RaftError> {
        self.plan_propose_with_options_on_group(
            group_id,
            MatrixRaftProposeOptions::default(),
            data,
        )
    }

    pub fn plan_propose_with_options_on_group(
        &self,
        group_id: GroupId,
        options: MatrixRaftProposeOptions,
        data: &Payload,
    ) -> Result<MatrixRaftProposeFanoutGroupPlan, RaftError> {
        Ok(self
            .propose_fanout_plan_from_groups(&[group_id], options, data)?
            .groups
            .into_iter()
            .next()
            .expect("single group propose fanout plan"))
    }

    pub fn plan_propose_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        data: &Payload,
    ) -> Result<MatrixRaftProposeFanoutPlan, RaftError> {
        self.plan_propose_with_options_for_groups(
            group_ids,
            MatrixRaftProposeOptions::default(),
            data,
        )
    }

    pub fn plan_propose_with_options_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftProposeOptions,
        data: &Payload,
    ) -> Result<MatrixRaftProposeFanoutPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.propose_fanout_plan_from_groups(&group_ids, options, data)
    }

    pub fn propose_to_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        data: Payload,
    ) -> Result<Vec<(GroupId, Vec<LogId>)>, RaftError> {
        self.propose_to_groups_with_options(group_ids, MatrixRaftProposeOptions::default(), data)
    }

    pub fn propose_to_groups_with_options(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftProposeOptions,
        data: Payload,
    ) -> Result<Vec<(GroupId, Vec<LogId>)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.propose_fanout_plan_from_groups(&group_ids, options, &data)?;

        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut logs = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                logs.push(
                    self.node(key.group_id, key.node_id)?
                        .propose_with_options(group.options.clone(), data.clone())?,
                );
            }
            groups.push((group.group_id, logs));
        }
        Ok(groups)
    }

    pub fn propose_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        data: Payload,
        callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        self.propose_with_options_callbacks_on_group(
            group_id,
            MatrixRaftProposeOptions::default(),
            data,
            callback_for_key,
            timeout_ms,
        )
    }

    pub fn propose_with_options_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        options: MatrixRaftProposeOptions,
        data: Payload,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_propose_with_options_on_group(group_id, options, &data)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            let callback = callback_for_key(key);
            let result = self.node(key.group_id, key.node_id)?.propose_with_options_callback(
                plan.options.clone(),
                data.clone(),
                callback,
                timeout_ms,
            );
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn propose_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        data: Payload,
        callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        self.propose_with_options_callbacks_for_groups(
            group_ids,
            MatrixRaftProposeOptions::default(),
            data,
            callback_for_key,
            timeout_ms,
        )
    }

    pub fn propose_with_options_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftProposeOptions,
        data: Payload,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.propose_fanout_plan_from_groups(&group_ids, options, &data)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self.node(key.group_id, key.node_id)?.propose_with_options_callback(
                    group.options.clone(),
                    data.clone(),
                    callback,
                    timeout_ms,
                );
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn propose_to_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        data: Payload,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.propose_to_groups_with_options_best_effort(
            group_ids,
            MatrixRaftProposeOptions::default(),
            data,
        )
    }

    pub fn propose_to_groups_with_options_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftProposeOptions,
        data: Payload,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.propose_fanout_plan_from_groups(&group_ids, options, &data)?;

        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let routed = MatrixRaftRoutedMessage::new(
                    key.group_id,
                    key.node_id,
                    MatrixRaftMessage::propose(
                        key.node_id,
                        key.node_id,
                        MatrixRaftPropose {
                            request_id: None,
                            data: data.clone(),
                            context: Vec::new(),
                            is_command: group.options.is_command,
                        },
                    ),
                );
                let routed_result = match self
                    .node(key.group_id, key.node_id)
                    .and_then(|node| node.propose_with_options(group.options.clone(), data.clone()))
                {
                    Ok(log_id) => {
                        let mut result = MatrixRaftRouteResult::delivered(
                            key,
                            MatrixRaftMessageType::Propose,
                            "propose delivered",
                        );
                        result.proposed_log_id = Some(log_id);
                        MatrixRaftBatchRouteResult::from_routed_result(&routed, result)
                    }
                    Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                };
                results.push(routed_result);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn read_index_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        min_commit_index: LogIndex,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.read_index_on_node_with_options(
            group_id,
            node_id,
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
        )
    }

    pub fn read_index_on_node_with_options(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        options: MatrixRaftReadIndexOptions,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.node(group_id, node_id)?.read_index_with_options(options)
    }

    pub fn group_read_indexes(
        &self,
        group_id: GroupId,
        min_commit_index: LogIndex,
    ) -> Result<Vec<ReadIndexResponse>, RaftError> {
        self.group_read_indexes_with_options(
            group_id,
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
        )
    }

    pub fn group_read_indexes_with_options(
        &self,
        group_id: GroupId,
        options: MatrixRaftReadIndexOptions,
    ) -> Result<Vec<ReadIndexResponse>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                self.node(key.group_id, key.node_id)?
                    .read_index_with_options(options)
            })
            .collect()
    }

    pub fn plan_read_indexes_on_group(
        &self,
        group_id: GroupId,
        min_commit_index: LogIndex,
    ) -> Result<MatrixRaftReadIndexFanoutGroupPlan, RaftError> {
        self.plan_read_indexes_with_options_on_group(
            group_id,
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
        )
    }

    pub fn plan_read_indexes_with_options_on_group(
        &self,
        group_id: GroupId,
        options: MatrixRaftReadIndexOptions,
    ) -> Result<MatrixRaftReadIndexFanoutGroupPlan, RaftError> {
        Ok(self
            .read_index_fanout_plan_from_groups(&[group_id], options)?
            .groups
            .into_iter()
            .next()
            .expect("single group read-index fanout plan"))
    }

    pub fn plan_read_indexes_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        min_commit_index: LogIndex,
    ) -> Result<MatrixRaftReadIndexFanoutPlan, RaftError> {
        self.plan_read_indexes_with_options_for_groups(
            group_ids,
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
        )
    }

    pub fn plan_read_indexes_with_options_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftReadIndexOptions,
    ) -> Result<MatrixRaftReadIndexFanoutPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.read_index_fanout_plan_from_groups(&group_ids, options)
    }

    pub fn read_indexes_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        min_commit_index: LogIndex,
    ) -> Result<Vec<(GroupId, Vec<ReadIndexResponse>)>, RaftError> {
        self.read_indexes_for_groups_with_options(
            group_ids,
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
        )
    }

    pub fn read_indexes_for_groups_with_options(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftReadIndexOptions,
    ) -> Result<Vec<(GroupId, Vec<ReadIndexResponse>)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.read_index_fanout_plan_from_groups(&group_ids, options)?;

        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut reads = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                reads.push(
                    self.node(key.group_id, key.node_id)?
                        .read_index_with_options(group.options)?,
                );
            }
            groups.push((group.group_id, reads));
        }
        Ok(groups)
    }

    pub fn read_indexes_on_group_best_effort(
        &self,
        group_id: GroupId,
        min_commit_index: LogIndex,
    ) -> Result<MatrixRaftReadIndexGroupResult, RaftError> {
        self.read_indexes_with_options_on_group_best_effort(
            group_id,
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
        )
    }

    pub fn read_indexes_with_options_on_group_best_effort(
        &self,
        group_id: GroupId,
        options: MatrixRaftReadIndexOptions,
    ) -> Result<MatrixRaftReadIndexGroupResult, RaftError> {
        let plan = self.plan_read_indexes_with_options_on_group(group_id, options)?;
        Ok(self.read_indexes_with_group_plan_best_effort(plan))
    }

    pub fn read_indexes_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        min_commit_index: LogIndex,
    ) -> Result<Vec<MatrixRaftReadIndexGroupResult>, RaftError> {
        self.read_indexes_with_options_for_groups_best_effort(
            group_ids,
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
        )
    }

    pub fn read_indexes_with_options_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftReadIndexOptions,
    ) -> Result<Vec<MatrixRaftReadIndexGroupResult>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.read_index_fanout_plan_from_groups(&group_ids, options)?;
        Ok(plan
            .groups
            .into_iter()
            .map(|group| self.read_indexes_with_group_plan_best_effort(group))
            .collect())
    }

    fn read_indexes_with_group_plan_best_effort(
        &self,
        group: MatrixRaftReadIndexFanoutGroupPlan,
    ) -> MatrixRaftReadIndexGroupResult {
        let mut results = Vec::with_capacity(group.node_count);
        for key in group.route_keys {
            let result = match self
                .node(key.group_id, key.node_id)
                .and_then(|node| node.read_index_with_options(group.options))
            {
                Ok(read_index) => MatrixRaftReadIndexNodeResult::ok(key, read_index),
                Err(error) => MatrixRaftReadIndexNodeResult::error(key, error),
            };
            results.push(result);
        }
        let ok_count = results.iter().filter(|result| result.is_ok()).count();
        let error_count = results.len().saturating_sub(ok_count);
        MatrixRaftReadIndexGroupResult {
            group_id: group.group_id,
            node_count: group.node_count,
            ok_count,
            error_count,
            results,
        }
    }

    pub fn bounded_stale_read_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        min_commit_index: LogIndex,
        max_stale_index_lag: LogIndex,
    ) -> Result<ReadPathReport, RaftError> {
        self.bounded_stale_read_on_node_with_options(
            group_id,
            node_id,
            MatrixRaftBoundedStaleReadOptions::new(min_commit_index, max_stale_index_lag),
        )
    }

    pub fn bounded_stale_read_on_node_with_options(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        options: MatrixRaftBoundedStaleReadOptions,
    ) -> Result<ReadPathReport, RaftError> {
        self.node(group_id, node_id)?
            .bounded_stale_read_index_with_options(options)
    }

    pub fn bounded_stale_reads_on_group(
        &self,
        group_id: GroupId,
        min_commit_index: LogIndex,
        max_stale_index_lag: LogIndex,
    ) -> Result<Vec<ReadPathReport>, RaftError> {
        self.bounded_stale_reads_with_options_on_group(
            group_id,
            MatrixRaftBoundedStaleReadOptions::new(min_commit_index, max_stale_index_lag),
        )
    }

    pub fn bounded_stale_reads_with_options_on_group(
        &self,
        group_id: GroupId,
        options: MatrixRaftBoundedStaleReadOptions,
    ) -> Result<Vec<ReadPathReport>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                self.node(key.group_id, key.node_id)?
                    .bounded_stale_read_index_with_options(options)
            })
            .collect()
    }

    pub fn plan_bounded_stale_reads_on_group(
        &self,
        group_id: GroupId,
        min_commit_index: LogIndex,
        max_stale_index_lag: LogIndex,
    ) -> Result<MatrixRaftBoundedStaleReadFanoutGroupPlan, RaftError> {
        self.plan_bounded_stale_reads_with_options_on_group(
            group_id,
            MatrixRaftBoundedStaleReadOptions::new(min_commit_index, max_stale_index_lag),
        )
    }

    pub fn plan_bounded_stale_reads_with_options_on_group(
        &self,
        group_id: GroupId,
        options: MatrixRaftBoundedStaleReadOptions,
    ) -> Result<MatrixRaftBoundedStaleReadFanoutGroupPlan, RaftError> {
        Ok(self
            .bounded_stale_read_fanout_plan_from_groups(&[group_id], options)?
            .groups
            .into_iter()
            .next()
            .expect("single group bounded-stale read fanout plan"))
    }

    pub fn plan_bounded_stale_reads_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        min_commit_index: LogIndex,
        max_stale_index_lag: LogIndex,
    ) -> Result<MatrixRaftBoundedStaleReadFanoutPlan, RaftError> {
        self.plan_bounded_stale_reads_with_options_for_groups(
            group_ids,
            MatrixRaftBoundedStaleReadOptions::new(min_commit_index, max_stale_index_lag),
        )
    }

    pub fn plan_bounded_stale_reads_with_options_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftBoundedStaleReadOptions,
    ) -> Result<MatrixRaftBoundedStaleReadFanoutPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.bounded_stale_read_fanout_plan_from_groups(&group_ids, options)
    }

    pub fn bounded_stale_reads_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        min_commit_index: LogIndex,
        max_stale_index_lag: LogIndex,
    ) -> Result<Vec<(GroupId, Vec<ReadPathReport>)>, RaftError> {
        self.bounded_stale_reads_with_options_for_groups(
            group_ids,
            MatrixRaftBoundedStaleReadOptions::new(min_commit_index, max_stale_index_lag),
        )
    }

    pub fn bounded_stale_reads_with_options_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftBoundedStaleReadOptions,
    ) -> Result<Vec<(GroupId, Vec<ReadPathReport>)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.bounded_stale_read_fanout_plan_from_groups(&group_ids, options)?;

        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut reads = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                reads.push(
                    self.node(key.group_id, key.node_id)?
                        .bounded_stale_read_index_with_options(group.options)?,
                );
            }
            groups.push((group.group_id, reads));
        }
        Ok(groups)
    }

    pub fn bounded_stale_reads_on_group_best_effort(
        &self,
        group_id: GroupId,
        min_commit_index: LogIndex,
        max_stale_index_lag: LogIndex,
    ) -> Result<MatrixRaftBoundedStaleReadGroupResult, RaftError> {
        self.bounded_stale_reads_with_options_on_group_best_effort(
            group_id,
            MatrixRaftBoundedStaleReadOptions::new(min_commit_index, max_stale_index_lag),
        )
    }

    pub fn bounded_stale_reads_with_options_on_group_best_effort(
        &self,
        group_id: GroupId,
        options: MatrixRaftBoundedStaleReadOptions,
    ) -> Result<MatrixRaftBoundedStaleReadGroupResult, RaftError> {
        let plan = self.plan_bounded_stale_reads_with_options_on_group(group_id, options)?;
        Ok(self.bounded_stale_reads_with_group_plan_best_effort(plan))
    }

    pub fn bounded_stale_reads_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        min_commit_index: LogIndex,
        max_stale_index_lag: LogIndex,
    ) -> Result<Vec<MatrixRaftBoundedStaleReadGroupResult>, RaftError> {
        self.bounded_stale_reads_with_options_for_groups_best_effort(
            group_ids,
            MatrixRaftBoundedStaleReadOptions::new(min_commit_index, max_stale_index_lag),
        )
    }

    pub fn bounded_stale_reads_with_options_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftBoundedStaleReadOptions,
    ) -> Result<Vec<MatrixRaftBoundedStaleReadGroupResult>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.bounded_stale_read_fanout_plan_from_groups(&group_ids, options)?;
        Ok(plan
            .groups
            .into_iter()
            .map(|group| self.bounded_stale_reads_with_group_plan_best_effort(group))
            .collect())
    }

    fn bounded_stale_reads_with_group_plan_best_effort(
        &self,
        group: MatrixRaftBoundedStaleReadFanoutGroupPlan,
    ) -> MatrixRaftBoundedStaleReadGroupResult {
        let mut results = Vec::with_capacity(group.node_count);
        for key in group.route_keys {
            let result = match self.node(key.group_id, key.node_id).and_then(|node| {
                node.bounded_stale_read_index_with_options(group.options)
            }) {
                Ok(report) => MatrixRaftBoundedStaleReadNodeResult::ok(key, report),
                Err(error) => MatrixRaftBoundedStaleReadNodeResult::error(key, error),
            };
            results.push(result);
        }
        let ok_count = results.iter().filter(|result| result.is_ok()).count();
        let error_count = results.len().saturating_sub(ok_count);
        MatrixRaftBoundedStaleReadGroupResult {
            group_id: group.group_id,
            node_count: group.node_count,
            ok_count,
            error_count,
            results,
        }
    }

    pub fn read_index_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        min_commit_index: LogIndex,
        callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        self.read_index_with_options_callbacks_on_group(
            group_id,
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
            callback_for_key,
            timeout_ms,
        )
    }

    pub fn read_index_with_options_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        options: MatrixRaftReadIndexOptions,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_read_indexes_with_options_on_group(group_id, options)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            let callback = callback_for_key(key);
            let result = self.node(key.group_id, key.node_id)?.read_index_with_options_callback(
                plan.options,
                callback,
                timeout_ms,
            );
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn read_index_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        min_commit_index: LogIndex,
        callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        self.read_index_with_options_callbacks_for_groups(
            group_ids,
            MatrixRaftReadIndexOptions::lease_read(min_commit_index),
            callback_for_key,
            timeout_ms,
        )
    }

    pub fn read_index_with_options_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        options: MatrixRaftReadIndexOptions,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.read_index_fanout_plan_from_groups(&group_ids, options)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self.node(key.group_id, key.node_id)?.read_index_with_options_callback(
                    group.options,
                    callback,
                    timeout_ms,
                );
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_membership_operation_to_node(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
        operation: MembershipOperation,
    ) -> Result<MembershipExecutionReport, RaftError> {
        self.node_mut(group_id, node_id)?
            .execute_membership_operation(operation)
    }

    pub fn plan_membership_operation_on_group(
        &self,
        group_id: GroupId,
        operation: MembershipOperation,
    ) -> Result<MatrixRaftMembershipFanoutGroupPlan, RaftError> {
        Ok(self
            .membership_fanout_plan_from_groups(&[group_id], operation)?
            .groups
            .into_iter()
            .next()
            .expect("single group membership fanout plan"))
    }

    pub fn plan_membership_operation_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        operation: MembershipOperation,
    ) -> Result<MatrixRaftMembershipFanoutPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.membership_fanout_plan_from_groups(&group_ids, operation)
    }

    fn membership_operation_callback_on_node<F>(
        &mut self,
        key: MatrixRaftRouteKey,
        operation: MembershipOperation,
        callback: F,
        timeout_ms: u64,
    ) -> Result<MatrixRaftAsyncResult, RaftError>
    where
        F: FnOnce(MatrixRaftAsyncResult),
    {
        let result = match operation {
            MembershipOperation::AddNode(peer) | MembershipOperation::AddVoter(peer) => {
                self.node_mut(key.group_id, key.node_id)?.add_node_callback(
                    MatrixRaftNodeId::from(&peer),
                    callback,
                    timeout_ms,
                )
            }
            MembershipOperation::AddLearner(peer) => self
                .node_mut(key.group_id, key.node_id)?
                .add_learner_callback(
                    MatrixRaftNodeId::from(&peer),
                    peer.auto_promote,
                    callback,
                    timeout_ms,
                ),
            MembershipOperation::AddWitness(peer) => self
                .node_mut(key.group_id, key.node_id)?
                .add_witness_callback(MatrixRaftNodeId::from(&peer), callback, timeout_ms),
            MembershipOperation::Promote(node_id) => {
                self.node_mut(key.group_id, key.node_id)?.promote_callback(
                    MatrixRaftNodeId {
                        peer_id: node_id,
                        raft_addr: String::new(),
                        snapshot_addr: String::new(),
                    },
                    callback,
                    timeout_ms,
                )
            }
            MembershipOperation::Remove(node_id) => self
                .node_mut(key.group_id, key.node_id)?
                .remove_node_callback(
                    MatrixRaftNodeId {
                        peer_id: node_id,
                        raft_addr: String::new(),
                        snapshot_addr: String::new(),
                    },
                    callback,
                    timeout_ms,
                ),
            MembershipOperation::TransferLeader(transferee_id) => self
                .node(key.group_id, key.node_id)?
                .transfer_leader_callback(transferee_id, callback, timeout_ms),
        };
        Ok(result)
    }

    pub fn membership_operation_callbacks_on_group<F, C>(
        &mut self,
        group_id: GroupId,
        operation: MembershipOperation,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_membership_operation_on_group(group_id, operation)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            let callback = callback_for_key(key);
            let result = self.membership_operation_callback_on_node(
                key,
                plan.operation.clone(),
                callback,
                timeout_ms,
            )?;
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn membership_operation_callbacks_for_groups<F, C>(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        operation: MembershipOperation,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_membership_operation_for_groups(group_ids, operation)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self.membership_operation_callback_on_node(
                    key,
                    group.operation.clone(),
                    callback,
                    timeout_ms,
                )?;
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_membership_operation_to_group(
        &mut self,
        group_id: GroupId,
        operation: MembershipOperation,
    ) -> Result<Vec<MembershipExecutionReport>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        let mut reports = Vec::with_capacity(keys.len());
        for key in keys {
            reports.push(
                self.node_mut(key.group_id, key.node_id)?
                    .execute_membership_operation(operation.clone())?,
            );
        }
        Ok(reports)
    }

    pub fn route_membership_operation_to_group_best_effort(
        &mut self,
        group_id: GroupId,
        operation: MembershipOperation,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let routed = MatrixRaftRoutedMessage::new(
                key.group_id,
                key.node_id,
                MatrixRaftMessage::membership_operation(key.node_id, key.node_id, operation.clone()),
            );
            let routed_result = match self
                .node_mut(key.group_id, key.node_id)
                .and_then(|node| node.execute_membership_operation(operation.clone()))
            {
                Ok(report) => {
                    let mut result = MatrixRaftRouteResult::delivered(
                        key,
                        MatrixRaftMessageType::MembershipOperation,
                        "membership operation delivered",
                    );
                    result.membership = Some(report);
                    MatrixRaftBatchRouteResult::from_routed_result(&routed, result)
                }
                Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
            };
            results.push(routed_result);
        }
        Ok(results)
    }

    pub fn route_membership_operation_to_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        operation: MembershipOperation,
    ) -> Result<Vec<(GroupId, Vec<MembershipExecutionReport>)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_membership_operation_for_groups(group_ids, operation.clone())?;
        let mut groups = Vec::new();
        for group in plan.groups {
            groups.push((
                group.group_id,
                self.route_membership_operation_to_group(group.group_id, operation.clone())?,
            ));
        }
        Ok(groups)
    }

    pub fn route_membership_operation_to_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        operation: MembershipOperation,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_membership_operation_for_groups(group_ids, operation.clone())?;
        let mut groups = Vec::new();
        for group in plan.groups {
            groups.push((
                group.group_id,
                self.route_membership_operation_to_group_best_effort(
                    group.group_id,
                    operation.clone(),
                )?,
            ));
        }
        Ok(groups)
    }

    pub fn route_membership_workflow_to_node(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
        operations: impl IntoIterator<Item = MembershipOperation>,
    ) -> Result<Vec<MembershipExecutionReport>, RaftError> {
        self.node_mut(group_id, node_id)?
            .execute_membership_workflow_with_rollback(operations)
    }

    pub fn plan_membership_workflow_on_group(
        &self,
        group_id: GroupId,
        operations: impl IntoIterator<Item = MembershipOperation>,
    ) -> Result<MatrixRaftMembershipWorkflowFanoutGroupPlan, RaftError> {
        Ok(self
            .membership_workflow_fanout_plan_from_groups(
                &[group_id],
                operations.into_iter().collect(),
            )?
            .groups
            .into_iter()
            .next()
            .expect("single group membership workflow fanout plan"))
    }

    pub fn plan_membership_workflow_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        operations: impl IntoIterator<Item = MembershipOperation>,
    ) -> Result<MatrixRaftMembershipWorkflowFanoutPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.membership_workflow_fanout_plan_from_groups(
            &group_ids,
            operations.into_iter().collect(),
        )
    }

    pub fn route_membership_workflow_to_group(
        &mut self,
        group_id: GroupId,
        operations: impl IntoIterator<Item = MembershipOperation>,
    ) -> Result<Vec<(MatrixRaftRouteKey, Vec<MembershipExecutionReport>)>, RaftError> {
        let plan = self.plan_membership_workflow_on_group(group_id, operations)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            let reports = self
                .node_mut(key.group_id, key.node_id)?
                .execute_membership_workflow_with_rollback(plan.operations.clone())?;
            results.push((key, reports));
        }
        Ok(results)
    }

    pub fn route_membership_workflow_to_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        operations: impl IntoIterator<Item = MembershipOperation>,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, Vec<MembershipExecutionReport>)>)>, RaftError>
    {
        let plan = self.plan_membership_workflow_for_groups(group_ids, operations)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let reports = self
                    .node_mut(key.group_id, key.node_id)?
                    .execute_membership_workflow_with_rollback(group.operations.clone())?;
                results.push((key, reports));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_membership_workflow_to_group_best_effort(
        &mut self,
        group_id: GroupId,
        operations: impl IntoIterator<Item = MembershipOperation>,
    ) -> Result<MatrixRaftMembershipWorkflowGroupResult, RaftError> {
        let plan = self.plan_membership_workflow_on_group(group_id, operations)?;
        Ok(self.membership_workflow_group_plan_best_effort(plan))
    }

    pub fn route_membership_workflow_to_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        operations: impl IntoIterator<Item = MembershipOperation>,
    ) -> Result<Vec<MatrixRaftMembershipWorkflowGroupResult>, RaftError> {
        let plan = self.plan_membership_workflow_for_groups(group_ids, operations)?;
        Ok(plan
            .groups
            .into_iter()
            .map(|group| self.membership_workflow_group_plan_best_effort(group))
            .collect())
    }

    fn membership_workflow_group_plan_best_effort(
        &mut self,
        group: MatrixRaftMembershipWorkflowFanoutGroupPlan,
    ) -> MatrixRaftMembershipWorkflowGroupResult {
        let mut results = Vec::with_capacity(group.node_count);
        for key in group.route_keys {
            let result = match self.node_mut(key.group_id, key.node_id).and_then(|node| {
                node.execute_membership_workflow_with_rollback(group.operations.clone())
            }) {
                Ok(reports) => MatrixRaftMembershipWorkflowNodeResult::ok(key, reports),
                Err(error) => MatrixRaftMembershipWorkflowNodeResult::error(key, error),
            };
            results.push(result);
        }
        let ok_count = results.iter().filter(|result| result.is_ok()).count();
        let error_count = results.len().saturating_sub(ok_count);
        MatrixRaftMembershipWorkflowGroupResult {
            group_id: group.group_id,
            node_count: group.node_count,
            ok_count,
            error_count,
            results,
        }
    }

    pub fn route_config_change_to_node(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
        change: MatrixRaftConfigChange,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_config_change(MatrixRaftRouteKey::new(group_id, node_id), change)
    }

    pub fn plan_config_change_on_group(
        &self,
        group_id: GroupId,
        change: MatrixRaftConfigChange,
    ) -> Result<MatrixRaftConfigChangeFanoutGroupPlan, RaftError> {
        Ok(self
            .config_change_fanout_plan_from_groups(&[group_id], change)?
            .groups
            .into_iter()
            .next()
            .expect("single group config-change fanout plan"))
    }

    pub fn plan_config_change_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        change: MatrixRaftConfigChange,
    ) -> Result<MatrixRaftConfigChangeFanoutPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.config_change_fanout_plan_from_groups(&group_ids, change)
    }

    pub fn route_config_change_to_group(
        &mut self,
        group_id: GroupId,
        change: MatrixRaftConfigChange,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.route_config_change(key, change.clone())?);
        }
        Ok(results)
    }

    pub fn route_config_change_to_group_best_effort(
        &mut self,
        group_id: GroupId,
        change: MatrixRaftConfigChange,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let routed = MatrixRaftRoutedMessage::new(
                key.group_id,
                key.node_id,
                MatrixRaftMessage::config_change(key.node_id, key.node_id, change.clone()),
            );
            let routed_result = match self.route_config_change(key, change.clone()) {
                Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
            };
            results.push(routed_result);
        }
        Ok(results)
    }

    pub fn route_config_change_to_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        change: MatrixRaftConfigChange,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_config_change_for_groups(group_ids, change.clone())?;
        let mut groups = Vec::new();
        for group in plan.groups {
            groups.push((
                group.group_id,
                self.route_config_change_to_group(group.group_id, change.clone())?,
            ));
        }
        Ok(groups)
    }

    pub fn route_config_change_to_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        change: MatrixRaftConfigChange,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_config_change_for_groups(group_ids, change.clone())?;
        let mut groups = Vec::new();
        for group in plan.groups {
            groups.push((
                group.group_id,
                self.route_config_change_to_group_best_effort(group.group_id, change.clone())?,
            ));
        }
        Ok(groups)
    }

    pub fn catch_up_peer_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> Result<LearnerCatchUpLoopReport, RaftError> {
        self.node(group_id, node_id)?.catch_up_peer(peer_id)
    }

    pub fn catch_up_peer_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<LearnerCatchUpLoopReport>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| self.node(key.group_id, key.node_id)?.catch_up_peer(peer_id))
            .collect()
    }

    pub fn catch_up_peer_on_group_best_effort(
        &mut self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_message_to_group_best_effort(
            group_id,
            MatrixRaftMessage::catch_up_peer(peer_id, peer_id),
        )
    }

    pub fn catch_up_peer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<LearnerCatchUpLoopReport>)>, RaftError> {
        let plan = self.plan_catch_up_peer_for_groups(group_ids, peer_id)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut reports = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                reports.push(self.node(key.group_id, key.node_id)?.catch_up_peer(peer_id)?);
            }
            groups.push((group.group_id, reports));
        }
        Ok(groups)
    }

    pub fn plan_catch_up_peer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::catch_up_peer(peer_id, peer_id),
        )
    }

    pub fn catch_up_peer_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::catch_up_peer(peer_id, peer_id),
        )
    }

    pub fn promote_peer_on_node(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftPromoteReport, RaftError> {
        self.node_mut(group_id, node_id)?
            .promote_after_catch_up(peer_id)
    }

    pub fn promote_peer_on_group(
        &mut self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftPromoteReport>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        let mut reports = Vec::with_capacity(keys.len());
        for key in keys {
            reports.push(
                self.node_mut(key.group_id, key.node_id)?
                    .promote_after_catch_up(peer_id)?,
            );
        }
        Ok(reports)
    }

    pub fn promote_peer_on_group_best_effort(
        &mut self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_message_to_group_best_effort(
            group_id,
            MatrixRaftMessage::promote_peer(peer_id, peer_id),
        )
    }

    pub fn promote_peer_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftPromoteReport>)>, RaftError> {
        let plan = self.plan_promote_peer_for_groups(group_ids, peer_id)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut reports = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                reports.push(
                    self.node_mut(key.group_id, key.node_id)?
                        .promote_after_catch_up(peer_id)?,
                );
            }
            groups.push((group.group_id, reports));
        }
        Ok(groups)
    }

    pub fn promote_peer_callbacks_on_group<F, C>(
        &mut self,
        group_id: GroupId,
        peer_id: NodeId,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_promote_peer_for_groups([group_id], peer_id)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            let callback = callback_for_key(key);
            let result = self.node_mut(key.group_id, key.node_id)?.promote_callback(
                MatrixRaftNodeId {
                    peer_id,
                    raft_addr: String::new(),
                    snapshot_addr: String::new(),
                },
                callback,
                timeout_ms,
            );
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn promote_peer_callbacks_for_groups<F, C>(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_promote_peer_for_groups(group_ids, peer_id)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self.node_mut(key.group_id, key.node_id)?.promote_callback(
                    MatrixRaftNodeId {
                        peer_id,
                        raft_addr: String::new(),
                        snapshot_addr: String::new(),
                    },
                    callback,
                    timeout_ms,
                );
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn plan_promote_peer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(group_ids, MatrixRaftMessage::promote_peer(peer_id, peer_id))
    }

    pub fn promote_peer_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::promote_peer(peer_id, peer_id),
        )
    }

    pub fn auto_promote_learner_on_node(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
        learner_id: NodeId,
    ) -> Result<LearnerAutoPromoteReport, RaftError> {
        self.node_mut(group_id, node_id)?
            .auto_promote_learner(learner_id)
    }

    pub fn auto_promote_learner_on_group(
        &mut self,
        group_id: GroupId,
        learner_id: NodeId,
    ) -> Result<Vec<LearnerAutoPromoteReport>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        let mut reports = Vec::with_capacity(keys.len());
        for key in keys {
            reports.push(
                self.node_mut(key.group_id, key.node_id)?
                    .auto_promote_learner(learner_id)?,
            );
        }
        Ok(reports)
    }

    pub fn auto_promote_learner_on_group_best_effort(
        &mut self,
        group_id: GroupId,
        learner_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_message_to_group_best_effort(
            group_id,
            MatrixRaftMessage::auto_promote_learner(learner_id, learner_id),
        )
    }

    pub fn auto_promote_learner_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        learner_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<LearnerAutoPromoteReport>)>, RaftError> {
        let plan = self.plan_auto_promote_learner_for_groups(group_ids, learner_id)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut reports = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                reports.push(
                    self.node_mut(key.group_id, key.node_id)?
                        .auto_promote_learner(learner_id)?,
                );
            }
            groups.push((group.group_id, reports));
        }
        Ok(groups)
    }

    pub fn auto_promote_learner_callbacks_on_group<F, C>(
        &mut self,
        group_id: GroupId,
        learner_id: NodeId,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_auto_promote_learner_for_groups([group_id], learner_id)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            let callback = callback_for_key(key);
            let result = self
                .node_mut(key.group_id, key.node_id)?
                .auto_promote_learner_callback(learner_id, callback, timeout_ms);
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn auto_promote_learner_callbacks_for_groups<F, C>(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        learner_id: NodeId,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_auto_promote_learner_for_groups(group_ids, learner_id)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self
                    .node_mut(key.group_id, key.node_id)?
                    .auto_promote_learner_callback(learner_id, callback, timeout_ms);
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn plan_auto_promote_learner_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        learner_id: NodeId,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::auto_promote_learner(learner_id, learner_id),
        )
    }

    pub fn auto_promote_learner_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        learner_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::auto_promote_learner(learner_id, learner_id),
        )
    }

    pub fn campaign_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        candidate_id: NodeId,
        forced: bool,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::campaign(candidate_id, forced),
        )
    }

    pub fn campaign_on_group(
        &self,
        group_id: GroupId,
        candidate_id: NodeId,
        forced: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::campaign(candidate_id, forced),
        )
    }

    pub fn campaign_on_group_best_effort(
        &self,
        group_id: GroupId,
        candidate_id: NodeId,
        forced: bool,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::campaign(candidate_id, forced),
        )
    }

    pub fn campaigns_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        candidate_id: NodeId,
        forced: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::campaign(candidate_id, forced),
        )
    }

    pub fn plan_campaigns_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        candidate_id: NodeId,
        forced: bool,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::campaign(candidate_id, forced),
        )
    }

    pub fn campaigns_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        candidate_id: NodeId,
        forced: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::campaign(candidate_id, forced),
        )
    }

    pub fn campaign_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let keys = self.group_route_keys(group_id)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let callback = callback_for_key(key);
            let result = self
                .node(key.group_id, key.node_id)?
                .campaign_callback(callback, timeout_ms);
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn campaign_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_campaigns_for_groups(group_ids.iter().copied(), 0, false)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self
                    .node(key.group_id, key.node_id)?
                    .campaign_callback(callback, timeout_ms);
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn forced_campaign_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let keys = self.group_route_keys(group_id)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let callback = callback_for_key(key);
            let result = self
                .node(key.group_id, key.node_id)?
                .forced_campaign_callback(callback, timeout_ms);
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn forced_campaign_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_campaigns_for_groups(group_ids.iter().copied(), 0, true)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self
                    .node(key.group_id, key.node_id)?
                    .forced_campaign_callback(callback, timeout_ms);
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn transfer_leader_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        transferee_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::transfer_leader(transferee_id),
        )
    }

    pub fn transfer_leader_on_group(
        &self,
        group_id: GroupId,
        transferee_id: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::transfer_leader(transferee_id),
        )
    }

    pub fn transfer_leader_on_group_best_effort(
        &self,
        group_id: GroupId,
        transferee_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::transfer_leader(transferee_id),
        )
    }

    pub fn transfer_leader_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        transferee_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::transfer_leader(transferee_id),
        )
    }

    pub fn plan_transfer_leader_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        transferee_id: NodeId,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::transfer_leader(transferee_id),
        )
    }

    pub fn transfer_leader_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        transferee_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::transfer_leader(transferee_id),
        )
    }

    pub fn transfer_leader_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        transferee_id: NodeId,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_transfer_leader_for_groups([group_id], transferee_id)?;
        let group = plan
            .groups
            .into_iter()
            .next()
            .expect("single group transfer callback plan");
        let mut results = Vec::with_capacity(group.node_count);
        for key in group.route_keys {
            let callback = callback_for_key(key);
            let result = self
                .node(key.group_id, key.node_id)?
                .transfer_leader_callback(transferee_id, callback, timeout_ms);
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn transfer_leader_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        transferee_id: NodeId,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_transfer_leader_for_groups(group_ids, transferee_id)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self
                    .node(key.group_id, key.node_id)?
                    .transfer_leader_callback(transferee_id, callback, timeout_ms);
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn complete_leader_transfer_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::complete_leader_transfer(),
        )
    }

    pub fn complete_leader_transfer_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(group_id, MatrixRaftAdminCommand::complete_leader_transfer())
    }

    pub fn complete_leader_transfer_on_group_best_effort(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::complete_leader_transfer(),
        )
    }

    pub fn complete_leader_transfer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::complete_leader_transfer(),
        )
    }

    pub fn plan_complete_leader_transfer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::complete_leader_transfer(),
        )
    }

    pub fn complete_leader_transfer_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::complete_leader_transfer(),
        )
    }

    pub fn abort_leader_transfer_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        reason: impl Into<String>,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::abort_leader_transfer(reason),
        )
    }

    pub fn abort_leader_transfer_on_group(
        &self,
        group_id: GroupId,
        reason: impl Into<String>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::abort_leader_transfer(reason),
        )
    }

    pub fn abort_leader_transfer_on_group_best_effort(
        &self,
        group_id: GroupId,
        reason: impl Into<String>,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::abort_leader_transfer(reason),
        )
    }

    pub fn abort_leader_transfer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        reason: impl Into<String>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let reason = reason.into();
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::abort_leader_transfer(reason),
        )
    }

    pub fn plan_abort_leader_transfer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        reason: impl Into<String>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::abort_leader_transfer(reason),
        )
    }

    pub fn abort_leader_transfer_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        reason: impl Into<String>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let reason = reason.into();
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::abort_leader_transfer(reason),
        )
    }

    pub fn step_down_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        transferee_id: Option<NodeId>,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::step_down(transferee_id),
        )
    }

    pub fn step_down_on_group(
        &self,
        group_id: GroupId,
        transferee_id: Option<NodeId>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(group_id, MatrixRaftAdminCommand::step_down(transferee_id))
    }

    pub fn step_down_on_group_best_effort(
        &self,
        group_id: GroupId,
        transferee_id: Option<NodeId>,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::step_down(transferee_id),
        )
    }

    pub fn step_down_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        transferee_id: Option<NodeId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::step_down(transferee_id),
        )
    }

    pub fn plan_step_down_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        transferee_id: Option<NodeId>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::step_down(transferee_id),
        )
    }

    pub fn step_down_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        transferee_id: Option<NodeId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::step_down(transferee_id),
        )
    }

    pub fn step_down_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        transferee_id: Option<NodeId>,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_step_down_for_groups([group_id], transferee_id)?;
        let group = plan
            .groups
            .into_iter()
            .next()
            .expect("single group step-down callback plan");
        let mut results = Vec::with_capacity(group.node_count);
        for key in group.route_keys {
            let callback = callback_for_key(key);
            let result = self
                .node(key.group_id, key.node_id)?
                .step_down_callback(transferee_id, callback, timeout_ms);
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn step_down_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        transferee_id: Option<NodeId>,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_step_down_for_groups(group_ids, transferee_id)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self
                    .node(key.group_id, key.node_id)?
                    .step_down_callback(transferee_id, callback, timeout_ms);
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn resign_leader_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::resign(),
        )
    }

    pub fn resign_leader_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(group_id, MatrixRaftAdminCommand::resign())
    }

    pub fn resign_leader_on_group_best_effort(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(group_id, MatrixRaftAdminCommand::resign())
    }

    pub fn resign_leader_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(group_ids, MatrixRaftAdminCommand::resign())
    }

    pub fn plan_resign_leader_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(group_ids, MatrixRaftAdminCommand::resign())
    }

    pub fn resign_leader_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::resign(),
        )
    }

    pub fn resign_leader_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        reason: impl Into<String>,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let reason = reason.into();
        let plan = self.plan_resign_leader_for_groups([group_id])?;
        let group = plan
            .groups
            .into_iter()
            .next()
            .expect("single group resign callback plan");
        let mut results = Vec::with_capacity(group.node_count);
        for key in group.route_keys {
            let callback = callback_for_key(key);
            let result = self.node(key.group_id, key.node_id)?.resign_leader_callback(
                reason.clone(),
                callback,
                timeout_ms,
            );
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn resign_leader_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        reason: impl Into<String>,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let reason = reason.into();
        let plan = self.plan_resign_leader_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self.node(key.group_id, key.node_id)?.resign_leader_callback(
                    reason.clone(),
                    callback,
                    timeout_ms,
                );
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn trigger_snapshot_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::trigger_snapshot(),
        )
    }

    pub fn trigger_snapshot_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(group_id, MatrixRaftAdminCommand::trigger_snapshot())
    }

    pub fn trigger_snapshot_on_group_best_effort(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(group_id, MatrixRaftAdminCommand::trigger_snapshot())
    }

    pub fn trigger_snapshot_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::trigger_snapshot(),
        )
    }

    pub fn plan_trigger_snapshot_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::trigger_snapshot(),
        )
    }

    pub fn trigger_snapshot_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::trigger_snapshot(),
        )
    }

    pub fn async_snapshot_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<SnapshotMetadata, RaftError> {
        self.node(group_id, node_id)?.async_snapshot()
    }

    pub fn async_snapshots_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<(MatrixRaftRouteKey, SnapshotMetadata)>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                Ok((
                    key,
                    self.node(key.group_id, key.node_id)?.async_snapshot()?,
                ))
            })
            .collect()
    }

    pub fn async_snapshots_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, SnapshotMetadata)>)>, RaftError>
    {
        let plan = self.plan_async_snapshots_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let snapshots = group
                .route_keys
                .into_iter()
                .map(|key| {
                    Ok::<(MatrixRaftRouteKey, SnapshotMetadata), RaftError>((
                        key,
                        self.nodes
                            .get(&key)
                            .ok_or(RaftError::NodeNotFound(key.node_id))?
                            .async_snapshot()?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, snapshots));
        }
        Ok(groups)
    }

    pub fn plan_async_snapshots_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_trigger_snapshot_for_groups(group_ids)
    }

    pub fn async_snapshot_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        mut callback_for_key: F,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let keys = self.group_route_keys(group_id)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let callback = callback_for_key(key);
            let result = self
                .node(key.group_id, key.node_id)?
                .async_snapshot_callback(callback);
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn async_snapshot_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        mut callback_for_key: F,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_async_snapshots_for_groups(group_ids.iter().copied())?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self
                    .node(key.group_id, key.node_id)?
                    .async_snapshot_callback(callback);
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    fn plan_node_snapshot_completion_batch<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
        operation: impl Into<String>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError>
    where
        S: AsRef<str>,
    {
        let operation = operation.into();
        let mut seen = BTreeSet::new();
        let mut route_keys = Vec::new();
        let mut group_plans = BTreeMap::<
            GroupId,
            (Vec<NodeId>, Vec<MatrixRaftRouteKey>),
        >::new();

        for (key, _) in node_snapshots {
            if !seen.insert(key) {
                return Err(RaftError::InvalidRequest(format!(
                    "matrixraft node {} in group {} appears more than once in {} batch",
                    key.node_id, key.group_id, operation
                )));
            }
            self.node(key.group_id, key.node_id)?;
            route_keys.push(key);
            let (node_ids, group_route_keys) = group_plans.entry(key.group_id).or_default();
            node_ids.push(key.node_id);
            group_route_keys.push(key);
        }

        let groups = group_plans
            .into_iter()
            .map(|(group_id, (node_ids, route_keys))| MatrixRaftQueryFanoutGroupPlan {
                group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                operation: operation.clone(),
            })
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        Ok(MatrixRaftQueryFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count: route_keys.len(),
            route_keys,
            operation,
            groups,
        })
    }

    pub fn async_snapshot_ready_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        snapshot_id: impl AsRef<str>,
        success: bool,
    ) -> Result<(), RaftError> {
        self.node(group_id, node_id)?
            .async_snapshot_ready(snapshot_id, success)
    }

    pub fn plan_async_snapshot_ready_for_nodes<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
        success: bool,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError>
    where
        S: AsRef<str>,
    {
        self.plan_node_snapshot_completion_batch(
            node_snapshots,
            format!("async_snapshot_ready:{success}"),
        )
    }

    pub fn async_snapshot_ready_for_nodes<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
        success: bool,
    ) -> Result<Vec<MatrixRaftRouteKey>, RaftError>
    where
        S: AsRef<str>,
    {
        let batch = node_snapshots
            .into_iter()
            .map(|(key, snapshot_id)| (key, snapshot_id.as_ref().to_string()))
            .collect::<Vec<_>>();
        let plan = self.plan_async_snapshot_ready_for_nodes(
            batch
                .iter()
                .map(|(key, snapshot_id)| (*key, snapshot_id.as_str())),
            success,
        )?;
        let mut completed = Vec::with_capacity(plan.node_count);
        for (key, snapshot_id) in batch {
            self.node(key.group_id, key.node_id)?
                .async_snapshot_ready(snapshot_id, success)?;
            completed.push(key);
        }
        Ok(completed)
    }

    pub fn async_snapshot_ready_for_nodes_best_effort<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
        success: bool,
    ) -> Vec<MatrixRaftNodeSnapshotCompletionResult>
    where
        S: AsRef<str>,
    {
        self.direct_node_snapshot_completion_best_effort(
            node_snapshots,
            format!("async_snapshot_ready:{success}"),
            |node, snapshot_id| node.async_snapshot_ready(snapshot_id, success),
        )
    }

    pub fn async_snapshot_applied_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        snapshot_id: impl AsRef<str>,
    ) -> Result<(), RaftError> {
        self.node(group_id, node_id)?
            .async_snapshot_applied(snapshot_id)
    }

    pub fn plan_async_snapshot_applied_for_nodes<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError>
    where
        S: AsRef<str>,
    {
        self.plan_node_snapshot_completion_batch(node_snapshots, "async_snapshot_applied")
    }

    pub fn async_snapshot_applied_for_nodes<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
    ) -> Result<Vec<MatrixRaftRouteKey>, RaftError>
    where
        S: AsRef<str>,
    {
        let batch = node_snapshots
            .into_iter()
            .map(|(key, snapshot_id)| (key, snapshot_id.as_ref().to_string()))
            .collect::<Vec<_>>();
        let plan = self.plan_async_snapshot_applied_for_nodes(
            batch
                .iter()
                .map(|(key, snapshot_id)| (*key, snapshot_id.as_str())),
        )?;
        let mut completed = Vec::with_capacity(plan.node_count);
        for (key, snapshot_id) in batch {
            self.node(key.group_id, key.node_id)?
                .async_snapshot_applied(snapshot_id)?;
            completed.push(key);
        }
        Ok(completed)
    }

    pub fn async_snapshot_applied_for_nodes_best_effort<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
    ) -> Vec<MatrixRaftNodeSnapshotCompletionResult>
    where
        S: AsRef<str>,
    {
        self.direct_node_snapshot_completion_best_effort(
            node_snapshots,
            "async_snapshot_applied",
            |node, snapshot_id| node.async_snapshot_applied(snapshot_id),
        )
    }

    fn direct_node_snapshot_completion_best_effort<S, F>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
        operation: impl Into<String>,
        mut complete: F,
    ) -> Vec<MatrixRaftNodeSnapshotCompletionResult>
    where
        S: AsRef<str>,
        F: FnMut(&MatrixRaftNode, &str) -> Result<(), RaftError>,
    {
        let operation = operation.into();
        let mut seen = BTreeSet::new();
        let mut results = Vec::new();
        for (key, snapshot_id) in node_snapshots {
            let snapshot_id = snapshot_id.as_ref().to_string();
            let result = if !seen.insert(key) {
                MatrixRaftNodeSnapshotCompletionResult::error(
                    key,
                    snapshot_id,
                    operation.clone(),
                    RaftError::InvalidRequest(format!(
                        "matrixraft node {} in group {} appears more than once in {} batch",
                        key.node_id, key.group_id, operation
                    )),
                )
            } else {
                match self
                    .node(key.group_id, key.node_id)
                    .and_then(|node| complete(node, &snapshot_id))
                {
                    Ok(()) => MatrixRaftNodeSnapshotCompletionResult::ok(
                        key,
                        snapshot_id,
                        operation.clone(),
                    ),
                    Err(error) => MatrixRaftNodeSnapshotCompletionResult::error(
                        key,
                        snapshot_id,
                        operation.clone(),
                        error,
                    ),
                }
            };
            results.push(result);
        }
        results
    }

    pub fn install_snapshot_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        target: NodeId,
        snapshot: RaftSnapshot,
        fence: ApplySnapshotFence,
    ) -> Result<MatrixRaftSnapshotInstallNodeResult, RaftError> {
        let key = MatrixRaftRouteKey::new(group_id, node_id);
        self.node(group_id, node_id)?
            .install_snapshot_to(target, snapshot.clone(), fence)?;
        Ok(MatrixRaftSnapshotInstallNodeResult::ok(
            key, target, &snapshot,
        ))
    }

    pub fn plan_install_snapshot_on_group(
        &self,
        group_id: GroupId,
        target: NodeId,
        snapshot: RaftSnapshot,
        fence: ApplySnapshotFence,
    ) -> Result<MatrixRaftSnapshotInstallFanoutGroupPlan, RaftError> {
        Ok(self
            .snapshot_install_plan_from_groups(&[(group_id, target, snapshot, fence)])?
            .groups
            .into_iter()
            .next()
            .expect("single group snapshot install fanout plan"))
    }

    pub fn plan_install_snapshots_for_groups(
        &self,
        group_installs: impl IntoIterator<
            Item = (
                GroupId,
                NodeId,
                RaftSnapshot,
                ApplySnapshotFence,
            ),
        >,
    ) -> Result<MatrixRaftSnapshotInstallFanoutPlan, RaftError> {
        let group_installs = group_installs.into_iter().collect::<Vec<_>>();
        self.snapshot_install_plan_from_groups(&group_installs)
    }

    pub fn install_snapshot_on_group(
        &self,
        group_id: GroupId,
        target: NodeId,
        snapshot: RaftSnapshot,
        fence: ApplySnapshotFence,
    ) -> Result<Vec<MatrixRaftSnapshotInstallNodeResult>, RaftError> {
        let plan = self.plan_install_snapshot_on_group(group_id, target, snapshot, fence)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            self.node(key.group_id, key.node_id)?.install_snapshot_to(
                plan.target,
                plan.snapshot.clone(),
                plan.fence.clone(),
            )?;
            results.push(MatrixRaftSnapshotInstallNodeResult::ok(
                key,
                plan.target,
                &plan.snapshot,
            ));
        }
        Ok(results)
    }

    pub fn install_snapshots_for_groups(
        &self,
        group_installs: impl IntoIterator<
            Item = (
                GroupId,
                NodeId,
                RaftSnapshot,
                ApplySnapshotFence,
            ),
        >,
    ) -> Result<Vec<MatrixRaftSnapshotInstallGroupResult>, RaftError> {
        let plan = self.plan_install_snapshots_for_groups(group_installs)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                self.node(key.group_id, key.node_id)?.install_snapshot_to(
                    group.target,
                    group.snapshot.clone(),
                    group.fence.clone(),
                )?;
                results.push(MatrixRaftSnapshotInstallNodeResult::ok(
                    key,
                    group.target,
                    &group.snapshot,
                ));
            }
            groups.push(MatrixRaftSnapshotInstallGroupResult {
                group_id: group.group_id,
                target: group.target,
                node_count: group.node_count,
                ok_count: results.len(),
                error_count: 0,
                results,
            });
        }
        Ok(groups)
    }

    pub fn install_snapshot_on_group_best_effort(
        &self,
        group_id: GroupId,
        target: NodeId,
        snapshot: RaftSnapshot,
        fence: ApplySnapshotFence,
    ) -> Result<MatrixRaftSnapshotInstallGroupResult, RaftError> {
        let plan = self.plan_install_snapshot_on_group(group_id, target, snapshot, fence)?;
        Ok(self.install_snapshot_group_plan_best_effort(plan))
    }

    pub fn install_snapshots_for_groups_best_effort(
        &self,
        group_installs: impl IntoIterator<
            Item = (
                GroupId,
                NodeId,
                RaftSnapshot,
                ApplySnapshotFence,
            ),
        >,
    ) -> Result<Vec<MatrixRaftSnapshotInstallGroupResult>, RaftError> {
        let plan = self.plan_install_snapshots_for_groups(group_installs)?;
        Ok(plan
            .groups
            .into_iter()
            .map(|group| self.install_snapshot_group_plan_best_effort(group))
            .collect())
    }

    fn install_snapshot_group_plan_best_effort(
        &self,
        group: MatrixRaftSnapshotInstallFanoutGroupPlan,
    ) -> MatrixRaftSnapshotInstallGroupResult {
        let mut results = Vec::with_capacity(group.node_count);
        for key in group.route_keys {
            let result = match self.node(key.group_id, key.node_id).and_then(|node| {
                node.install_snapshot_to(group.target, group.snapshot.clone(), group.fence.clone())
            }) {
                Ok(()) => MatrixRaftSnapshotInstallNodeResult::ok(
                    key,
                    group.target,
                    &group.snapshot,
                ),
                Err(error) => MatrixRaftSnapshotInstallNodeResult::error(
                    key,
                    group.target,
                    &group.snapshot,
                    error,
                ),
            };
            results.push(result);
        }
        let ok_count = results.iter().filter(|result| result.is_ok()).count();
        let error_count = results.len().saturating_sub(ok_count);
        MatrixRaftSnapshotInstallGroupResult {
            group_id: group.group_id,
            target: group.target,
            node_count: group.node_count,
            ok_count,
            error_count,
            results,
        }
    }

    pub fn mark_snapshot_ready_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        snapshot_id: impl Into<String>,
        success: bool,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::snapshot_ready(snapshot_id, success),
        )
    }

    pub fn mark_snapshot_ready_on_group(
        &self,
        group_id: GroupId,
        snapshot_id: impl Into<String>,
        success: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::snapshot_ready(snapshot_id, success),
        )
    }

    pub fn mark_snapshot_ready_on_group_best_effort(
        &self,
        group_id: GroupId,
        snapshot_id: impl Into<String>,
        success: bool,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::snapshot_ready(snapshot_id, success),
        )
    }

    pub fn mark_snapshot_ready_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        snapshot_id: impl Into<String>,
        success: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::snapshot_ready(snapshot_id, success),
        )
    }

    pub fn plan_mark_snapshot_ready_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        snapshot_id: impl Into<String>,
        success: bool,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::snapshot_ready(snapshot_id, success),
        )
    }

    pub fn mark_snapshot_ready_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        snapshot_id: impl Into<String>,
        success: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::snapshot_ready(snapshot_id, success),
        )
    }

    pub fn mark_snapshot_ready_for_group_snapshots<S>(
        &self,
        group_snapshots: impl IntoIterator<Item = (GroupId, S)>,
        success: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError>
    where
        S: Into<String>,
    {
        let mut groups = Vec::new();
        for (group_id, snapshot_id) in group_snapshots {
            groups.push((
                group_id,
                self.mark_snapshot_ready_on_group(group_id, snapshot_id.into(), success)?,
            ));
        }
        Ok(groups)
    }

    pub fn plan_mark_snapshot_ready_for_node_snapshots<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
        success: bool,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError>
    where
        S: AsRef<str>,
    {
        self.plan_node_snapshot_completion_batch(
            node_snapshots,
            format!("mark_snapshot_ready:{success}"),
        )
    }

    pub fn mark_snapshot_ready_for_node_snapshots<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
        success: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError>
    where
        S: AsRef<str>,
    {
        let batch = node_snapshots
            .into_iter()
            .map(|(key, snapshot_id)| (key, snapshot_id.as_ref().to_string()))
            .collect::<Vec<_>>();
        self.plan_mark_snapshot_ready_for_node_snapshots(
            batch
                .iter()
                .map(|(key, snapshot_id)| (*key, snapshot_id.as_str())),
            success,
        )?;
        let mut results = Vec::with_capacity(batch.len());
        for (key, snapshot_id) in batch {
            results.push(self.route_admin_command(
                key,
                MatrixRaftAdminCommand::snapshot_ready(snapshot_id, success),
            )?);
        }
        Ok(results)
    }

    pub fn mark_snapshot_ready_for_node_snapshots_best_effort<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
        success: bool,
    ) -> Vec<MatrixRaftBatchRouteResult>
    where
        S: AsRef<str>,
    {
        self.route_node_snapshot_completion_best_effort(
            node_snapshots,
            format!("mark_snapshot_ready:{success}"),
            |snapshot_id| MatrixRaftAdminCommand::snapshot_ready(snapshot_id, success),
        )
    }

    pub fn mark_snapshot_ready_for_group_snapshots_best_effort<S>(
        &self,
        group_snapshots: impl IntoIterator<Item = (GroupId, S)>,
        success: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError>
    where
        S: Into<String>,
    {
        let mut groups = Vec::new();
        for (group_id, snapshot_id) in group_snapshots {
            groups.push((
                group_id,
                self.mark_snapshot_ready_on_group_best_effort(group_id, snapshot_id.into(), success)?,
            ));
        }
        Ok(groups)
    }

    pub fn mark_snapshot_applied_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::snapshot_applied(snapshot_id),
        )
    }

    pub fn mark_snapshot_applied_on_group(
        &self,
        group_id: GroupId,
        snapshot_id: impl Into<String>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::snapshot_applied(snapshot_id),
        )
    }

    pub fn mark_snapshot_applied_on_group_best_effort(
        &self,
        group_id: GroupId,
        snapshot_id: impl Into<String>,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::snapshot_applied(snapshot_id),
        )
    }

    pub fn mark_snapshot_applied_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        snapshot_id: impl Into<String>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::snapshot_applied(snapshot_id),
        )
    }

    pub fn plan_mark_snapshot_applied_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        snapshot_id: impl Into<String>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::snapshot_applied(snapshot_id),
        )
    }

    pub fn mark_snapshot_applied_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        snapshot_id: impl Into<String>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::snapshot_applied(snapshot_id),
        )
    }

    pub fn mark_snapshot_applied_for_group_snapshots<S>(
        &self,
        group_snapshots: impl IntoIterator<Item = (GroupId, S)>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError>
    where
        S: Into<String>,
    {
        let mut groups = Vec::new();
        for (group_id, snapshot_id) in group_snapshots {
            groups.push((
                group_id,
                self.mark_snapshot_applied_on_group(group_id, snapshot_id.into())?,
            ));
        }
        Ok(groups)
    }

    pub fn plan_mark_snapshot_applied_for_node_snapshots<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError>
    where
        S: AsRef<str>,
    {
        self.plan_node_snapshot_completion_batch(node_snapshots, "mark_snapshot_applied")
    }

    pub fn mark_snapshot_applied_for_node_snapshots<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError>
    where
        S: AsRef<str>,
    {
        let batch = node_snapshots
            .into_iter()
            .map(|(key, snapshot_id)| (key, snapshot_id.as_ref().to_string()))
            .collect::<Vec<_>>();
        self.plan_mark_snapshot_applied_for_node_snapshots(
            batch
                .iter()
                .map(|(key, snapshot_id)| (*key, snapshot_id.as_str())),
        )?;
        let mut results = Vec::with_capacity(batch.len());
        for (key, snapshot_id) in batch {
            results.push(
                self.route_admin_command(key, MatrixRaftAdminCommand::snapshot_applied(snapshot_id))?,
            );
        }
        Ok(results)
    }

    pub fn mark_snapshot_applied_for_node_snapshots_best_effort<S>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
    ) -> Vec<MatrixRaftBatchRouteResult>
    where
        S: AsRef<str>,
    {
        self.route_node_snapshot_completion_best_effort(
            node_snapshots,
            "mark_snapshot_applied",
            MatrixRaftAdminCommand::snapshot_applied,
        )
    }

    fn route_node_snapshot_completion_best_effort<S, F>(
        &self,
        node_snapshots: impl IntoIterator<Item = (MatrixRaftRouteKey, S)>,
        operation: impl Into<String>,
        mut command_for_snapshot: F,
    ) -> Vec<MatrixRaftBatchRouteResult>
    where
        S: AsRef<str>,
        F: FnMut(String) -> MatrixRaftAdminCommand,
    {
        let operation = operation.into();
        let mut seen = BTreeSet::new();
        let mut results = Vec::new();
        for (key, snapshot_id) in node_snapshots {
            let snapshot_id = snapshot_id.as_ref().to_string();
            let command = command_for_snapshot(snapshot_id);
            let routed = MatrixRaftRoutedMessage::new(
                key.group_id,
                key.node_id,
                MatrixRaftMessage::admin(key.node_id, key.node_id, command.clone()),
            );
            let routed_result = if !seen.insert(key) {
                MatrixRaftBatchRouteResult::from_routed_error(
                    &routed,
                    RaftError::InvalidRequest(format!(
                        "matrixraft node {} in group {} appears more than once in {} batch",
                        key.node_id, key.group_id, operation
                    )),
                )
            } else {
                match self.route_admin_command(key, command) {
                    Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                    Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                }
            };
            results.push(routed_result);
        }
        results
    }

    pub fn mark_snapshot_applied_for_group_snapshots_best_effort<S>(
        &self,
        group_snapshots: impl IntoIterator<Item = (GroupId, S)>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError>
    where
        S: Into<String>,
    {
        let mut groups = Vec::new();
        for (group_id, snapshot_id) in group_snapshots {
            groups.push((
                group_id,
                self.mark_snapshot_applied_on_group_best_effort(group_id, snapshot_id.into())?,
            ));
        }
        Ok(groups)
    }

    pub fn begin_snapshot_send_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::begin_snapshot_send(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn begin_snapshot_send_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::begin_snapshot_send(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn begin_snapshot_send_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::begin_snapshot_send(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn begin_snapshot_send_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::begin_snapshot_send(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn plan_begin_snapshot_send_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::begin_snapshot_send(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn begin_snapshot_send_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::begin_snapshot_send(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn record_snapshot_chunk_sent_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
        bytes: u64,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::record_snapshot_chunk_sent(peer_id, bytes),
        )
    }

    pub fn record_snapshot_chunk_sent_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        bytes: u64,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::record_snapshot_chunk_sent(peer_id, bytes),
        )
    }

    pub fn record_snapshot_chunk_sent_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        bytes: u64,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::record_snapshot_chunk_sent(peer_id, bytes),
        )
    }

    pub fn record_snapshot_chunk_sent_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        bytes: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::record_snapshot_chunk_sent(peer_id, bytes),
        )
    }

    pub fn plan_record_snapshot_chunk_sent_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        bytes: u64,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::record_snapshot_chunk_sent(peer_id, bytes),
        )
    }

    pub fn record_snapshot_chunk_sent_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        bytes: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::record_snapshot_chunk_sent(peer_id, bytes),
        )
    }

    pub fn acknowledge_snapshot_chunk_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::acknowledge_snapshot_chunk(peer_id),
        )
    }

    pub fn acknowledge_snapshot_chunk_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::acknowledge_snapshot_chunk(peer_id),
        )
    }

    pub fn acknowledge_snapshot_chunk_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::acknowledge_snapshot_chunk(peer_id),
        )
    }

    pub fn acknowledge_snapshot_chunk_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::acknowledge_snapshot_chunk(peer_id),
        )
    }

    pub fn plan_acknowledge_snapshot_chunk_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::acknowledge_snapshot_chunk(peer_id),
        )
    }

    pub fn acknowledge_snapshot_chunk_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::acknowledge_snapshot_chunk(peer_id),
        )
    }

    pub fn retry_snapshot_chunk_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::retry_snapshot_chunk(peer_id),
        )
    }

    pub fn retry_snapshot_chunk_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::retry_snapshot_chunk(peer_id),
        )
    }

    pub fn retry_snapshot_chunk_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::retry_snapshot_chunk(peer_id),
        )
    }

    pub fn retry_snapshot_chunk_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::retry_snapshot_chunk(peer_id),
        )
    }

    pub fn plan_retry_snapshot_chunk_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::retry_snapshot_chunk(peer_id),
        )
    }

    pub fn retry_snapshot_chunk_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::retry_snapshot_chunk(peer_id),
        )
    }

    pub fn cancel_snapshot_send_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::cancel_snapshot_send(peer_id),
        )
    }

    pub fn cancel_snapshot_send_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::cancel_snapshot_send(peer_id),
        )
    }

    pub fn cancel_snapshot_send_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::cancel_snapshot_send(peer_id),
        )
    }

    pub fn cancel_snapshot_send_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::cancel_snapshot_send(peer_id),
        )
    }

    pub fn plan_cancel_snapshot_send_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::cancel_snapshot_send(peer_id),
        )
    }

    pub fn cancel_snapshot_send_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::cancel_snapshot_send(peer_id),
        )
    }

    pub fn begin_snapshot_install_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::begin_snapshot_install(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn begin_snapshot_install_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::begin_snapshot_install(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn begin_snapshot_install_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::begin_snapshot_install(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn begin_snapshot_install_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::begin_snapshot_install(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn plan_begin_snapshot_install_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::begin_snapshot_install(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn begin_snapshot_install_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::begin_snapshot_install(
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            ),
        )
    }

    pub fn receive_snapshot_chunk_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
        bytes: u64,
        done: bool,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::receive_snapshot_chunk(peer_id, bytes, done),
        )
    }

    pub fn receive_snapshot_chunk_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        bytes: u64,
        done: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::receive_snapshot_chunk(peer_id, bytes, done),
        )
    }

    pub fn receive_snapshot_chunk_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        bytes: u64,
        done: bool,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::receive_snapshot_chunk(peer_id, bytes, done),
        )
    }

    pub fn receive_snapshot_chunk_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        bytes: u64,
        done: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::receive_snapshot_chunk(peer_id, bytes, done),
        )
    }

    pub fn plan_receive_snapshot_chunk_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        bytes: u64,
        done: bool,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::receive_snapshot_chunk(peer_id, bytes, done),
        )
    }

    pub fn receive_snapshot_chunk_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        bytes: u64,
        done: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::receive_snapshot_chunk(peer_id, bytes, done),
        )
    }

    pub fn rollback_snapshot_install_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::rollback_snapshot_install(peer_id),
        )
    }

    pub fn rollback_snapshot_install_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::rollback_snapshot_install(peer_id),
        )
    }

    pub fn rollback_snapshot_install_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::rollback_snapshot_install(peer_id),
        )
    }

    pub fn rollback_snapshot_install_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::rollback_snapshot_install(peer_id),
        )
    }

    pub fn plan_rollback_snapshot_install_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::rollback_snapshot_install(peer_id),
        )
    }

    pub fn rollback_snapshot_install_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::rollback_snapshot_install(peer_id),
        )
    }

    pub fn partition_peer_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::partition_peer(peer_id),
        )
    }

    pub fn partition_peer_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(group_id, MatrixRaftAdminCommand::partition_peer(peer_id))
    }

    pub fn partition_peer_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::partition_peer(peer_id),
        )
    }

    pub fn partition_peer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::partition_peer(peer_id),
        )
    }

    pub fn plan_partition_peer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::partition_peer(peer_id),
        )
    }

    pub fn partition_peer_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::partition_peer(peer_id),
        )
    }

    pub fn heal_peer_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::heal_peer(peer_id),
        )
    }

    pub fn heal_peer_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(group_id, MatrixRaftAdminCommand::heal_peer(peer_id))
    }

    pub fn heal_peer_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::heal_peer(peer_id),
        )
    }

    pub fn heal_peer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::heal_peer(peer_id),
        )
    }

    pub fn plan_heal_peer_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(group_ids, MatrixRaftAdminCommand::heal_peer(peer_id))
    }

    pub fn heal_peer_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::heal_peer(peer_id),
        )
    }

    pub fn set_node_healthy_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        target_node_id: NodeId,
        healthy: bool,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::set_node_healthy(target_node_id, healthy),
        )
    }

    pub fn set_node_healthy_on_group(
        &self,
        group_id: GroupId,
        target_node_id: NodeId,
        healthy: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::set_node_healthy(target_node_id, healthy),
        )
    }

    pub fn set_node_healthy_on_group_best_effort(
        &self,
        group_id: GroupId,
        target_node_id: NodeId,
        healthy: bool,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::set_node_healthy(target_node_id, healthy),
        )
    }

    pub fn set_node_healthy_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        target_node_id: NodeId,
        healthy: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::set_node_healthy(target_node_id, healthy),
        )
    }

    pub fn plan_set_node_healthy_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        target_node_id: NodeId,
        healthy: bool,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::set_node_healthy(target_node_id, healthy),
        )
    }

    pub fn set_node_healthy_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        target_node_id: NodeId,
        healthy: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::set_node_healthy(target_node_id, healthy),
        )
    }

    pub fn fire_fatal_event_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        target_node_id: NodeId,
        reason: impl Into<String>,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::fire_fatal_event(target_node_id, reason),
        )
    }

    pub fn fire_fatal_event_on_group(
        &self,
        group_id: GroupId,
        target_node_id: NodeId,
        reason: impl Into<String>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::fire_fatal_event(target_node_id, reason),
        )
    }

    pub fn fire_fatal_event_on_group_best_effort(
        &self,
        group_id: GroupId,
        target_node_id: NodeId,
        reason: impl Into<String>,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::fire_fatal_event(target_node_id, reason),
        )
    }

    pub fn fire_fatal_event_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        target_node_id: NodeId,
        reason: impl Into<String>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::fire_fatal_event(target_node_id, reason),
        )
    }

    pub fn plan_fire_fatal_event_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        target_node_id: NodeId,
        reason: impl Into<String>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::fire_fatal_event(target_node_id, reason),
        )
    }

    pub fn fire_fatal_event_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        target_node_id: NodeId,
        reason: impl Into<String>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::fire_fatal_event(target_node_id, reason),
        )
    }

    pub fn receive_out_of_order_append_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
        entry: MatrixRaftEntry,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::receive_out_of_order_append(peer_id, entry),
        )
    }

    pub fn receive_out_of_order_append_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        entry: MatrixRaftEntry,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::receive_out_of_order_append(peer_id, entry),
        )
    }

    pub fn receive_out_of_order_append_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        entry: MatrixRaftEntry,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::receive_out_of_order_append(peer_id, entry),
        )
    }

    pub fn receive_out_of_order_append_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        entry: MatrixRaftEntry,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::receive_out_of_order_append(peer_id, entry),
        )
    }

    pub fn plan_receive_out_of_order_append_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        entry: MatrixRaftEntry,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::receive_out_of_order_append(peer_id, entry),
        )
    }

    pub fn receive_out_of_order_append_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        entry: MatrixRaftEntry,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::receive_out_of_order_append(peer_id, entry),
        )
    }

    pub fn expire_peer_reorder_queue_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::expire_peer_reorder_queue(peer_id),
        )
    }

    pub fn expire_peer_reorder_queue_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::expire_peer_reorder_queue(peer_id),
        )
    }

    pub fn expire_peer_reorder_queue_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::expire_peer_reorder_queue(peer_id),
        )
    }

    pub fn expire_peer_reorder_queue_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::expire_peer_reorder_queue(peer_id),
        )
    }

    pub fn plan_expire_peer_reorder_queue_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::expire_peer_reorder_queue(peer_id),
        )
    }

    pub fn expire_peer_reorder_queue_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::expire_peer_reorder_queue(peer_id),
        )
    }

    pub fn set_prohibits_election_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        prohibits: bool,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::prohibits_election(prohibits),
        )
    }

    pub fn set_prohibits_election_on_group(
        &self,
        group_id: GroupId,
        prohibits: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::prohibits_election(prohibits),
        )
    }

    pub fn set_prohibits_election_on_group_best_effort(
        &self,
        group_id: GroupId,
        prohibits: bool,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::prohibits_election(prohibits),
        )
    }

    pub fn set_prohibits_election_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        prohibits: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::prohibits_election(prohibits),
        )
    }

    pub fn plan_set_prohibits_election_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        prohibits: bool,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::prohibits_election(prohibits),
        )
    }

    pub fn set_prohibits_election_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        prohibits: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::prohibits_election(prohibits),
        )
    }

    pub fn set_ignore_witness_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        ignore: bool,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::ignore_witness(ignore),
        )
    }

    pub fn set_ignore_witness_on_group(
        &self,
        group_id: GroupId,
        ignore: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(group_id, MatrixRaftAdminCommand::ignore_witness(ignore))
    }

    pub fn set_ignore_witness_on_group_best_effort(
        &self,
        group_id: GroupId,
        ignore: bool,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::ignore_witness(ignore),
        )
    }

    pub fn set_ignore_witness_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        ignore: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::ignore_witness(ignore),
        )
    }

    pub fn plan_set_ignore_witness_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        ignore: bool,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::ignore_witness(ignore),
        )
    }

    pub fn set_ignore_witness_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        ignore: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::ignore_witness(ignore),
        )
    }

    pub fn set_leader_lease_valid_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        valid: bool,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::set_leader_lease_valid(valid),
        )
    }

    pub fn set_leader_lease_valid_on_group(
        &self,
        group_id: GroupId,
        valid: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::set_leader_lease_valid(valid),
        )
    }

    pub fn set_leader_lease_valid_on_group_best_effort(
        &self,
        group_id: GroupId,
        valid: bool,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::set_leader_lease_valid(valid),
        )
    }

    pub fn set_leader_lease_valid_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        valid: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::set_leader_lease_valid(valid),
        )
    }

    pub fn plan_set_leader_lease_valid_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        valid: bool,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::set_leader_lease_valid(valid),
        )
    }

    pub fn set_leader_lease_valid_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        valid: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::set_leader_lease_valid(valid),
        )
    }

    pub fn receive_leader_lease_confirmation_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        confirmer_id: NodeId,
        confirmation_epoch: u64,
        duration_ms: Option<u64>,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::receive_leader_lease_confirmation(
                confirmer_id,
                confirmation_epoch,
                duration_ms,
            ),
        )
    }

    pub fn receive_leader_lease_confirmation_on_group(
        &self,
        group_id: GroupId,
        confirmer_id: NodeId,
        confirmation_epoch: u64,
        duration_ms: Option<u64>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::receive_leader_lease_confirmation(
                confirmer_id,
                confirmation_epoch,
                duration_ms,
            ),
        )
    }

    pub fn receive_leader_lease_confirmation_on_group_best_effort(
        &self,
        group_id: GroupId,
        confirmer_id: NodeId,
        confirmation_epoch: u64,
        duration_ms: Option<u64>,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::receive_leader_lease_confirmation(
                confirmer_id,
                confirmation_epoch,
                duration_ms,
            ),
        )
    }

    pub fn receive_leader_lease_confirmation_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        confirmer_id: NodeId,
        confirmation_epoch: u64,
        duration_ms: Option<u64>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::receive_leader_lease_confirmation(
                confirmer_id,
                confirmation_epoch,
                duration_ms,
            ),
        )
    }

    pub fn plan_receive_leader_lease_confirmation_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        confirmer_id: NodeId,
        confirmation_epoch: u64,
        duration_ms: Option<u64>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::receive_leader_lease_confirmation(
                confirmer_id,
                confirmation_epoch,
                duration_ms,
            ),
        )
    }

    pub fn receive_leader_lease_confirmation_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        confirmer_id: NodeId,
        confirmation_epoch: u64,
        duration_ms: Option<u64>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::receive_leader_lease_confirmation(
                confirmer_id,
                confirmation_epoch,
                duration_ms,
            ),
        )
    }

    pub fn tick_leader_lease_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        elapsed_ms: u64,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::tick_leader_lease(elapsed_ms),
        )
    }

    pub fn tick_leader_lease_on_group(
        &self,
        group_id: GroupId,
        elapsed_ms: u64,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::tick_leader_lease(elapsed_ms),
        )
    }

    pub fn tick_leader_lease_on_group_best_effort(
        &self,
        group_id: GroupId,
        elapsed_ms: u64,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::tick_leader_lease(elapsed_ms),
        )
    }

    pub fn tick_leader_lease_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        elapsed_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::tick_leader_lease(elapsed_ms),
        )
    }

    pub fn plan_tick_leader_lease_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        elapsed_ms: u64,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::tick_leader_lease(elapsed_ms),
        )
    }

    pub fn tick_leader_lease_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        elapsed_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::tick_leader_lease(elapsed_ms),
        )
    }

    pub fn receive_follower_lease_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        epoch: u64,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::receive_follower_lease(epoch),
        )
    }

    pub fn receive_follower_lease_on_group(
        &self,
        group_id: GroupId,
        epoch: u64,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::receive_follower_lease(epoch),
        )
    }

    pub fn receive_follower_lease_on_group_best_effort(
        &self,
        group_id: GroupId,
        epoch: u64,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::receive_follower_lease(epoch),
        )
    }

    pub fn receive_follower_lease_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        epoch: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::receive_follower_lease(epoch),
        )
    }

    pub fn plan_receive_follower_lease_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        epoch: u64,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::receive_follower_lease(epoch),
        )
    }

    pub fn receive_follower_lease_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        epoch: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::receive_follower_lease(epoch),
        )
    }

    pub fn tick_follower_lease_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        elapsed_ms: u64,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::tick_follower_lease(elapsed_ms),
        )
    }

    pub fn tick_follower_lease_on_group(
        &self,
        group_id: GroupId,
        elapsed_ms: u64,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::tick_follower_lease(elapsed_ms),
        )
    }

    pub fn tick_follower_lease_on_group_best_effort(
        &self,
        group_id: GroupId,
        elapsed_ms: u64,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::tick_follower_lease(elapsed_ms),
        )
    }

    pub fn tick_follower_lease_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        elapsed_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::tick_follower_lease(elapsed_ms),
        )
    }

    pub fn plan_tick_follower_lease_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        elapsed_ms: u64,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::tick_follower_lease(elapsed_ms),
        )
    }

    pub fn tick_follower_lease_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        elapsed_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::tick_follower_lease(elapsed_ms),
        )
    }

    pub fn synced_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        first_index: Option<LogIndex>,
        last_index: Option<LogIndex>,
        stabled_config_change_index: LogIndex,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::synced(
                first_index,
                last_index,
                stabled_config_change_index,
            ),
        )
    }

    pub fn synced_on_group(
        &self,
        group_id: GroupId,
        first_index: Option<LogIndex>,
        last_index: Option<LogIndex>,
        stabled_config_change_index: LogIndex,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::synced(first_index, last_index, stabled_config_change_index),
        )
    }

    pub fn synced_on_group_best_effort(
        &self,
        group_id: GroupId,
        first_index: Option<LogIndex>,
        last_index: Option<LogIndex>,
        stabled_config_change_index: LogIndex,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::synced(first_index, last_index, stabled_config_change_index),
        )
    }

    pub fn synced_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        first_index: Option<LogIndex>,
        last_index: Option<LogIndex>,
        stabled_config_change_index: LogIndex,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::synced(first_index, last_index, stabled_config_change_index),
        )
    }

    pub fn plan_synced_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        first_index: Option<LogIndex>,
        last_index: Option<LogIndex>,
        stabled_config_change_index: LogIndex,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::synced(first_index, last_index, stabled_config_change_index),
        )
    }

    pub fn synced_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        first_index: Option<LogIndex>,
        last_index: Option<LogIndex>,
        stabled_config_change_index: LogIndex,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::synced(first_index, last_index, stabled_config_change_index),
        )
    }

    pub fn applied_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        applied_node_id: NodeId,
        applied_index: LogIndex,
        rejected: bool,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::applied(applied_node_id, applied_index, rejected),
        )
    }

    pub fn applied_on_group(
        &self,
        group_id: GroupId,
        applied_node_id: NodeId,
        applied_index: LogIndex,
        rejected: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::applied(applied_node_id, applied_index, rejected),
        )
    }

    pub fn applied_on_group_best_effort(
        &self,
        group_id: GroupId,
        applied_node_id: NodeId,
        applied_index: LogIndex,
        rejected: bool,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::applied(applied_node_id, applied_index, rejected),
        )
    }

    pub fn applied_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        applied_node_id: NodeId,
        applied_index: LogIndex,
        rejected: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::applied(applied_node_id, applied_index, rejected),
        )
    }

    pub fn plan_applied_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        applied_node_id: NodeId,
        applied_index: LogIndex,
        rejected: bool,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::applied(applied_node_id, applied_index, rejected),
        )
    }

    pub fn applied_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        applied_node_id: NodeId,
        applied_index: LogIndex,
        rejected: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::applied(applied_node_id, applied_index, rejected),
        )
    }

    pub fn apply_task_inflight_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        applied_node_id: NodeId,
        applied_index: LogIndex,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::apply_task_inflight(applied_node_id, applied_index),
        )
    }

    pub fn apply_task_inflight_on_group(
        &self,
        group_id: GroupId,
        applied_node_id: NodeId,
        applied_index: LogIndex,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::apply_task_inflight(applied_node_id, applied_index),
        )
    }

    pub fn apply_task_inflight_on_group_best_effort(
        &self,
        group_id: GroupId,
        applied_node_id: NodeId,
        applied_index: LogIndex,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::apply_task_inflight(applied_node_id, applied_index),
        )
    }

    pub fn apply_task_inflight_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        applied_node_id: NodeId,
        applied_index: LogIndex,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::apply_task_inflight(applied_node_id, applied_index),
        )
    }

    pub fn plan_apply_task_inflight_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        applied_node_id: NodeId,
        applied_index: LogIndex,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::apply_task_inflight(applied_node_id, applied_index),
        )
    }

    pub fn apply_task_inflight_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        applied_node_id: NodeId,
        applied_index: LogIndex,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::apply_task_inflight(applied_node_id, applied_index),
        )
    }

    pub fn replicated_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
        success: bool,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::replicated(peer_id, success),
        )
    }

    pub fn replicated_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        success: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::replicated(peer_id, success),
        )
    }

    pub fn replicated_on_group_best_effort(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
        success: bool,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::replicated(peer_id, success),
        )
    }

    pub fn replicated_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        success: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::replicated(peer_id, success),
        )
    }

    pub fn plan_replicated_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        success: bool,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::replicated(peer_id, success),
        )
    }

    pub fn replicated_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
        success: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::replicated(peer_id, success),
        )
    }

    pub fn compact_logs_through_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        log_index: LogIndex,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::compact_logs_through(log_index),
        )
    }

    pub fn compact_logs_through_on_group(
        &self,
        group_id: GroupId,
        log_index: LogIndex,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::compact_logs_through(log_index),
        )
    }

    pub fn compact_logs_through_on_group_best_effort(
        &self,
        group_id: GroupId,
        log_index: LogIndex,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::compact_logs_through(log_index),
        )
    }

    pub fn compact_logs_through_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        log_index: LogIndex,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::compact_logs_through(log_index),
        )
    }

    pub fn plan_compact_logs_through_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        log_index: LogIndex,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::compact_logs_through(log_index),
        )
    }

    pub fn compact_logs_through_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        log_index: LogIndex,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::compact_logs_through(log_index),
        )
    }

    pub fn compact_logs_with_storage_fence_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        log_index: LogIndex,
        fence: StorageApplyFence,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::compact_logs_with_storage_fence(log_index, fence),
        )
    }

    pub fn compact_logs_with_storage_fence_on_group(
        &self,
        group_id: GroupId,
        log_index: LogIndex,
        fence: StorageApplyFence,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::compact_logs_with_storage_fence(log_index, fence),
        )
    }

    pub fn compact_logs_with_storage_fence_on_group_best_effort(
        &self,
        group_id: GroupId,
        log_index: LogIndex,
        fence: StorageApplyFence,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::compact_logs_with_storage_fence(log_index, fence),
        )
    }

    pub fn compact_logs_with_storage_fences_for_groups(
        &self,
        group_fences: impl IntoIterator<Item = (GroupId, StorageApplyFence)>,
        log_index: LogIndex,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_compact_logs_with_storage_fences_for_groups(group_fences, log_index)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                results.push(self.route_admin_command(key, group.command.clone())?);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn plan_compact_logs_with_storage_fences_for_groups(
        &self,
        group_fences: impl IntoIterator<Item = (GroupId, StorageApplyFence)>,
        log_index: LogIndex,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        let group_commands = group_fences.into_iter().map(|(group_id, fence)| {
            (
                group_id,
                MatrixRaftAdminCommand::compact_logs_with_storage_fence(log_index, fence),
            )
        });
        self.admin_command_fanout_plan_from_group_commands(
            MatrixRaftAdminCommandType::CompactLogsWithStorageFence,
            group_commands,
        )
    }

    pub fn compact_logs_with_storage_fences_for_groups_best_effort(
        &self,
        group_fences: impl IntoIterator<Item = (GroupId, StorageApplyFence)>,
        log_index: LogIndex,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let plan = self.plan_compact_logs_with_storage_fences_for_groups(group_fences, log_index)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let routed = MatrixRaftRoutedMessage::new(
                    key.group_id,
                    key.node_id,
                    MatrixRaftMessage::admin(key.node_id, key.node_id, group.command.clone()),
                );
                let routed_result = match self.route_admin_command(key, group.command.clone()) {
                    Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                    Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                };
                results.push(routed_result);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn checkpoint_snapshot_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        target_node_id: NodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::checkpoint_snapshot(target_node_id, snapshot_id),
        )
    }

    pub fn checkpoint_snapshot_on_group(
        &self,
        group_id: GroupId,
        target_node_id: NodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(
            group_id,
            MatrixRaftAdminCommand::checkpoint_snapshot(target_node_id, snapshot_id),
        )
    }

    pub fn checkpoint_snapshot_on_group_best_effort(
        &self,
        group_id: GroupId,
        target_node_id: NodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(
            group_id,
            MatrixRaftAdminCommand::checkpoint_snapshot(target_node_id, snapshot_id),
        )
    }

    pub fn checkpoint_snapshot_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        target_node_id: NodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::checkpoint_snapshot(target_node_id, snapshot_id),
        )
    }

    pub fn plan_checkpoint_snapshot_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        target_node_id: NodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::checkpoint_snapshot(target_node_id, snapshot_id),
        )
    }

    pub fn checkpoint_snapshot_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        target_node_id: NodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::checkpoint_snapshot(target_node_id, snapshot_id),
        )
    }

    pub fn witness_quorum_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        acknowledgements: impl IntoIterator<Item = NodeId>,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::witness_quorum(acknowledgements),
        )
    }

    pub fn witness_quorum_on_group(
        &self,
        group_id: GroupId,
        acknowledgements: impl IntoIterator<Item = NodeId>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let command = MatrixRaftAdminCommand::witness_quorum(acknowledgements);
        self.route_admin_command_to_group(group_id, command)
    }

    pub fn witness_quorum_on_group_best_effort(
        &self,
        group_id: GroupId,
        acknowledgements: impl IntoIterator<Item = NodeId>,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let command = MatrixRaftAdminCommand::witness_quorum(acknowledgements);
        self.route_admin_command_to_group_best_effort(group_id, command)
    }

    pub fn witness_quorum_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        acknowledgements: impl IntoIterator<Item = NodeId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::witness_quorum(acknowledgements),
        )
    }

    pub fn plan_witness_quorum_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        acknowledgements: impl IntoIterator<Item = NodeId>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(
            group_ids,
            MatrixRaftAdminCommand::witness_quorum(acknowledgements),
        )
    }

    pub fn witness_quorum_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        acknowledgements: impl IntoIterator<Item = NodeId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::witness_quorum(acknowledgements),
        )
    }

    pub fn release_memory_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_admin_command(
            MatrixRaftRouteKey::new(group_id, node_id),
            MatrixRaftAdminCommand::release_memory(),
        )
    }

    pub fn release_memory_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_admin_command_to_group(group_id, MatrixRaftAdminCommand::release_memory())
    }

    pub fn release_memory_on_group_best_effort(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_admin_command_to_group_best_effort(group_id, MatrixRaftAdminCommand::release_memory())
    }

    pub fn release_memory_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped(
            group_ids,
            MatrixRaftAdminCommand::release_memory(),
        )
    }

    pub fn plan_release_memory_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        self.plan_route_admin_command_to_groups(group_ids, MatrixRaftAdminCommand::release_memory())
    }

    pub fn release_memory_for_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_admin_command_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftAdminCommand::release_memory(),
        )
    }

    pub fn group_statuses(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftStatus>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                self.nodes
                    .get(&key)
                    .ok_or(RaftError::NodeNotFound(key.node_id))?
                    .get_status()
            })
            .collect()
    }

    pub fn statuses_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftStatus>)>, RaftError> {
        let plan = self.plan_statuses_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let statuses = group
                .route_keys
                .into_iter()
                .map(|key| {
                    self.nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .get_status()
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, statuses));
        }
        Ok(groups)
    }

    pub fn plan_statuses_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "statuses")
    }

    pub fn group_local_statuses(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<MatrixRaftLocalStatus>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                self.nodes
                    .get(&key)
                    .ok_or(RaftError::NodeNotFound(key.node_id))?
                    .get_local_status()
            })
            .collect()
    }

    pub fn local_statuses_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftLocalStatus>)>, RaftError> {
        let plan = self.plan_local_statuses_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let statuses = group
                .route_keys
                .into_iter()
                .map(|key| {
                    self.nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .get_local_status()
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, statuses));
        }
        Ok(groups)
    }

    pub fn plan_local_statuses_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "local_statuses")
    }

    pub fn start_index_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<LogIndex, RaftError> {
        Ok(self.node(group_id, node_id)?.start_index())
    }

    pub fn start_indices_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<LogIndex>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| Ok(self.node(key.group_id, key.node_id)?.start_index()))
            .collect()
    }

    pub fn start_indices_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<LogIndex>)>, RaftError> {
        let plan = self.plan_start_indices_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let start_indices = group
                .route_keys
                .into_iter()
                .map(|key| {
                    Ok::<LogIndex, RaftError>(self
                        .nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .start_index())
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, start_indices));
        }
        Ok(groups)
    }

    pub fn plan_start_indices_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "start_indices")
    }

    pub fn recover_fsm_from_snapshot_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<bool, RaftError> {
        Ok(self.node(group_id, node_id)?.recover_fsm_from_snapshot())
    }

    pub fn recover_fsm_from_snapshot_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<bool>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                Ok(self
                    .node(key.group_id, key.node_id)?
                    .recover_fsm_from_snapshot())
            })
            .collect()
    }

    pub fn recover_fsm_from_snapshot_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<bool>)>, RaftError> {
        let plan = self.plan_recover_fsm_from_snapshot_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let recover_flags = group
                .route_keys
                .into_iter()
                .map(|key| {
                    Ok::<bool, RaftError>(self
                        .nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .recover_fsm_from_snapshot())
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, recover_flags));
        }
        Ok(groups)
    }

    pub fn plan_recover_fsm_from_snapshot_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "recover_fsm_from_snapshot")
    }

    pub fn group_leaders(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<Option<MatrixRaftNodeId>>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                self.nodes
                    .get(&key)
                    .ok_or(RaftError::NodeNotFound(key.node_id))?
                    .leader_node()
            })
            .collect()
    }

    pub fn leaders_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<Option<MatrixRaftNodeId>>)>, RaftError> {
        let plan = self.plan_leaders_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let leaders = group
                .route_keys
                .into_iter()
                .map(|key| {
                    self.nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .leader_node()
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, leaders));
        }
        Ok(groups)
    }

    pub fn plan_leaders_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "leaders")
    }

    pub fn in_lease_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        term: Option<Term>,
    ) -> Result<bool, RaftError> {
        self.node(group_id, node_id)?.in_lease(term)
    }

    pub fn in_lease_on_group(
        &self,
        group_id: GroupId,
        term: Option<Term>,
    ) -> Result<Vec<bool>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| self.node(key.group_id, key.node_id)?.in_lease(term))
            .collect()
    }

    pub fn in_lease_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        term: Option<Term>,
    ) -> Result<Vec<(GroupId, Vec<bool>)>, RaftError> {
        let plan = self.plan_in_lease_for_groups(group_ids, term)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let leases = group
                .route_keys
                .into_iter()
                .map(|key| {
                    self.nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .in_lease(term)
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, leases));
        }
        Ok(groups)
    }

    pub fn plan_in_lease_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        term: Option<Term>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        let operation = match term {
            Some(term) => format!("in_lease:{term}"),
            None => "in_lease:any".to_string(),
        };
        self.plan_query_for_groups(group_ids, operation)
    }

    pub fn callback_scheduler_len_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<usize, RaftError> {
        Ok(self.node(group_id, node_id)?.callback_scheduler_len())
    }

    pub fn callback_scheduler_lens_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<usize>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| Ok(self.node(key.group_id, key.node_id)?.callback_scheduler_len()))
            .collect()
    }

    pub fn callback_scheduler_lens_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<usize>)>, RaftError> {
        let plan = self.plan_callback_scheduler_lens_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let lengths = group
                .route_keys
                .into_iter()
                .map(|key| {
                    Ok::<usize, RaftError>(self
                        .nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .callback_scheduler_len())
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, lengths));
        }
        Ok(groups)
    }

    pub fn plan_callback_scheduler_lens_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "callback_scheduler_lens")
    }

    pub fn callback_scheduler_next_timeout_ms_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        now_ms: u64,
    ) -> Result<u64, RaftError> {
        Ok(self
            .node(group_id, node_id)?
            .callback_scheduler_next_timeout_ms(now_ms))
    }

    pub fn callback_scheduler_next_timeout_ms_on_group(
        &self,
        group_id: GroupId,
        now_ms: u64,
    ) -> Result<Vec<u64>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                Ok(self
                    .node(key.group_id, key.node_id)?
                    .callback_scheduler_next_timeout_ms(now_ms))
            })
            .collect()
    }

    pub fn callback_scheduler_next_timeout_ms_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        now_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<u64>)>, RaftError> {
        let plan = self.plan_callback_scheduler_next_timeout_ms_for_groups(group_ids, now_ms)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let timeouts = group
                .route_keys
                .into_iter()
                .map(|key| {
                    Ok::<u64, RaftError>(self
                        .nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .callback_scheduler_next_timeout_ms(now_ms))
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, timeouts));
        }
        Ok(groups)
    }

    pub fn plan_callback_scheduler_next_timeout_ms_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        now_ms: u64,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, format!("callback_scheduler_next_timeout_ms:{now_ms}"))
    }

    pub fn drain_lapsed_callbacks_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<MatrixRaftAsyncResult>, RaftError> {
        Ok(self
            .node(group_id, node_id)?
            .drain_lapsed_callbacks(now_ms, limit))
    }

    pub fn drain_lapsed_callbacks_on_group(
        &self,
        group_id: GroupId,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<Vec<MatrixRaftAsyncResult>>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                Ok(self
                    .node(key.group_id, key.node_id)?
                    .drain_lapsed_callbacks(now_ms, limit))
            })
            .collect()
    }

    pub fn drain_lapsed_callbacks_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<(GroupId, Vec<Vec<MatrixRaftAsyncResult>>)>, RaftError> {
        let plan = self.plan_drain_lapsed_callbacks_for_groups(group_ids, now_ms, limit)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let results = group
                .route_keys
                .into_iter()
                .map(|key| {
                    Ok::<Vec<MatrixRaftAsyncResult>, RaftError>(self
                        .nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .drain_lapsed_callbacks(now_ms, limit))
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn plan_drain_lapsed_callbacks_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        now_ms: u64,
        limit: usize,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, format!("drain_lapsed_callbacks:{now_ms}:{limit}"))
    }

    pub fn cancel_callback_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        request_id: u64,
    ) -> Result<Option<MatrixRaftAsyncResult>, RaftError> {
        Ok(self.node(group_id, node_id)?.cancel_callback(request_id))
    }

    pub fn cancel_callback_on_group(
        &self,
        group_id: GroupId,
        request_id: u64,
    ) -> Result<Vec<Option<MatrixRaftAsyncResult>>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| Ok(self.node(key.group_id, key.node_id)?.cancel_callback(request_id)))
            .collect()
    }

    pub fn cancel_callback_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        request_id: u64,
    ) -> Result<Vec<(GroupId, Vec<Option<MatrixRaftAsyncResult>>)>, RaftError> {
        let plan = self.plan_cancel_callback_for_groups(group_ids, request_id)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let results = group
                .route_keys
                .into_iter()
                .map(|key| {
                    Ok::<Option<MatrixRaftAsyncResult>, RaftError>(self
                        .nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .cancel_callback(request_id))
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn plan_cancel_callback_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        request_id: u64,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, format!("cancel_callback:{request_id}"))
    }

    pub fn resolve_address_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftNodeId, RaftError> {
        self.node(group_id, node_id)?.resolve_address(peer_id)
    }

    pub fn resolve_address_on_group(
        &self,
        group_id: GroupId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftNodeId>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| self.node(key.group_id, key.node_id)?.resolve_address(peer_id))
            .collect()
    }

    pub fn resolve_address_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftNodeId>)>, RaftError> {
        let plan = self.plan_resolve_address_for_groups(group_ids, peer_id)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let nodes = group
                .route_keys
                .into_iter()
                .map(|key| self.node(key.group_id, key.node_id)?.resolve_address(peer_id))
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, nodes));
        }
        Ok(groups)
    }

    pub fn plan_resolve_address_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        peer_id: NodeId,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, format!("resolve_address:{peer_id}"))
    }

    pub fn fatal_blockers_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<Vec<Blocker>, RaftError> {
        self.node(group_id, node_id)?.get_fatal_blockers()
    }

    pub fn fatal_blockers_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<Vec<Blocker>>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| self.node(key.group_id, key.node_id)?.get_fatal_blockers())
            .collect()
    }

    pub fn fatal_blockers_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<Vec<Blocker>>)>, RaftError> {
        let plan = self.plan_fatal_blockers_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let blockers = group
                .route_keys
                .into_iter()
                .map(|key| {
                    self.nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .get_fatal_blockers()
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, blockers));
        }
        Ok(groups)
    }

    pub fn plan_fatal_blockers_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "fatal_blockers")
    }

    pub fn fatal_events_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<Vec<MatrixRaftFatalEvent>, RaftError> {
        self.node(group_id, node_id)?.get_fatal_events()
    }

    pub fn fatal_events_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<Vec<MatrixRaftFatalEvent>>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| self.node(key.group_id, key.node_id)?.get_fatal_events())
            .collect()
    }

    pub fn fatal_events_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<Vec<MatrixRaftFatalEvent>>)>, RaftError> {
        let plan = self.plan_fatal_events_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let events = group
                .route_keys
                .into_iter()
                .map(|key| {
                    self.nodes
                        .get(&key)
                        .ok_or(RaftError::NodeNotFound(key.node_id))?
                        .get_fatal_events()
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, events));
        }
        Ok(groups)
    }

    pub fn plan_fatal_events_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "fatal_events")
    }

    pub fn snapshot_routes_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotDesc>)>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        Ok(keys
            .into_iter()
            .map(|key| (key, self.snapshot_routes.get(&key).cloned()))
            .collect())
    }

    pub fn snapshot_routes_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, Option<MatrixRaftSnapshotDesc>)>)>, RaftError>
    {
        let plan = self.plan_snapshot_routes_for_groups(group_ids)?;
        Ok(plan
            .groups
            .into_iter()
            .map(|group| {
                (
                    group.group_id,
                    group
                        .route_keys
                        .into_iter()
                        .map(|key| (key, self.snapshot_routes.get(&key).cloned()))
                        .collect(),
                )
            })
            .collect())
    }

    pub fn plan_snapshot_routes_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "snapshot_routes")
    }

    pub fn memberships_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<Vec<MatrixRaftNodeId>>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| self.node(key.group_id, key.node_id)?.get_membership())
            .collect()
    }

    pub fn memberships_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<Vec<MatrixRaftNodeId>>)>, RaftError> {
        let plan = self.plan_memberships_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let memberships = group
                .route_keys
                .into_iter()
                .map(|key| self.node(key.group_id, key.node_id)?.get_membership())
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, memberships));
        }
        Ok(groups)
    }

    pub fn plan_memberships_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "memberships")
    }

    pub fn membership_members_on_group(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<Vec<MatrixRaftMemberId>>, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        keys.into_iter()
            .map(|key| {
                self.node(key.group_id, key.node_id)?
                    .get_membership_members()
            })
            .collect()
    }

    pub fn membership_members_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, Vec<Vec<MatrixRaftMemberId>>)>, RaftError> {
        let plan = self.plan_membership_members_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let members = group
                .route_keys
                .into_iter()
                .map(|key| {
                    self.node(key.group_id, key.node_id)?
                        .get_membership_members()
                })
                .collect::<Result<Vec<_>, _>>()?;
            groups.push((group.group_id, members));
        }
        Ok(groups)
    }

    pub fn plan_membership_members_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "membership_members")
    }

    pub fn timeout_now_on_node(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        from: NodeId,
        target: NodeId,
    ) -> Result<TimeoutNowResponse, RaftError> {
        self.node(group_id, node_id)?.timeout_now(from, target)
    }

    pub fn timeout_now_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        target: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(group_id, MatrixRaftMessage::timeout_now(from, target))
    }

    pub fn timeout_now_on_group_best_effort(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        target: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_message_to_group_best_effort(
            group_id,
            MatrixRaftMessage::timeout_now(from, target),
        )
    }

    pub fn timeout_now_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        target: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(group_ids, MatrixRaftMessage::timeout_now(from, target))
    }

    pub fn plan_timeout_now_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        target: NodeId,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(group_ids, MatrixRaftMessage::timeout_now(from, target))
    }

    pub fn timeout_now_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        target: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::timeout_now(from, target),
        )
    }

    pub fn timeout_now_callbacks_on_group<F, C>(
        &self,
        group_id: GroupId,
        from: NodeId,
        target: NodeId,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_timeout_now_for_groups([group_id], from, target)?;
        let group = plan
            .groups
            .into_iter()
            .next()
            .expect("single group timeout-now callback plan");
        let mut results = Vec::with_capacity(group.node_count);
        for key in group.route_keys {
            let callback = callback_for_key(key);
            let result = self
                .node(key.group_id, key.node_id)?
                .timeout_now_callback(from, target, callback, timeout_ms);
            results.push((key, result));
        }
        Ok(results)
    }

    pub fn timeout_now_callbacks_for_groups<F, C>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        target: NodeId,
        mut callback_for_key: F,
        timeout_ms: u64,
    ) -> Result<Vec<(GroupId, Vec<(MatrixRaftRouteKey, MatrixRaftAsyncResult)>)>, RaftError>
    where
        F: FnMut(MatrixRaftRouteKey) -> C,
        C: FnOnce(MatrixRaftAsyncResult),
    {
        let plan = self.plan_timeout_now_for_groups(group_ids, from, target)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let callback = callback_for_key(key);
                let result = self
                    .node(key.group_id, key.node_id)?
                    .timeout_now_callback(from, target, callback, timeout_ms);
                results.push((key, result));
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn plan_vote_request_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: VoteRequest,
        pre_vote: bool,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::vote(from, to, request, pre_vote),
        )
    }

    pub fn vote_request_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        request: VoteRequest,
        pre_vote: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(group_id, MatrixRaftMessage::vote(from, to, request, pre_vote))
    }

    pub fn vote_requests_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: VoteRequest,
        pre_vote: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(
            group_ids,
            MatrixRaftMessage::vote(from, to, request, pre_vote),
        )
    }

    pub fn vote_requests_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: VoteRequest,
        pre_vote: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::vote(from, to, request, pre_vote),
        )
    }

    pub fn plan_pre_vote_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(group_ids, MatrixRaftMessage::pre_vote(from, to))
    }

    pub fn pre_vote_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(group_id, MatrixRaftMessage::pre_vote(from, to))
    }

    pub fn pre_votes_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(group_ids, MatrixRaftMessage::pre_vote(from, to))
    }

    pub fn pre_votes_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::pre_vote(from, to),
        )
    }

    pub fn plan_vote_response_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: VoteResponse,
        pre_vote: bool,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::vote_response(from, to, response, pre_vote),
        )
    }

    pub fn vote_response_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        response: VoteResponse,
        pre_vote: bool,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(
            group_id,
            MatrixRaftMessage::vote_response(from, to, response, pre_vote),
        )
    }

    pub fn vote_responses_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: VoteResponse,
        pre_vote: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(
            group_ids,
            MatrixRaftMessage::vote_response(from, to, response, pre_vote),
        )
    }

    pub fn vote_responses_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: VoteResponse,
        pre_vote: bool,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::vote_response(from, to, response, pre_vote),
        )
    }

    pub fn plan_append_entries_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::append_entries(from, to, request),
        )
    }

    pub fn append_entries_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(group_id, MatrixRaftMessage::append_entries(from, to, request))
    }

    pub fn append_entries_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(
            group_ids,
            MatrixRaftMessage::append_entries(from, to, request),
        )
    }

    pub fn append_entries_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::append_entries(from, to, request),
        )
    }

    pub fn plan_append_entries_response_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::append_entries_response(from, to, response),
        )
    }

    pub fn append_entries_response_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(
            group_id,
            MatrixRaftMessage::append_entries_response(from, to, response),
        )
    }

    pub fn append_entries_responses_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(
            group_ids,
            MatrixRaftMessage::append_entries_response(from, to, response),
        )
    }

    pub fn append_entries_responses_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::append_entries_response(from, to, response),
        )
    }

    fn heartbeat_merge_plan_from_group_messages(
        &self,
        group_messages: impl IntoIterator<Item = (GroupId, NodeId, NodeId, MatrixRaftMessage)>,
    ) -> Result<MatrixRaftHeartbeatMergePlan, RaftError> {
        let mut groups = Vec::new();
        let mut batches = BTreeMap::<String, MatrixRaftHeartbeatMergeBatchPlan>::new();
        let mut message_type = None;
        for (group_id, from, to, message) in group_messages {
            if let Some(expected) = message_type {
                if expected != message.message_type {
                    return Err(RaftError::InvalidRequest(
                        "matrixraft heartbeat merge requires one message type".to_string(),
                    ));
                }
            } else {
                message_type = Some(message.message_type);
            }
            let source_key = MatrixRaftRouteKey::new(group_id, from);
            let route_key = MatrixRaftRouteKey::new(group_id, to);
            let raft_addr = self
                .nodes
                .get(&source_key)
                .ok_or(RaftError::NodeNotFound(from))?
                .resolve_address(to)?
                .raft_addr;
            if !self.nodes.contains_key(&route_key) {
                return Err(RaftError::NodeNotFound(to));
            }
            batches
                .entry(raft_addr.clone())
                .and_modify(|batch| {
                    batch.message_count += 1;
                    batch.route_keys.push(route_key);
                    batch.messages.push(message.clone());
                })
                .or_insert_with(|| MatrixRaftHeartbeatMergeBatchPlan {
                    raft_addr: raft_addr.clone(),
                    message_count: 1,
                    route_keys: vec![route_key],
                    messages: vec![message.clone()],
                });
            groups.push(MatrixRaftHeartbeatMergeGroupPlan {
                group_id,
                from,
                to,
                route_key,
                raft_addr,
                message_type: message.message_type,
                message,
            });
        }
        let batches = batches.into_values().collect::<Vec<_>>();
        let group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let route_keys = groups.iter().map(|group| group.route_key).collect::<Vec<_>>();
        Ok(MatrixRaftHeartbeatMergePlan {
            group_count: groups.len(),
            group_ids,
            message_count: groups.len(),
            batch_count: batches.len(),
            route_keys,
            message_type: message_type.unwrap_or(MatrixRaftMessageType::AppendEntriesRequest),
            groups,
            batches,
        })
    }

    pub fn plan_merged_heartbeat_requests_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
    ) -> Result<MatrixRaftHeartbeatMergePlan, RaftError> {
        if !request.entries.is_empty() {
            return Err(RaftError::InvalidRequest(
                "matrixraft merged heartbeat request requires empty append entries".to_string(),
            ));
        }
        self.heartbeat_merge_plan_from_group_messages(group_ids.into_iter().map(|group_id| {
            (
                group_id,
                from,
                to,
                MatrixRaftMessage::append_entries(from, to, request),
            )
        }))
    }

    pub fn merged_heartbeat_requests_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_merged_heartbeat_requests_for_groups([group_id], from, to, request)?;
        plan.groups
            .into_iter()
            .map(|group| {
                self.route_message(group.route_key.group_id, group.route_key.node_id, group.message)
            })
            .collect()
    }

    pub fn merged_heartbeat_requests_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_merged_heartbeat_requests_for_groups(group_ids, from, to, request)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let result = self.route_message(group.route_key.group_id, group.route_key.node_id, group.message)?;
            groups.push((group.group_id, vec![result]));
        }
        Ok(groups)
    }

    pub fn merged_heartbeat_requests_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let plan = self.plan_merged_heartbeat_requests_for_groups(group_ids, from, to, request)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let routed =
                MatrixRaftRoutedMessage::new(group.route_key.group_id, group.route_key.node_id, group.message);
            groups.push((group.group_id, self.route_message_batch_best_effort(vec![routed])));
        }
        Ok(groups)
    }

    pub fn plan_merged_heartbeat_responses_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
    ) -> Result<MatrixRaftHeartbeatMergePlan, RaftError> {
        self.heartbeat_merge_plan_from_group_messages(group_ids.into_iter().map(|group_id| {
            (
                group_id,
                from,
                to,
                MatrixRaftMessage::append_entries_response(from, to, response),
            )
        }))
    }

    pub fn merged_heartbeat_response_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_merged_heartbeat_responses_for_groups([group_id], from, to, response)?;
        plan.groups
            .into_iter()
            .map(|group| {
                self.route_message(group.route_key.group_id, group.route_key.node_id, group.message)
            })
            .collect()
    }

    pub fn merged_heartbeat_responses_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_merged_heartbeat_responses_for_groups(group_ids, from, to, response)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let result = self.route_message(group.route_key.group_id, group.route_key.node_id, group.message)?;
            groups.push((group.group_id, vec![result]));
        }
        Ok(groups)
    }

    pub fn merged_heartbeat_responses_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let plan = self.plan_merged_heartbeat_responses_for_groups(group_ids, from, to, response)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let routed =
                MatrixRaftRoutedMessage::new(group.route_key.group_id, group.route_key.node_id, group.message);
            groups.push((group.group_id, self.route_message_batch_best_effort(vec![routed])));
        }
        Ok(groups)
    }

    pub fn plan_lease_request_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
        lease_request: MatrixRaftLeaseRequest,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::append_entries_lease_request(from, to, request, lease_request),
        )
    }

    pub fn lease_request_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
        lease_request: MatrixRaftLeaseRequest,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(
            group_id,
            MatrixRaftMessage::append_entries_lease_request(from, to, request, lease_request),
        )
    }

    pub fn lease_requests_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
        lease_request: MatrixRaftLeaseRequest,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(
            group_ids,
            MatrixRaftMessage::append_entries_lease_request(from, to, request, lease_request),
        )
    }

    pub fn lease_requests_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: &AppendEntriesRequest,
        lease_request: MatrixRaftLeaseRequest,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::append_entries_lease_request(from, to, request, lease_request),
        )
    }

    pub fn plan_lease_response_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
        lease_response: MatrixRaftLeaseResponse,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::append_entries_lease_response(from, to, response, lease_response),
        )
    }

    pub fn lease_response_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
        lease_response: MatrixRaftLeaseResponse,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(
            group_id,
            MatrixRaftMessage::append_entries_lease_response(from, to, response, lease_response),
        )
    }

    pub fn lease_responses_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
        lease_response: MatrixRaftLeaseResponse,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(
            group_ids,
            MatrixRaftMessage::append_entries_lease_response(from, to, response, lease_response),
        )
    }

    pub fn lease_responses_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: &AppendEntriesResponse,
        lease_response: MatrixRaftLeaseResponse,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::append_entries_lease_response(from, to, response, lease_response),
        )
    }

    pub fn plan_install_snapshot_request_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: InstallSnapshotRequest,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::install_snapshot(from, to, request),
        )
    }

    pub fn install_snapshot_request_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        request: InstallSnapshotRequest,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(group_id, MatrixRaftMessage::install_snapshot(from, to, request))
    }

    pub fn install_snapshot_requests_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: InstallSnapshotRequest,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(
            group_ids,
            MatrixRaftMessage::install_snapshot(from, to, request),
        )
    }

    pub fn install_snapshot_requests_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: InstallSnapshotRequest,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::install_snapshot(from, to, request),
        )
    }

    pub fn plan_install_snapshot_response_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: InstallSnapshotResponse,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::install_snapshot_response(from, to, response),
        )
    }

    pub fn install_snapshot_response_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        response: InstallSnapshotResponse,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(
            group_id,
            MatrixRaftMessage::install_snapshot_response(from, to, response),
        )
    }

    pub fn install_snapshot_responses_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: InstallSnapshotResponse,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(
            group_ids,
            MatrixRaftMessage::install_snapshot_response(from, to, response),
        )
    }

    pub fn install_snapshot_responses_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        response: InstallSnapshotResponse,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::install_snapshot_response(from, to, response),
        )
    }

    pub fn plan_read_index_request_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: ReadIndexRequest,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(group_ids, MatrixRaftMessage::read_index(from, to, request))
    }

    pub fn read_index_request_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        request: ReadIndexRequest,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(group_id, MatrixRaftMessage::read_index(from, to, request))
    }

    pub fn read_index_requests_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: ReadIndexRequest,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(group_ids, MatrixRaftMessage::read_index(from, to, request))
    }

    pub fn read_index_requests_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        request: ReadIndexRequest,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::read_index(from, to, request),
        )
    }

    pub fn plan_propose_request_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        propose: MatrixRaftPropose,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let mut groups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let route_keys = self.group_route_keys(group_id)?;
            let leader_id = route_keys
                .iter()
                .filter_map(|key| self.nodes.get(key).and_then(|node| node.leader().ok()).flatten())
                .next()
                .ok_or_else(|| RaftError::InvalidRequest(format!("group {group_id} has no leader")))?;
            let key = MatrixRaftRouteKey::new(group_id, leader_id);
            if !self.nodes.contains_key(&key) {
                return Err(RaftError::NodeNotFound(leader_id));
            }
            groups.push(MatrixRaftMessageFanoutGroupPlan {
                group_id,
                node_ids: vec![leader_id],
                route_keys: vec![key],
                node_count: 1,
                message_type: MatrixRaftMessageType::Propose,
                message: MatrixRaftMessage::propose(from, leader_id, propose.clone()),
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftMessageFanoutPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            message_type: MatrixRaftMessageType::Propose,
            groups,
        })
    }

    pub fn propose_request_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        propose: MatrixRaftPropose,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_propose_request_for_groups([group_id], from, propose)?;
        let group = plan
            .groups
            .into_iter()
            .next()
            .expect("single group proposal fanout plan");
        let mut results = Vec::with_capacity(group.node_count);
        for key in group.route_keys {
            results.push(self.route_message(key.group_id, key.node_id, group.message.clone())?);
        }
        Ok(results)
    }

    pub fn propose_requests_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        propose: MatrixRaftPropose,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_propose_request_for_groups(group_ids, from, propose)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                results.push(self.route_message(key.group_id, key.node_id, group.message.clone())?);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn propose_requests_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        propose: MatrixRaftPropose,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let plan = self.plan_propose_request_for_groups(group_ids, from, propose)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let messages = group
                .route_keys
                .into_iter()
                .map(|key| MatrixRaftRoutedMessage::new(key.group_id, key.node_id, group.message.clone()))
                .collect();
            groups.push((group.group_id, self.route_message_batch_best_effort(messages)));
        }
        Ok(groups)
    }

    pub fn network_error_on_node(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
        from: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_message(
            group_id,
            node_id,
            MatrixRaftMessage::network_error(from, peer_id),
        )
    }

    pub fn network_error_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(group_id, MatrixRaftMessage::network_error(from, peer_id))
    }

    pub fn network_error_on_group_best_effort(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        peer_id: NodeId,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_message_to_group_best_effort(
            group_id,
            MatrixRaftMessage::network_error(from, peer_id),
        )
    }

    pub fn network_error_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(
            group_ids,
            MatrixRaftMessage::network_error(from, peer_id),
        )
    }

    pub fn plan_network_error_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        peer_id: NodeId,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(group_ids, MatrixRaftMessage::network_error(from, peer_id))
    }

    pub fn network_error_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        peer_id: NodeId,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::network_error(from, peer_id),
        )
    }

    pub fn snapshot_progress_on_node(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
        from: NodeId,
        to: NodeId,
        progress: MatrixRaftSnapshotProgress,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        self.route_message(
            group_id,
            node_id,
            MatrixRaftMessage::snapshot_progress(from, to, progress),
        )
    }

    pub fn snapshot_progress_on_group(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        progress: MatrixRaftSnapshotProgress,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        self.route_message_to_group(group_id, MatrixRaftMessage::snapshot_progress(from, to, progress))
    }

    pub fn snapshot_progress_on_group_best_effort(
        &mut self,
        group_id: GroupId,
        from: NodeId,
        to: NodeId,
        progress: MatrixRaftSnapshotProgress,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        self.route_message_to_group_best_effort(
            group_id,
            MatrixRaftMessage::snapshot_progress(from, to, progress),
        )
    }

    pub fn snapshot_progress_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        progress: MatrixRaftSnapshotProgress,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped(
            group_ids,
            MatrixRaftMessage::snapshot_progress(from, to, progress),
        )
    }

    pub fn plan_snapshot_progress_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        progress: MatrixRaftSnapshotProgress,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        self.plan_route_message_to_groups(
            group_ids,
            MatrixRaftMessage::snapshot_progress(from, to, progress),
        )
    }

    pub fn snapshot_progress_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        from: NodeId,
        to: NodeId,
        progress: MatrixRaftSnapshotProgress,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        self.route_message_to_groups_grouped_best_effort(
            group_ids,
            MatrixRaftMessage::snapshot_progress(from, to, progress),
        )
    }

    pub fn sync_fsm_runtime_on_node<F>(
        &self,
        group_id: GroupId,
        node_id: NodeId,
        binding: &mut MatrixRaftFsmRuntimeBinding<F>,
    ) -> Result<MatrixRaftFsmRuntimeHookReport, RaftError>
    where
        F: MatrixRaftFsm,
    {
        self.node(group_id, node_id)?.sync_fsm_runtime(binding)
    }

    pub fn sync_fsm_runtimes_on_group<F>(
        &self,
        group_id: GroupId,
        bindings: &mut BTreeMap<MatrixRaftRouteKey, MatrixRaftFsmRuntimeBinding<F>>,
    ) -> Result<Vec<(MatrixRaftRouteKey, MatrixRaftFsmRuntimeHookReport)>, RaftError>
    where
        F: MatrixRaftFsm,
    {
        let keys = self.group_route_keys(group_id)?;
        let mut reports = Vec::with_capacity(keys.len());
        for key in keys {
            let binding = bindings.get_mut(&key).ok_or_else(|| {
                RaftError::InvalidRequest(format!(
                    "matrixraft fsm runtime binding missing for node {} in group {}",
                    key.node_id, key.group_id
                ))
            })?;
            reports.push((key, self.node(key.group_id, key.node_id)?.sync_fsm_runtime(binding)?));
        }
        Ok(reports)
    }

    fn sync_fsm_runtime_on_key_best_effort<F>(
        &self,
        key: MatrixRaftRouteKey,
        bindings: &mut BTreeMap<MatrixRaftRouteKey, MatrixRaftFsmRuntimeBinding<F>>,
    ) -> MatrixRaftFsmRuntimeSyncNodeResult
    where
        F: MatrixRaftFsm,
    {
        let Some(binding) = bindings.get_mut(&key) else {
            return MatrixRaftFsmRuntimeSyncNodeResult::error(
                key,
                RaftError::InvalidRequest(format!(
                    "matrixraft fsm runtime binding missing for node {} in group {}",
                    key.node_id, key.group_id
                )),
            );
        };
        match self
            .node(key.group_id, key.node_id)
            .and_then(|node| node.sync_fsm_runtime(binding))
        {
            Ok(report) => MatrixRaftFsmRuntimeSyncNodeResult::ok(key, report),
            Err(error) => MatrixRaftFsmRuntimeSyncNodeResult::error(key, error),
        }
    }

    pub fn sync_fsm_runtimes_on_group_best_effort<F>(
        &self,
        group_id: GroupId,
        bindings: &mut BTreeMap<MatrixRaftRouteKey, MatrixRaftFsmRuntimeBinding<F>>,
    ) -> Result<MatrixRaftFsmRuntimeSyncGroupResult, RaftError>
    where
        F: MatrixRaftFsm,
    {
        let plan = self.plan_sync_fsm_runtimes_for_groups([group_id])?;
        Ok(self
            .sync_fsm_runtime_plan_best_effort(plan, bindings)
            .into_iter()
            .next()
            .expect("single group fsm runtime sync result"))
    }

    pub fn sync_fsm_runtimes_for_groups<F>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        bindings: &mut BTreeMap<MatrixRaftRouteKey, MatrixRaftFsmRuntimeBinding<F>>,
    ) -> Result<
        Vec<(
            GroupId,
            Vec<(MatrixRaftRouteKey, MatrixRaftFsmRuntimeHookReport)>,
        )>,
        RaftError,
    >
    where
        F: MatrixRaftFsm,
    {
        let plan = self.plan_sync_fsm_runtimes_for_groups(group_ids)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut reports = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let binding = bindings.get_mut(&key).ok_or_else(|| {
                    RaftError::InvalidRequest(format!(
                        "matrixraft fsm runtime binding missing for node {} in group {}",
                        key.node_id, key.group_id
                    ))
                })?;
                reports.push((
                    key,
                    self.node(key.group_id, key.node_id)?
                        .sync_fsm_runtime(binding)?,
                ));
            }
            groups.push((group.group_id, reports));
        }
        Ok(groups)
    }

    pub fn sync_fsm_runtimes_for_groups_best_effort<F>(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        bindings: &mut BTreeMap<MatrixRaftRouteKey, MatrixRaftFsmRuntimeBinding<F>>,
    ) -> Result<Vec<MatrixRaftFsmRuntimeSyncGroupResult>, RaftError>
    where
        F: MatrixRaftFsm,
    {
        let plan = self.plan_sync_fsm_runtimes_for_groups(group_ids)?;
        Ok(self.sync_fsm_runtime_plan_best_effort(plan, bindings))
    }

    fn sync_fsm_runtime_plan_best_effort<F>(
        &self,
        plan: MatrixRaftQueryFanoutPlan,
        bindings: &mut BTreeMap<MatrixRaftRouteKey, MatrixRaftFsmRuntimeBinding<F>>,
    ) -> Vec<MatrixRaftFsmRuntimeSyncGroupResult>
    where
        F: MatrixRaftFsm,
    {
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                results.push(self.sync_fsm_runtime_on_key_best_effort(key, bindings));
            }
            let ok_count = results.iter().filter(|result| result.is_ok()).count();
            let error_count = results.len().saturating_sub(ok_count);
            groups.push(MatrixRaftFsmRuntimeSyncGroupResult {
                group_id: group.group_id,
                node_count: group.node_count,
                ok_count,
                error_count,
                results,
            });
        }
        groups
    }

    pub fn plan_sync_fsm_runtimes_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftQueryFanoutPlan, RaftError> {
        self.plan_query_for_groups(group_ids, "sync_fsm_runtimes")
    }

    pub fn start_all(&mut self, start_index: LogIndex) -> Result<(), RaftError> {
        for node in self.nodes.values_mut() {
            node.start(start_index)?;
        }
        Ok(())
    }

    pub fn start_group(
        &mut self,
        group_id: GroupId,
        start_index: LogIndex,
    ) -> Result<usize, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        for key in &keys {
            self.nodes
                .get_mut(key)
                .ok_or(RaftError::NodeNotFound(key.node_id))?
                .start(start_index)?;
        }
        Ok(keys.len())
    }

    pub fn plan_start_group(
        &self,
        group_id: GroupId,
        start_index: LogIndex,
    ) -> Result<MatrixRaftLifecycleGroupPlan, RaftError> {
        Ok(self
            .lifecycle_batch_plan_from_groups(
                MatrixRaftLifecycleAction::Start,
                &[group_id],
                Some(start_index),
                None,
            )?
            .groups
            .into_iter()
            .next()
            .expect("single group lifecycle plan"))
    }

    pub fn plan_start_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        start_index: LogIndex,
    ) -> Result<MatrixRaftLifecycleBatchPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.lifecycle_batch_plan_from_groups(
            MatrixRaftLifecycleAction::Start,
            &group_ids,
            Some(start_index),
            None,
        )
    }

    fn execute_lifecycle_action_on_key_best_effort(
        &mut self,
        action: MatrixRaftLifecycleAction,
        key: MatrixRaftRouteKey,
        start_index: Option<LogIndex>,
        recover_fsm_from_snapshot: Option<bool>,
    ) -> MatrixRaftLifecycleNodeResult {
        let result = self
            .nodes
            .get_mut(&key)
            .ok_or(RaftError::NodeNotFound(key.node_id))
            .and_then(|node| match action {
                MatrixRaftLifecycleAction::Start => node.start(
                    start_index.expect("lifecycle start action requires start index"),
                ),
                MatrixRaftLifecycleAction::Stop => node.stop(),
                MatrixRaftLifecycleAction::Restart => node.restart(
                    recover_fsm_from_snapshot
                        .expect("lifecycle restart action requires recover flag"),
                ),
                MatrixRaftLifecycleAction::Shutdown => node.shutdown(),
            });
        match result {
            Ok(()) => MatrixRaftLifecycleNodeResult::ok(key, action),
            Err(error) => MatrixRaftLifecycleNodeResult::error(key, action, error),
        }
    }

    fn execute_lifecycle_plan_best_effort(
        &mut self,
        plan: MatrixRaftLifecycleBatchPlan,
    ) -> Vec<MatrixRaftLifecycleGroupResult> {
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                results.push(self.execute_lifecycle_action_on_key_best_effort(
                    group.action,
                    key,
                    group.start_index,
                    group.recover_fsm_from_snapshot,
                ));
            }
            let ok_count = results.iter().filter(|result| result.is_ok()).count();
            let error_count = results.len().saturating_sub(ok_count);
            groups.push(MatrixRaftLifecycleGroupResult {
                group_id: group.group_id,
                action: group.action,
                node_count: group.node_count,
                ok_count,
                error_count,
                results,
            });
        }
        groups
    }

    pub fn start_group_best_effort(
        &mut self,
        group_id: GroupId,
        start_index: LogIndex,
    ) -> Result<MatrixRaftLifecycleGroupResult, RaftError> {
        let plan = self.plan_start_group(group_id, start_index)?;
        Ok(self
            .execute_lifecycle_plan_best_effort(MatrixRaftLifecycleBatchPlan {
                action: plan.action,
                group_count: 1,
                group_ids: vec![group_id],
                node_count: plan.node_count,
                route_keys: plan.route_keys.clone(),
                groups: vec![plan],
                start_index: Some(start_index),
                recover_fsm_from_snapshot: None,
            })
            .into_iter()
            .next()
            .expect("single group lifecycle result"))
    }

    pub fn start_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        start_index: LogIndex,
    ) -> Result<Vec<MatrixRaftLifecycleGroupResult>, RaftError> {
        let plan = self.plan_start_groups(group_ids, start_index)?;
        Ok(self.execute_lifecycle_plan_best_effort(plan))
    }

    pub fn start_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        start_index: LogIndex,
    ) -> Result<Vec<(GroupId, usize)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_start_groups(group_ids, start_index)?;
        let mut counts = Vec::new();
        for group in plan.groups {
            for key in &group.route_keys {
                self.nodes
                    .get_mut(key)
                    .ok_or(RaftError::NodeNotFound(key.node_id))?
                    .start(start_index)?;
            }
            counts.push((group.group_id, group.node_count));
        }
        Ok(counts)
    }

    pub fn stop_all(&mut self) -> Result<(), RaftError> {
        for node in self.nodes.values_mut() {
            node.stop()?;
        }
        Ok(())
    }

    pub fn stop_group(&mut self, group_id: GroupId) -> Result<usize, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        for key in &keys {
            self.nodes
                .get_mut(key)
                .ok_or(RaftError::NodeNotFound(key.node_id))?
                .stop()?;
        }
        Ok(keys.len())
    }

    pub fn plan_stop_group(
        &self,
        group_id: GroupId,
    ) -> Result<MatrixRaftLifecycleGroupPlan, RaftError> {
        Ok(self
            .lifecycle_batch_plan_from_groups(
                MatrixRaftLifecycleAction::Stop,
                &[group_id],
                None,
                None,
            )?
            .groups
            .into_iter()
            .next()
            .expect("single group lifecycle plan"))
    }

    pub fn plan_stop_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftLifecycleBatchPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.lifecycle_batch_plan_from_groups(
            MatrixRaftLifecycleAction::Stop,
            &group_ids,
            None,
            None,
        )
    }

    pub fn stop_group_best_effort(
        &mut self,
        group_id: GroupId,
    ) -> Result<MatrixRaftLifecycleGroupResult, RaftError> {
        let plan = self.plan_stop_group(group_id)?;
        Ok(self
            .execute_lifecycle_plan_best_effort(MatrixRaftLifecycleBatchPlan {
                action: plan.action,
                group_count: 1,
                group_ids: vec![group_id],
                node_count: plan.node_count,
                route_keys: plan.route_keys.clone(),
                groups: vec![plan],
                start_index: None,
                recover_fsm_from_snapshot: None,
            })
            .into_iter()
            .next()
            .expect("single group lifecycle result"))
    }

    pub fn stop_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<MatrixRaftLifecycleGroupResult>, RaftError> {
        let plan = self.plan_stop_groups(group_ids)?;
        Ok(self.execute_lifecycle_plan_best_effort(plan))
    }

    pub fn stop_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, usize)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_stop_groups(group_ids)?;
        let mut counts = Vec::new();
        for group in plan.groups {
            for key in &group.route_keys {
                self.nodes
                    .get_mut(key)
                    .ok_or(RaftError::NodeNotFound(key.node_id))?
                    .stop()?;
            }
            counts.push((group.group_id, group.node_count));
        }
        Ok(counts)
    }

    pub fn restart_group(
        &mut self,
        group_id: GroupId,
        recover_fsm_from_snapshot: bool,
    ) -> Result<usize, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        for key in &keys {
            self.nodes
                .get_mut(key)
                .ok_or(RaftError::NodeNotFound(key.node_id))?
                .restart(recover_fsm_from_snapshot)?;
        }
        Ok(keys.len())
    }

    pub fn plan_restart_group(
        &self,
        group_id: GroupId,
        recover_fsm_from_snapshot: bool,
    ) -> Result<MatrixRaftLifecycleGroupPlan, RaftError> {
        Ok(self
            .lifecycle_batch_plan_from_groups(
                MatrixRaftLifecycleAction::Restart,
                &[group_id],
                None,
                Some(recover_fsm_from_snapshot),
            )?
            .groups
            .into_iter()
            .next()
            .expect("single group lifecycle plan"))
    }

    pub fn plan_restart_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        recover_fsm_from_snapshot: bool,
    ) -> Result<MatrixRaftLifecycleBatchPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.lifecycle_batch_plan_from_groups(
            MatrixRaftLifecycleAction::Restart,
            &group_ids,
            None,
            Some(recover_fsm_from_snapshot),
        )
    }

    pub fn restart_group_best_effort(
        &mut self,
        group_id: GroupId,
        recover_fsm_from_snapshot: bool,
    ) -> Result<MatrixRaftLifecycleGroupResult, RaftError> {
        let plan = self.plan_restart_group(group_id, recover_fsm_from_snapshot)?;
        Ok(self
            .execute_lifecycle_plan_best_effort(MatrixRaftLifecycleBatchPlan {
                action: plan.action,
                group_count: 1,
                group_ids: vec![group_id],
                node_count: plan.node_count,
                route_keys: plan.route_keys.clone(),
                groups: vec![plan],
                start_index: None,
                recover_fsm_from_snapshot: Some(recover_fsm_from_snapshot),
            })
            .into_iter()
            .next()
            .expect("single group lifecycle result"))
    }

    pub fn restart_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        recover_fsm_from_snapshot: bool,
    ) -> Result<Vec<MatrixRaftLifecycleGroupResult>, RaftError> {
        let plan = self.plan_restart_groups(group_ids, recover_fsm_from_snapshot)?;
        Ok(self.execute_lifecycle_plan_best_effort(plan))
    }

    pub fn restart_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        recover_fsm_from_snapshot: bool,
    ) -> Result<Vec<(GroupId, usize)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_restart_groups(group_ids, recover_fsm_from_snapshot)?;
        let mut counts = Vec::new();
        for group in plan.groups {
            for key in &group.route_keys {
                self.nodes
                    .get_mut(key)
                    .ok_or(RaftError::NodeNotFound(key.node_id))?
                    .restart(recover_fsm_from_snapshot)?;
            }
            counts.push((group.group_id, group.node_count));
        }
        Ok(counts)
    }

    pub fn shutdown_all(&mut self) -> Result<(), RaftError> {
        for node in self.nodes.values_mut() {
            node.shutdown()?;
        }
        Ok(())
    }

    pub fn shutdown_group(&mut self, group_id: GroupId) -> Result<usize, RaftError> {
        let keys = self.group_route_keys(group_id)?;
        for key in &keys {
            self.nodes
                .get_mut(key)
                .ok_or(RaftError::NodeNotFound(key.node_id))?
                .shutdown()?;
        }
        Ok(keys.len())
    }

    pub fn plan_shutdown_group(
        &self,
        group_id: GroupId,
    ) -> Result<MatrixRaftLifecycleGroupPlan, RaftError> {
        Ok(self
            .lifecycle_batch_plan_from_groups(
                MatrixRaftLifecycleAction::Shutdown,
                &[group_id],
                None,
                None,
            )?
            .groups
            .into_iter()
            .next()
            .expect("single group lifecycle plan"))
    }

    pub fn plan_shutdown_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<MatrixRaftLifecycleBatchPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.lifecycle_batch_plan_from_groups(
            MatrixRaftLifecycleAction::Shutdown,
            &group_ids,
            None,
            None,
        )
    }

    pub fn shutdown_group_best_effort(
        &mut self,
        group_id: GroupId,
    ) -> Result<MatrixRaftLifecycleGroupResult, RaftError> {
        let plan = self.plan_shutdown_group(group_id)?;
        Ok(self
            .execute_lifecycle_plan_best_effort(MatrixRaftLifecycleBatchPlan {
                action: plan.action,
                group_count: 1,
                group_ids: vec![group_id],
                node_count: plan.node_count,
                route_keys: plan.route_keys.clone(),
                groups: vec![plan],
                start_index: None,
                recover_fsm_from_snapshot: None,
            })
            .into_iter()
            .next()
            .expect("single group lifecycle result"))
    }

    pub fn shutdown_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<MatrixRaftLifecycleGroupResult>, RaftError> {
        let plan = self.plan_shutdown_groups(group_ids)?;
        Ok(self.execute_lifecycle_plan_best_effort(plan))
    }

    pub fn shutdown_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Result<Vec<(GroupId, usize)>, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        let plan = self.plan_shutdown_groups(group_ids)?;
        let mut counts = Vec::new();
        for group in plan.groups {
            for key in &group.route_keys {
                self.nodes
                    .get_mut(key)
                    .ok_or(RaftError::NodeNotFound(key.node_id))?
                    .shutdown()?;
            }
            counts.push((group.group_id, group.node_count));
        }
        Ok(counts)
    }

    pub fn publish_snapshot_route(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
        snapshot: MatrixRaftSnapshotDesc,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        let key = MatrixRaftRouteKey::new(group_id, node_id);
        self.ensure_node(key)?;
        self.snapshot_routes.insert(key, snapshot.clone());
        Ok(MatrixRaftRouteResult {
            key,
            message_type: MatrixRaftMessageType::Snapshot,
            kind: MatrixRaftRouteResultKind::SnapshotRegistered,
            handled: true,
            detail: "snapshot route registered".to_string(),
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
            snapshot: Some(snapshot),
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
        })
    }

    pub fn snapshot_route(
        &self,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Option<&MatrixRaftSnapshotDesc> {
        self.snapshot_routes
            .get(&MatrixRaftRouteKey::new(group_id, node_id))
    }

    pub fn snapshot_route_count(&self) -> usize {
        self.snapshot_routes.len()
    }

    fn snapshot_route_routed_message(
        key: MatrixRaftRouteKey,
        snapshot: MatrixRaftSnapshotDesc,
    ) -> MatrixRaftRoutedMessage {
        MatrixRaftRoutedMessage::new(
            key.group_id,
            key.node_id,
            MatrixRaftMessage {
                message_type: MatrixRaftMessageType::Snapshot,
                from: Some(key.node_id),
                raft_addr: None,
                snapshot_addr: None,
                to: Some(key.node_id),
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
                snapshot: Some(snapshot),
                snapshot_progress: None,
                require_snapshot: None,
                to_conf_state: MatrixRaftConfState::default(),
                auto_promote: false,
                lease_request: None,
                lease_response: None,
                bytes_size: 0,
                command: None,
            },
        )
    }

    fn snapshot_finish_routed_message(
        key: MatrixRaftRouteKey,
        finish: MatrixRaftOldSnapshotFinish,
    ) -> MatrixRaftRoutedMessage {
        MatrixRaftRoutedMessage::new(
            key.group_id,
            key.node_id,
            MatrixRaftMessage {
                message_type: MatrixRaftMessageType::SnapshotFinish,
                from: Some(key.node_id),
                raft_addr: None,
                snapshot_addr: None,
                to: Some(key.node_id),
                term: None,
                committed_index: Some(finish.snapshot_index),
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
                old_snapshot_finish: Some(finish),
                timestamp: None,
                snapshot_state: None,
                snapshot: None,
                snapshot_progress: None,
                require_snapshot: None,
                to_conf_state: MatrixRaftConfState::default(),
                auto_promote: false,
                lease_request: None,
                lease_response: None,
                bytes_size: 0,
                command: None,
            },
        )
    }

    pub fn publish_snapshot_route_on_group(
        &mut self,
        group_id: GroupId,
        snapshot: MatrixRaftSnapshotDesc,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_publish_snapshot_route_on_group(group_id, snapshot)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            results.push(self.publish_snapshot_route(
                key.group_id,
                key.node_id,
                plan.snapshot.clone(),
            )?);
        }
        Ok(results)
    }

    pub fn publish_snapshot_route_on_group_best_effort(
        &mut self,
        group_id: GroupId,
        snapshot: MatrixRaftSnapshotDesc,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let plan = self.plan_publish_snapshot_route_on_group(group_id, snapshot)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            let routed = Self::snapshot_route_routed_message(key, plan.snapshot.clone());
            let routed_result =
                match self.publish_snapshot_route(key.group_id, key.node_id, plan.snapshot.clone())
                {
                    Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                    Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                };
            results.push(routed_result);
        }
        Ok(results)
    }

    pub fn plan_publish_snapshot_route_on_group(
        &self,
        group_id: GroupId,
        snapshot: MatrixRaftSnapshotDesc,
    ) -> Result<MatrixRaftSnapshotPublishGroupPlan, RaftError> {
        Ok(self
            .snapshot_publish_plan_from_groups(&[(group_id, snapshot)])?
            .groups
            .into_iter()
            .next()
            .expect("single group snapshot publish plan"))
    }

    pub fn plan_publish_snapshot_routes_for_groups(
        &self,
        group_snapshots: impl IntoIterator<Item = (GroupId, MatrixRaftSnapshotDesc)>,
    ) -> Result<MatrixRaftSnapshotPublishPlan, RaftError> {
        let group_snapshots = group_snapshots.into_iter().collect::<Vec<_>>();
        self.snapshot_publish_plan_from_groups(&group_snapshots)
    }

    pub fn publish_snapshot_routes_for_groups(
        &mut self,
        group_snapshots: impl IntoIterator<Item = (GroupId, MatrixRaftSnapshotDesc)>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_publish_snapshot_routes_for_groups(group_snapshots)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                results.push(self.publish_snapshot_route(
                    key.group_id,
                    key.node_id,
                    group.snapshot.clone(),
                )?);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn publish_snapshot_routes_for_groups_best_effort(
        &mut self,
        group_snapshots: impl IntoIterator<Item = (GroupId, MatrixRaftSnapshotDesc)>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let plan = self.plan_publish_snapshot_routes_for_groups(group_snapshots)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let routed = Self::snapshot_route_routed_message(key, group.snapshot.clone());
                let routed_result = match self.publish_snapshot_route(
                    key.group_id,
                    key.node_id,
                    group.snapshot.clone(),
                ) {
                    Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                    Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                };
                results.push(routed_result);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn finish_snapshot_route(
        &mut self,
        group_id: GroupId,
        node_id: NodeId,
        finish: MatrixRaftOldSnapshotFinish,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        let key = MatrixRaftRouteKey::new(group_id, node_id);
        self.ensure_node(key)?;
        let removed = self.snapshot_routes.remove(&key);
        Ok(MatrixRaftRouteResult {
            key,
            message_type: MatrixRaftMessageType::SnapshotFinish,
            kind: MatrixRaftRouteResultKind::SnapshotFinished,
            handled: true,
            detail: format!(
                "snapshot route finished: {:?} at index {}{}",
                finish.finish_state,
                finish.snapshot_index,
                if removed.is_some() {
                    ""
                } else {
                    " (no active route)"
                }
            ),
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
            snapshot: removed,
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
        })
    }

    pub fn finish_snapshot_route_on_group(
        &mut self,
        group_id: GroupId,
        finish: MatrixRaftOldSnapshotFinish,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_finish_snapshot_route_on_group(group_id, finish)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            results.push(self.finish_snapshot_route(
                key.group_id,
                key.node_id,
                plan.finish.clone(),
            )?);
        }
        Ok(results)
    }

    pub fn finish_snapshot_route_on_group_best_effort(
        &mut self,
        group_id: GroupId,
        finish: MatrixRaftOldSnapshotFinish,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let plan = self.plan_finish_snapshot_route_on_group(group_id, finish)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            let routed = Self::snapshot_finish_routed_message(key, plan.finish.clone());
            let routed_result =
                match self.finish_snapshot_route(key.group_id, key.node_id, plan.finish.clone()) {
                    Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                    Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                };
            results.push(routed_result);
        }
        Ok(results)
    }

    pub fn plan_finish_snapshot_route_on_group(
        &self,
        group_id: GroupId,
        finish: MatrixRaftOldSnapshotFinish,
    ) -> Result<MatrixRaftSnapshotFinishGroupPlan, RaftError> {
        Ok(self
            .snapshot_finish_plan_from_groups(&[group_id], finish)?
            .groups
            .into_iter()
            .next()
            .expect("single group snapshot finish plan"))
    }

    pub fn plan_finish_snapshot_routes_for_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        finish: MatrixRaftOldSnapshotFinish,
    ) -> Result<MatrixRaftSnapshotFinishPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.snapshot_finish_plan_from_groups(&group_ids, finish)
    }

    pub fn finish_snapshot_routes_for_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        finish: MatrixRaftOldSnapshotFinish,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_finish_snapshot_routes_for_groups(group_ids, finish)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                results.push(self.finish_snapshot_route(
                    key.group_id,
                    key.node_id,
                    group.finish.clone(),
                )?);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn finish_snapshot_routes_for_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        finish: MatrixRaftOldSnapshotFinish,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let plan = self.plan_finish_snapshot_routes_for_groups(group_ids, finish)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let routed = Self::snapshot_finish_routed_message(key, group.finish.clone());
                let routed_result =
                    match self.finish_snapshot_route(key.group_id, key.node_id, group.finish.clone())
                    {
                        Ok(result) => {
                            MatrixRaftBatchRouteResult::from_routed_result(&routed, result)
                        }
                        Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                    };
                results.push(routed_result);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_message(
        &mut self,
        group_id: GroupId,
        runtime_node_id: NodeId,
        message: MatrixRaftMessage,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        let key = MatrixRaftRouteKey::new(group_id, runtime_node_id);
        if message.message_type == MatrixRaftMessageType::ConfigChange {
            let change = message.config_change.ok_or_else(|| {
                RaftError::InvalidRequest("config-change message missing payload".to_string())
            })?;
            return self.route_config_change(key, change);
        }
        if message.message_type == MatrixRaftMessageType::MembershipOperation {
            let operation = message.membership_operation.ok_or_else(|| {
                RaftError::InvalidRequest("membership-operation message missing payload".to_string())
            })?;
            let node = self
                .nodes
                .get_mut(&key)
                .ok_or(RaftError::NodeNotFound(runtime_node_id))?;
            let report = node.execute_membership_operation(operation)?;
            let mut result = MatrixRaftRouteResult::delivered(
                key,
                MatrixRaftMessageType::MembershipOperation,
                "membership operation delivered",
            );
            result.membership = Some(report);
            return Ok(result);
        }
        if message.message_type == MatrixRaftMessageType::PromotePeer {
            let peer_id = message.to.ok_or_else(|| {
                RaftError::InvalidRequest("promote message missing peer".to_string())
            })?;
            let node = self
                .nodes
                .get_mut(&key)
                .ok_or(RaftError::NodeNotFound(runtime_node_id))?;
            let report = node.promote_after_catch_up(peer_id)?;
            let mut result = MatrixRaftRouteResult::delivered(
                key,
                MatrixRaftMessageType::PromotePeer,
                "promote delivered",
            );
            result.promote = Some(report);
            return Ok(result);
        }
        if message.message_type == MatrixRaftMessageType::AutoPromoteLearner {
            let learner_id = message.to.ok_or_else(|| {
                RaftError::InvalidRequest("auto-promote message missing learner".to_string())
            })?;
            let node = self
                .nodes
                .get_mut(&key)
                .ok_or(RaftError::NodeNotFound(runtime_node_id))?;
            let report = node.auto_promote_learner(learner_id)?;
            let mut result = MatrixRaftRouteResult::delivered(
                key,
                MatrixRaftMessageType::AutoPromoteLearner,
                "auto-promote delivered",
            );
            result.auto_promote = Some(report);
            return Ok(result);
        }
        let node = self
            .nodes
            .get(&key)
            .ok_or(RaftError::NodeNotFound(runtime_node_id))?;
        match message.message_type {
            MatrixRaftMessageType::Propose => {
                let propose = message.propose.ok_or_else(|| {
                    RaftError::InvalidRequest("propose message missing payload".to_string())
                })?;
                let log_id = node.runtime.propose_with_options(
                    propose.data,
                    ProposeOptions {
                        expected_term: message.term,
                        is_command: propose.is_command,
                        is_membership_change: false,
                    },
                )?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::Propose,
                    "propose delivered",
                );
                result.proposed_log_id = Some(log_id);
                Ok(result)
            }
            MatrixRaftMessageType::ReadIndexRequest => {
                let mut request = message.read_index_request.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "read-index message missing request payload".to_string(),
                    )
                })?;
                request.group_id = group_id;
                request.requester_id = message.from.unwrap_or(runtime_node_id);
                let response = node.runtime.read_index_request(request)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::ReadIndexRequest,
                    "read-index delivered",
                );
                result.read_index_response = Some(response);
                Ok(result)
            }
            MatrixRaftMessageType::CatchUpPeer => {
                let peer_id = message.to.ok_or_else(|| {
                    RaftError::InvalidRequest("catch-up message missing peer".to_string())
                })?;
                let report = node.catch_up_peer(peer_id)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::CatchUpPeer,
                    "catch-up delivered",
                );
                result.catch_up = Some(report);
                Ok(result)
            }
            MatrixRaftMessageType::AppendEntriesRequest => {
                let target = message.to.ok_or_else(|| {
                    RaftError::InvalidRequest("append-entries message missing target".to_string())
                })?;
                let request = message.append_entries_request.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "append-entries message missing request payload".to_string(),
                    )
                })?;
                let response = node.runtime.append_entries_to(
                    target,
                    AppendEntriesRequest {
                        group_id,
                        term: message.term.unwrap_or_default(),
                        leader_id: message.from.unwrap_or(runtime_node_id),
                        prev_log_id: (request.prev_term != 0 || request.prev_index != 0)
                            .then_some(LogId {
                                term: request.prev_term,
                                index: request.prev_index,
                            }),
                        entries: request
                            .entries
                            .iter()
                            .map(MatrixRaftEntry::to_log_entry)
                            .collect(),
                        leader_commit: message.committed_index.unwrap_or_default(),
                        lease_epoch: message
                            .lease_request
                            .as_ref()
                            .map(|lease| lease.epoch_id.max(0) as u64)
                            .unwrap_or_default(),
                    },
                )?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AppendEntriesRequest,
                    "append-entries delivered",
                );
                result.append_entries_response = Some(MatrixRaftAppendEntriesResponse::from(&response));
                Ok(result)
            }
            MatrixRaftMessageType::AppendEntriesResponse => {
                let peer_id = message.from.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "append-entries response missing source peer".to_string(),
                    )
                })?;
                let local_node_id = message.to.unwrap_or(runtime_node_id);
                let response = message.append_entries_response.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "append-entries response missing payload".to_string(),
                    )
                })?;
                let step = node.runtime.step(Message::AppendEntriesResponse {
                    local_node_id,
                    peer_id,
                    response: AppendEntriesResponse {
                        term: message.term.unwrap_or_default(),
                        success: response.received,
                        match_index: response.matched_index.unwrap_or_default(),
                        rejection_hint: response.rejected_hint,
                        rejected_index: response.rejected_index,
                        require_snapshot: message
                            .require_snapshot
                            .map(|snapshot| snapshot.required_index),
                        snapshot_state: message.snapshot_state.unwrap_or(SnapshotState::None),
                        lease_confirmation_epoch: message
                            .lease_response
                            .as_ref()
                            .map(|lease| lease.max_met_epoch_id.max(0) as u64)
                            .unwrap_or_default(),
                        lease_duration_ms: message
                            .lease_response
                            .as_ref()
                            .map(|lease| lease.duration_ms)
                            .unwrap_or_default(),
                    },
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected append-entries response result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AppendEntriesResponse,
                    "append-entries response delivered",
                );
                result.append_entries_response = Some(response);
                Ok(result)
            }
            MatrixRaftMessageType::InstallSnapshotResponse => {
                let peer_id = message.from.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "install-snapshot response missing source peer".to_string(),
                    )
                })?;
                let local_node_id = message.to.unwrap_or(runtime_node_id);
                let response = message.install_snapshot_response.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "install-snapshot response missing payload".to_string(),
                    )
                })?;
                let step = node.runtime.step(Message::InstallSnapshotResponse {
                    local_node_id,
                    peer_id,
                    response: response.clone(),
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected install-snapshot response result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::InstallSnapshotResponse,
                    "install-snapshot response delivered",
                );
                result.install_snapshot_response = Some(response);
                Ok(result)
            }
            MatrixRaftMessageType::InstallSnapshotRequest => {
                let target = message.to.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "install-snapshot request missing target".to_string(),
                    )
                })?;
                let mut request = message.install_snapshot_request.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "install-snapshot request missing payload".to_string(),
                    )
                })?;
                request.group_id = group_id;
                request.leader_id = message.from.unwrap_or(request.leader_id);
                request.term = message.term.unwrap_or(request.term);
                let response = node.runtime.install_snapshot_chunk_to(target, request)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::InstallSnapshotRequest,
                    "install-snapshot request delivered",
                );
                result.install_snapshot_response = Some(response);
                Ok(result)
            }
            MatrixRaftMessageType::VoteRequest | MatrixRaftMessageType::PreVoteRequest => {
                let target = message.to.ok_or_else(|| {
                    RaftError::InvalidRequest("vote message missing target".to_string())
                })?;
                if target == runtime_node_id {
                    if let Some(role) = node.local_replica_role() {
                        if !role.participates_in_quorum() {
                            return Err(RaftError::InvalidRequest(format!(
                                "matrixraft {:?} node does not accept vote traffic",
                                role
                            )));
                        }
                    }
                }
                let mut request = message.vote_request.ok_or_else(|| {
                    RaftError::InvalidRequest("vote message missing request payload".to_string())
                })?;
                request.group_id = group_id;
                request.pre_vote = message.message_type == MatrixRaftMessageType::PreVoteRequest;
                let response = node.runtime.vote_to(target, request)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    message.message_type,
                    "vote delivered",
                );
                result.vote_response = Some(response);
                Ok(result)
            }
            MatrixRaftMessageType::PreVote => {
                let candidate_id = message.from.ok_or_else(|| {
                    RaftError::InvalidRequest("pre-vote message missing candidate".to_string())
                })?;
                let step = node
                    .runtime
                    .step(Message::PreVote { candidate_id })?;
                match step {
                    StepResult::PreVote(response) => {
                        let mut result = MatrixRaftRouteResult::delivered(
                            key,
                            MatrixRaftMessageType::PreVote,
                            "pre-vote delivered",
                        );
                        result.vote_response = Some(response);
                        Ok(result)
                    }
                    other => Err(RaftError::InvalidRequest(format!(
                        "unexpected pre-vote result: {other:?}"
                    ))),
                }
            }
            MatrixRaftMessageType::VoteResponse | MatrixRaftMessageType::PreVoteResponse => {
                let peer_id = message.from.ok_or_else(|| {
                    RaftError::InvalidRequest("vote response missing source peer".to_string())
                })?;
                let local_node_id = message.to.unwrap_or(runtime_node_id);
                let response = message.vote_response.ok_or_else(|| {
                    RaftError::InvalidRequest("vote response missing payload".to_string())
                })?;
                let pre_vote = message.message_type == MatrixRaftMessageType::PreVoteResponse;
                let step = node.runtime.step(Message::VoteResponse {
                    local_node_id,
                    peer_id: Some(peer_id),
                    response: response.clone(),
                    pre_vote,
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected vote response result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    message.message_type,
                    "vote response delivered",
                );
                result.vote_response = Some(response);
                Ok(result)
            }
            MatrixRaftMessageType::TimeoutNow => {
                let from = message.from.ok_or_else(|| {
                    RaftError::InvalidRequest("timeout-now message missing source".to_string())
                })?;
                let target = message.to.ok_or_else(|| {
                    RaftError::InvalidRequest("timeout-now message missing target".to_string())
                })?;
                let response = node.runtime.timeout_now(from, target)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::TimeoutNow,
                    "timeout-now delivered",
                );
                result.timeout_now_response = Some(response);
                Ok(result)
            }
            MatrixRaftMessageType::AdminCommand => {
                let command = message.command.ok_or_else(|| {
                    RaftError::InvalidRequest("admin message missing command".to_string())
                })?;
                self.route_admin_command(key, command)
            }
            MatrixRaftMessageType::NetworkError => {
                let peer_id = message.to.or(message.from).ok_or_else(|| {
                    RaftError::InvalidRequest("network-error message missing peer".to_string())
                })?;
                node.runtime.record_network_error_for(peer_id)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::NetworkError,
                    "network error recorded for peer",
                );
                result.snapshot_peer_report = Some(node.snapshot_peer_report(peer_id)?);
                Ok(result)
            }
            MatrixRaftMessageType::SnapshotProgress => {
                let peer_id = message.from.or(message.to).ok_or_else(|| {
                    RaftError::InvalidRequest("snapshot-progress message missing peer".to_string())
                })?;
                let progress = message.snapshot_progress.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "snapshot-progress message missing payload".to_string(),
                    )
                })?;
                let step = node.runtime.step(Message::SnapshotProgress {
                    peer_id,
                    remote_receiving: progress.remote_receiving,
                    elapsed_since_last_receiving_ms: progress.elapsed_since_last_receiving_ms,
                    send_timeout_ms: progress.send_timeout_ms,
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected snapshot-progress result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::SnapshotProgress,
                    "snapshot progress delivered",
                );
                result.snapshot_peer_report = Some(node.snapshot_peer_report(peer_id)?);
                Ok(result)
            }
            MatrixRaftMessageType::Snapshot => {
                let snapshot = message.snapshot.ok_or_else(|| {
                    RaftError::InvalidRequest("snapshot message missing descriptor".to_string())
                })?;
                self.publish_snapshot_route(group_id, runtime_node_id, snapshot)
            }
            MatrixRaftMessageType::SnapshotFinish => {
                let finish = message.old_snapshot_finish.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "snapshot-finish message missing finish state".to_string(),
                    )
                })?;
                self.finish_snapshot_route(group_id, runtime_node_id, finish)
            }
            other => Ok(MatrixRaftRouteResult::accepted_metadata(
                key,
                other,
                "message type is accepted as MatrixRaft metadata on this facade",
            )),
        }
    }

    pub fn route_messages(
        &mut self,
        messages: Vec<MatrixRaftRoutedMessage>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let mut results = Vec::with_capacity(messages.len());
        for routed in messages {
            results.push(self.route_message(
                routed.group_id,
                routed.runtime_node_id,
                routed.message,
            )?);
        }
        Ok(results)
    }

    pub fn route_message_batch(
        &mut self,
        messages: Vec<MatrixRaftRoutedMessage>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_route_message_batch(messages)?;
        self.route_messages(plan.messages)
    }

    pub fn route_message_batch_grouped(
        &mut self,
        messages: Vec<MatrixRaftRoutedMessage>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_route_message_batch(messages)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.message_count);
            for message in plan
                .messages
                .iter()
                .filter(|message| message.group_id == group.group_id)
            {
                results.push(self.route_message(
                    message.group_id,
                    message.runtime_node_id,
                    message.message.clone(),
                )?);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_message_batch_best_effort(
        &mut self,
        messages: Vec<MatrixRaftRoutedMessage>,
    ) -> Vec<MatrixRaftBatchRouteResult> {
        let mut results = Vec::with_capacity(messages.len());
        for routed in messages {
            let routed_result = match self.route_message(
                routed.group_id,
                routed.runtime_node_id,
                routed.message.clone(),
            ) {
                Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
            };
            results.push(routed_result);
        }
        results
    }

    pub fn route_message_batch_grouped_best_effort(
        &mut self,
        messages: Vec<MatrixRaftRoutedMessage>,
    ) -> Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)> {
        let mut groups = Vec::<(GroupId, Vec<MatrixRaftBatchRouteResult>)>::new();
        for result in self.route_message_batch_best_effort(messages) {
            if let Some((_, results)) = groups
                .iter_mut()
                .find(|(group_id, _)| *group_id == result.group_id)
            {
                results.push(result);
            } else {
                groups.push((result.group_id, vec![result]));
            }
        }
        groups
    }

    pub fn plan_route_message_batch(
        &self,
        messages: Vec<MatrixRaftRoutedMessage>,
    ) -> Result<MatrixRaftRouteBatchPlan, RaftError> {
        let route_keys = messages
            .iter()
            .map(MatrixRaftRoutedMessage::route_key)
            .collect::<Vec<_>>();
        for key in &route_keys {
            if !self.nodes.contains_key(key) {
                return Err(RaftError::NodeNotFound(key.node_id));
            }
        }
        let node_ids = route_keys.iter().fold(Vec::new(), |mut ids, key| {
            if !ids.contains(&key.node_id) {
                ids.push(key.node_id);
            }
            ids
        });

        let mut message_types = Vec::new();
        let mut grouped = BTreeMap::<GroupId, MatrixRaftRouteBatchGroupPlan>::new();
        for (message, key) in messages.iter().zip(route_keys.iter()) {
            if !message_types.contains(&message.message.message_type) {
                message_types.push(message.message.message_type);
            }
            let group = grouped
                .entry(message.group_id)
                .or_insert_with(|| MatrixRaftRouteBatchGroupPlan {
                    group_id: message.group_id,
                    message_count: 0,
                    node_ids: Vec::new(),
                    route_keys: Vec::new(),
                    message_types: Vec::new(),
                });
            group.message_count += 1;
            if !group.node_ids.contains(&message.runtime_node_id) {
                group.node_ids.push(message.runtime_node_id);
            }
            group.route_keys.push(*key);
            if !group.message_types.contains(&message.message.message_type) {
                group.message_types.push(message.message.message_type);
            }
        }
        let groups = grouped.into_values().collect::<Vec<_>>();
        let group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        Ok(MatrixRaftRouteBatchPlan {
            message_count: messages.len(),
            group_count: groups.len(),
            group_ids,
            node_count: node_ids.len(),
            node_ids,
            route_keys,
            message_types,
            groups,
            messages,
        })
    }

    pub fn plan_priority_route_message_batch(
        &self,
        mut messages: Vec<MatrixRaftPriorityRoutedMessage>,
    ) -> Result<MatrixRaftPriorityRouteBatchPlan, RaftError> {
        messages.sort_by_key(|message| message.priority);
        let route_keys = messages
            .iter()
            .map(MatrixRaftPriorityRoutedMessage::route_key)
            .collect::<Vec<_>>();
        for key in &route_keys {
            if !self.nodes.contains_key(key) {
                return Err(RaftError::NodeNotFound(key.node_id));
            }
        }
        let mut priority_groups = Vec::new();
        for priority in [
            MailPriority::Urgent,
            MailPriority::Normal,
            MailPriority::Slowly,
        ] {
            let priority_messages = messages
                .iter()
                .filter(|message| message.priority == priority)
                .collect::<Vec<_>>();
            let priority_route_keys = messages
                .iter()
                .filter(|message| message.priority == priority)
                .map(MatrixRaftPriorityRoutedMessage::route_key)
                .collect::<Vec<_>>();
            if !priority_route_keys.is_empty() {
                let group_ids = priority_messages.iter().fold(Vec::new(), |mut ids, message| {
                    if !ids.contains(&message.routed.group_id) {
                        ids.push(message.routed.group_id);
                    }
                    ids
                });
                let node_ids = priority_messages.iter().fold(Vec::new(), |mut ids, message| {
                    if !ids.contains(&message.routed.runtime_node_id) {
                        ids.push(message.routed.runtime_node_id);
                    }
                    ids
                });
                let message_types =
                    priority_messages
                        .iter()
                        .fold(Vec::new(), |mut types, message| {
                            let message_type = message.routed.message.message_type;
                            if !types.contains(&message_type) {
                                types.push(message_type);
                            }
                            types
                        });
                priority_groups.push(MatrixRaftPriorityRouteGroupPlan {
                    priority,
                    message_count: priority_route_keys.len(),
                    group_count: group_ids.len(),
                    group_ids,
                    route_keys: priority_route_keys,
                    node_ids,
                    message_types,
                });
            }
        }
        let routed_plan = self.plan_route_message_batch(
            messages
                .iter()
                .map(|message| message.routed.clone())
                .collect(),
        )?;
        Ok(MatrixRaftPriorityRouteBatchPlan {
            message_count: messages.len(),
            group_count: routed_plan.group_count,
            group_ids: routed_plan.group_ids,
            node_count: routed_plan.node_count,
            node_ids: routed_plan.node_ids,
            route_keys,
            message_types: routed_plan.message_types,
            priority_groups,
            groups: routed_plan.groups,
            messages,
        })
    }

    pub fn route_priority_message_batch(
        &mut self,
        messages: Vec<MatrixRaftPriorityRoutedMessage>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_priority_route_message_batch(messages)?;
        plan.messages
            .into_iter()
            .map(|message| {
                self.route_message(
                    message.routed.group_id,
                    message.routed.runtime_node_id,
                    message.routed.message,
                )
            })
            .collect()
    }

    pub fn route_priority_message_batch_grouped(
        &mut self,
        messages: Vec<MatrixRaftPriorityRoutedMessage>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_priority_route_message_batch(messages)?;
        self.route_message_batch_grouped(
            plan.messages
                .into_iter()
                .map(|message| message.routed)
                .collect(),
        )
    }

    pub fn route_priority_message_batch_best_effort(
        &mut self,
        mut messages: Vec<MatrixRaftPriorityRoutedMessage>,
    ) -> Vec<MatrixRaftBatchRouteResult> {
        messages.sort_by_key(|message| message.priority);
        self.route_message_batch_best_effort(
            messages
                .into_iter()
                .map(|message| message.routed)
                .collect(),
        )
    }

    pub fn route_priority_message_batch_grouped_best_effort(
        &mut self,
        mut messages: Vec<MatrixRaftPriorityRoutedMessage>,
    ) -> Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)> {
        messages.sort_by_key(|message| message.priority);
        self.route_message_batch_grouped_best_effort(
            messages
                .into_iter()
                .map(|message| message.routed)
                .collect(),
        )
    }

    pub fn route_message_to_group(
        &mut self,
        group_id: GroupId,
        message: MatrixRaftMessage,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_route_message_to_group(group_id, message)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            results.push(self.route_message(key.group_id, key.node_id, plan.message.clone())?);
        }
        Ok(results)
    }

    pub fn plan_route_message_to_group(
        &self,
        group_id: GroupId,
        message: MatrixRaftMessage,
    ) -> Result<MatrixRaftMessageFanoutGroupPlan, RaftError> {
        Ok(self
            .message_fanout_plan_from_groups(&[group_id], message)?
            .groups
            .into_iter()
            .next()
            .expect("single group message fanout plan"))
    }

    pub fn plan_route_message_to_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        message: MatrixRaftMessage,
    ) -> Result<MatrixRaftMessageFanoutPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.message_fanout_plan_from_groups(&group_ids, message)
    }

    pub fn route_message_to_group_best_effort(
        &mut self,
        group_id: GroupId,
        message: MatrixRaftMessage,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let plan = self.plan_route_message_to_group(group_id, message)?;
        let messages: Vec<_> = plan
            .route_keys
            .into_iter()
            .map(|key| MatrixRaftRoutedMessage::new(key.group_id, key.node_id, plan.message.clone()))
            .collect();
        Ok(self.route_message_batch_best_effort(messages))
    }

    pub fn route_message_to_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        message: MatrixRaftMessage,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_route_message_to_groups(group_ids, message)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for group in plan.groups {
            for key in group.route_keys {
                results.push(self.route_message(key.group_id, key.node_id, group.message.clone())?);
            }
        }
        Ok(results)
    }

    pub fn route_message_to_groups_grouped(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        message: MatrixRaftMessage,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_route_message_to_groups(group_ids, message)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                results.push(self.route_message(key.group_id, key.node_id, group.message.clone())?);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_message_to_groups_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        message: MatrixRaftMessage,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let plan = self.plan_route_message_to_groups(group_ids, message)?;
        let messages = plan
            .groups
            .into_iter()
            .flat_map(|group| {
                group.route_keys.into_iter().map(move |key| {
                    MatrixRaftRoutedMessage::new(key.group_id, key.node_id, group.message.clone())
                })
            })
            .collect();
        Ok(self.route_message_batch_best_effort(messages))
    }

    pub fn route_message_to_groups_grouped_best_effort(
        &mut self,
        group_ids: impl IntoIterator<Item = GroupId>,
        message: MatrixRaftMessage,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let plan = self.plan_route_message_to_groups(group_ids, message)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let messages = group
                .route_keys
                .into_iter()
                .map(|key| MatrixRaftRoutedMessage::new(key.group_id, key.node_id, group.message.clone()))
                .collect();
            groups.push((group.group_id, self.route_message_batch_best_effort(messages)));
        }
        Ok(groups)
    }

    pub fn plan_route_admin_command_to_group(
        &self,
        group_id: GroupId,
        command: MatrixRaftAdminCommand,
    ) -> Result<MatrixRaftAdminCommandFanoutGroupPlan, RaftError> {
        Ok(self
            .admin_command_fanout_plan_from_groups(&[group_id], command)?
            .groups
            .into_iter()
            .next()
            .expect("single group admin-command fanout plan"))
    }

    pub fn plan_route_admin_command_to_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        command: MatrixRaftAdminCommand,
    ) -> Result<MatrixRaftAdminCommandFanoutPlan, RaftError> {
        let group_ids = group_ids.into_iter().collect::<Vec<_>>();
        self.admin_command_fanout_plan_from_groups(&group_ids, command)
    }

    pub fn plan_route_admin_commands_for_groups(
        &self,
        group_commands: impl IntoIterator<Item = (GroupId, MatrixRaftAdminCommand)>,
    ) -> Result<MatrixRaftAdminCommandBatchPlan, RaftError> {
        let mut command_types = Vec::new();
        let mut groups = Vec::new();
        for (group_id, command) in group_commands {
            if !command_types.contains(&command.command_type) {
                command_types.push(command.command_type);
            }
            let route_keys = self.group_route_keys(group_id)?;
            let node_ids = route_keys.iter().map(|key| key.node_id).collect::<Vec<_>>();
            groups.push(MatrixRaftAdminCommandFanoutGroupPlan {
                group_id,
                node_count: route_keys.len(),
                route_keys,
                node_ids,
                command_type: command.command_type,
                command,
            });
        }
        let route_keys = groups
            .iter()
            .flat_map(|group| group.route_keys.iter().copied())
            .collect::<Vec<_>>();
        let planned_group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        let node_count = groups.iter().map(|group| group.node_count).sum();
        Ok(MatrixRaftAdminCommandBatchPlan {
            group_count: groups.len(),
            group_ids: planned_group_ids,
            node_count,
            route_keys,
            command_types,
            groups,
        })
    }

    pub fn plan_route_admin_command_batch(
        &self,
        commands: Vec<MatrixRaftRoutedAdminCommand>,
    ) -> Result<MatrixRaftRoutedAdminCommandBatchPlan, RaftError> {
        let route_keys = commands
            .iter()
            .map(MatrixRaftRoutedAdminCommand::route_key)
            .collect::<Vec<_>>();
        for key in &route_keys {
            if !self.nodes.contains_key(key) {
                return Err(RaftError::NodeNotFound(key.node_id));
            }
        }
        let node_ids = route_keys.iter().fold(Vec::new(), |mut ids, key| {
            if !ids.contains(&key.node_id) {
                ids.push(key.node_id);
            }
            ids
        });

        let mut command_types = Vec::new();
        let mut grouped =
            BTreeMap::<GroupId, MatrixRaftRoutedAdminCommandBatchGroupPlan>::new();
        for (command, key) in commands.iter().zip(route_keys.iter()) {
            if !command_types.contains(&command.command.command_type) {
                command_types.push(command.command.command_type);
            }
            let group = grouped.entry(command.group_id).or_insert_with(|| {
                MatrixRaftRoutedAdminCommandBatchGroupPlan {
                    group_id: command.group_id,
                    command_count: 0,
                    node_ids: Vec::new(),
                    route_keys: Vec::new(),
                    command_types: Vec::new(),
                }
            });
            group.command_count += 1;
            if !group.node_ids.contains(&command.runtime_node_id) {
                group.node_ids.push(command.runtime_node_id);
            }
            group.route_keys.push(*key);
            if !group.command_types.contains(&command.command.command_type) {
                group.command_types.push(command.command.command_type);
            }
        }
        let groups = grouped.into_values().collect::<Vec<_>>();
        let group_ids = groups.iter().map(|group| group.group_id).collect::<Vec<_>>();
        Ok(MatrixRaftRoutedAdminCommandBatchPlan {
            command_count: commands.len(),
            group_count: groups.len(),
            group_ids,
            node_count: node_ids.len(),
            node_ids,
            route_keys,
            command_types,
            groups,
            commands,
        })
    }

    pub fn plan_priority_route_admin_command_batch(
        &self,
        mut commands: Vec<MatrixRaftPriorityRoutedAdminCommand>,
    ) -> Result<MatrixRaftPriorityAdminCommandBatchPlan, RaftError> {
        commands.sort_by_key(|command| command.priority);
        let routed_commands = commands
            .iter()
            .map(|command| command.routed.clone())
            .collect::<Vec<_>>();
        let routed_plan = self.plan_route_admin_command_batch(routed_commands)?;

        let mut priority_groups = Vec::new();
        for priority in [
            MailPriority::Urgent,
            MailPriority::Normal,
            MailPriority::Slowly,
        ] {
            let mut route_keys = Vec::new();
            let mut group_ids = Vec::new();
            let mut node_ids = Vec::new();
            let mut command_types = Vec::new();
            for command in commands.iter().filter(|command| command.priority == priority) {
                route_keys.push(command.route_key());
                if !group_ids.contains(&command.routed.group_id) {
                    group_ids.push(command.routed.group_id);
                }
                if !node_ids.contains(&command.routed.runtime_node_id) {
                    node_ids.push(command.routed.runtime_node_id);
                }
                if !command_types.contains(&command.routed.command.command_type) {
                    command_types.push(command.routed.command.command_type);
                }
            }
            if !route_keys.is_empty() {
                priority_groups.push(MatrixRaftPriorityAdminCommandGroupPlan {
                    priority,
                    command_count: route_keys.len(),
                    group_count: group_ids.len(),
                    group_ids,
                    route_keys,
                    node_ids,
                    command_types,
                });
            }
        }

        Ok(MatrixRaftPriorityAdminCommandBatchPlan {
            command_count: commands.len(),
            group_count: routed_plan.group_count,
            group_ids: routed_plan.group_ids,
            node_count: routed_plan.node_count,
            node_ids: routed_plan.node_ids,
            route_keys: routed_plan.route_keys,
            command_types: routed_plan.command_types,
            priority_groups,
            groups: routed_plan.groups,
            commands,
        })
    }

    pub fn route_admin_command_to_group(
        &self,
        group_id: GroupId,
        command: MatrixRaftAdminCommand,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_route_admin_command_to_group(group_id, command)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            results.push(self.route_admin_command(key, plan.command.clone())?);
        }
        Ok(results)
    }

    pub fn route_admin_command_to_groups(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        command: MatrixRaftAdminCommand,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_route_admin_command_to_groups(group_ids, command)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for group in plan.groups {
            for key in group.route_keys {
                results.push(self.route_admin_command(key, group.command.clone())?);
            }
        }
        Ok(results)
    }

    pub fn route_admin_command_to_groups_grouped(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        command: MatrixRaftAdminCommand,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_route_admin_command_to_groups(group_ids, command)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                results.push(self.route_admin_command(key, group.command.clone())?);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_admin_commands_for_groups(
        &self,
        group_commands: impl IntoIterator<Item = (GroupId, MatrixRaftAdminCommand)>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_route_admin_commands_for_groups(group_commands)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for group in plan.groups {
            for key in group.route_keys {
                results.push(self.route_admin_command(key, group.command.clone())?);
            }
        }
        Ok(results)
    }

    pub fn route_admin_commands_for_groups_grouped(
        &self,
        group_commands: impl IntoIterator<Item = (GroupId, MatrixRaftAdminCommand)>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_route_admin_commands_for_groups(group_commands)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                results.push(self.route_admin_command(key, group.command.clone())?);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_admin_command_batch(
        &self,
        commands: Vec<MatrixRaftRoutedAdminCommand>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_route_admin_command_batch(commands)?;
        plan.commands
            .into_iter()
            .map(|command| self.route_admin_command(command.route_key(), command.command))
            .collect()
    }

    pub fn route_admin_command_batch_grouped(
        &self,
        commands: Vec<MatrixRaftRoutedAdminCommand>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_route_admin_command_batch(commands)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.command_count);
            for command in plan
                .commands
                .iter()
                .filter(|command| command.group_id == group.group_id)
            {
                results.push(self.route_admin_command(command.route_key(), command.command.clone())?);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_priority_admin_command_batch(
        &self,
        commands: Vec<MatrixRaftPriorityRoutedAdminCommand>,
    ) -> Result<Vec<MatrixRaftRouteResult>, RaftError> {
        let plan = self.plan_priority_route_admin_command_batch(commands)?;
        plan.commands
            .into_iter()
            .map(|command| {
                self.route_admin_command(command.routed.route_key(), command.routed.command)
            })
            .collect()
    }

    pub fn route_priority_admin_command_batch_grouped(
        &self,
        commands: Vec<MatrixRaftPriorityRoutedAdminCommand>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftRouteResult>)>, RaftError> {
        let plan = self.plan_priority_route_admin_command_batch(commands)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.command_count);
            for command in plan
                .commands
                .iter()
                .filter(|command| command.routed.group_id == group.group_id)
            {
                results.push(self.route_admin_command(
                    command.routed.route_key(),
                    command.routed.command.clone(),
                )?);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_admin_command_to_group_best_effort(
        &self,
        group_id: GroupId,
        command: MatrixRaftAdminCommand,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let plan = self.plan_route_admin_command_to_group(group_id, command)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for key in plan.route_keys {
            let routed = MatrixRaftRoutedMessage::new(
                key.group_id,
                key.node_id,
                MatrixRaftMessage::admin(key.node_id, key.node_id, plan.command.clone()),
            );
            let routed_result = match self.route_admin_command(key, plan.command.clone()) {
                Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
            };
            results.push(routed_result);
        }
        Ok(results)
    }

    pub fn route_admin_command_to_groups_grouped_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        command: MatrixRaftAdminCommand,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let plan = self.plan_route_admin_command_to_groups(group_ids, command)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let routed = MatrixRaftRoutedMessage::new(
                    key.group_id,
                    key.node_id,
                    MatrixRaftMessage::admin(key.node_id, key.node_id, group.command.clone()),
                );
                let routed_result = match self.route_admin_command(key, group.command.clone()) {
                    Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                    Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                };
                results.push(routed_result);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_admin_commands_for_groups_grouped_best_effort(
        &self,
        group_commands: impl IntoIterator<Item = (GroupId, MatrixRaftAdminCommand)>,
    ) -> Result<Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)>, RaftError> {
        let plan = self.plan_route_admin_commands_for_groups(group_commands)?;
        let mut groups = Vec::with_capacity(plan.groups.len());
        for group in plan.groups {
            let mut results = Vec::with_capacity(group.node_count);
            for key in group.route_keys {
                let routed = MatrixRaftRoutedMessage::new(
                    key.group_id,
                    key.node_id,
                    MatrixRaftMessage::admin(key.node_id, key.node_id, group.command.clone()),
                );
                let routed_result = match self.route_admin_command(key, group.command.clone()) {
                    Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                    Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                };
                results.push(routed_result);
            }
            groups.push((group.group_id, results));
        }
        Ok(groups)
    }

    pub fn route_admin_command_batch_best_effort(
        &self,
        commands: Vec<MatrixRaftRoutedAdminCommand>,
    ) -> Vec<MatrixRaftBatchRouteResult> {
        let mut results = Vec::with_capacity(commands.len());
        for command in commands {
            let routed = MatrixRaftRoutedMessage::new(
                command.group_id,
                command.runtime_node_id,
                MatrixRaftMessage::admin(
                    command.runtime_node_id,
                    command.runtime_node_id,
                    command.command.clone(),
                ),
            );
            let routed_result = match self.route_admin_command(command.route_key(), command.command)
            {
                Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
            };
            results.push(routed_result);
        }
        results
    }

    pub fn route_admin_command_batch_grouped_best_effort(
        &self,
        commands: Vec<MatrixRaftRoutedAdminCommand>,
    ) -> Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)> {
        let mut groups = Vec::<(GroupId, Vec<MatrixRaftBatchRouteResult>)>::new();
        for result in self.route_admin_command_batch_best_effort(commands) {
            if let Some((_, results)) = groups
                .iter_mut()
                .find(|(group_id, _)| *group_id == result.group_id)
            {
                results.push(result);
            } else {
                groups.push((result.group_id, vec![result]));
            }
        }
        groups
    }

    pub fn route_priority_admin_command_batch_best_effort(
        &self,
        mut commands: Vec<MatrixRaftPriorityRoutedAdminCommand>,
    ) -> Vec<MatrixRaftBatchRouteResult> {
        commands.sort_by_key(|command| command.priority);
        self.route_admin_command_batch_best_effort(
            commands
                .into_iter()
                .map(|command| command.routed)
                .collect(),
        )
    }

    pub fn route_priority_admin_command_batch_grouped_best_effort(
        &self,
        mut commands: Vec<MatrixRaftPriorityRoutedAdminCommand>,
    ) -> Vec<(GroupId, Vec<MatrixRaftBatchRouteResult>)> {
        commands.sort_by_key(|command| command.priority);
        self.route_admin_command_batch_grouped_best_effort(
            commands
                .into_iter()
                .map(|command| command.routed)
                .collect(),
        )
    }

    pub fn route_admin_command_to_groups_best_effort(
        &self,
        group_ids: impl IntoIterator<Item = GroupId>,
        command: MatrixRaftAdminCommand,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let plan = self.plan_route_admin_command_to_groups(group_ids, command)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for group in plan.groups {
            for key in group.route_keys {
                let routed = MatrixRaftRoutedMessage::new(
                    key.group_id,
                    key.node_id,
                    MatrixRaftMessage::admin(key.node_id, key.node_id, group.command.clone()),
                );
                let routed_result = match self.route_admin_command(key, group.command.clone()) {
                    Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                    Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                };
                results.push(routed_result);
            }
        }
        Ok(results)
    }

    pub fn route_admin_commands_for_groups_best_effort(
        &self,
        group_commands: impl IntoIterator<Item = (GroupId, MatrixRaftAdminCommand)>,
    ) -> Result<Vec<MatrixRaftBatchRouteResult>, RaftError> {
        let plan = self.plan_route_admin_commands_for_groups(group_commands)?;
        let mut results = Vec::with_capacity(plan.node_count);
        for group in plan.groups {
            for key in group.route_keys {
                let routed = MatrixRaftRoutedMessage::new(
                    key.group_id,
                    key.node_id,
                    MatrixRaftMessage::admin(key.node_id, key.node_id, group.command.clone()),
                );
                let routed_result = match self.route_admin_command(key, group.command.clone()) {
                    Ok(result) => MatrixRaftBatchRouteResult::from_routed_result(&routed, result),
                    Err(error) => MatrixRaftBatchRouteResult::from_routed_error(&routed, error),
                };
                results.push(routed_result);
            }
        }
        Ok(results)
    }

    fn route_config_change(
        &mut self,
        key: MatrixRaftRouteKey,
        change: MatrixRaftConfigChange,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        let node = self
            .nodes
            .get_mut(&key)
            .ok_or(RaftError::NodeNotFound(key.node_id))?;
        let member = MatrixRaftNodeId {
            peer_id: change.member_id,
            raft_addr: change.raft_addr,
            snapshot_addr: change.snapshot_addr,
        };
        let report = match change.change_type {
            MatrixRaftConfigChangeType::AddNode => match change.conf_state {
                MatrixRaftConfState::Learner => node.add_learner(member, change.auto_promote)?,
                MatrixRaftConfState::Witness => node.add_witness(member)?,
                MatrixRaftConfState::Voter => node.add_node(member)?,
            },
            MatrixRaftConfigChangeType::RemoveNode => node.remove_node(change.member_id)?,
        };
        let mut result = MatrixRaftRouteResult::delivered(
            key,
            MatrixRaftMessageType::ConfigChange,
            "config change delivered",
        );
        result.membership = Some(report);
        Ok(result)
    }

    fn route_admin_command(
        &self,
        key: MatrixRaftRouteKey,
        command: MatrixRaftAdminCommand,
    ) -> Result<MatrixRaftRouteResult, RaftError> {
        let node = self.ensure_node(key)?;
        match command.command_type {
            MatrixRaftAdminCommandType::Election => {
                let candidate_id = command.node_id.unwrap_or(key.node_id);
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::Campaign {
                        candidate_id,
                        forced: command.forced_campaign,
                    },
                })?;
                match step {
                    StepResult::Handled => {}
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected campaign admin result: {other:?}"
                        )));
                    }
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "campaign delivered",
                );
                result.campaign_candidate_id = Some(candidate_id);
                result.campaign_forced = Some(command.forced_campaign);
                Ok(result)
            }
            MatrixRaftAdminCommandType::TransferLeader => {
                let target = command.transferee_id.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "transfer-leader admin command missing transferee".to_string(),
                    )
                })?;
                let report = node.transfer_leader_with_report(target)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "leader transfer delivered",
                );
                result.transfer_leader = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::CompleteLeaderTransfer => {
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::CompleteLeaderTransfer,
                })?;
                let completed = match step {
                    StepResult::LeaderTransferCompleted(completed) => completed,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected complete-leader-transfer admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "complete-leader-transfer admin command delivered",
                );
                result.leader_transfer_completed = Some(completed);
                Ok(result)
            }
            MatrixRaftAdminCommandType::AbortLeaderTransfer => {
                let reason = command
                    .reason
                    .unwrap_or_else(|| "matrixraft admin abort leader transfer".to_string());
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::AbortLeaderTransfer { reason },
                })?;
                let aborted = match step {
                    StepResult::LeaderTransferAborted(aborted) => aborted,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected abort-leader-transfer admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "abort-leader-transfer admin command delivered",
                );
                result.leader_transfer_aborted = Some(aborted);
                Ok(result)
            }
            MatrixRaftAdminCommandType::StepDown => {
                let report = node.step_down(command.transferee_id)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "step-down delivered",
                );
                result.step_down = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::PartitionPeer => {
                let peer_id = command.require_snapshot_peer_id()?;
                node.runtime.partition_peer(peer_id)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "peer partition delivered",
                );
                result.snapshot_peer_report = Some(node.snapshot_peer_report(peer_id)?);
                Ok(result)
            }
            MatrixRaftAdminCommandType::HealPeer => {
                let peer_id = command.require_snapshot_peer_id()?;
                let report = node.runtime.heal_peer(peer_id)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "peer heal delivered",
                );
                result.catch_up = Some(report);
                result.snapshot_peer_report = Some(node.snapshot_peer_report(peer_id)?);
                Ok(result)
            }
            MatrixRaftAdminCommandType::SetNodeHealthy => {
                let node_id = command.require_node_id("set-node-healthy")?;
                let healthy = command.healthy.unwrap_or(true);
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::SetNodeHealthy { node_id, healthy },
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected set-node-healthy admin result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "set-node-healthy admin command delivered",
                );
                result.node_healthy = Some(healthy);
                result.snapshot_peer_report = Some(node.snapshot_peer_report(node_id)?);
                Ok(result)
            }
            MatrixRaftAdminCommandType::FireFatalEvent => {
                let node_id = command.require_node_id("fire-fatal-event")?;
                let reason = command
                    .reason
                    .unwrap_or_else(|| "matrixraft admin fatal event".to_string());
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::FireFatalEvent { node_id, reason },
                })?;
                let transfer_target = match step {
                    StepResult::FatalEvent(transfer_target) => transfer_target,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected fatal-event admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "fatal-event admin command delivered",
                );
                result.fatal_event_transfer_target = transfer_target;
                Ok(result)
            }
            MatrixRaftAdminCommandType::ReceiveOutOfOrderAppend => {
                let peer_id = command.require_node_id("receive-out-of-order-append")?;
                let entry = command.entry.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "receive-out-of-order-append admin command missing entry".to_string(),
                    )
                })?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::ReceiveOutOfOrderAppend {
                        peer_id,
                        entry: entry.to_log_entry(),
                    },
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected receive-out-of-order-append admin result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "receive-out-of-order-append admin command delivered",
                );
                result.snapshot_peer_report = Some(node.snapshot_peer_report(peer_id)?);
                Ok(result)
            }
            MatrixRaftAdminCommandType::ExpirePeerReorderQueue => {
                let peer_id = command.require_node_id("expire-peer-reorder-queue")?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::ExpirePeerReorderQueue { peer_id },
                })?;
                let dropped = match step {
                    StepResult::CompactedLogs(dropped) => dropped,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected expire-peer-reorder-queue admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "expire-peer-reorder-queue admin command delivered",
                );
                result.reorder_queue_dropped = Some(dropped);
                result.snapshot_peer_report = Some(node.snapshot_peer_report(peer_id)?);
                Ok(result)
            }
            MatrixRaftAdminCommandType::ProhibitsElection => {
                node.alter_attribute(
                    MatrixRaftAttribute::ProhibitsElection,
                    command.prohibits_election.unwrap_or(true),
                )?;
                Ok(MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "prohibits-election attribute updated",
                ))
            }
            MatrixRaftAdminCommandType::IgnoreWitness => {
                node.alter_attribute(
                    MatrixRaftAttribute::IgnoreWitness,
                    command.ignore_witness.unwrap_or(true),
                )?;
                Ok(MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "ignore-witness attribute updated",
                ))
            }
            MatrixRaftAdminCommandType::Resign => {
                let report = node.resign_leader("matrixraft admin resign")?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "resign delivered",
                );
                result.resign = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::TriggerSnapshot => {
                let snapshot = node.runtime.trigger_snapshot()?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot trigger delivered",
                );
                result.snapshot = Some(MatrixRaftSnapshotDesc::from_snapshot_meta(&snapshot));
                Ok(result)
            }
            MatrixRaftAdminCommandType::SnapshotReady => {
                let snapshot_id = command.snapshot_id.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "snapshot-ready admin command missing snapshot id".to_string(),
                    )
                })?;
                node.async_snapshot_ready(
                    &snapshot_id,
                    command
                        .status
                        .as_ref()
                        .is_none_or(|status| status.success),
                )?;
                Ok(MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot-ready delivered",
                ))
            }
            MatrixRaftAdminCommandType::SnapshotApplied => {
                let snapshot_id = command.snapshot_id.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "snapshot-applied admin command missing snapshot id".to_string(),
                    )
                })?;
                node.async_snapshot_applied(&snapshot_id)?;
                Ok(MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot-applied delivered",
                ))
            }
            MatrixRaftAdminCommandType::SetLeaderLeaseValid => {
                let valid = command.lease_valid.unwrap_or(true);
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::SetLeaderLeaseValid { valid },
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected set-leader-lease-valid admin result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "set-leader-lease-valid admin command delivered",
                );
                result.leader_lease_valid = Some(valid);
                Ok(result)
            }
            MatrixRaftAdminCommandType::ReceiveLeaderLeaseConfirmation => {
                let node_id = command.require_node_id("receive-leader-lease-confirmation")?;
                let confirmation_epoch =
                    command.require_lease_epoch("receive-leader-lease-confirmation")?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::ReceiveLeaderLeaseConfirmation {
                        node_id,
                        confirmation_epoch,
                        duration_ms: command.lease_duration_ms,
                    },
                })?;
                let confirmed = match step {
                    StepResult::LeaderLeaseConfirmed(confirmed) => confirmed,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected leader-lease-confirmation admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "leader-lease-confirmation admin command delivered",
                );
                result.leader_lease_confirmed = Some(confirmed);
                Ok(result)
            }
            MatrixRaftAdminCommandType::TickLeaderLease => {
                let elapsed_ms = command.require_elapsed_ms("tick-leader-lease")?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::TickLeaderLease { elapsed_ms },
                })?;
                let expired = match step {
                    StepResult::LeaderLeaseExpired(expired) => expired,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected tick-leader-lease admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "tick-leader-lease admin command delivered",
                );
                result.leader_lease_expired = Some(expired);
                Ok(result)
            }
            MatrixRaftAdminCommandType::ReceiveFollowerLease => {
                let epoch = command.require_lease_epoch("receive-follower-lease")?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::ReceiveFollowerLease { epoch },
                })?;
                let received = match step {
                    StepResult::FollowerLeaseReceived(received) => received,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected receive-follower-lease admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "receive-follower-lease admin command delivered",
                );
                result.follower_lease_received = Some(received);
                Ok(result)
            }
            MatrixRaftAdminCommandType::TickFollowerLease => {
                let elapsed_ms = command.require_elapsed_ms("tick-follower-lease")?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::TickFollowerLease { elapsed_ms },
                })?;
                let expired = match step {
                    StepResult::FollowerLeaseExpired(expired) => expired,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected tick-follower-lease admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "tick-follower-lease admin command delivered",
                );
                result.follower_lease_expired = Some(expired);
                Ok(result)
            }
            MatrixRaftAdminCommandType::BeginSnapshotSend => {
                let peer_id = command.require_snapshot_peer_id()?;
                let report = node.begin_snapshot_send_to(
                    peer_id,
                    command.require_snapshot_id()?,
                    command.require_snapshot_index()?,
                    command.require_snapshot_total_chunks()?,
                )?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot-send begin delivered",
                );
                result.snapshot_peer_report = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::RecordSnapshotChunkSent => {
                let peer_id = command.require_snapshot_peer_id()?;
                let report =
                    node.record_snapshot_chunk_sent_to(peer_id, command.require_snapshot_bytes()?)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot sent-chunk delivered",
                );
                result.snapshot_peer_report = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::AcknowledgeSnapshotChunk => {
                let peer_id = command.require_snapshot_peer_id()?;
                let report = node.acknowledge_snapshot_chunk_to(peer_id)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot chunk acknowledgement delivered",
                );
                result.snapshot_peer_report = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::RetrySnapshotChunk => {
                let peer_id = command.require_snapshot_peer_id()?;
                let report = node.retry_snapshot_chunk_to(peer_id)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot chunk retry delivered",
                );
                result.snapshot_peer_report = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::CancelSnapshotSend => {
                let peer_id = command.require_snapshot_peer_id()?;
                let report = node.cancel_snapshot_send_to(peer_id)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot-send cancel delivered",
                );
                result.snapshot_peer_report = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::BeginSnapshotInstall => {
                let peer_id = command.require_snapshot_peer_id()?;
                let report = node.begin_snapshot_install_from(
                    peer_id,
                    command.require_snapshot_id()?,
                    command.require_snapshot_index()?,
                    command.require_snapshot_total_chunks()?,
                )?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot-install begin delivered",
                );
                result.snapshot_peer_report = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::ReceiveSnapshotChunk => {
                let peer_id = command.require_snapshot_peer_id()?;
                let report = node.receive_snapshot_chunk_from(
                    peer_id,
                    command.require_snapshot_bytes()?,
                    command.snapshot_done,
                )?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot receive-chunk delivered",
                );
                result.snapshot_peer_report = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::RollbackSnapshotInstall => {
                let peer_id = command.require_snapshot_peer_id()?;
                let report = node.rollback_snapshot_install_from(peer_id)?;
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "snapshot-install rollback delivered",
                );
                result.snapshot_peer_report = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::Synced => {
                let stabled_config_change_index =
                    command.stabled_config_change_index.unwrap_or_default();
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::StabledResult {
                        first_index: command.first_index,
                        last_index: command.last_index,
                        stabled_membership_change_index: stabled_config_change_index,
                    },
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected synced admin result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "synced admin command delivered",
                );
                result.synced = Some(MatrixRaftSyncedReport {
                    first_index: command.first_index,
                    last_index: command.last_index,
                    stabled_config_change_index,
                });
                Ok(result)
            }
            MatrixRaftAdminCommandType::Applied => {
                let node_id = command.require_node_id("applied")?;
                let applied_index = command.applied_index.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "applied admin command missing applied index".to_string(),
                    )
                })?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::ApplyResult {
                        node_id,
                        applied_index,
                        rejected: command.apply_task_rejected,
                    },
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected applied admin result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "applied admin command delivered",
                );
                result.apply_result = Some(MatrixRaftApplyResultReport {
                    node_id,
                    applied_index,
                    rejected: command.apply_task_rejected,
                });
                Ok(result)
            }
            MatrixRaftAdminCommandType::ApplyTaskInflight => {
                let node_id = command.require_node_id("apply-task-inflight")?;
                let applied_index = command.applied_index.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "apply-task-inflight admin command missing applied index".to_string(),
                    )
                })?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::ApplyTaskInflight {
                        node_id,
                        applied_index,
                    },
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected apply-task-inflight admin result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "apply-task-inflight admin command delivered",
                );
                result.apply_result = Some(MatrixRaftApplyResultReport {
                    node_id,
                    applied_index,
                    rejected: false,
                });
                Ok(result)
            }
            MatrixRaftAdminCommandType::Replicated => {
                let peer_id = command.require_node_id("replicated")?;
                let success = command.status.as_ref().is_none_or(|status| status.success);
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::Replicated { peer_id, success },
                })?;
                if step != StepResult::Handled {
                    return Err(RaftError::InvalidRequest(format!(
                        "unexpected replicated admin result: {step:?}"
                    )));
                }
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "replicated admin command delivered",
                );
                result.replicated = Some(MatrixRaftReplicatedReport { peer_id, success });
                result.snapshot_peer_report = Some(node.snapshot_peer_report(peer_id)?);
                Ok(result)
            }
            MatrixRaftAdminCommandType::WitnessQuorum => {
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::WitnessQuorum {
                        acknowledgements: command.acknowledgements,
                    },
                })?;
                let report = match step {
                    StepResult::WitnessQuorum(report) => report,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected witness-quorum admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "witness-quorum admin command delivered",
                );
                result.witness_quorum = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::ReleaseMemory => {
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::ReleaseMemory,
                })?;
                let released = match step {
                    StepResult::ReleasedMemory(released) => released,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected release-memory admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "release-memory admin command delivered",
                );
                result.released_memory = Some(released);
                Ok(result)
            }
            MatrixRaftAdminCommandType::CompactLogsThrough => {
                let log_index = command.require_log_index("compact-logs-through")?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::CompactLogsThrough { log_index },
                })?;
                let compacted = match step {
                    StepResult::CompactedLogs(compacted) => compacted,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected compact-logs admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "compact-logs admin command delivered",
                );
                result.compacted_logs = Some(compacted);
                Ok(result)
            }
            MatrixRaftAdminCommandType::CompactLogsWithStorageFence => {
                let log_index = command.require_log_index("compact-logs-with-storage-fence")?;
                let fence = command.storage_fence.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "compact-logs-with-storage-fence admin command missing storage fence"
                            .to_string(),
                    )
                })?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::CompactLogsWithStorageFence { log_index, fence },
                })?;
                let report = match step {
                    StepResult::FencedCompaction(report) => report,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected fenced-compaction admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "fenced compact-logs admin command delivered",
                );
                result.fenced_compaction = Some(report);
                Ok(result)
            }
            MatrixRaftAdminCommandType::CheckpointSnapshot => {
                let node_id = command.require_node_id("checkpoint-snapshot")?;
                let snapshot_id = command.snapshot_id.ok_or_else(|| {
                    RaftError::InvalidRequest(
                        "checkpoint-snapshot admin command missing snapshot id".to_string(),
                    )
                })?;
                let step = node.runtime.step(Message::Admin {
                    command: AdminCommand::CheckpointSnapshot {
                        target: node_id,
                        snapshot_id,
                    },
                })?;
                let checkpoint = match step {
                    StepResult::CheckpointedSnapshot(snapshot) => snapshot,
                    other => {
                        return Err(RaftError::InvalidRequest(format!(
                            "unexpected checkpoint-snapshot admin result: {other:?}"
                        )));
                    }
                };
                let mut result = MatrixRaftRouteResult::delivered(
                    key,
                    MatrixRaftMessageType::AdminCommand,
                    "checkpoint-snapshot admin command delivered",
                );
                result.snapshot = Some(MatrixRaftSnapshotDesc::from_snapshot_meta(&checkpoint.meta));
                result.checkpoint = Some(checkpoint);
                Ok(result)
            }
        }
    }

    fn ensure_node(&self, key: MatrixRaftRouteKey) -> Result<&MatrixRaftNode, RaftError> {
        self.nodes
            .get(&key)
            .ok_or(RaftError::NodeNotFound(key.node_id))
    }
}

pub type MatrixRaftMultiSnapshotServer = MatrixRaftMultiRaftServer;
pub type MatrixRaftGroupHost = MatrixRaftMultiRaftServer;

pub type MatrixRaftAutoPromoteReport = LearnerAutoPromoteReport;
pub type MatrixRaftSnapshotRequest = InstallSnapshotRequest;
pub type MatrixRaftSnapshotResponse = InstallSnapshotResponse;
