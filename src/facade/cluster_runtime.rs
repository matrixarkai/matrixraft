// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// in-process cluster runtime and consensus behavior.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RaftError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("node {0} not found")]
    NodeNotFound(NodeId),
    #[error("no leader is available")]
    NoLeader,
    #[error("node {0} is not the leader")]
    NotLeader(NodeId),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("storage error: {0}")]
    Storage(String),
}

impl From<ConfigError> for RaftError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Node {
    id: NodeId,
    replica_role: ReplicaRole,
    raft_role: StateRole,
    hard_state: HardState,
    /// Entries are shared, not copied per node.
    ///
    /// A `RaftCluster` models every node of a group in one object, so a
    /// proposal used to clone the whole entry -- payload included -- into each
    /// node's log. Memory then grew with the group: measured at exactly 7x the
    /// logical data for a seven-node group. An entry is immutable once
    /// appended (conflict handling truncates, it never edits), so the nodes can
    /// share one allocation behind an `Arc`.
    log: Vec<Arc<LogEntry>>,
    // Running total of `log` payload bytes. Maintained by every log mutation so
    // that the admission checks on the propose path do not sum the log.
    #[serde(default)]
    retained_log_bytes: u64,
    installed_snapshot: Option<SnapshotMetadata>,
    commit_index: LogIndex,
    applied_index: LogIndex,
    safety_applied_index: LogIndex,
    rejected_apply_index: Option<LogIndex>,
    witness_ack_index: LogIndex,
    healthy: bool,
    liveness_elapsed_ms: u64,
    auto_promote: bool,
    auto_promote_state: LearnerAutoPromoteState,
}

impl Node {
    fn new(id: NodeId, replica_role: ReplicaRole, auto_promote: bool) -> Self {
        Self {
            id,
            replica_role,
            raft_role: if replica_role == ReplicaRole::Learner {
                StateRole::Learner
            } else {
                StateRole::Follower
            },
            hard_state: HardState {
                current_term: 0,
                voted_for: None,
                committed: None,
            },
            log: Vec::new(),
            retained_log_bytes: 0,
            installed_snapshot: None,
            commit_index: 0,
            applied_index: 0,
            safety_applied_index: 0,
            rejected_apply_index: None,
            witness_ack_index: 0,
            healthy: true,
            liveness_elapsed_ms: 0,
            auto_promote: replica_role == ReplicaRole::Learner && auto_promote,
            auto_promote_state: LearnerAutoPromoteState::Stop,
        }
    }

    fn match_index(&self) -> LogIndex {
        if self.replica_role == ReplicaRole::Witness {
            return self.witness_ack_index;
        }
        let log_index = self
            .log
            .last()
            .map(|entry| entry.log_id.index)
            .unwrap_or_default();
        let snapshot_index = self
            .installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_log_id.index)
            .unwrap_or_default();
        log_index.max(snapshot_index)
    }

    /// Slot holding `log_index`, or `None` when the index is outside the
    /// retained log.
    ///
    /// `log` is ordered by index, so a contiguous log answers by subtracting
    /// the first retained index. The binary search only runs if a gap ever puts
    /// the entry somewhere other than its arithmetic slot.
    fn log_position(&self, log_index: LogIndex) -> Option<usize> {
        let first_index = self.log.first()?.log_id.index;
        let offset = log_index.checked_sub(first_index)? as usize;
        if self.log.get(offset).map(|entry| entry.log_id.index) == Some(log_index) {
            return Some(offset);
        }
        self.log
            .binary_search_by(|entry| entry.log_id.index.cmp(&log_index))
            .ok()
    }

    /// Slot of the first entry at or after `log_index`, or `None` when every
    /// retained entry is below it.
    fn log_position_at_or_after(&self, log_index: LogIndex) -> Option<usize> {
        if self
            .log
            .last()
            .is_none_or(|last| last.log_id.index < log_index)
        {
            return None;
        }
        Some(
            self.log
                .partition_point(|entry| entry.log_id.index < log_index),
        )
    }

    fn truncate_log_at(&mut self, position: usize) {
        let released: u64 = self.log[position..]
            .iter()
            .map(|entry| entry.payload.len() as u64)
            .sum();
        self.retained_log_bytes = self.retained_log_bytes.saturating_sub(released);
        self.log.truncate(position);
    }

    /// Drops every retained entry at or below `log_index` and reports how many
    /// went away.
    fn discard_log_through(&mut self, log_index: LogIndex) -> usize {
        let cut = self
            .log
            .partition_point(|entry| entry.log_id.index <= log_index);
        if cut == 0 {
            return 0;
        }
        let released: u64 = self.log[..cut]
            .iter()
            .map(|entry| entry.payload.len() as u64)
            .sum();
        self.retained_log_bytes = self.retained_log_bytes.saturating_sub(released);
        self.log.drain(..cut);
        cut
    }

    fn set_log(&mut self, log: Vec<LogEntry>) {
        self.retained_log_bytes = log.iter().map(|entry| entry.payload.len() as u64).sum();
        self.log = log.into_iter().map(Arc::new).collect();
    }

    /// The log as owned entries, for callers that hand it to something outside
    /// the cluster. This copies every payload, so it is for record-building and
    /// RPC construction rather than anything on the propose path.
    fn log_entries(&self) -> Vec<LogEntry> {
        self.log.iter().map(|entry| (**entry).clone()).collect()
    }

    fn log_entries_from(&self, position: usize) -> Vec<LogEntry> {
        self.log[position..]
            .iter()
            .map(|entry| (**entry).clone())
            .collect()
    }

    fn log_term_at(&self, log_index: LogIndex) -> Option<Term> {
        if log_index == 0 {
            return Some(0);
        }
        if let Some(snapshot) = &self.installed_snapshot {
            if snapshot.last_log_id.index == log_index {
                return Some(snapshot.last_log_id.term);
            }
        }
        self.log_position(log_index)
            .map(|position| self.log[position].log_id.term)
    }

    fn is_fresh_candidate_log(&self, candidate_last_log_id: Option<&LogId>) -> bool {
        let candidate = candidate_last_log_id
            .cloned()
            .unwrap_or(LogId { term: 0, index: 0 });
        let local_index = self.match_index();
        let local_term = self.log_term_at(local_index).unwrap_or_default();
        candidate.term > local_term
            || (candidate.term == local_term && candidate.index >= local_index)
    }

    /// Appends a shared entry. Callers appending the same entry to several
    /// nodes should build one `Arc` and hand each node a clone of the handle.
    fn append_entry(&mut self, entry: Arc<LogEntry>) {
        // An entry past the tail cannot collide with a retained index, so the
        // steady-state append never looks at the rest of the log.
        let extends_tail = self
            .log
            .last()
            .is_none_or(|last| last.log_id.index < entry.log_id.index);
        if !extends_tail {
            if let Some(position) = self.log_position(entry.log_id.index) {
                self.truncate_log_at(position);
            }
        }
        self.retained_log_bytes = self
            .retained_log_bytes
            .saturating_add(entry.payload.len() as u64);
        self.log.push(entry);
    }

    fn truncate_log_from(&mut self, log_index: LogIndex) {
        if let Some(position) = self.log_position_at_or_after(log_index) {
            self.truncate_log_at(position);
        }
    }

    fn conflict_next_index(&self, prev_index: LogIndex) -> LogIndex {
        let local_last_index = self.match_index();
        if prev_index > local_last_index {
            return local_last_index.saturating_add(1);
        }
        let Some(conflict_term) = self.log_term_at(prev_index) else {
            return local_last_index.saturating_add(1);
        };
        let mut index = prev_index;
        while index > self.commit_index {
            if self.log_term_at(index) != Some(conflict_term) {
                return index.saturating_add(1);
            }
            index = index.saturating_sub(1);
        }
        self.commit_index.saturating_add(1)
    }

    fn acknowledge_witness_index(&mut self, log_index: LogIndex) {
        if self.replica_role == ReplicaRole::Witness {
            self.witness_ack_index = self.witness_ack_index.max(log_index);
        }
    }

    /// A witness stores a rewritten entry -- `is_command` cleared, and the
    /// payload dropped unless preserved -- so it cannot share the caller's
    /// allocation and gets its own.
    fn append_witness_entry(&mut self, entry: &LogEntry, preserve_payload: bool) {
        self.acknowledge_witness_index(entry.log_id.index);
        let stored = LogEntry {
            log_id: entry.log_id.clone(),
            payload: if preserve_payload {
                entry.payload.clone()
            } else {
                Vec::new()
            },
            is_command: false,
        };
        self.append_entry(Arc::new(stored));
    }

    fn advance_commit(&mut self, commit_index: LogIndex) {
        self.commit_index = self.commit_index.max(commit_index.min(self.match_index()));
        if (self.replica_role.can_serve_data() || self.replica_role == ReplicaRole::Witness)
            && self.rejected_apply_index.is_none()
        {
            let has_inflight_apply = self.safety_applied_index < self.applied_index;
            self.applied_index = self.applied_index.max(self.commit_index);
            if !has_inflight_apply {
                self.safety_applied_index = self.safety_applied_index.max(self.applied_index);
            }
        }
        let committed_term = self
            .log_term_at(self.commit_index)
            .unwrap_or(self.hard_state.current_term);
        self.hard_state.committed = (self.commit_index > 0).then_some(LogId {
            term: committed_term,
            index: self.commit_index,
        });
    }

    fn install_snapshot(&mut self, snapshot: RaftSnapshot) {
        let snapshot_index = snapshot.meta.last_log_id.index;
        let snapshot_log_id = snapshot.meta.last_log_id.clone();
        self.installed_snapshot = Some(snapshot.meta);
        self.discard_log_through(snapshot_index);
        if self.replica_role == ReplicaRole::Witness {
            self.witness_ack_index = self.witness_ack_index.max(snapshot_index);
        }
        self.commit_index = self.commit_index.max(snapshot_index);
        if self
            .rejected_apply_index
            .is_some_and(|required| required <= snapshot_index)
        {
            self.rejected_apply_index = None;
        }
        if self.replica_role.can_serve_data() {
            self.applied_index = self.applied_index.max(snapshot_index);
            self.safety_applied_index = self.safety_applied_index.max(snapshot_index);
        }
        self.hard_state.committed = Some(snapshot_log_id);
    }

    fn compact_log_through(&mut self, log_index: LogIndex) -> u64 {
        let safe_compaction_index = if self.replica_role.can_serve_data() {
            self.safety_applied_index
        } else {
            self.commit_index
        };
        let log_index = log_index.min(safe_compaction_index);
        self.discard_log_through(log_index) as u64
    }

    fn retained_log_bytes(&self) -> u64 {
        self.retained_log_bytes
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotTriggerState {
    pub meta: SnapshotMetadata,
    pub elapsed_ticks: u64,
    pub timeout_ticks: u64,
    pub timed_out: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotTriggerStatus {
    pub in_progress: bool,
    pub snapshot_id: Option<SnapshotId>,
    pub last_log_id: Option<LogId>,
    pub elapsed_ticks: u64,
    pub timeout_ticks: u64,
    pub timed_out: bool,
    pub duplicate_requests: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LeaderLeaseConfirmation {
    role: ReplicaRole,
    epoch: u64,
    confirmation_epoch: u64,
    #[serde(default)]
    duration_ms: u64,
    #[serde(default)]
    elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ReorderedAppend {
    request: AppendEntriesRequest,
    membership_change_indexes: Vec<LogIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PendingSnapshotInstall {
    snapshot: RaftSnapshot,
    fence: ApplySnapshotFence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftCluster {
    pub group_id: GroupId,
    pub config: Config,
    nodes: BTreeMap<NodeId, Node>,
    peer_pipelines: BTreeMap<NodeId, ReplicationPipeline>,
    #[serde(default)]
    snapshot_installers: BTreeMap<NodeId, SnapshotInstallState>,
    #[serde(default)]
    pending_snapshots: BTreeMap<NodeId, PendingSnapshotInstall>,
    leader_id: Option<NodeId>,
    current_term: Term,
    commit_index: LogIndex,
    applied_index: LogIndex,
    last_log_index: LogIndex,
    last_index_before_current_term: LogIndex,
    pending_membership_change_index: Option<LogIndex>,
    #[serde(default)]
    membership_change_indexes: BTreeSet<LogIndex>,
    #[serde(default)]
    saving_membership_change_index: Option<LogIndex>,
    #[serde(default)]
    stabled_membership_change_index: LogIndex,
    running: bool,
    leader_lease_valid: bool,
    #[serde(default)]
    leader_lease_elapsed_ms: u64,
    follower_lease_valid: bool,
    follower_lease_elapsed_ms: u64,
    #[serde(default)]
    follower_lease_duration_ms: u64,
    follower_lease_epoch: u64,
    leader_lease_epoch: u64,
    leader_lease_confirmations: BTreeMap<NodeId, LeaderLeaseConfirmation>,
    leader_lease_confirmation_epochs: BTreeMap<NodeId, u64>,
    #[serde(default)]
    reorder_queues: BTreeMap<NodeId, BTreeMap<LogIndex, ReorderedAppend>>,
    ignore_witness: bool,
    count_witness_in_commit_quorum: bool,
    prohibits_election: bool,
    leader_transfer: Option<LeaderTransferState>,
    leader_transfer_timeout_ticks: u64,
    aborted_leader_transfers: u64,
    duplicate_leader_transfer_requests: u64,
    snapshot_trigger: Option<SnapshotTriggerState>,
    duplicate_snapshot_trigger_requests: u64,
    vote_responses: BTreeMap<NodeId, bool>,
    pre_vote_responses: BTreeMap<NodeId, bool>,
}

impl RaftCluster {
    pub fn new(
        group_id: GroupId,
        config: Config,
        peers: Vec<Peer>,
    ) -> Result<Self, RaftError> {
        config.validate()?;
        if peers.is_empty() {
            return Err(RaftError::InvalidRequest(
                "raft cluster requires at least one peer".to_string(),
            ));
        }

        let mut nodes = BTreeMap::new();
        for peer in peers {
            if nodes
                .insert(
                    peer.node_id,
                    Node::new(peer.node_id, peer.role, peer.auto_promote),
                )
                .is_some()
            {
                return Err(RaftError::InvalidRequest(format!(
                    "duplicate raft node id {}",
                    peer.node_id
                )));
            }
        }
        if !nodes.values().any(|node| node.replica_role.can_be_leader()) {
            return Err(RaftError::InvalidRequest(
                "raft cluster requires at least one voter".to_string(),
            ));
        }

        let peer_pipelines = nodes
            .keys()
            .copied()
            .map(|node_id| {
                (
                    node_id,
                    ReplicationPipeline::new(node_id, 1, PipelineLimits::default()),
                )
            })
            .collect();

        let last_follower_lease_ms = if config.enable_lease_read && config.assume_lease_when_start {
            if config.last_follower_lease_ms == 0 {
                config.leader_lease_ms
            } else {
                config.last_follower_lease_ms
            }
        } else {
            0
        };
        let follower_lease_valid = last_follower_lease_ms > 0;

        Ok(Self {
            group_id,
            leader_transfer_timeout_ticks: config
                .election_timeout_ms
                .saturating_div(config.heartbeat_interval_ms.max(1))
                .max(1),
            config,
            nodes,
            peer_pipelines,
            snapshot_installers: BTreeMap::new(),
            pending_snapshots: BTreeMap::new(),
            leader_id: None,
            current_term: 0,
            commit_index: 0,
            applied_index: 0,
            last_log_index: 0,
            last_index_before_current_term: 0,
            pending_membership_change_index: None,
            membership_change_indexes: BTreeSet::new(),
            saving_membership_change_index: None,
            stabled_membership_change_index: 0,
            running: false,
            leader_lease_valid: false,
            leader_lease_elapsed_ms: 0,
            follower_lease_valid,
            follower_lease_elapsed_ms: 0,
            follower_lease_duration_ms: last_follower_lease_ms,
            follower_lease_epoch: 0,
            leader_lease_epoch: 0,
            leader_lease_confirmations: BTreeMap::new(),
            leader_lease_confirmation_epochs: BTreeMap::new(),
            reorder_queues: BTreeMap::new(),
            ignore_witness: false,
            count_witness_in_commit_quorum: false,
            prohibits_election: false,
            leader_transfer: None,
            aborted_leader_transfers: 0,
            duplicate_leader_transfer_requests: 0,
            snapshot_trigger: None,
            duplicate_snapshot_trigger_requests: 0,
            vote_responses: BTreeMap::new(),
            pre_vote_responses: BTreeMap::new(),
        })
    }

    pub fn start(&mut self) -> Result<(), RaftError> {
        self.running = true;
        if self.leader_id.is_none() {
            let leader = self
                .nodes
                .values()
                .find(|node| node.replica_role.can_be_leader() && node.healthy)
                .map(|node| node.id)
                .ok_or_else(|| RaftError::InvalidRequest("no healthy voter".to_string()))?;
            self.campaign(leader, true)?;
        }
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), RaftError> {
        self.running = false;
        self.invalidate_leader_lease();
        Ok(())
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    pub fn set_node_healthy(
        &mut self,
        node_id: NodeId,
        healthy: bool,
    ) -> Result<(), RaftError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.healthy = healthy;
        node.liveness_elapsed_ms = 0;
        if healthy {
            if let Some(pipeline) = self.peer_pipelines.get_mut(&node_id) {
                pipeline.record_peer_active();
            }
        }
        if self.leader_id == Some(node_id) && !healthy {
            self.invalidate_leader_lease();
        }
        Ok(())
    }

    pub fn partition_peer(&mut self, node_id: NodeId) -> Result<(), RaftError> {
        self.set_node_healthy(node_id, false)
    }

    pub fn heal_peer(
        &mut self,
        node_id: NodeId,
    ) -> Result<LearnerCatchUpLoopReport, RaftError> {
        self.set_node_healthy(node_id, true)?;
        self.catch_up_peer_with_reason(node_id, "healed_peer")
    }

    fn mark_peer_active(&mut self, node_id: NodeId) -> Result<(), RaftError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.healthy = true;
        node.liveness_elapsed_ms = 0;
        if let Some(pipeline) = self.peer_pipelines.get_mut(&node_id) {
            pipeline.record_peer_active();
        }
        Ok(())
    }

    fn peer_response_is_active(&self, node_id: NodeId) -> Result<bool, RaftError> {
        self.nodes
            .get(&node_id)
            .map(|node| node.healthy)
            .ok_or(RaftError::NodeNotFound(node_id))
    }

    pub fn tick_peer_liveness(&mut self, elapsed_ms: u64) -> Vec<NodeId> {
        let Some(leader_id) = self.leader_id else {
            return Vec::new();
        };
        let timeout_ms = self.config.election_timeout_ms.max(1);
        let mut timed_out = Vec::new();
        for (node_id, node) in self.nodes.iter_mut() {
            if *node_id == leader_id {
                if node.healthy {
                    node.liveness_elapsed_ms = 0;
                    if let Some(pipeline) = self.peer_pipelines.get_mut(node_id) {
                        pipeline.record_peer_active();
                    }
                }
                continue;
            }
            if !node.healthy {
                continue;
            }
            node.liveness_elapsed_ms = node.liveness_elapsed_ms.saturating_add(elapsed_ms);
            if node.liveness_elapsed_ms > timeout_ms {
                node.healthy = false;
                timed_out.push(*node_id);
                if let Some(pipeline) = self.peer_pipelines.get_mut(node_id) {
                    pipeline.record_offline_timeout();
                }
            }
        }
        timed_out
    }

    pub fn broadcast_heartbeat(&mut self) -> Result<u64, RaftError> {
        let Some(leader_id) = self.leader_id else {
            return Ok(0);
        };
        if !self
            .nodes
            .get(&leader_id)
            .map(|node| node.healthy)
            .unwrap_or(false)
        {
            return Ok(0);
        }
        self.mark_peer_active(leader_id)?;
        let peer_ids = self
            .nodes
            .keys()
            .copied()
            .filter(|peer_id| *peer_id != leader_id)
            .collect::<Vec<_>>();
        let mut sent = 0_u64;
        for peer_id in peer_ids {
            let peer_healthy = self
                .nodes
                .get(&peer_id)
                .map(|node| node.healthy)
                .unwrap_or(false);
            let liveness_timed_out = self
                .peer_pipelines
                .get(&peer_id)
                .map(|pipeline| pipeline.status().offline_timeout_reached)
                .unwrap_or(false);
            if !peer_healthy && !liveness_timed_out {
                continue;
            }
            if self
                .peer_pipelines
                .get(&peer_id)
                .map(|pipeline| {
                    let status = pipeline.status();
                    status.snapshot_sending || status.snapshot_installing
                })
                .unwrap_or(false)
            {
                continue;
            }
            let peer_match_index = self
                .nodes
                .get(&peer_id)
                .map(Node::match_index)
                .unwrap_or_default();
            let request = AppendEntriesRequest {
                group_id: self.group_id,
                term: self.current_term,
                leader_id,
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: self.commit_index.min(peer_match_index),
                lease_epoch: self.leader_lease_epoch,
            };
            let response = self.append_entries_to(peer_id, request)?;
            self.handle_append_entries_response(leader_id, peer_id, response)?;
            if let Some(pipeline) = self.peer_pipelines.get_mut(&peer_id) {
                pipeline.record_heartbeat_response();
            }
            sent = sent.saturating_add(1);

            let should_probe_tail = self
                .peer_pipelines
                .get(&peer_id)
                .map(|pipeline| {
                    let status = pipeline.status();
                    status.match_index < self.last_log_index
                        && status.next_index == self.last_log_index.saturating_add(1)
                })
                .unwrap_or(false);
            if should_probe_tail {
                let _ = self.catch_up_peer_with_reason(peer_id, "heartbeat_tail_probe")?;
            }
        }
        Ok(sent)
    }

    pub fn set_leader_lease_valid(&mut self, valid: bool) {
        if valid {
            self.renew_leader_lease_from_live_quorum();
        } else {
            self.invalidate_leader_lease();
        }
    }

    pub fn set_follower_lease_valid(&mut self, valid: bool) {
        if valid {
            self.renew_follower_lease();
        } else {
            self.invalidate_follower_lease();
        }
    }

    pub fn is_follower_lease_valid(&self) -> bool {
        self.config.enable_lease_read && self.follower_lease_valid
    }

    pub fn tick_follower_lease(&mut self, elapsed_ms: u64) -> bool {
        if !self.is_follower_lease_valid() {
            return false;
        }
        self.follower_lease_elapsed_ms = self.follower_lease_elapsed_ms.saturating_add(elapsed_ms);
        if self.follower_lease_elapsed_ms >= self.follower_lease_duration_ms {
            self.expire_follower_lease();
            return true;
        }
        false
    }

    pub fn receive_follower_lease_item(&mut self, epoch: u64) -> bool {
        if epoch <= self.follower_lease_epoch {
            return false;
        }
        self.follower_lease_epoch = epoch;
        self.follower_lease_valid = self.config.enable_lease_read;
        self.follower_lease_elapsed_ms = 0;
        self.follower_lease_duration_ms = self.config.leader_lease_ms;
        self.is_follower_lease_valid()
    }

    fn renew_follower_lease(&mut self) {
        let next_epoch = self.follower_lease_epoch.saturating_add(1);
        let _ = self.receive_follower_lease_item(next_epoch);
    }

    fn renew_follower_lease_from_append_entries(&mut self, lease_epoch: u64) {
        if lease_epoch > 0 {
            let _ = self.receive_follower_lease_item(lease_epoch);
        } else {
            self.renew_follower_lease();
        }
    }

    fn invalidate_follower_lease(&mut self) {
        self.follower_lease_valid = false;
        self.follower_lease_elapsed_ms = 0;
        self.follower_lease_duration_ms = 0;
        self.follower_lease_epoch = 0;
    }

    fn expire_follower_lease(&mut self) {
        self.follower_lease_valid = false;
        self.follower_lease_elapsed_ms = 0;
        self.follower_lease_duration_ms = 0;
    }

    pub fn renew_leader_lease_from_live_quorum(&mut self) -> bool {
        let Some(leader_id) = self.leader_id else {
            self.invalidate_leader_lease();
            return false;
        };
        if !self
            .nodes
            .get(&leader_id)
            .map(|node| node.healthy && node.replica_role.can_be_leader())
            .unwrap_or(false)
        {
            self.invalidate_leader_lease();
            return false;
        }
        let acknowledgements = self
            .nodes
            .values()
            .filter(|node| {
                node.healthy
                    && node.replica_role.participates_in_quorum()
                    && !(self.ignore_witness && node.replica_role == ReplicaRole::Witness)
            })
            .map(|node| node.id)
            .collect::<Vec<_>>();
        self.renew_leader_lease_from_acknowledgements(acknowledgements)
    }

    fn invalidate_leader_lease(&mut self) {
        self.leader_lease_valid = false;
        self.leader_lease_elapsed_ms = 0;
        self.leader_lease_confirmations.clear();
        self.leader_lease_confirmation_epochs.clear();
    }

    pub fn expire_leader_lease(&mut self) {
        self.leader_lease_valid = false;
        self.leader_lease_elapsed_ms = self.config.leader_lease_ms;
        self.leader_lease_confirmations.clear();
    }

    pub fn tick_leader_lease(&mut self, elapsed_ms: u64) -> bool {
        if !self.config.enable_lease_read {
            let was_valid = self.leader_lease_valid;
            self.expire_leader_lease();
            return was_valid;
        }
        self.leader_lease_elapsed_ms = self.leader_lease_elapsed_ms.saturating_add(elapsed_ms);
        for confirmation in self.leader_lease_confirmations.values_mut() {
            confirmation.elapsed_ms = confirmation.elapsed_ms.saturating_add(elapsed_ms);
        }
        let was_valid = self.leader_lease_valid;
        self.leader_lease_valid = self.leader_lease_quorum_reached();
        self.refresh_witness_commit_quorum_policy();
        was_valid && !self.leader_lease_valid
    }

    fn reset_leader_lease_epoch(&mut self) {
        self.leader_lease_epoch = self.leader_lease_epoch.saturating_add(1);
        self.leader_lease_elapsed_ms = 0;
        self.leader_lease_confirmations.clear();
        self.leader_lease_confirmation_epochs.clear();
        self.leader_lease_valid = false;
    }

    pub fn receive_leader_lease_confirmation(
        &mut self,
        node_id: NodeId,
        confirmation_epoch: u64,
    ) -> bool {
        self.receive_leader_lease_confirmation_with_duration(
            node_id,
            confirmation_epoch,
            self.config.leader_lease_ms,
        )
    }

    pub fn receive_leader_lease_confirmation_with_duration(
        &mut self,
        node_id: NodeId,
        confirmation_epoch: u64,
        duration_ms: u64,
    ) -> bool {
        self.receive_leader_lease_confirmation_inner(node_id, confirmation_epoch, duration_ms, true)
    }

    /// `recompute_quorum` lets a caller recording a whole round of
    /// confirmations evaluate the lease quorum once at the end rather than
    /// after each one.
    ///
    /// `leader_lease_quorum_reached` walks every confirmation and looks up its
    /// node, so recomputing per confirmation makes a round of N cost O(N^2).
    /// The result depends only on the final set, so the intermediate passes
    /// cannot change it.
    fn receive_leader_lease_confirmation_inner(
        &mut self,
        node_id: NodeId,
        confirmation_epoch: u64,
        duration_ms: u64,
        recompute_quorum: bool,
    ) -> bool {
        if self.leader_id == Some(node_id) {
            return false;
        }
        if duration_ms == 0 {
            return false;
        }
        let Some(node) = self.nodes.get(&node_id) else {
            return false;
        };
        if !node.replica_role.participates_in_quorum()
            || (self.ignore_witness && node.replica_role == ReplicaRole::Witness)
        {
            return false;
        }
        if confirmation_epoch
            <= self
                .leader_lease_confirmation_epochs
                .get(&node_id)
                .copied()
                .unwrap_or_default()
        {
            return false;
        }
        self.leader_lease_confirmation_epochs
            .insert(node_id, confirmation_epoch);

        if let Some(existing) = self.leader_lease_confirmations.get_mut(&node_id) {
            let existing_remaining_ms = existing.duration_ms.saturating_sub(existing.elapsed_ms);
            if existing.epoch == self.leader_lease_epoch
                && existing.role == node.replica_role
                && existing_remaining_ms >= duration_ms
            {
                existing.confirmation_epoch = confirmation_epoch;
                if recompute_quorum {
                    self.leader_lease_valid = self.leader_lease_quorum_reached();
                }
                return true;
            }
        }

        self.leader_lease_elapsed_ms = 0;
        self.leader_lease_confirmations.insert(
            node_id,
            LeaderLeaseConfirmation {
                role: node.replica_role,
                epoch: self.leader_lease_epoch,
                confirmation_epoch,
                duration_ms,
                elapsed_ms: 0,
            },
        );
        if recompute_quorum {
            self.leader_lease_valid = self.leader_lease_quorum_reached();
        }
        true
    }

    /// Record a self-generated confirmation for `node_id`.
    ///
    /// `recompute_quorum` is false when the caller records a round of these and
    /// evaluates the quorum once afterwards.
    fn record_leader_lease_confirmation_inner(&mut self, node_id: NodeId, recompute_quorum: bool) {
        let confirmation_epoch = self
            .leader_lease_confirmation_epochs
            .get(&node_id)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        let _ = self.receive_leader_lease_confirmation_inner(
            node_id,
            confirmation_epoch,
            self.config.leader_lease_ms,
            recompute_quorum,
        );
    }

    fn renew_leader_lease_from_acknowledgements<I>(&mut self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = NodeId>,
    {
        if self.leader_id.is_none() {
            self.invalidate_leader_lease();
            return false;
        }
        self.leader_lease_epoch = self.leader_lease_epoch.saturating_add(1);
        self.leader_lease_elapsed_ms = 0;
        self.leader_lease_confirmations.clear();
        for node_id in acknowledgements {
            // Deferred: the quorum is evaluated once below, so evaluating it
            // per acknowledgement was O(N^2) for a result that only depends on
            // the final set.
            self.record_leader_lease_confirmation_inner(node_id, false);
        }
        self.leader_lease_valid = self.leader_lease_quorum_reached();
        self.leader_lease_valid
    }

    fn leader_lease_quorum_reached(&self) -> bool {
        let Some(leader_id) = self.leader_id else {
            return false;
        };
        if !self
            .nodes
            .get(&leader_id)
            .map(|node| {
                node.healthy
                    && node.replica_role.can_be_leader()
                    && self.leader_lease_elapsed_ms < self.config.leader_lease_ms
            })
            .unwrap_or(false)
        {
            return false;
        }
        let mut acknowledgements = vec![leader_id];
        acknowledgements.extend(self.leader_lease_confirmations.iter().filter_map(
            |(node_id, confirmation)| {
                let node = self.nodes.get(node_id)?;
                (confirmation.epoch == self.leader_lease_epoch
                    && confirmation.role == node.replica_role
                    && confirmation.elapsed_ms < confirmation.duration_ms
                    && node.replica_role.participates_in_quorum()
                    && !(self.ignore_witness && node.replica_role == ReplicaRole::Witness))
                    .then_some(*node_id)
            },
        ));
        self.membership()
            .quorum_reached_with_witness_policy(acknowledgements, self.ignore_witness)
    }

    pub fn set_ignore_witness(&mut self, ignore_witness: bool) {
        if self.ignore_witness == ignore_witness {
            return;
        }
        self.ignore_witness = ignore_witness;
        if ignore_witness {
            self.count_witness_in_commit_quorum = false;
            let witnesses = self.membership().witnesses;
            for witness_id in witnesses {
                self.remove_election_response_from(witness_id);
            }
        }
        self.leader_lease_valid = self.leader_lease_quorum_reached();
        self.refresh_commit_index();
    }

    pub fn ignore_witness(&self) -> bool {
        self.ignore_witness
    }

    pub fn count_witness_in_commit_quorum(&self) -> bool {
        self.count_witness_in_commit_quorum
    }

    pub fn refresh_witness_commit_quorum_policy(&mut self) -> bool {
        let membership = self.membership();
        let live_voters = membership
            .voters
            .iter()
            .filter(|node_id| {
                self.nodes
                    .get(node_id)
                    .map(|node| node.healthy)
                    .unwrap_or(false)
            })
            .count();
        let normal_voter_quorum = membership.voters.len() / 2 + 1;
        let should_count_witness = if self.ignore_witness || live_voters >= normal_voter_quorum {
            false
        } else {
            let live_witnesses = membership
                .witnesses
                .iter()
                .filter(|node_id| {
                    self.nodes
                        .get(node_id)
                        .map(|node| node.healthy)
                        .unwrap_or(false)
                })
                .count();
            live_voters + live_witnesses >= membership.quorum_size_with_witness_policy(false)
        };
        let changed = self.count_witness_in_commit_quorum != should_count_witness;
        self.count_witness_in_commit_quorum = should_count_witness;
        if changed {
            self.refresh_commit_index();
        }
        changed
    }

    pub fn step_down_leader_if_lost_quorum(&mut self) -> bool {
        if self.leader_id.is_none() || self.has_live_quorum() || self.leader_lease_valid {
            return false;
        }
        self.leader_id = None;
        self.invalidate_leader_lease();
        self.abort_leader_transfer("lost_quorum");
        for node in self.nodes.values_mut() {
            if node.raft_role == StateRole::Leader {
                node.raft_role = StateRole::Follower;
            }
        }
        true
    }

    pub fn pending_membership_change_index(&self) -> Option<LogIndex> {
        self.pending_membership_change_index
    }

    pub fn saving_membership_change_index(&self) -> Option<LogIndex> {
        self.saving_membership_change_index
    }

    pub fn stabled_membership_change_index(&self) -> LogIndex {
        self.stabled_membership_change_index
    }

    fn membership_change_fence_active(&self) -> bool {
        self.pending_membership_change_index
            .is_some_and(|pending| pending > self.applied_index)
            || self
                .saving_membership_change_index
                .is_some_and(|saving| saving > self.stabled_membership_change_index)
    }

    pub fn begin_pending_membership_change(
        &mut self,
        log_index: LogIndex,
    ) -> Result<(), RaftError> {
        if log_index == 0 {
            return Err(RaftError::InvalidRequest(
                "membership change log index must be non-zero".to_string(),
            ));
        }
        if let Some(saving_index) = self.saving_membership_change_index {
            if saving_index > self.stabled_membership_change_index {
                return Err(RaftError::InvalidRequest(format!(
                    "saving_membership_change_index:{saving_index}"
                )));
            }
        }
        if let Some(pending_index) = self.pending_membership_change_index {
            if pending_index > self.applied_index {
                return Err(RaftError::InvalidRequest(format!(
                    "pending_membership_change_index:{pending_index}"
                )));
            }
        }
        self.pending_membership_change_index = Some(log_index);
        self.membership_change_indexes.insert(log_index);
        Ok(())
    }

    pub fn begin_saving_membership_change(
        &mut self,
        log_index: LogIndex,
    ) -> Result<(), RaftError> {
        if log_index == 0 {
            return Err(RaftError::InvalidRequest(
                "membership change log index must be non-zero".to_string(),
            ));
        }
        if let Some(saving_index) = self.saving_membership_change_index {
            if saving_index > self.stabled_membership_change_index {
                return Err(RaftError::InvalidRequest(format!(
                    "saving_membership_change_index:{saving_index}"
                )));
            }
        }
        if let Some(pending_index) = self.pending_membership_change_index {
            if pending_index != log_index && pending_index > self.applied_index {
                return Err(RaftError::InvalidRequest(format!(
                    "pending_membership_change_index:{pending_index}"
                )));
            }
        }
        self.pending_membership_change_index = Some(log_index);
        self.membership_change_indexes.insert(log_index);
        self.saving_membership_change_index = Some(log_index);
        Ok(())
    }

    pub fn mark_membership_change_stabled(&mut self, log_index: LogIndex) {
        self.stabled_membership_change_index = self.stabled_membership_change_index.max(log_index);
        if self
            .saving_membership_change_index
            .is_some_and(|saving| saving <= self.stabled_membership_change_index)
        {
            self.saving_membership_change_index = None;
        }
        if self.applied_index >= log_index {
            self.mark_membership_change_applied(log_index);
        }
    }

    pub fn submit_stabled_result(
        &mut self,
        first_index: Option<LogIndex>,
        last_index: Option<LogIndex>,
        stabled_membership_change_index: LogIndex,
    ) -> Result<bool, RaftError> {
        match (first_index, last_index) {
            (Some(first), Some(last)) if first == 0 || last == 0 || first > last => {
                return Err(RaftError::InvalidRequest(format!(
                    "invalid stabled result range: {first:?}..={last:?}"
                )));
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(RaftError::InvalidRequest(
                    "stabled result range must include both first_index and last_index".to_string(),
                ));
            }
            _ => {}
        }
        if stabled_membership_change_index > 0 {
            self.mark_membership_change_stabled(stabled_membership_change_index);
        }
        if let (Some(_first), Some(_last)) = (first_index, last_index) {
            self.refresh_commit_index();
            return Ok(true);
        }
        Ok(false)
    }

    pub fn mark_membership_change_applied(&mut self, log_index: LogIndex) {
        self.applied_index = self.applied_index.max(log_index);
        if self
            .pending_membership_change_index
            .is_some_and(|pending| pending <= log_index)
            && !self
                .saving_membership_change_index
                .is_some_and(|saving| saving <= log_index)
        {
            self.pending_membership_change_index = None;
        }
    }

    pub fn mark_snapshot_membership_floor_applied(&mut self, snapshot_index: LogIndex) {
        self.applied_index = self.applied_index.max(snapshot_index);
        self.membership_change_indexes
            .retain(|index| *index > snapshot_index);
        if self
            .saving_membership_change_index
            .is_some_and(|saving| saving <= snapshot_index)
        {
            self.saving_membership_change_index = None;
            self.stabled_membership_change_index =
                self.stabled_membership_change_index.max(snapshot_index);
        }
        self.pending_membership_change_index =
            self.membership_change_indexes.iter().next_back().copied();
    }

    pub fn reset_pending_membership_change_after_truncation(
        &mut self,
        last_retained_log_index: LogIndex,
        committed_index: LogIndex,
    ) {
        self.membership_change_indexes
            .retain(|index| *index <= last_retained_log_index);
        if self
            .saving_membership_change_index
            .is_some_and(|saving| saving > last_retained_log_index)
        {
            self.saving_membership_change_index = None;
        }
        self.pending_membership_change_index = self
            .membership_change_indexes
            .iter()
            .rev()
            .copied()
            .find(|index| *index > committed_index);
    }

    pub fn rejected_apply_index(
        &self,
        node_id: NodeId,
    ) -> Result<Option<LogIndex>, RaftError> {
        Ok(self
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?
            .rejected_apply_index)
    }

    pub fn safety_applied_index(
        &self,
        node_id: NodeId,
    ) -> Result<LogIndex, RaftError> {
        Ok(self
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?
            .safety_applied_index)
    }

    pub fn min_replicated_index(
        &self,
        node_id: NodeId,
    ) -> Result<LogIndex, RaftError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        let mut replicated_index = node.safety_applied_index;
        if node.raft_role == StateRole::Leader {
            for peer in self.nodes.values() {
                if peer.id != node_id && peer.healthy {
                    replicated_index = replicated_index.min(peer.match_index());
                }
            }
        }
        Ok(replicated_index)
    }

    pub fn mark_apply_task_inflight(
        &mut self,
        node_id: NodeId,
        applied_index: LogIndex,
    ) -> Result<(), RaftError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if applied_index > node.commit_index {
            return Err(RaftError::InvalidRequest(format!(
                "inflight applied_index {applied_index} exceeds commit index {}",
                node.commit_index
            )));
        }
        if applied_index < node.safety_applied_index {
            return Err(RaftError::InvalidRequest(format!(
                "inflight applied_index {applied_index} is below safety applied index {}",
                node.safety_applied_index
            )));
        }
        node.applied_index = node.applied_index.max(applied_index);
        node.safety_applied_index = node
            .safety_applied_index
            .min(applied_index.saturating_sub(1));
        self.refresh_cluster_indexes();
        Ok(())
    }

    pub fn submit_apply_result(
        &mut self,
        node_id: NodeId,
        applied_index: LogIndex,
        apply_task_rejected: bool,
    ) -> Result<(), RaftError> {
        let leader_apply_rejected = {
            let node = self
                .nodes
                .get_mut(&node_id)
                .ok_or(RaftError::NodeNotFound(node_id))?;
            if applied_index < node.safety_applied_index || applied_index > node.applied_index {
                return Err(RaftError::InvalidRequest(format!(
                    "applied_index {applied_index} outside safety/applied range {}..{}",
                    node.safety_applied_index, node.applied_index
                )));
            }
            node.safety_applied_index = applied_index;
            if apply_task_rejected {
                node.rejected_apply_index = Some(node.safety_applied_index.saturating_add(1));
            }
            apply_task_rejected && node.raft_role == StateRole::Leader
        };
        if leader_apply_rejected {
            let transfer_target = self.closest_follower();
            if let Some(node) = self.nodes.get_mut(&node_id) {
                node.raft_role = StateRole::Follower;
            }
            if !transfer_target
                .map(|target| self.campaign(target, true).is_ok())
                .unwrap_or(false)
                && self.leader_id == Some(node_id)
            {
                self.leader_id = None;
                self.invalidate_leader_lease();
            }
        }
        let _ = self.try_install_pending_snapshot_to(node_id)?;
        self.refresh_cluster_indexes();
        Ok(())
    }

    pub fn set_prohibits_election(&mut self, prohibits_election: bool) {
        self.prohibits_election = prohibits_election;
    }

    pub fn prohibits_election(&self) -> bool {
        self.prohibits_election
    }

    pub fn campaign(
        &mut self,
        candidate_id: NodeId,
        forced: bool,
    ) -> Result<(), RaftError> {
        if !self.running && !forced {
            return Err(RaftError::InvalidRequest(
                "cannot campaign before cluster start".to_string(),
            ));
        }
        if self.prohibits_election && !forced {
            return Err(RaftError::InvalidRequest(
                "election is prohibited".to_string(),
            ));
        }
        if self.is_follower_lease_valid() && !forced {
            return Err(RaftError::InvalidRequest(
                "follower is still in leader lease".to_string(),
            ));
        }
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        if !candidate.replica_role.can_be_leader() {
            return Err(RaftError::InvalidRequest(format!(
                "node {} cannot become leader",
                candidate_id
            )));
        }
        if !candidate.healthy {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is not healthy",
                candidate_id
            )));
        }
        if self.leader_id == Some(candidate_id) && candidate.raft_role == StateRole::Leader {
            return Ok(());
        }

        self.last_index_before_current_term = self.last_log_index;
        let append_leader_noop = self.running;
        self.current_term += 1;
        self.leader_id = Some(candidate_id);
        self.vote_responses.clear();
        self.pre_vote_responses.clear();
        self.clear_reorder_queues();
        if let Some(transferee_id) = self
            .leader_transfer
            .as_ref()
            .map(|transfer| transfer.transferee_id)
        {
            if transferee_id == candidate_id {
                self.leader_transfer = None;
            } else {
                self.abort_leader_transfer("leader_changed_before_transfer_complete");
            }
        }
        self.reset_leader_lease_epoch();
        self.invalidate_follower_lease();
        for node in self.nodes.values_mut() {
            node.hard_state.current_term = self.current_term;
            node.hard_state.voted_for = Some(candidate_id);
            node.raft_role = if node.id == candidate_id {
                StateRole::Leader
            } else if node.replica_role == ReplicaRole::Learner {
                StateRole::Learner
            } else {
                StateRole::Follower
            };
        }
        self.reset_replication_pipelines_for_leader(candidate_id);
        if append_leader_noop {
            self.broadcast_leader_noop()?;
        }
        Ok(())
    }

    fn broadcast_leader_noop(&mut self) -> Result<LogId, RaftError> {
        let leader_id = self.leader_id.ok_or(RaftError::NoLeader)?;
        let log_id = LogId {
            term: self.current_term,
            index: self.last_log_index + 1,
        };
        let entry = LogEntry {
            log_id: log_id.clone(),
            payload: b"no-op".to_vec(),
            is_command: true,
        };
        self.last_log_index = log_id.index;
        let entry = Arc::new(entry);

        let node_ids: Vec<_> = self.nodes.keys().copied().collect();
        for node_id in node_ids {
            let Some(node) = self.nodes.get_mut(&node_id) else {
                continue;
            };
            if node_id == leader_id {
                node.append_entry(Arc::clone(&entry));
                continue;
            }
            if node.healthy && node.match_index().saturating_add(1) == entry.log_id.index {
                if node.replica_role.can_serve_data() {
                    node.append_entry(Arc::clone(&entry));
                } else if node.replica_role == ReplicaRole::Witness {
                    node.append_witness_entry(&entry, false);
                }
            }
        }
        self.refresh_cluster_indexes();
        Ok(log_id)
    }

    fn clear_election_responses(&mut self) {
        self.vote_responses.clear();
        self.pre_vote_responses.clear();
    }

    fn remove_election_response_from(&mut self, node_id: NodeId) {
        self.vote_responses.remove(&node_id);
        self.pre_vote_responses.remove(&node_id);
    }

    fn clear_reorder_queues(&mut self) {
        self.reorder_queues.clear();
    }

    fn clear_reorder_queue_for(&mut self, node_id: NodeId) {
        self.reorder_queues.remove(&node_id);
    }

    fn become_leader_current_term(
        &mut self,
        candidate_id: NodeId,
    ) -> Result<(), RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        if !candidate.replica_role.can_be_leader() {
            return Err(RaftError::InvalidRequest(format!(
                "node {} cannot become leader",
                candidate_id
            )));
        }
        if !candidate.healthy {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is not healthy",
                candidate_id
            )));
        }
        self.leader_id = Some(candidate_id);
        self.last_index_before_current_term = self.last_log_index;
        self.clear_election_responses();
        self.clear_reorder_queues();
        self.reset_leader_lease_epoch();
        self.invalidate_follower_lease();
        for node in self.nodes.values_mut() {
            node.hard_state.current_term = self.current_term;
            node.hard_state.voted_for = Some(candidate_id);
            node.raft_role = if node.id == candidate_id {
                StateRole::Leader
            } else if node.replica_role == ReplicaRole::Learner {
                StateRole::Learner
            } else {
                StateRole::Follower
            };
        }
        self.reset_replication_pipelines_for_leader(candidate_id);
        if self.running && self.last_index_before_current_term > 0 {
            self.propose(b"no-op".to_vec())?;
        }
        Ok(())
    }

    fn start_vote_after_pre_vote_quorum(
        &mut self,
        candidate_id: NodeId,
    ) -> Result<(), RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        if !candidate.replica_role.can_be_leader() {
            return Err(RaftError::InvalidRequest(format!(
                "node {} cannot become candidate",
                candidate_id
            )));
        }
        if !candidate.healthy {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is not healthy",
                candidate_id
            )));
        }

        self.current_term = self.current_term.saturating_add(1);
        self.leader_id = None;
        self.last_index_before_current_term = self.last_log_index;
        self.clear_election_responses();
        self.clear_reorder_queues();
        self.reset_leader_lease_epoch();
        self.invalidate_follower_lease();
        for node in self.nodes.values_mut() {
            node.hard_state.current_term = self.current_term;
            node.hard_state.voted_for = Some(candidate_id);
            node.raft_role = if node.id == candidate_id {
                StateRole::Candidate
            } else if node.replica_role == ReplicaRole::Learner {
                StateRole::Learner
            } else {
                StateRole::Follower
            };
        }
        self.vote_responses.insert(candidate_id, true);
        Ok(())
    }

    pub fn timeout_now(
        &mut self,
        from: NodeId,
        target: NodeId,
    ) -> Result<TimeoutNowResponse, RaftError> {
        let (role, replica_role, term) = {
            let node = self
                .nodes
                .get(&target)
                .ok_or(RaftError::NodeNotFound(target))?;
            (
                node.raft_role,
                node.replica_role,
                node.hard_state.current_term,
            )
        };
        if role != StateRole::Follower || !replica_role.can_be_leader() {
            return Ok(TimeoutNowResponse {
                node_id: target,
                from,
                campaigned: false,
                term,
                leader_id: self.leader_id,
                reason: format!("timeout_now_ignored_{replica_role:?}"),
            });
        }

        self.campaign(target, true)?;
        Ok(TimeoutNowResponse {
            node_id: target,
            from,
            campaigned: true,
            term: self.current_term,
            leader_id: self.leader_id,
            reason: "timeout_now_campaign".to_string(),
        })
    }

    pub fn step(&mut self, message: Message) -> Result<StepResult, RaftError> {
        match message {
            Message::Admin { command } => self.step_admin(command),
            Message::Propose { payload, options } => self
                .propose_with_options(payload, options)
                .map(StepResult::Proposed),
            Message::Membership { operation } => {
                let mut executor = MembershipExecutor::new();
                executor
                    .execute(self, operation)
                    .map(StepResult::Membership)
            }
            Message::AutoPromoteLearner { learner_id } => self
                .auto_promote_learner(learner_id)
                .map(StepResult::AutoPromoteLearner),
            Message::CatchUpPeer { peer_id } => self
                .catch_up_peer(peer_id)
                .map(StepResult::CatchUpPeer),
            Message::PreVote { candidate_id } => {
                self.pre_vote(candidate_id).map(StepResult::PreVote)
            }
            Message::AppendEntries { target, request } => self
                .append_entries_to(target, request)
                .map(StepResult::AppendEntries),
            Message::AppendEntriesResponse {
                local_node_id,
                peer_id,
                response,
            } => self
                .handle_append_entries_response(local_node_id, peer_id, response)
                .map(|()| StepResult::Handled),
            Message::Vote { target, request } => {
                self.vote_to(target, request).map(StepResult::Vote)
            }
            Message::VoteResponse {
                local_node_id,
                peer_id,
                response,
                pre_vote,
            } => match peer_id {
                Some(peer_id) => self
                    .handle_vote_response_from(local_node_id, peer_id, response, pre_vote)
                    .map(|()| StepResult::Handled),
                None => self
                    .handle_vote_response(local_node_id, response, pre_vote)
                    .map(|()| StepResult::Handled),
            },
            Message::InstallSnapshot { target, request } => self
                .install_snapshot_chunk_to(target, request)
                .map(StepResult::InstallSnapshot),
            Message::InstallSnapshotResponse {
                local_node_id,
                peer_id,
                response,
            } => self
                .handle_install_snapshot_response(local_node_id, peer_id, response)
                .map(|()| StepResult::Handled),
            Message::NetworkError { peer_id } => self
                .record_network_error_for(peer_id)
                .map(|()| StepResult::Handled),
            Message::SnapshotFinish {
                peer_id,
                accepted,
                committed_index,
            } => self
                .handle_snapshot_finish_from(peer_id, accepted, committed_index)
                .map(|()| StepResult::Handled),
            Message::SnapshotProgress {
                peer_id,
                remote_receiving,
                elapsed_since_last_receiving_ms,
                send_timeout_ms,
            } => self
                .update_snapshot_progress_from(
                    peer_id,
                    remote_receiving,
                    elapsed_since_last_receiving_ms,
                    send_timeout_ms,
                )
                .map(|_| StepResult::Handled),
            Message::ReadIndex { request } => {
                self.read_index(request).map(StepResult::ReadIndex)
            }
            Message::TimeoutNow { from, target } => self
                .timeout_now(from, target)
                .map(StepResult::TimeoutNow),
        }
    }

    fn step_admin(
        &mut self,
        command: AdminCommand,
    ) -> Result<StepResult, RaftError> {
        match command {
            AdminCommand::Campaign {
                candidate_id,
                forced,
            } => self
                .campaign(candidate_id, forced)
                .map(|()| StepResult::Handled),
            AdminCommand::TransferLeader { target } => self
                .transfer_leader(target)
                .map(|()| StepResult::Handled),
            AdminCommand::CompleteLeaderTransfer => self
                .try_complete_leader_transfer()
                .map(StepResult::LeaderTransferCompleted),
            AdminCommand::AbortLeaderTransfer { reason } => Ok(
                StepResult::LeaderTransferAborted(self.abort_leader_transfer(reason)),
            ),
            AdminCommand::FireFatalEvent { node_id, .. } => {
                let target = if self.leader_id() == Some(node_id) {
                    self.step_down(None)?
                } else {
                    None
                };
                self.set_node_healthy(node_id, false)?;
                Ok(StepResult::FatalEvent(target))
            }
            AdminCommand::StepDown { transferee } => {
                self.step_down(transferee).map(StepResult::StepDown)
            }
            AdminCommand::Resign { reason } => self
                .resign_leader(&reason)
                .map(StepResult::LeaderResigned),
            AdminCommand::TriggerSnapshot => self
                .trigger_snapshot()
                .map(StepResult::SnapshotTriggered),
            AdminCommand::SnapshotReady {
                snapshot_id,
                success,
            } => self
                .handle_snapshot_ready(&snapshot_id, success)
                .map(|()| StepResult::Handled),
            AdminCommand::SnapshotApplied { snapshot_id } => self
                .complete_snapshot_trigger(&snapshot_id)
                .map(|()| StepResult::Handled),
            AdminCommand::BeginSnapshotSend {
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            } => self
                .begin_snapshot_send_to(peer_id, snapshot_id, snapshot_index, total_chunks)
                .map(|()| StepResult::Handled),
            AdminCommand::RecordSnapshotChunkSent { peer_id, bytes } => self
                .record_snapshot_chunk_sent_to(peer_id, bytes)
                .map(|()| StepResult::Handled),
            AdminCommand::AcknowledgeSnapshotChunk { peer_id } => self
                .acknowledge_snapshot_chunk_to(peer_id)
                .map(|()| StepResult::Handled),
            AdminCommand::RetrySnapshotChunk { peer_id } => self
                .retry_snapshot_chunk_to(peer_id)
                .map(|()| StepResult::Handled),
            AdminCommand::CancelSnapshotSend { peer_id } => self
                .cancel_snapshot_send_to(peer_id)
                .map(|()| StepResult::Handled),
            AdminCommand::BeginSnapshotInstall {
                peer_id,
                snapshot_id,
                snapshot_index,
                total_chunks,
            } => self
                .begin_snapshot_install_from(peer_id, snapshot_id, snapshot_index, total_chunks)
                .map(|()| StepResult::Handled),
            AdminCommand::ReceiveSnapshotChunk {
                peer_id,
                bytes,
                done,
            } => self
                .receive_snapshot_chunk_from(peer_id, bytes, done)
                .map(|()| StepResult::Handled),
            AdminCommand::RollbackSnapshotInstall { peer_id } => self
                .rollback_snapshot_install_from(peer_id)
                .map(|()| StepResult::Handled),
            AdminCommand::ApplyResult {
                node_id,
                applied_index,
                rejected,
            } => self
                .submit_apply_result(node_id, applied_index, rejected)
                .map(|()| StepResult::Handled),
            AdminCommand::ApplyTaskInflight {
                node_id,
                applied_index,
            } => self
                .mark_apply_task_inflight(node_id, applied_index)
                .map(|()| StepResult::Handled),
            AdminCommand::StabledResult {
                first_index,
                last_index,
                stabled_membership_change_index,
            } => self
                .submit_stabled_result(first_index, last_index, stabled_membership_change_index)
                .map(|_| StepResult::Handled),
            AdminCommand::Replicated { peer_id, success } => self
                .record_replication_task_result_for(peer_id, success)
                .map(|_| StepResult::Handled),
            AdminCommand::CompactLogsThrough { log_index } => Ok(
                StepResult::CompactedLogs(self.compact_logs_through(log_index)),
            ),
            AdminCommand::CompactLogsWithStorageFence { log_index, fence } => self
                .compact_logs_with_storage_fence(log_index, fence)
                .map(StepResult::FencedCompaction),
            AdminCommand::CheckpointSnapshot {
                target,
                snapshot_id,
            } => self
                .checkpoint_snapshot(target, snapshot_id)
                .map(StepResult::CheckpointedSnapshot),
            AdminCommand::WitnessQuorum { acknowledgements } => Ok(
                StepResult::WitnessQuorum(self.witness_quorum_report(acknowledgements)),
            ),
            AdminCommand::PartitionPeer { peer_id } => self
                .partition_peer(peer_id)
                .map(|()| StepResult::Handled),
            AdminCommand::HealPeer { peer_id } => {
                self.heal_peer(peer_id).map(StepResult::CatchUpPeer)
            }
            AdminCommand::ReceiveOutOfOrderAppend { peer_id, entry } => self
                .receive_out_of_order_append_for(peer_id, entry)
                .map(|()| StepResult::Handled),
            AdminCommand::ExpirePeerReorderQueue { peer_id } => self
                .expire_peer_reorder_queue(peer_id)
                .map(StepResult::CompactedLogs),
            AdminCommand::SetNodeHealthy { node_id, healthy } => self
                .set_node_healthy(node_id, healthy)
                .map(|()| StepResult::Handled),
            AdminCommand::SetLeaderLeaseValid { valid } => {
                self.set_leader_lease_valid(valid);
                Ok(StepResult::Handled)
            }
            AdminCommand::ReceiveLeaderLeaseConfirmation {
                node_id,
                confirmation_epoch,
                duration_ms,
            } => {
                let confirmed = match duration_ms {
                    Some(duration_ms) => self.receive_leader_lease_confirmation_with_duration(
                        node_id,
                        confirmation_epoch,
                        duration_ms,
                    ),
                    None => self.receive_leader_lease_confirmation(node_id, confirmation_epoch),
                };
                Ok(StepResult::LeaderLeaseConfirmed(confirmed))
            }
            AdminCommand::TickLeaderLease { elapsed_ms } => Ok(
                StepResult::LeaderLeaseExpired(self.tick_leader_lease(elapsed_ms)),
            ),
            AdminCommand::ReceiveFollowerLease { epoch } => Ok(
                StepResult::FollowerLeaseReceived(self.receive_follower_lease_item(epoch)),
            ),
            AdminCommand::TickFollowerLease { elapsed_ms } => Ok(
                StepResult::FollowerLeaseExpired(self.tick_follower_lease(elapsed_ms)),
            ),
            AdminCommand::ProhibitsElection { prohibits } => {
                self.set_prohibits_election(prohibits);
                Ok(StepResult::Handled)
            }
            AdminCommand::IgnoreWitness { ignore } => {
                self.set_ignore_witness(ignore);
                Ok(StepResult::Handled)
            }
            AdminCommand::ReleaseMemory => self
                .release_memory()
                .map(StepResult::ReleasedMemory),
        }
    }

    pub fn step_batch(
        &mut self,
        messages: Vec<Message>,
    ) -> Result<Vec<StepResult>, RaftError> {
        messages
            .into_iter()
            .map(|message| self.step(message))
            .collect()
    }

    pub fn pre_vote(&self, candidate_id: NodeId) -> Result<VoteResponse, RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        if self.prohibits_election {
            return Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
                reason: "election_prohibited".to_string(),
            });
        }
        if !self.config.enable_pre_vote {
            return Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
                reason: "pre_vote_disabled".to_string(),
            });
        }
        if !candidate.replica_role.can_be_leader() {
            return Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
                reason: "candidate_cannot_be_leader".to_string(),
            });
        }
        if !candidate.healthy {
            return Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
                reason: "candidate_unhealthy".to_string(),
            });
        }
        Ok(VoteResponse {
            term: self.current_term + 1,
            vote_granted: true,
            reason: "pre_vote_granted".to_string(),
        })
    }

    pub fn vote_to(
        &mut self,
        target: NodeId,
        request: VoteRequest,
    ) -> Result<VoteResponse, RaftError> {
        if request.group_id != self.group_id {
            return Ok(VoteResponse {
                term: self.current_term.max(request.term),
                vote_granted: false,
                reason: "group_id_mismatch".to_string(),
            });
        }
        let target_replica_role = {
            let target_node = self
                .nodes
                .get(&target)
                .ok_or(RaftError::NodeNotFound(target))?;
            target_node.replica_role
        };
        if !target_replica_role.participates_in_quorum() {
            return Ok(VoteResponse {
                term: self.current_term.max(request.term),
                vote_granted: false,
                reason: "target_cannot_vote".to_string(),
            });
        }
        self.observe_vote_request_candidate(target, request.candidate_id, request.pre_vote);
        let (target_in_lease, target_current_term) = {
            let target_node = self
                .nodes
                .get(&target)
                .ok_or(RaftError::NodeNotFound(target))?;
            (
                request.term > target_node.hard_state.current_term
                    && !request.force
                    && self.config.enable_lease_read
                    && if target_node.raft_role == StateRole::Leader {
                        self.leader_lease_valid
                    } else {
                        self.is_follower_lease_valid()
                    },
                target_node.hard_state.current_term,
            )
        };
        if target_in_lease {
            return Ok(VoteResponse {
                term: target_current_term,
                vote_granted: false,
                reason: "in_lease".to_string(),
            });
        }
        if request.pre_vote {
            if !self.config.enable_pre_vote {
                return Ok(VoteResponse {
                    term: target_current_term,
                    vote_granted: false,
                    reason: "pre_vote_disabled".to_string(),
                });
            }
            let Some(candidate) = self.nodes.get(&request.candidate_id) else {
                return Ok(VoteResponse {
                    term: target_current_term,
                    vote_granted: false,
                    reason: "candidate_not_member".to_string(),
                });
            };
            if !candidate.replica_role.can_be_leader() {
                return Ok(VoteResponse {
                    term: target_current_term,
                    vote_granted: false,
                    reason: "candidate_cannot_be_leader".to_string(),
                });
            }
            if !candidate.healthy {
                return Ok(VoteResponse {
                    term: target_current_term,
                    vote_granted: false,
                    reason: "candidate_unhealthy".to_string(),
                });
            }
            let target_node = self
                .nodes
                .get(&target)
                .ok_or(RaftError::NodeNotFound(target))?;
            if request.term <= target_node.hard_state.current_term {
                return Ok(VoteResponse {
                    term: target_node.hard_state.current_term,
                    vote_granted: false,
                    reason: "stale_pre_vote_term".to_string(),
                });
            }
            if !target_node.is_fresh_candidate_log(request.last_log_id.as_ref()) {
                return Ok(VoteResponse {
                    term: target_node.hard_state.current_term,
                    vote_granted: false,
                    reason: "candidate_log_stale".to_string(),
                });
            }
            return Ok(VoteResponse {
                term: request.term,
                vote_granted: true,
                reason: "pre_vote_granted".to_string(),
            });
        }
        {
            let Some(candidate) = self.nodes.get(&request.candidate_id) else {
                return Ok(VoteResponse {
                    term: self.current_term.max(request.term),
                    vote_granted: false,
                    reason: "candidate_not_member".to_string(),
                });
            };
            if !candidate.replica_role.can_be_leader() {
                return Ok(VoteResponse {
                    term: self.current_term.max(request.term),
                    vote_granted: false,
                    reason: "candidate_cannot_be_leader".to_string(),
                });
            }
            if !candidate.healthy {
                return Ok(VoteResponse {
                    term: self.current_term.max(request.term),
                    vote_granted: false,
                    reason: "candidate_unhealthy".to_string(),
                });
            }
        }
        if request.term > self.current_term {
            self.current_term = request.term;
            self.leader_id = None;
            self.clear_reorder_queues();
            self.invalidate_follower_lease();
            self.invalidate_leader_lease();
            let target_node = self
                .nodes
                .get_mut(&target)
                .ok_or(RaftError::NodeNotFound(target))?;
            target_node.hard_state.current_term = request.term;
            target_node.hard_state.voted_for = None;
            for node in self.nodes.values_mut() {
                if node.replica_role == ReplicaRole::Learner {
                    node.raft_role = StateRole::Learner;
                } else {
                    node.raft_role = StateRole::Follower;
                }
            }
        }
        let target_node = self
            .nodes
            .get(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        let known_leader_blocks_vote = self.leader_id.is_some();
        if known_leader_blocks_vote
            && target_node.hard_state.voted_for != Some(request.candidate_id)
        {
            return Ok(VoteResponse {
                term: target_node.hard_state.current_term,
                vote_granted: false,
                reason: "known_leader".to_string(),
            });
        }
        if !target_node.is_fresh_candidate_log(request.last_log_id.as_ref()) {
            return Ok(VoteResponse {
                term: self.current_term.max(request.term),
                vote_granted: false,
                reason: "candidate_log_stale".to_string(),
            });
        }
        let mut reset_election_leases = false;
        let response_term = {
            let target_node = self
                .nodes
                .get_mut(&target)
                .ok_or(RaftError::NodeNotFound(target))?;
            if request.term < target_node.hard_state.current_term {
                return Ok(VoteResponse {
                    term: target_node.hard_state.current_term,
                    vote_granted: false,
                    reason: "stale_term".to_string(),
                });
            }
            if request.term > target_node.hard_state.current_term {
                target_node.hard_state.current_term = request.term;
                target_node.hard_state.voted_for = None;
                reset_election_leases = true;
            }
            if known_leader_blocks_vote
                && target_node.hard_state.voted_for != Some(request.candidate_id)
            {
                return Ok(VoteResponse {
                    term: target_node.hard_state.current_term,
                    vote_granted: false,
                    reason: "known_leader".to_string(),
                });
            }
            if target_node
                .hard_state
                .voted_for
                .is_some_and(|voted_for| voted_for != request.candidate_id)
            {
                return Ok(VoteResponse {
                    term: target_node.hard_state.current_term,
                    vote_granted: false,
                    reason: "already_voted".to_string(),
                });
            }
            target_node.hard_state.voted_for = Some(request.candidate_id);
            target_node.hard_state.current_term
        };
        if reset_election_leases {
            self.clear_reorder_queues();
            self.invalidate_follower_lease();
            self.invalidate_leader_lease();
        }
        self.current_term = self.current_term.max(response_term);
        Ok(VoteResponse {
            term: response_term,
            vote_granted: true,
            reason: "vote_granted".to_string(),
        })
    }

    fn observe_vote_request_candidate(
        &mut self,
        target: NodeId,
        candidate_id: NodeId,
        pre_vote: bool,
    ) {
        let target_is_leader = self
            .nodes
            .get(&target)
            .is_some_and(|node| node.raft_role == StateRole::Leader);
        if !target_is_leader {
            return;
        }
        if let Some(candidate) = self.nodes.get_mut(&candidate_id) {
            if candidate.replica_role.can_be_leader() {
                candidate.raft_role = if pre_vote {
                    StateRole::PreCandidate
                } else {
                    StateRole::Candidate
                };
            }
        }
    }

    pub fn handle_vote_response(
        &mut self,
        local_node_id: NodeId,
        response: VoteResponse,
        pre_vote: bool,
    ) -> Result<(), RaftError> {
        self.handle_vote_response_inner(local_node_id, None, response, pre_vote)
    }

    pub fn handle_vote_response_from(
        &mut self,
        local_node_id: NodeId,
        peer_id: NodeId,
        response: VoteResponse,
        pre_vote: bool,
    ) -> Result<(), RaftError> {
        self.handle_vote_response_inner(local_node_id, Some(peer_id), response, pre_vote)
    }

    fn handle_vote_response_inner(
        &mut self,
        local_node_id: NodeId,
        peer_id: Option<NodeId>,
        response: VoteResponse,
        pre_vote: bool,
    ) -> Result<(), RaftError> {
        self.nodes
            .get(&local_node_id)
            .ok_or(RaftError::NodeNotFound(local_node_id))?;
        if response.term <= self.current_term {
            if let Some(peer_id) = peer_id {
                self.record_vote_response(local_node_id, peer_id, response.vote_granted, pre_vote)?;
            }
            return Ok(());
        }
        if let Some(peer_id) = peer_id {
            if !self.peer_response_is_active(peer_id)? {
                return Ok(());
            }
        }
        if pre_vote && response.vote_granted {
            if let Some(peer_id) = peer_id {
                self.record_vote_response(local_node_id, peer_id, true, pre_vote)?;
            }
            return Ok(());
        }
        self.current_term = response.term;
        self.leader_id = None;
        self.clear_reorder_queues();
        self.invalidate_follower_lease();
        self.invalidate_leader_lease();
        self.clear_election_responses();
        for node in self.nodes.values_mut() {
            node.hard_state.current_term = response.term;
            node.hard_state.voted_for = None;
            node.raft_role = if node.replica_role == ReplicaRole::Learner {
                StateRole::Learner
            } else {
                StateRole::Follower
            };
        }
        Ok(())
    }

    fn record_vote_response(
        &mut self,
        local_node_id: NodeId,
        peer_id: NodeId,
        granted: bool,
        pre_vote: bool,
    ) -> Result<(), RaftError> {
        let peer = self
            .nodes
            .get(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?;
        if !peer.replica_role.participates_in_quorum()
            || (self.ignore_witness && peer.replica_role == ReplicaRole::Witness)
        {
            return Ok(());
        }
        if pre_vote {
            self.pre_vote_responses.entry(local_node_id).or_insert(true);
            self.pre_vote_responses.insert(peer_id, granted);
        } else {
            self.vote_responses.entry(local_node_id).or_insert(true);
            self.vote_responses.insert(peer_id, granted);
        }

        let votes = if pre_vote {
            &self.pre_vote_responses
        } else {
            &self.vote_responses
        };
        let membership = self.membership();
        let granted_ids = votes
            .iter()
            .filter_map(|(node_id, granted)| granted.then_some(*node_id));
        if membership.quorum_reached_with_witness_policy(granted_ids, self.ignore_witness) {
            if pre_vote {
                self.start_vote_after_pre_vote_quorum(local_node_id)?;
            } else {
                self.become_leader_current_term(local_node_id)?;
            }
            return Ok(());
        }

        let rejected = votes.values().filter(|granted| !**granted).count();
        let participants = membership.voters.len() + membership.witnesses.len();
        if rejected
            > participants
                .saturating_sub(membership.quorum_size_with_witness_policy(self.ignore_witness))
        {
            self.leader_id = None;
            self.invalidate_follower_lease();
            self.invalidate_leader_lease();
            self.clear_election_responses();
            if let Some(node) = self.nodes.get_mut(&local_node_id) {
                node.raft_role = StateRole::Follower;
            }
        }
        Ok(())
    }

    pub fn handle_append_entries_response(
        &mut self,
        local_node_id: NodeId,
        peer_id: NodeId,
        response: AppendEntriesResponse,
    ) -> Result<(), RaftError> {
        self.handle_append_entries_response_inner(local_node_id, peer_id, response, true)
    }

    /// `recompute_aggregates` exists so a caller applying a whole round of
    /// responses can recompute the two group-wide aggregates -- the commit
    /// index and the leader-lease quorum -- once at the end, instead of once
    /// per response.
    ///
    /// Both walk every node: `refresh_commit_index` collects, sorts and
    /// re-walks the node set, and `leader_lease_quorum_reached` walks every
    /// confirmation and looks up its node. Doing either per response makes a
    /// round of N responses cost O(N^2). Both results depend only on the final
    /// state, so the intermediate passes cannot change the outcome -- only how
    /// long it takes to reach it.
    fn handle_append_entries_response_inner(
        &mut self,
        local_node_id: NodeId,
        peer_id: NodeId,
        response: AppendEntriesResponse,
        recompute_aggregates: bool,
    ) -> Result<(), RaftError> {
        self.nodes
            .get(&local_node_id)
            .ok_or(RaftError::NodeNotFound(local_node_id))?;
        if response.term > self.current_term && !self.peer_response_is_active(peer_id)? {
            return Ok(());
        }
        self.mark_peer_active(peer_id)?;
        if let Some(peer) = self.nodes.get_mut(&peer_id) {
            if peer_id != local_node_id && peer.replica_role.can_be_leader() {
                peer.raft_role = StateRole::Follower;
            } else if peer.replica_role == ReplicaRole::Learner {
                peer.raft_role = StateRole::Learner;
            }
        }
        if response.term > self.current_term {
            self.current_term = response.term;
            self.leader_id = None;
            self.clear_reorder_queues();
            self.invalidate_follower_lease();
            self.invalidate_leader_lease();
            self.abort_leader_transfer("append_response_high_term");
            for node in self.nodes.values_mut() {
                node.hard_state.current_term = response.term;
                node.hard_state.voted_for = None;
                node.raft_role = if node.replica_role == ReplicaRole::Learner {
                    StateRole::Learner
                } else {
                    StateRole::Follower
                };
            }
            return Ok(());
        }
        if response.lease_confirmation_epoch > 0 && response.lease_duration_ms > 0 {
            let _ = self.receive_leader_lease_confirmation_inner(
                peer_id,
                response.lease_confirmation_epoch,
                response.lease_duration_ms,
                recompute_aggregates,
            );
        } else if self.config.enable_lease_read {
            self.record_leader_lease_confirmation_inner(peer_id, recompute_aggregates);
        }
        let mut append_response_result = Ok(());
        if let Some(pipeline) = self.peer_pipelines.get_mut(&peer_id) {
            pipeline.update_snapshot_progress(
                response.snapshot_state == SnapshotState::Receiving,
                self.config.heartbeat_interval_ms.max(1),
                self.config.election_timeout_ms.max(1),
            );
            append_response_result = pipeline.handle_append_response(&response);
            if recompute_aggregates {
                self.refresh_commit_index();
            }
        }
        if let Some(required_snapshot_index) = response.require_snapshot {
            self.trigger_new_snapshot_if_leader_snapshot_is_stale(
                peer_id,
                required_snapshot_index,
            )?;
        }
        let retry_append_after_rejection = match &append_response_result {
            Err(RaftError::InvalidRequest(reason))
                if !response.success && reason == "append rejected by peer pipeline" =>
            {
                self.leader_id.is_some()
                    && self
                        .peer_pipelines
                        .get(&peer_id)
                        .map(|pipeline| {
                            let status = pipeline.status();
                            !status.paused
                                && !status.snapshot_sending
                                && !status.snapshot_installing
                                && status.required_snapshot_index == status.acked_snapshot_index
                        })
                        .unwrap_or(false)
            }
            _ => false,
        };
        match append_response_result {
            Err(RaftError::InvalidRequest(reason))
                if !response.success && reason == "append rejected by peer pipeline" => {}
            other => other?,
        }
        if retry_append_after_rejection {
            let _ = self.catch_up_peer_with_reason(peer_id, "append_reject_retry")?;
        }
        if response.success {
            self.maybe_auto_promote_zero_lag_learner(peer_id)?;
        }
        if response.success
            && self
                .leader_transfer
                .as_ref()
                .is_some_and(|transfer| transfer.transferee_id == peer_id)
            && response.match_index >= self.last_log_index
        {
            self.campaign(peer_id, true)?;
        }
        Ok(())
    }

    fn trigger_new_snapshot_if_leader_snapshot_is_stale(
        &mut self,
        peer_id: NodeId,
        required_snapshot_index: LogIndex,
    ) -> Result<(), RaftError> {
        let Some(leader_id) = self.leader_id else {
            return Ok(());
        };
        let leader_snapshot_index = self
            .nodes
            .get(&leader_id)
            .and_then(|leader| leader.installed_snapshot.as_ref())
            .map(|snapshot| snapshot.last_log_id.index)
            .unwrap_or_default();
        if leader_snapshot_index >= required_snapshot_index {
            return Ok(());
        }
        if let Some(pipeline) = self.peer_pipelines.get_mut(&peer_id) {
            pipeline.cancel_snapshot_send_for_new_snapshot();
        }
        if !self.snapshot_trigger_status().in_progress {
            let _ = self.trigger_snapshot()?;
        }
        Ok(())
    }

    fn maybe_auto_promote_zero_lag_learner(
        &mut self,
        learner_id: NodeId,
    ) -> Result<(), RaftError> {
        let should_promote = self
            .nodes
            .get(&learner_id)
            .map(|node| {
                node.replica_role == ReplicaRole::Learner
                    && node.auto_promote
                    && node.match_index() > 0
                    && node.match_index() >= self.last_log_index
            })
            .unwrap_or(false);
        if should_promote {
            if self.membership_change_fence_active() {
                if let Some(learner) = self.nodes.get_mut(&learner_id) {
                    learner.auto_promote_state = LearnerAutoPromoteState::Promoting;
                }
            } else {
                self.promote_peer(learner_id)?;
            }
        }
        Ok(())
    }

    pub fn handle_install_snapshot_response(
        &mut self,
        local_node_id: NodeId,
        peer_id: NodeId,
        response: InstallSnapshotResponse,
    ) -> Result<(), RaftError> {
        self.nodes
            .get(&local_node_id)
            .ok_or(RaftError::NodeNotFound(local_node_id))?;
        if response.term > self.current_term && !self.peer_response_is_active(peer_id)? {
            return Ok(());
        }
        if response.term <= self.current_term {
            if response.term == self.current_term {
                let snapshot_send_active = self
                    .peer_pipelines
                    .get(&peer_id)
                    .map(|pipeline| pipeline.status().snapshot_sending)
                    .unwrap_or(false);
                if snapshot_send_active {
                    self.handle_snapshot_finish_from(
                        peer_id,
                        response.accepted,
                        response.committed_index,
                    )?;
                    if !response.accepted && !self.snapshot_trigger_status().in_progress {
                        let _ = self.trigger_snapshot()?;
                    }
                }
            }
            return Ok(());
        }
        self.current_term = response.term;
        self.leader_id = None;
        self.clear_reorder_queues();
        self.invalidate_follower_lease();
        self.invalidate_leader_lease();
        self.abort_leader_transfer("snapshot_response_high_term");
        for node in self.nodes.values_mut() {
            node.hard_state.current_term = response.term;
            node.hard_state.voted_for = None;
            node.raft_role = if node.replica_role == ReplicaRole::Learner {
                StateRole::Learner
            } else {
                StateRole::Follower
            };
        }
        Ok(())
    }

    pub fn propose(&mut self, payload: Payload) -> Result<LogId, RaftError> {
        self.propose_with_options(payload, ProposeOptions::default())
    }

    pub fn propose_with_options(
        &mut self,
        payload: Payload,
        options: ProposeOptions,
    ) -> Result<LogId, RaftError> {
        if !options.is_membership_change {
            if let Some(expected_term) = options.expected_term {
                if expected_term != self.current_term {
                    return Err(RaftError::InvalidRequest(format!(
                        "expected term {} does not match current term {}",
                        expected_term, self.current_term
                    )));
                }
            }
        }
        let leader_id = self.leader_id.ok_or(RaftError::NoLeader)?;
        if !self.running {
            return Err(RaftError::InvalidRequest(
                "cannot propose while cluster is stopped".to_string(),
            ));
        }
        if payload.len() as u64 > self.config.max_payload_bytes {
            return Err(RaftError::InvalidRequest(
                "payload exceeds max_payload_bytes".to_string(),
            ));
        }
        let leader = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::NodeNotFound(leader_id))?;
        if !leader.healthy {
            return Err(RaftError::NotLeader(leader_id));
        }
        let leader_safety_applied_index = leader.safety_applied_index;
        if self.should_release_memory() {
            let _ = self.release_memory()?;
        }
        if self.is_busy() && !self.release_memory()? {
            return Err(RaftError::InvalidRequest(
                "out of memory buffer".to_string(),
            ));
        }

        let log_id = LogId {
            term: self.current_term,
            index: self.last_log_index + 1,
        };
        let membership_change_is_pending = self
            .pending_membership_change_index
            .is_some_and(|pending| pending > leader_safety_applied_index);
        let proposed_as_membership_change =
            options.is_membership_change && !membership_change_is_pending;
        if proposed_as_membership_change {
            self.pending_membership_change_index = Some(log_id.index);
            self.membership_change_indexes.insert(log_id.index);
        }
        let entry = LogEntry {
            log_id: log_id.clone(),
            payload,
            is_command: options.is_command && !proposed_as_membership_change,
        };
        self.last_log_index = log_id.index;
        // One allocation for the payload, shared by every node's log, rather
        // than a clone per node.
        let entry = Arc::new(entry);

        let mut lease_acknowledgements = vec![leader_id];
        let node_ids: Vec<_> = self.nodes.keys().copied().collect();
        for node_id in node_ids {
            let Some(node) = self.nodes.get_mut(&node_id) else {
                continue;
            };
            if node_id == leader_id {
                node.append_entry(Arc::clone(&entry));
                continue;
            }
            let response = if let Some(pipeline) = self.peer_pipelines.get_mut(&node_id) {
                pipeline.queue_append(&entry)?;
                let _ = pipeline.flush_append_batch(64, self.config.max_payload_bytes.max(1));
                if node.healthy {
                    let match_index = node.match_index();
                    let append_is_contiguous = match_index.saturating_add(1) == entry.log_id.index;
                    if append_is_contiguous {
                        if node.replica_role.can_serve_data() {
                            node.append_entry(Arc::clone(&entry));
                        } else if node.replica_role == ReplicaRole::Witness {
                            node.append_witness_entry(&entry, proposed_as_membership_change);
                        }
                    }
                    let match_index = node.match_index();
                    Some(AppendEntriesResponse {
                        term: node.hard_state.current_term,
                        success: append_is_contiguous,
                        match_index,
                        rejection_hint: (!append_is_contiguous)
                            .then_some(match_index.saturating_add(1)),
                        rejected_index: (!append_is_contiguous).then_some(entry.log_id.index),
                        require_snapshot: None,
                        snapshot_state: SnapshotState::None,
                        lease_confirmation_epoch: 0,
                        lease_duration_ms: 0,
                    })
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(response) = response {
                if response.success {
                    lease_acknowledgements.push(node_id);
                }
                // Defer the commit-index refresh: this loop applies one response
                // per node and then refreshes once below, so refreshing inside
                // each response made a proposal cost O(N^2 log N) in group size.
                let _ = self.handle_append_entries_response_inner(
                    leader_id, node_id, response, false,
                );
            }
        }
        self.refresh_commit_index();
        self.renew_leader_lease_from_acknowledgements(lease_acknowledgements);
        Ok(log_id)
    }

    pub fn wal_record_for(&self, node_id: NodeId) -> Result<WalRecord, RaftError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        let installed_snapshot = node.installed_snapshot.clone();
        let snapshot_index = installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_log_id.index)
            .unwrap_or_default();
        let mut record = WalRecord {
            entries_are_delta: false,
            group_id: self.group_id,
            node_id,
            hard_state: node.hard_state.clone(),
            membership: self.membership(),
            entries: node.log_entries(),
            installed_snapshot,
            apply_snapshot_fence: ApplySnapshotFence {
                applied_index: node.applied_index,
                commit_index: node.commit_index,
                installed_snapshot_index: snapshot_index,
                first_retained_log_index: if snapshot_index > 0 {
                    snapshot_index + 1
                } else {
                    node.log
                        .first()
                        .map(|entry| entry.log_id.index)
                        .unwrap_or_default()
                },
            },
            checksum: String::new(),
        };
        record.checksum = matrixraft_wal_checksum(&record);
        Ok(record)
    }

    /// Builds a WAL record carrying only the entries a WAL does not already
    /// hold.
    ///
    /// `coverage` is what the WAL's active segment already describes, as
    /// (first index, last index, term at the last index). When the node's log
    /// still extends that, only the tail past it is copied; otherwise -- a log
    /// truncated and rewritten, a tail rewritten under a newer term, or a log
    /// compacted past where the segment starts -- the whole log is copied, and
    /// the record says so. Passing `None` always copies the whole log.
    ///
    /// This is what keeps the per-proposal cost off the length of the log: the
    /// whole log is only materialised when a whole record is actually needed.
    pub fn wal_record_for_coverage(
        &self,
        node_id: NodeId,
        coverage: Option<(LogIndex, LogIndex, Term)>,
    ) -> Result<WalRecord, RaftError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        let extends = coverage.and_then(|(first_index, last_index, last_term)| {
            let log_first = node.log.first()?.log_id.index;
            if log_first > first_index {
                return None;
            }
            let position = node.log_position(last_index)?;
            if node.log[position].log_id.term != last_term {
                return None;
            }
            Some(position + 1)
        });
        let mut record = self.wal_record_shell(node_id, node);
        match extends {
            Some(from) => {
                record.entries = node.log_entries_from(from);
                record.entries_are_delta = true;
            }
            None => record.entries = node.log_entries(),
        }
        record.checksum = matrixraft_wal_checksum(&record);
        Ok(record)
    }

    /// Everything in a WAL record except the entries and the checksum.
    fn wal_record_shell(&self, node_id: NodeId, node: &Node) -> WalRecord {
        let installed_snapshot = node.installed_snapshot.clone();
        let snapshot_index = installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_log_id.index)
            .unwrap_or_default();
        WalRecord {
            entries_are_delta: false,
            group_id: self.group_id,
            node_id,
            hard_state: node.hard_state.clone(),
            membership: self.membership(),
            entries: Vec::new(),
            installed_snapshot,
            apply_snapshot_fence: ApplySnapshotFence {
                applied_index: node.applied_index,
                commit_index: node.commit_index,
                installed_snapshot_index: snapshot_index,
                first_retained_log_index: if snapshot_index > 0 {
                    snapshot_index + 1
                } else {
                    node.log
                        .first()
                        .map(|entry| entry.log_id.index)
                        .unwrap_or_default()
                },
            },
            checksum: String::new(),
        }
    }

    pub fn restore_wal_record(&mut self, record: WalRecord) -> Result<(), RaftError> {
        if record.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "WAL record group id mismatch".to_string(),
            ));
        }
        let node = self
            .nodes
            .get_mut(&record.node_id)
            .ok_or(RaftError::NodeNotFound(record.node_id))?;
        node.hard_state = record.hard_state;
        node.set_log(record.entries);
        node.installed_snapshot = record.installed_snapshot;
        node.commit_index = record.apply_snapshot_fence.commit_index;
        node.applied_index = record.apply_snapshot_fence.applied_index;
        node.safety_applied_index = record.apply_snapshot_fence.applied_index;
        if node.replica_role == ReplicaRole::Witness {
            let restored_witness_index = node
                .hard_state
                .committed
                .as_ref()
                .map(|log_id| log_id.index)
                .unwrap_or_default()
                .max(node.commit_index)
                .max(
                    node.installed_snapshot
                        .as_ref()
                        .map(|snapshot| snapshot.last_log_id.index)
                        .unwrap_or_default(),
                )
                .max(
                    node.log
                        .last()
                        .map(|entry| entry.log_id.index)
                        .unwrap_or_default(),
                );
            node.acknowledge_witness_index(restored_witness_index);
        }
        self.last_log_index = self.last_log_index.max(node.match_index());
        self.current_term = self.current_term.max(node.hard_state.current_term);
        self.last_index_before_current_term = self.last_log_index;
        self.refresh_cluster_indexes();
        self.refresh_replication_pipelines();
        Ok(())
    }

    pub fn append_entries_to(
        &mut self,
        target: NodeId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.append_entries_with_membership_change_indexes_to(target, request, &[])
    }

    pub fn append_entries_with_membership_change_indexes_to(
        &mut self,
        target: NodeId,
        request: AppendEntriesRequest,
        membership_change_indexes: &[LogIndex],
    ) -> Result<AppendEntriesResponse, RaftError> {
        if request.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "append entries group id mismatch".to_string(),
            ));
        }
        let request_prev_index = request
            .prev_log_id
            .as_ref()
            .map(|prev| prev.index)
            .unwrap_or_default();
        if !self
            .nodes
            .get(&request.leader_id)
            .map(|node| node.replica_role.can_be_leader() && node.healthy)
            .unwrap_or(false)
        {
            let node = self
                .nodes
                .get(&target)
                .ok_or(RaftError::NodeNotFound(target))?;
            let require_snapshot = node.rejected_apply_index;
            return Ok(AppendEntriesResponse {
                term: node.hard_state.current_term.max(self.current_term),
                success: false,
                match_index: node.match_index(),
                rejection_hint: Some(node.match_index().saturating_add(1)),
                rejected_index: Some(request_prev_index),
                require_snapshot,
                snapshot_state: SnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            });
        }
        if request.term < self.current_term {
            let node = self
                .nodes
                .get(&target)
                .ok_or(RaftError::NodeNotFound(target))?;
            let require_snapshot = node.rejected_apply_index;
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: node.match_index(),
                rejection_hint: Some(node.match_index().saturating_add(1)),
                rejected_index: Some(request_prev_index),
                require_snapshot,
                snapshot_state: SnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            });
        }
        let renew_follower_lease = self.config.enable_lease_read && target != request.leader_id;
        if request.term >= self.current_term && self.leader_id != Some(request.leader_id) {
            if request.term > self.current_term {
                self.last_index_before_current_term = self.last_log_index;
            }
            self.current_term = request.term;
            self.leader_id = Some(request.leader_id);
            self.clear_reorder_queues();
            if let Some(transferee_id) = self
                .leader_transfer
                .as_ref()
                .map(|transfer| transfer.transferee_id)
            {
                if transferee_id == request.leader_id {
                    self.leader_transfer = None;
                } else {
                    self.abort_leader_transfer("leader_changed_before_transfer_complete");
                }
            }
            self.reset_leader_lease_epoch();
            self.invalidate_follower_lease();
            for peer in self.nodes.values_mut() {
                peer.raft_role = if peer.id == request.leader_id {
                    StateRole::Leader
                } else if peer.replica_role == ReplicaRole::Learner {
                    StateRole::Learner
                } else {
                    StateRole::Follower
                };
            }
        }
        let (lease_confirmation_epoch, lease_duration_ms) = if renew_follower_lease {
            self.renew_follower_lease_from_append_entries(request.lease_epoch);
            (self.follower_lease_epoch, self.config.leader_lease_ms)
        } else {
            (0, 0)
        };
        let node = self
            .nodes
            .get_mut(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        if request.term < node.hard_state.current_term {
            let require_snapshot = node.rejected_apply_index;
            return Ok(AppendEntriesResponse {
                term: node.hard_state.current_term,
                success: false,
                match_index: node.match_index(),
                rejection_hint: Some(node.match_index().saturating_add(1)),
                rejected_index: Some(request_prev_index),
                require_snapshot,
                snapshot_state: SnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            });
        }
        if request.term > node.hard_state.current_term {
            node.hard_state.current_term = request.term;
            node.raft_role = if node.replica_role == ReplicaRole::Learner {
                StateRole::Learner
            } else {
                StateRole::Follower
            };
        }
        if self
            .peer_pipelines
            .get(&target)
            .map(|pipeline| pipeline.status().snapshot_installing)
            .unwrap_or(false)
        {
            let require_snapshot = node.rejected_apply_index;
            return Ok(AppendEntriesResponse {
                term: node.hard_state.current_term,
                success: true,
                match_index: node.commit_index,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot,
                snapshot_state: SnapshotState::Receiving,
                lease_confirmation_epoch,
                lease_duration_ms,
            });
        }
        if request.prev_log_id.is_none() && request.entries.is_empty() {
            let match_index = node.match_index();
            node.advance_commit(request.leader_commit.min(match_index));
            let term = node.hard_state.current_term;
            let require_snapshot = node.rejected_apply_index;
            self.refresh_cluster_indexes();
            return Ok(AppendEntriesResponse {
                term,
                success: true,
                match_index,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot,
                snapshot_state: SnapshotState::None,
                lease_confirmation_epoch,
                lease_duration_ms,
            });
        }
        if request_prev_index < node.commit_index {
            let require_snapshot = node.rejected_apply_index;
            return Ok(AppendEntriesResponse {
                term: node.hard_state.current_term,
                success: true,
                match_index: node.commit_index,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot,
                snapshot_state: SnapshotState::None,
                lease_confirmation_epoch,
                lease_duration_ms,
            });
        }
        if let Some(prev) = &request.prev_log_id {
            if prev.index > 0 {
                let match_index = node.match_index();
                if match_index < prev.index {
                    let term = node.hard_state.current_term;
                    let require_snapshot = node.rejected_apply_index;
                    if !request.entries.is_empty() {
                        self.cache_reordered_append(
                            target,
                            request.clone(),
                            membership_change_indexes,
                        );
                    }
                    return Ok(AppendEntriesResponse {
                        term,
                        success: false,
                        match_index,
                        rejection_hint: Some(match_index.saturating_add(1)),
                        rejected_index: Some(prev.index.saturating_add(1)),
                        require_snapshot,
                        snapshot_state: SnapshotState::None,
                        lease_confirmation_epoch,
                        lease_duration_ms,
                    });
                }
                match node.log_term_at(prev.index) {
                    Some(local_term) if local_term == prev.term => {}
                    Some(_) => {
                        let rejection_hint = node.conflict_next_index(prev.index);
                        let require_snapshot = node.rejected_apply_index;
                        return Ok(AppendEntriesResponse {
                            term: node.hard_state.current_term,
                            success: false,
                            match_index,
                            rejection_hint: Some(rejection_hint),
                            rejected_index: Some(prev.index.saturating_add(1)),
                            require_snapshot,
                            snapshot_state: SnapshotState::None,
                            lease_confirmation_epoch,
                            lease_duration_ms,
                        });
                    }
                    None => {
                        let snapshot_index = node
                            .installed_snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.last_log_id.index)
                            .unwrap_or_default();
                        let require_snapshot = node
                            .rejected_apply_index
                            .or((snapshot_index > 0 && prev.index < snapshot_index)
                                .then_some(snapshot_index));
                        return Ok(AppendEntriesResponse {
                            term: node.hard_state.current_term,
                            success: false,
                            match_index,
                            rejection_hint: Some(match_index.saturating_add(1)),
                            rejected_index: Some(prev.index.saturating_add(1)),
                            require_snapshot,
                            snapshot_state: SnapshotState::None,
                            lease_confirmation_epoch,
                            lease_duration_ms,
                        });
                    }
                }
            }
        }
        for (offset, entry) in request.entries.iter().enumerate() {
            let expected_index = request_prev_index + offset as u64 + 1;
            if entry.log_id.index != expected_index {
                let match_index = node.match_index();
                let require_snapshot = node.rejected_apply_index;
                return Ok(AppendEntriesResponse {
                    term: node.hard_state.current_term,
                    success: false,
                    match_index,
                    rejection_hint: Some(match_index.saturating_add(1)),
                    rejected_index: Some(entry.log_id.index),
                    require_snapshot,
                    snapshot_state: SnapshotState::None,
                    lease_confirmation_epoch,
                    lease_duration_ms,
                });
            }
        }

        node.hard_state.current_term = request.term;
        node.raft_role = if node.replica_role == ReplicaRole::Learner {
            StateRole::Learner
        } else {
            StateRole::Follower
        };
        let mut pending_membership_change_index = self.pending_membership_change_index;
        let mut last_index_before_current_term = self.last_index_before_current_term;
        let safety_applied_index = node.safety_applied_index;
        let mut last_retained_after_truncation = None;
        let mut clear_reorder_queue_after_truncation = false;
        let mut appended_entries_count = 0_u64;
        let last_append_index = request_prev_index.saturating_add(request.entries.len() as u64);
        let cap_busy_data_append_batch = node.replica_role.can_serve_data()
            && !request.entries.is_empty()
            && node.retained_log_bytes() >= self.config.max_log_buffer_bytes;
        for entry in request.entries {
            let is_membership_change = membership_change_indexes.contains(&entry.log_id.index);
            if node.replica_role.can_serve_data() {
                if let Some(local_term) = node.log_term_at(entry.log_id.index) {
                    if local_term == entry.log_id.term {
                        continue;
                    }
                    node.truncate_log_from(entry.log_id.index);
                    let last_retained = entry.log_id.index.saturating_sub(1);
                    last_index_before_current_term = last_index_before_current_term.min(last_retained);
                    clear_reorder_queue_after_truncation = true;
                    last_retained_after_truncation = Some(last_retained);
                }
                if is_membership_change {
                    if pending_membership_change_index
                        .is_some_and(|pending| safety_applied_index < pending)
                    {
                        if appended_entries_count == 0 {
                            let match_index = node.match_index();
                            let require_snapshot = node.rejected_apply_index;
                            return Ok(AppendEntriesResponse {
                                term: node.hard_state.current_term,
                                success: false,
                                match_index,
                                rejection_hint: Some(
                                    std::cmp::min(
                                        match_index,
                                        entry.log_id.index.saturating_sub(1),
                                    )
                                    .saturating_add(1),
                                ),
                                rejected_index: Some(entry.log_id.index),
                                require_snapshot,
                                snapshot_state: SnapshotState::None,
                                lease_confirmation_epoch,
                                lease_duration_ms,
                            });
                        }
                        break;
                    }
                    pending_membership_change_index = Some(entry.log_id.index);
                    self.membership_change_indexes.insert(entry.log_id.index);
                }
                node.append_entry(Arc::new(entry));
                appended_entries_count = appended_entries_count.saturating_add(1);
                if cap_busy_data_append_batch {
                    break;
                }
            } else if node.replica_role == ReplicaRole::Witness {
                node.append_witness_entry(&entry, is_membership_change);
                appended_entries_count = appended_entries_count.saturating_add(1);
            }
        }
        let match_index = if appended_entries_count == 0 {
            node.match_index().min(last_append_index)
        } else {
            node.match_index()
        };
        node.advance_commit(request.leader_commit.min(match_index));
        let node_commit_index = node.commit_index;
        let term = node.hard_state.current_term;
        let require_snapshot = node.rejected_apply_index;
        self.last_index_before_current_term = last_index_before_current_term;
        self.pending_membership_change_index = pending_membership_change_index;
        if let Some(last_retained) = last_retained_after_truncation {
            self.reset_pending_membership_change_after_truncation(last_retained, node_commit_index);
        }
        if clear_reorder_queue_after_truncation {
            self.clear_reorder_queue_for(target);
        }
        self.refresh_cluster_indexes();
        self.drain_reordered_appends(target)?;
        Ok(AppendEntriesResponse {
            term,
            success: true,
            match_index,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot,
            snapshot_state: SnapshotState::None,
            lease_confirmation_epoch,
            lease_duration_ms,
        })
    }

    fn cache_reordered_append(
        &mut self,
        target: NodeId,
        request: AppendEntriesRequest,
        membership_change_indexes: &[LogIndex],
    ) {
        let Some(prev_log_id) = request.prev_log_id.as_ref() else {
            return;
        };
        self.reorder_queues.entry(target).or_default().insert(
            prev_log_id.index,
            ReorderedAppend {
                request,
                membership_change_indexes: membership_change_indexes.to_vec(),
            },
        );
    }

    fn drain_reordered_appends(&mut self, target: NodeId) -> Result<(), RaftError> {
        loop {
            let match_index = self
                .nodes
                .get(&target)
                .ok_or(RaftError::NodeNotFound(target))?
                .match_index();
            let next_key = self
                .reorder_queues
                .get(&target)
                .and_then(|queue| queue.range(..=match_index).next().map(|(key, _)| *key));
            let Some(prev_index) = next_key else {
                break;
            };
            let Some(reordered) = self
                .reorder_queues
                .get_mut(&target)
                .and_then(|queue| queue.remove(&prev_index))
            else {
                break;
            };
            let response = self.append_entries_with_membership_change_indexes_to(
                target,
                reordered.request,
                &reordered.membership_change_indexes,
            )?;
            if !response.success {
                break;
            }
        }
        if self
            .reorder_queues
            .get(&target)
            .is_some_and(BTreeMap::is_empty)
        {
            self.reorder_queues.remove(&target);
        }
        Ok(())
    }

    pub fn read_index(
        &self,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        if request.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "read index group id mismatch".to_string(),
            ));
        }
        let node = self
            .nodes
            .get(&request.requester_id)
            .ok_or(RaftError::NodeNotFound(request.requester_id))?;
        if !node.healthy {
            return Ok(ReadIndexResponse {
                safe: false,
                read_index: node.commit_index,
                lease_read: false,
                reason: "node_unhealthy".to_string(),
            });
        }
        if self.leader_id != Some(request.requester_id) {
            return Ok(ReadIndexResponse {
                safe: false,
                read_index: node.commit_index,
                lease_read: false,
                reason: "not_leader".to_string(),
            });
        }
        let lease_read = request.allow_lease_read
            && self.config.enable_lease_read
            && self.leader_lease_valid;
        if !self.has_live_quorum() && !lease_read {
            return Ok(ReadIndexResponse {
                safe: false,
                read_index: node.commit_index,
                lease_read: false,
                reason: "no_live_quorum".to_string(),
            });
        }
        if request.min_commit_index > node.safety_applied_index {
            return Ok(ReadIndexResponse {
                safe: false,
                read_index: node.commit_index,
                lease_read: false,
                reason: "applied_index_behind_min_commit".to_string(),
            });
        }
        let read_index = node
            .commit_index
            .max(self.last_index_before_current_term.saturating_add(1));
        if read_index > node.safety_applied_index {
            return Ok(ReadIndexResponse {
                safe: false,
                read_index,
                lease_read: false,
                reason: "applied_index_behind_read_index".to_string(),
            });
        }
        Ok(ReadIndexResponse {
            safe: true,
            read_index,
            lease_read,
            reason: if lease_read {
                "lease_read".to_string()
            } else {
                "read_index".to_string()
            },
        })
    }

    pub fn read_path_report(
        &self,
        request: ReadIndexRequest,
        max_stale_index_lag: LogIndex,
    ) -> Result<ReadPathReport, RaftError> {
        if request.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "read path group id mismatch".to_string(),
            ));
        }
        let node = self
            .nodes
            .get(&request.requester_id)
            .ok_or(RaftError::NodeNotFound(request.requester_id))?;
        let quorum = self.read_quorum_report();
        let applied_index_fence = matrixraft_applied_index_fence_report(
            request.min_commit_index,
            node.commit_index,
            node.applied_index,
        );
        let lease_read_eligibility = matrixraft_lease_read_eligibility_report(
            request.requester_id,
            self.leader_id,
            self.config.enable_lease_read && request.allow_lease_read,
            self.leader_lease_valid,
            applied_index_fence.passed,
        );
        let bounded_stale = self.leader_id.map(|leader_id| {
            matrixraft_bounded_stale_read_report(
                request.requester_id,
                leader_id,
                node.commit_index,
                self.commit_index,
                max_stale_index_lag,
            )
        });
        let stale_leader_rejected = request.allow_lease_read
            && self.leader_id == Some(request.requester_id)
            && !self.leader_lease_valid;
        let read_index = if self.leader_id == Some(request.requester_id) {
            self.read_index(request)?
        } else {
            ReadIndexResponse {
                safe: node.healthy && quorum.reached && applied_index_fence.passed,
                read_index: node.commit_index,
                lease_read: false,
                reason: "bounded_stale_read".to_string(),
            }
        };
        let safe = read_index.safe
            && quorum.reached
            && applied_index_fence.passed
            && bounded_stale
                .as_ref()
                .map(|report| report.allowed)
                .unwrap_or(true);
        let reason = if !node.healthy {
            "node_unhealthy"
        } else if !quorum.reached {
            "no_live_quorum"
        } else if !applied_index_fence.passed {
            applied_index_fence.reason.as_str()
        } else if bounded_stale
            .as_ref()
            .map(|report| !report.allowed)
            .unwrap_or(false)
        {
            "replica_lagging"
        } else if stale_leader_rejected {
            "stale_leader_lease"
        } else if read_index.lease_read {
            "lease_read"
        } else {
            "read_index_quorum"
        };
        Ok(ReadPathReport {
            safe,
            read_index: read_index.read_index,
            lease_read: read_index.lease_read && safe,
            stale_leader_rejected,
            reason: reason.to_string(),
            quorum,
            applied_index_fence,
            lease_read_eligibility,
            bounded_stale,
        })
    }

    pub fn lease_read_eligible(
        &self,
        node_id: NodeId,
        min_commit_index: LogIndex,
    ) -> Result<bool, RaftError> {
        let response = self.read_index(ReadIndexRequest {
            group_id: self.group_id,
            requester_id: node_id,
            min_commit_index,
            allow_lease_read: true,
        })?;
        Ok(response.safe && response.lease_read)
    }

    pub fn read_quorum_report(&self) -> ReadQuorumReport {
        let membership = self.membership();
        let live_voters = membership
            .voters
            .iter()
            .copied()
            .filter(|node_id| {
                self.nodes
                    .get(node_id)
                    .map(|node| node.healthy)
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        let live_witnesses = if self.ignore_witness {
            Vec::new()
        } else {
            membership
                .witnesses
                .iter()
                .copied()
                .filter(|node_id| {
                    self.nodes
                        .get(node_id)
                        .map(|node| node.healthy)
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>()
        };
        let acknowledgements = live_voters
            .iter()
            .chain(live_witnesses.iter())
            .copied()
            .collect::<Vec<_>>();
        let required = membership.quorum_size_with_witness_policy(self.ignore_witness) as u64;
        ReadQuorumReport {
            required,
            live_voters,
            live_witnesses,
            reached: acknowledgements.len() as u64 >= required,
            acknowledgements,
        }
    }

    pub fn begin_leader_transfer(
        &mut self,
        target: NodeId,
    ) -> Result<Option<LeaderTransferState>, RaftError> {
        let Some(leader_id) = self.leader_id else {
            return Err(RaftError::NoLeader);
        };
        let Some(leader) = self.nodes.get(&leader_id) else {
            return Err(RaftError::NoLeader);
        };
        if leader.raft_role != StateRole::Leader || !leader.healthy {
            return Err(RaftError::NoLeader);
        }
        let current_transferee_id = self
            .leader_transfer
            .as_ref()
            .map(|transfer| transfer.transferee_id);
        let target_role = self.nodes.get(&target).map(|node| node.replica_role);
        let admission = crate::matrixraft_leader_transfer_admission(
            leader_id,
            target,
            current_transferee_id,
            target_role,
        );
        if admission.is_duplicate() {
            self.duplicate_leader_transfer_requests =
                self.duplicate_leader_transfer_requests.saturating_add(1);
            if let Some(transfer) = self.leader_transfer.as_mut() {
                transfer.duplicate_requests = transfer.duplicate_requests.saturating_add(1);
                transfer.reason = admission.reason;
            }
            return Ok(self.leader_transfer.clone());
        }
        if !admission.is_accepted() {
            return Ok(None);
        }
        let target_node = self
            .nodes
            .get(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        let target_caught_up = target_node.match_index() >= self.last_log_index;
        let aborted_transfers =
            self.aborted_leader_transfers + u64::from(self.leader_transfer.is_some());
        if self.leader_transfer.is_some() {
            self.aborted_leader_transfers = aborted_transfers;
        }
        self.leader_transfer = Some(LeaderTransferState {
            transferee_id: target,
            elapsed_ticks: 0,
            timeout_ticks: self.leader_transfer_timeout_ticks,
            aborted_transfers,
            duplicate_requests: self.duplicate_leader_transfer_requests,
            reason: if !target_node.healthy {
                "waiting_for_transferee_available".to_string()
            } else if target_caught_up {
                "transfer_ready".to_string()
            } else {
                "waiting_for_transferee_catchup".to_string()
            },
        });
        Ok(self.leader_transfer.clone())
    }

    pub fn transfer_leader(&mut self, target: NodeId) -> Result<(), RaftError> {
        self.transfer_leader_outcome(target).map(|_| ())
    }

    /// Transfer leadership, reporting which of the three things happened.
    ///
    /// `transfer_leader` collapses all three into `Ok(())`, which is why a
    /// report built from its return value cannot tell "leadership moved" from
    /// "the request was ignored".
    pub fn transfer_leader_outcome(
        &mut self,
        target: NodeId,
    ) -> Result<LeaderTransferOutcome, RaftError> {
        if self.begin_leader_transfer(target)?.is_none() {
            return Ok(LeaderTransferOutcome::Ignored);
        }
        if !self.leader_transfer_target_caught_up(target)? {
            return Ok(LeaderTransferOutcome::Pending);
        }
        self.campaign(target, true)?;
        self.leader_transfer = None;
        Ok(LeaderTransferOutcome::Transferred)
    }

    pub fn try_complete_leader_transfer(&mut self) -> Result<bool, RaftError> {
        let Some(target) = self
            .leader_transfer
            .as_ref()
            .map(|transfer| transfer.transferee_id)
        else {
            return Ok(false);
        };
        if !self.leader_transfer_target_caught_up(target)? {
            return Ok(false);
        }
        self.campaign(target, true)?;
        self.leader_transfer = None;
        Ok(true)
    }

    fn leader_transfer_target_caught_up(&self, target: NodeId) -> Result<bool, RaftError> {
        let target_node = self
            .nodes
            .get(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        Ok(target_node.healthy && target_node.match_index() >= self.last_log_index)
    }

    pub fn closest_follower(&self) -> Option<NodeId> {
        let leader_id = self.leader_id?;
        let max_match_index = self
            .nodes
            .values()
            .filter(|node| {
                node.id != leader_id && node.healthy && node.replica_role.can_be_leader()
            })
            .map(Node::match_index)
            .max()?;
        let mut candidates = self
            .nodes
            .values()
            .filter(|node| {
                node.id != leader_id
                    && node.healthy
                    && node.replica_role.can_be_leader()
                    && node.match_index() == max_match_index
            })
            .map(|node| node.id)
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates
            .iter()
            .copied()
            .find(|candidate| *candidate > leader_id)
            .or_else(|| candidates.first().copied())
    }

    pub fn step_down(
        &mut self,
        transferee: Option<NodeId>,
    ) -> Result<Option<NodeId>, RaftError> {
        let target = transferee
            .or_else(|| self.closest_follower())
            .ok_or_else(|| {
                RaftError::InvalidRequest("no healthy follower to step down to".into())
            })?;
        self.transfer_leader(target)?;
        Ok(Some(target))
    }

    pub fn resign_leader(&mut self, _reason: &str) -> Result<bool, RaftError> {
        let Some(leader_id) = self.leader_id else {
            return Ok(false);
        };
        let Some(leader) = self.nodes.get_mut(&leader_id) else {
            self.leader_id = None;
            self.invalidate_leader_lease();
            self.invalidate_follower_lease();
            self.abort_leader_transfer("resign_missing_leader");
            self.clear_election_responses();
            self.clear_reorder_queues();
            return Ok(false);
        };
        if leader.replica_role.can_be_leader() {
            leader.raft_role = StateRole::Follower;
            leader.auto_promote = false;
        }
        self.leader_id = None;
        self.invalidate_leader_lease();
        self.invalidate_follower_lease();
        self.abort_leader_transfer("resign_leader");
        self.clear_election_responses();
        self.clear_reorder_queues();
        self.refresh_replication_pipelines();
        Ok(true)
    }

    pub fn abort_leader_transfer(&mut self, reason: impl Into<String>) -> bool {
        if let Some(mut transfer) = self.leader_transfer.take() {
            transfer.reason = reason.into();
            self.aborted_leader_transfers = self.aborted_leader_transfers.saturating_add(1);
            true
        } else {
            false
        }
    }

    pub fn tick_leader_transfer(&mut self) -> bool {
        let Some(transfer) = self.leader_transfer.as_mut() else {
            return false;
        };
        transfer.elapsed_ticks = transfer.elapsed_ticks.saturating_add(1);
        if transfer.elapsed_ticks >= transfer.timeout_ticks {
            self.abort_leader_transfer("transfer_timeout")
        } else {
            false
        }
    }

    pub fn leader_transfer_state(&self) -> Option<LeaderTransferState> {
        self.leader_transfer.clone()
    }

    pub fn membership(&self) -> Membership {
        Membership {
            group_id: self.group_id,
            voters: self
                .nodes
                .values()
                .filter(|node| node.replica_role == ReplicaRole::Voter)
                .map(|node| node.id)
                .collect(),
            learners: self
                .nodes
                .values()
                .filter(|node| node.replica_role == ReplicaRole::Learner)
                .map(|node| node.id)
                .collect(),
            witnesses: self
                .nodes
                .values()
                .filter(|node| node.replica_role == ReplicaRole::Witness)
                .map(|node| node.id)
                .collect(),
            epoch: self.current_term,
        }
    }

    pub fn add_peer(&mut self, peer: Peer) -> Result<(), RaftError> {
        if self.nodes.contains_key(&peer.node_id) {
            return Err(RaftError::InvalidRequest(format!(
                "duplicate raft node id {}",
                peer.node_id
            )));
        }
        let peer_id = peer.node_id;
        self.snapshot_installers.remove(&peer_id);
        self.pending_snapshots.remove(&peer_id);
        self.clear_reorder_queue_for(peer_id);
        self.nodes.insert(
            peer_id,
            Node::new(peer_id, peer.role, peer.auto_promote),
        );
        self.peer_pipelines.insert(
            peer_id,
            ReplicationPipeline::new(
                peer_id,
                self.last_log_index + 1,
                PipelineLimits::default(),
            ),
        );
        self.reset_leader_lease_epoch();
        self.refresh_replication_pipelines();
        if self.leader_id.is_some_and(|leader_id| leader_id != peer_id) {
            let _ = self.catch_up_peer_with_reason(peer_id, "added_peer")?;
        }
        Ok(())
    }

    pub fn add_learner(&mut self, mut peer: Peer) -> Result<(), RaftError> {
        peer.role = ReplicaRole::Learner;
        self.add_peer(peer)
    }

    pub fn promote_peer(&mut self, node_id: NodeId) -> Result<(), RaftError> {
        let commit_index = self.commit_index;
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if node.replica_role != ReplicaRole::Learner {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is not a learner",
                node_id
            )));
        }
        if node.match_index() < commit_index {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is behind committed index {}",
                node_id, commit_index
            )));
        }
        node.replica_role = ReplicaRole::Voter;
        node.raft_role = StateRole::Follower;
        node.auto_promote = false;
        node.auto_promote_state = LearnerAutoPromoteState::Promoted;
        self.reset_leader_lease_epoch();
        Ok(())
    }

    pub fn add_witness(&mut self, mut peer: Peer) -> Result<(), RaftError> {
        peer.role = ReplicaRole::Witness;
        self.add_peer(peer)
    }

    pub fn remove_peer(&mut self, node_id: NodeId) -> Result<(), RaftError> {
        let removing_leader = self.leader_id == Some(node_id);
        let transfer_target = removing_leader.then(|| self.closest_follower()).flatten();
        if !self.nodes.contains_key(&node_id) {
            return Ok(());
        }
        self.nodes.remove(&node_id);
        self.peer_pipelines.remove(&node_id);
        self.snapshot_installers.remove(&node_id);
        self.pending_snapshots.remove(&node_id);
        self.clear_reorder_queue_for(node_id);
        self.remove_election_response_from(node_id);
        if self
            .leader_transfer
            .as_ref()
            .map(|transfer| transfer.transferee_id == node_id)
            .unwrap_or(false)
        {
            self.abort_leader_transfer("transferee_removed");
        }
        if self.leader_id == Some(node_id) {
            self.leader_id = None;
            self.invalidate_leader_lease();
            if let Some(target) = transfer_target {
                self.campaign(target, true)?;
            }
        } else {
            self.reset_leader_lease_epoch();
            self.refresh_commit_index();
        }
        self.refresh_replication_pipelines();
        Ok(())
    }

    fn reset_membership_from_snapshot(
        &mut self,
        target: NodeId,
        snapshot_membership: &[NodeId],
        snapshot_members: &[Peer],
    ) {
        let mut retained: BTreeSet<_> = snapshot_membership.iter().copied().collect();
        retained.extend(snapshot_members.iter().map(|peer| peer.node_id));
        if self.nodes.contains_key(&target) {
            retained.insert(target);
        }

        let removed = self
            .nodes
            .keys()
            .copied()
            .filter(|node_id| !retained.contains(node_id))
            .collect::<Vec<_>>();
        for node_id in removed {
            self.nodes.remove(&node_id);
            self.peer_pipelines.remove(&node_id);
            self.snapshot_installers.remove(&node_id);
            self.pending_snapshots.remove(&node_id);
            self.clear_reorder_queue_for(node_id);
            self.remove_election_response_from(node_id);
            if self
                .leader_transfer
                .as_ref()
                .is_some_and(|transfer| transfer.transferee_id == node_id)
            {
                self.abort_leader_transfer("snapshot_membership_removed_transferee");
            }
            if self.leader_id == Some(node_id) {
                self.leader_id = None;
                self.invalidate_leader_lease();
            }
        }

        for peer in snapshot_members {
            self.nodes
                .entry(peer.node_id)
                .and_modify(|node| {
                    node.replica_role = peer.role;
                    node.auto_promote =
                        peer.role == ReplicaRole::Learner && peer.auto_promote;
                    if node.replica_role == ReplicaRole::Learner {
                        node.raft_role = StateRole::Learner;
                    }
                })
                .or_insert_with(|| Node::new(peer.node_id, peer.role, peer.auto_promote));
        }
        for node_id in retained {
            self.nodes
                .entry(node_id)
                .or_insert_with(|| Node::new(node_id, ReplicaRole::Voter, false));
        }

        self.reset_leader_lease_epoch();
        self.refresh_commit_index();
        self.refresh_replication_pipelines();
        if let Some(leader_id) = self.leader_id {
            for node in self.nodes.values_mut() {
                node.raft_role = if node.id == leader_id {
                    StateRole::Leader
                } else if node.replica_role == ReplicaRole::Learner {
                    StateRole::Learner
                } else {
                    StateRole::Follower
                };
            }
        }
    }

    pub fn apply_committed_membership_operation(
        &mut self,
        operation: MembershipOperation,
    ) -> Result<bool, RaftError> {
        match operation {
            MembershipOperation::AddNode(peer) => self.apply_committed_add(peer),
            MembershipOperation::AddVoter(mut peer) => {
                peer.role = ReplicaRole::Voter;
                self.apply_committed_add(peer)
            }
            MembershipOperation::AddLearner(mut peer) => {
                peer.role = ReplicaRole::Learner;
                self.apply_committed_add(peer)
            }
            MembershipOperation::AddWitness(mut peer) => {
                peer.role = ReplicaRole::Witness;
                self.apply_committed_add(peer)
            }
            MembershipOperation::Promote(node_id) => {
                let node = self
                    .nodes
                    .get_mut(&node_id)
                    .ok_or(RaftError::NodeNotFound(node_id))?;
                if node.replica_role == ReplicaRole::Voter {
                    return Ok(false);
                }
                if node.replica_role != ReplicaRole::Learner {
                    return Err(RaftError::InvalidRequest(format!(
                        "node {} is not a learner",
                        node_id
                    )));
                }
                node.replica_role = ReplicaRole::Voter;
                node.raft_role = StateRole::Follower;
                node.auto_promote = false;
                node.auto_promote_state = LearnerAutoPromoteState::Promoted;
                self.reset_leader_lease_epoch();
                self.refresh_replication_pipelines();
                Ok(true)
            }
            MembershipOperation::Remove(node_id) => {
                if !self.nodes.contains_key(&node_id) {
                    return Ok(false);
                }
                self.remove_peer(node_id)?;
                Ok(true)
            }
            MembershipOperation::TransferLeader(node_id) => {
                self.transfer_leader(node_id)?;
                Ok(true)
            }
        }
    }

    fn apply_committed_add(&mut self, peer: Peer) -> Result<bool, RaftError> {
        match self.nodes.get_mut(&peer.node_id) {
            None => {
                self.add_peer(peer)?;
                Ok(true)
            }
            Some(existing)
                if existing.replica_role == ReplicaRole::Learner
                    && peer.role == ReplicaRole::Voter =>
            {
                existing.replica_role = ReplicaRole::Voter;
                existing.raft_role = StateRole::Follower;
                existing.auto_promote = false;
                existing.auto_promote_state = LearnerAutoPromoteState::Promoted;
                self.reset_leader_lease_epoch();
                self.refresh_replication_pipelines();
                Ok(true)
            }
            Some(_) => Ok(false),
        }
    }

    pub fn catchup_report(
        &self,
        learner_id: NodeId,
    ) -> Result<LearnerCatchUpReport, RaftError> {
        let node = self
            .nodes
            .get(&learner_id)
            .ok_or(RaftError::NodeNotFound(learner_id))?;
        Ok(self
            .membership()
            .catchup_report(learner_id, node.match_index(), self.commit_index))
    }

    pub fn learner_catch_up_loop(
        &mut self,
        learner_id: NodeId,
    ) -> Result<LearnerCatchUpLoopReport, RaftError> {
        let learner = self
            .nodes
            .get(&learner_id)
            .ok_or(RaftError::NodeNotFound(learner_id))?;
        if learner.replica_role != ReplicaRole::Learner {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is not a learner",
                learner_id
            )));
        }
        self.catch_up_peer_with_reason(learner_id, "learner")
    }

    pub fn catch_up_peer(
        &mut self,
        peer_id: NodeId,
    ) -> Result<LearnerCatchUpLoopReport, RaftError> {
        self.catch_up_peer_with_reason(peer_id, "peer")
    }

    pub fn broadcast_commit_index_to_old_paused_peers(&mut self) -> Result<u64, RaftError> {
        let Some(leader_id) = self.leader_id else {
            return Ok(0);
        };
        let peer_ids = self
            .peer_pipelines
            .keys()
            .copied()
            .filter(|peer_id| *peer_id != leader_id)
            .filter(|peer_id| {
                self.nodes
                    .get(peer_id)
                    .map(|node| node.healthy)
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        let mut sent = 0_u64;
        for peer_id in peer_ids {
            let should_send = self
                .peer_pipelines
                .get_mut(&peer_id)
                .map(|pipeline| {
                    let status = pipeline.status();
                    !status.snapshot_sending
                        && !status.snapshot_installing
                        && pipeline.take_empty_append_due_to_old_pause()
                })
                .unwrap_or(false);
            if !should_send {
                continue;
            }
            let peer_match_index = self
                .nodes
                .get(&peer_id)
                .map(Node::match_index)
                .unwrap_or_default();
            let request = AppendEntriesRequest {
                group_id: self.group_id,
                term: self.current_term,
                leader_id,
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: self.commit_index.min(peer_match_index),
                lease_epoch: self.leader_lease_epoch,
            };
            let response = self.append_entries_to(peer_id, request)?;
            self.handle_append_entries_response(leader_id, peer_id, response)?;
            sent = sent.saturating_add(1);
        }
        Ok(sent)
    }

    fn catch_up_peer_with_reason(
        &mut self,
        peer_id: NodeId,
        reason_prefix: &str,
    ) -> Result<LearnerCatchUpLoopReport, RaftError> {
        let leader_id = self.leader_id.ok_or(RaftError::NoLeader)?;
        let leader_snapshot = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::NodeNotFound(leader_id))?
            .installed_snapshot
            .clone();
        let leader_commit_index = self.commit_index;

        let peer = self
            .nodes
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?;
        let learner_match_index_before = peer.match_index();
        let mut installed_snapshot_index = None;
        if let Some(snapshot) = leader_snapshot {
            if snapshot.last_log_id.index > peer.match_index() {
                installed_snapshot_index = Some(snapshot.last_log_id.index);
                peer.installed_snapshot = Some(snapshot);
            }
        }
        let current_match = peer.match_index();

        // Copy only the tail the peer is missing rather than the whole leader
        // log, which this runs over once per lagging peer per tick.
        let leader = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::NodeNotFound(leader_id))?;
        let missing_tail: Vec<LogEntry> = leader
            .log_position_at_or_after(current_match.saturating_add(1))
            .map(|position| leader.log_entries_from(position))
            .unwrap_or_default();

        let peer = self
            .nodes
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?;
        for entry in missing_tail {
            if peer.replica_role == ReplicaRole::Witness {
                peer.append_witness_entry(&entry, false);
            } else {
                peer.append_entry(Arc::new(entry));
            }
        }
        peer.advance_commit(leader_commit_index.min(peer.match_index()));
        let learner_match_index_after = peer.match_index();

        let caught_up = if let Some(pipeline) = self.peer_pipelines.get_mut(&peer_id) {
            if let Some(snapshot_index) = installed_snapshot_index {
                let snapshot_id = format!("catchup-{peer_id}-{snapshot_index}");
                pipeline
                    .begin_snapshot_install(snapshot_id, snapshot_index, 1)
                    .ok();
                pipeline.receive_snapshot_chunk(0, true).ok();
                pipeline.mark_snapshot_rejoin_after_compacted_log();
            }
            let response = AppendEntriesResponse {
                term: self.current_term,
                success: true,
                match_index: learner_match_index_after,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: SnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            };
            let _ = pipeline.handle_append_response(&response);
            pipeline.record_learner_catchup_round(leader_commit_index)
        } else {
            learner_match_index_after >= leader_commit_index
        };
        self.mark_peer_active(peer_id)?;
        self.refresh_cluster_indexes();
        if caught_up {
            self.promote_caught_up_auto_learner(peer_id)?;
        }

        Ok(LearnerCatchUpLoopReport {
            learner_id: peer_id,
            leader_commit_index,
            learner_match_index_before,
            learner_match_index_after,
            rounds: self
                .peer_pipelines
                .get(&peer_id)
                .map(|pipeline| pipeline.status().learner_catchup_rounds)
                .unwrap_or_default(),
            caught_up,
            reason: if caught_up {
                format!("{reason_prefix}_caught_up")
            } else {
                format!("{reason_prefix}_still_lagging")
            },
        })
    }

    fn promote_caught_up_auto_learner(
        &mut self,
        learner_id: NodeId,
    ) -> Result<bool, RaftError> {
        let should_promote = self
            .nodes
            .get(&learner_id)
            .map(|learner| {
                learner.replica_role == ReplicaRole::Learner
                    && learner.auto_promote
                    && learner.match_index() > 0
                    && learner.match_index() >= self.commit_index
            })
            .unwrap_or(false);
        if !should_promote {
            return Ok(false);
        }
        if let Some(learner) = self.nodes.get_mut(&learner_id) {
            learner.auto_promote_state = LearnerAutoPromoteState::Promoting;
        }
        self.promote_peer(learner_id)?;
        Ok(true)
    }

    pub fn auto_promote_learner(
        &mut self,
        learner_id: NodeId,
    ) -> Result<LearnerAutoPromoteReport, RaftError> {
        let (auto_promote, state_before, learner_match_index) = {
            let learner = self
                .nodes
                .get(&learner_id)
                .ok_or(RaftError::NodeNotFound(learner_id))?;
            if learner.replica_role == ReplicaRole::Voter
                && learner.auto_promote_state == LearnerAutoPromoteState::Promoted
            {
                return Ok(LearnerAutoPromoteReport {
                    learner_id,
                    auto_promote: true,
                    state_before: LearnerAutoPromoteState::Promoted,
                    state_after: LearnerAutoPromoteState::Promoted,
                    catchup: None,
                    promoted: true,
                    reason: "learner_promoted".to_string(),
                });
            }
            if learner.replica_role != ReplicaRole::Learner {
                return Err(RaftError::InvalidRequest(format!(
                    "node {} is not a learner",
                    learner_id
                )));
            }
            (
                learner.auto_promote,
                learner.auto_promote_state,
                learner.match_index(),
            )
        };

        if !auto_promote {
            return Ok(LearnerAutoPromoteReport {
                learner_id,
                auto_promote,
                state_before,
                state_after: state_before,
                catchup: None,
                promoted: false,
                reason: "auto_promote_disabled".to_string(),
            });
        }

        if state_before == LearnerAutoPromoteState::Stop && learner_match_index == 0 {
            return Ok(LearnerAutoPromoteReport {
                learner_id,
                auto_promote,
                state_before,
                state_after: state_before,
                catchup: None,
                promoted: false,
                reason: "learner_has_no_matched_log".to_string(),
            });
        }

        let catchup_state = if state_before == LearnerAutoPromoteState::Promoting {
            LearnerAutoPromoteState::Promoting
        } else {
            LearnerAutoPromoteState::Check
        };
        if let Some(learner) = self.nodes.get_mut(&learner_id) {
            learner.auto_promote_state = catchup_state;
        }

        if let Some(learner) = self.nodes.get_mut(&learner_id) {
            learner.auto_promote = false;
        }
        let catchup = self.learner_catch_up_loop(learner_id);
        if let Some(learner) = self.nodes.get_mut(&learner_id) {
            learner.auto_promote = auto_promote;
        }
        let catchup = catchup?;
        if state_before == LearnerAutoPromoteState::Stop
            && learner_match_index < self.last_log_index
        {
            return Ok(LearnerAutoPromoteReport {
                learner_id,
                auto_promote,
                state_before,
                state_after: catchup_state,
                catchup: Some(catchup),
                promoted: false,
                reason: "learner_check_turn_started".to_string(),
            });
        }
        if !catchup.caught_up {
            return Ok(LearnerAutoPromoteReport {
                learner_id,
                auto_promote,
                state_before,
                state_after: catchup_state,
                catchup: Some(catchup),
                promoted: false,
                reason: "learner_still_lagging".to_string(),
            });
        }

        if self.membership_change_fence_active() {
            return Ok(LearnerAutoPromoteReport {
                learner_id,
                auto_promote,
                state_before,
                state_after: catchup_state,
                catchup: Some(catchup),
                promoted: false,
                reason: "membership_change_pending".to_string(),
            });
        }

        if self
            .nodes
            .get(&learner_id)
            .map(|node| node.replica_role == ReplicaRole::Voter)
            .unwrap_or(false)
        {
            return Ok(LearnerAutoPromoteReport {
                learner_id,
                auto_promote,
                state_before,
                state_after: LearnerAutoPromoteState::Promoted,
                catchup: Some(catchup),
                promoted: true,
                reason: "learner_promoted".to_string(),
            });
        }

        if let Some(learner) = self.nodes.get_mut(&learner_id) {
            learner.auto_promote_state = LearnerAutoPromoteState::Promoting;
        }
        self.promote_peer(learner_id)?;
        Ok(LearnerAutoPromoteReport {
            learner_id,
            auto_promote,
            state_before,
            state_after: LearnerAutoPromoteState::Promoted,
            catchup: Some(catchup),
            promoted: true,
            reason: "learner_promoted".to_string(),
        })
    }

    pub fn witness_quorum_report<I>(&mut self, acknowledgements: I) -> WitnessQuorumReport
    where
        I: IntoIterator<Item = NodeId>,
    {
        let membership = self.membership();
        let acknowledgements: Vec<_> = acknowledgements.into_iter().collect();
        let acknowledged = membership
            .voters
            .iter()
            .chain(
                (!self.ignore_witness)
                    .then_some(&membership.witnesses)
                    .into_iter()
                    .flatten(),
            )
            .filter(|node_id| acknowledgements.contains(node_id))
            .count() as u64;
        let required = membership.quorum_size_with_witness_policy(self.ignore_witness) as u64;
        let reached = acknowledged >= required;
        for witness_id in &membership.witnesses {
            if let Some(pipeline) = self.peer_pipelines.get_mut(witness_id) {
                pipeline.record_witness_quorum(acknowledged, required);
            }
        }
        WitnessQuorumReport {
            required,
            acknowledged,
            reached,
            voters: membership.voters,
            witnesses: membership.witnesses,
        }
    }

    pub fn install_snapshot_to(
        &mut self,
        target: NodeId,
        snapshot: RaftSnapshot,
        fence: ApplySnapshotFence,
    ) -> Result<(), RaftError> {
        if snapshot.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "snapshot group id mismatch".to_string(),
            ));
        }
        matrixraft_validate_snapshot_install(&snapshot, &fence)?;
        let node = self
            .nodes
            .get_mut(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        let snapshot_index = snapshot.meta.last_log_id.index;
        match node.rejected_apply_index {
            Some(required_index) => {
                if snapshot_index < required_index {
                    return Err(RaftError::InvalidRequest(format!(
                        "snapshot index {snapshot_index} is below rejected apply index {required_index}"
                    )));
                }
            }
            None => {
                if node.safety_applied_index < node.applied_index {
                    let safety_applied_index = node.safety_applied_index;
                    let applied_index = node.applied_index;
                    return Err(RaftError::InvalidRequest(format!(
                        "cannot install snapshot while node {target} has inflight apply tasks {safety_applied_index}..{applied_index}"
                    )));
                }
                if node.applied_index < node.commit_index {
                    let applied_index = node.applied_index;
                    let commit_index = node.commit_index;
                    return Err(RaftError::InvalidRequest(format!(
                        "cannot install snapshot while node {target} has unapplied entries {applied_index}..{commit_index}"
                    )));
                }
                if snapshot_index <= node.commit_index {
                    let committed_index = node.commit_index;
                    return Err(RaftError::InvalidRequest(format!(
                        "snapshot index {snapshot_index} is not newer than committed index {committed_index}"
                    )));
                }
            }
        }
        let snapshot_membership = snapshot.meta.membership.clone();
        let snapshot_members = snapshot.meta.members.clone();
        node.install_snapshot(snapshot);
        self.clear_reorder_queue_for(target);
        self.reset_membership_from_snapshot(target, &snapshot_membership, &snapshot_members);
        self.mark_snapshot_membership_floor_applied(snapshot_index);
        self.snapshot_installers.remove(&target);
        self.pending_snapshots.remove(&target);
        if let Some(pipeline) = self.peer_pipelines.get_mut(&target) {
            pipeline
                .begin_snapshot_install(
                    format!("installed-{target}-{snapshot_index}"),
                    snapshot_index,
                    1,
                )
                .ok();
            pipeline.receive_snapshot_chunk(0, true).ok();
        }
        self.refresh_cluster_indexes();
        Ok(())
    }

    pub fn install_snapshot_with_tail_to(
        &mut self,
        target: NodeId,
        snapshot: RaftSnapshot,
        fence: ApplySnapshotFence,
        tail_entries: Vec<LogEntry>,
    ) -> Result<(), RaftError> {
        let snapshot_index = snapshot.meta.last_log_id.index;
        if let Some(first_tail) = tail_entries.first() {
            if first_tail.log_id.index <= snapshot_index {
                return Err(RaftError::InvalidRequest(
                    "tail catch-up entry overlaps installed snapshot".to_string(),
                ));
            }
        }
        let installed_snapshot_index = self
            .nodes
            .get(&target)
            .ok_or(RaftError::NodeNotFound(target))?
            .installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_log_id.index);
        match installed_snapshot_index {
            Some(index) if index == snapshot_index => {}
            Some(index) if index > snapshot_index => {
                return Err(RaftError::InvalidRequest(format!(
                    "snapshot index {snapshot_index} is older than installed snapshot index {index}"
                )));
            }
            _ => self.install_snapshot_to(target, snapshot, fence)?,
        }
        let node = self
            .nodes
            .get_mut(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        for entry in tail_entries {
            node.append_entry(Arc::new(entry));
        }
        node.advance_commit(self.commit_index.max(node.match_index()));
        if let Some(pipeline) = self.peer_pipelines.get_mut(&target) {
            let response = AppendEntriesResponse {
                term: self.current_term,
                success: true,
                match_index: node.match_index(),
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: SnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            };
            let _ = pipeline.handle_append_response(&response);
            pipeline.mark_snapshot_rejoin_after_compacted_log();
        }
        self.refresh_cluster_indexes();
        Ok(())
    }

    pub fn compact_logs_through(&mut self, log_index: LogIndex) -> u64 {
        let leader_id = self.leader_id;
        let leader_compaction_limit = self
            .leader_id
            .and_then(|leader_id| self.min_replicated_index(leader_id).ok());
        let removed = self
            .nodes
            .values_mut()
            .map(|node| {
                let log_index = if Some(node.id) == leader_id {
                    leader_compaction_limit
                        .map(|limit| log_index.min(limit))
                        .unwrap_or(log_index)
                } else {
                    log_index
                };
                node.compact_log_through(log_index)
            })
            .sum();
        self.refresh_cluster_indexes();
        removed
    }

    pub fn compact_logs_with_storage_fence(
        &mut self,
        log_index: LogIndex,
        fence: StorageApplyFence,
    ) -> Result<WalCompactionReport, RaftError> {
        if let Err(error) = matrixraft_validate_storage_apply_fence(&fence) {
            return Ok(WalCompactionReport {
                requested_log_index: log_index,
                released_segments: 0,
                retained_range: self.log_retained_range(),
                fence_valid: false,
                blocker: Some(error.to_string()),
            });
        }
        if fence.durable_applied_index < log_index || fence.storage_flushed_index < log_index {
            return Ok(WalCompactionReport {
                requested_log_index: log_index,
                released_segments: 0,
                retained_range: self.log_retained_range(),
                fence_valid: false,
                blocker: Some("compaction fence is behind requested log index".to_string()),
            });
        }
        let removed_entries = self.compact_logs_through(log_index);
        Ok(WalCompactionReport {
            requested_log_index: log_index,
            released_segments: removed_entries,
            retained_range: self.log_retained_range(),
            fence_valid: true,
            blocker: None,
        })
    }

    fn log_retained_range(&self) -> LogRetainedRange {
        // Each node's log is index-ordered, so its bounds are its ends.
        let first_log_index = self
            .nodes
            .values()
            .filter_map(|node| node.log.first().map(|entry| entry.log_id.index))
            .min()
            .unwrap_or_default();
        let last_log_index = self
            .nodes
            .values()
            .filter_map(|node| node.log.last().map(|entry| entry.log_id.index))
            .max()
            .unwrap_or(self.last_log_index);
        let record_count = self.nodes.values().map(|node| node.log.len() as u64).sum();
        LogRetainedRange {
            first_log_index,
            last_log_index,
            first_segment_id: 0,
            last_segment_id: 0,
            record_count,
        }
    }

    pub fn release_memory(&mut self) -> Result<bool, RaftError> {
        let release_index = if let Some(leader_id) = self.leader_id {
            self.min_replicated_index(leader_id)?
        } else {
            self.nodes
                .values()
                .map(|node| node.safety_applied_index)
                .min()
                .unwrap_or_default()
        };
        if release_index <= 1 {
            return Ok(false);
        }
        Ok(self.compact_logs_through(release_index - 1) > 0)
    }

    pub fn is_busy(&self) -> bool {
        let Some(leader_id) = self.leader_id else {
            return false;
        };
        let Some(leader) = self.nodes.get(&leader_id) else {
            return false;
        };
        leader.retained_log_bytes() >= self.config.max_log_buffer_bytes
    }

    pub fn should_release_memory(&self) -> bool {
        let Some(leader_id) = self.leader_id else {
            return false;
        };
        let Some(leader) = self.nodes.get(&leader_id) else {
            return false;
        };
        let high_watermark = self
            .config
            .max_log_buffer_bytes
            .saturating_mul(9)
            .saturating_div(10)
            .max(1);
        leader.retained_log_bytes() >= high_watermark
    }

    pub fn checkpoint_snapshot(
        &self,
        node_id: NodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<RaftSnapshot, RaftError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        let snapshot_index = node.commit_index.max(node.match_index());
        Ok(RaftSnapshot {
            group_id: self.group_id,
            meta: SnapshotMetadata {
                snapshot_id: snapshot_id.into(),
                last_log_id: LogId {
                    term: node.hard_state.current_term,
                    index: snapshot_index,
                },
                membership: self.node_ids(),
                members: self.snapshot_members(),
            },
            payload: serde_json::to_vec(&node.log).map_err(|err| {
                RaftError::Storage(format!("failed to encode checkpoint snapshot: {err}"))
            })?,
        })
    }

    pub fn install_snapshot_chunk_to(
        &mut self,
        target: NodeId,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        if request.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "snapshot install group id mismatch".to_string(),
            ));
        }
        let next_offset = request.chunk.offset + request.chunk.data.len() as u64;
        if let Some(response) =
            self.reject_snapshot_before_observe(target, &request, next_offset)?
        {
            return Ok(response);
        }
        self.observe_snapshot_leader(&request);
        if let Some(response) = self.stale_snapshot_chunk_response(target, &request, next_offset)? {
            return Ok(response);
        }
        let installed_snapshot_index = self
            .nodes
            .get(&target)
            .ok_or(RaftError::NodeNotFound(target))?
            .installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_log_id.index)
            .unwrap_or_default();
        if request.chunk.meta.last_log_id.index <= installed_snapshot_index {
            self.snapshot_installers.remove(&target);
            return Ok(InstallSnapshotResponse {
                term: self.current_term.max(request.term),
                accepted: true,
                next_offset,
                committed_index: installed_snapshot_index,
                reason: "stale_snapshot_ignored".to_string(),
            });
        }

        if let Some(installer) = self.snapshot_installers.get(&target) {
            if request.chunk.offset == 0 && installer.meta != request.chunk.meta {
                return Ok(InstallSnapshotResponse {
                    term: self.current_term.max(request.term),
                    accepted: true,
                    next_offset: installer.next_offset,
                    committed_index: 0,
                    reason: "snapshot_install_ignored_while_receiving".to_string(),
                });
            }
        }

        let installer = match self.snapshot_installers.get_mut(&target) {
            Some(installer) if request.chunk.offset == 0 && installer.next_offset != 0 => {
                self.snapshot_installers.insert(
                    target,
                    SnapshotInstallState::new(request.chunk.meta.clone()),
                );
                self.snapshot_installers
                    .get_mut(&target)
                    .expect("snapshot installer inserted")
            }
            Some(installer) => installer,
            None => {
                if request.chunk.offset != 0 {
                    return Err(RaftError::InvalidRequest(format!(
                        "snapshot chunk offset {} arrived before offset 0",
                        request.chunk.offset
                    )));
                }
                self.snapshot_installers.insert(
                    target,
                    SnapshotInstallState::new(request.chunk.meta.clone()),
                );
                self.snapshot_installers
                    .get_mut(&target)
                    .expect("snapshot installer inserted")
            }
        };
        installer.install_chunk(request.chunk)?;
        if !installer.complete {
            return Ok(InstallSnapshotResponse {
                term: self.current_term.max(request.term),
                accepted: true,
                next_offset,
                committed_index: 0,
                reason: "snapshot_chunk_accepted".to_string(),
            });
        }

        let install = self
            .snapshot_installers
            .remove(&target)
            .expect("completed snapshot installer");
        let snapshot = install.finish(self.group_id)?;
        let snapshot_index = snapshot.meta.last_log_id.index;
        let fence = ApplySnapshotFence {
            applied_index: snapshot_index,
            commit_index: snapshot_index,
            installed_snapshot_index: snapshot_index,
            first_retained_log_index: snapshot_index + 1,
        };
        if !self.install_or_queue_completed_snapshot(target, snapshot, fence)? {
            return Ok(InstallSnapshotResponse {
                term: self.current_term.max(request.term),
                accepted: true,
                next_offset,
                committed_index: 0,
                reason: "snapshot_pending_apply".to_string(),
            });
        }
        Ok(InstallSnapshotResponse {
            term: self.current_term.max(request.term),
            accepted: true,
            next_offset,
            committed_index: snapshot_index,
            reason: "snapshot_installed".to_string(),
        })
    }

    pub fn install_snapshot_lifecycle_request_to(
        &mut self,
        target: NodeId,
        lifecycle: &mut SnapshotLifecycle,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        if request.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "snapshot lifecycle group id mismatch".to_string(),
            ));
        }
        let request_term = request.term;
        let next_offset = request.chunk.offset + request.chunk.data.len() as u64;
        if let Some(response) =
            self.reject_snapshot_before_observe(target, &request, next_offset)?
        {
            return Ok(response);
        }
        self.observe_snapshot_leader(&request);
        if let Some(response) = self.stale_snapshot_chunk_response(target, &request, next_offset)? {
            return Ok(response);
        }
        let installed_snapshot_index = self
            .nodes
            .get(&target)
            .ok_or(RaftError::NodeNotFound(target))?
            .installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_log_id.index)
            .unwrap_or_default();
        match lifecycle.install_request(request)? {
            Some(snapshot) => {
                let snapshot_index = snapshot.meta.last_log_id.index;
                if snapshot_index <= installed_snapshot_index {
                    return Ok(InstallSnapshotResponse {
                        term: self.current_term.max(request_term),
                        accepted: true,
                        next_offset,
                        committed_index: installed_snapshot_index,
                        reason: "stale_snapshot_ignored".to_string(),
                    });
                }
                self.install_snapshot_to(
                    target,
                    snapshot,
                    ApplySnapshotFence {
                        applied_index: snapshot_index,
                        commit_index: snapshot_index,
                        installed_snapshot_index: snapshot_index,
                        first_retained_log_index: snapshot_index + 1,
                    },
                )?;
                Ok(InstallSnapshotResponse {
                    term: self.current_term.max(request_term),
                    accepted: true,
                    next_offset,
                    committed_index: snapshot_index,
                    reason: "snapshot_installed".to_string(),
                })
            }
            None => Ok(InstallSnapshotResponse {
                term: self.current_term.max(request_term),
                accepted: true,
                next_offset,
                committed_index: 0,
                reason: "snapshot_chunk_accepted".to_string(),
            }),
        }
    }

    fn stale_snapshot_chunk_response(
        &mut self,
        target: NodeId,
        request: &InstallSnapshotRequest,
        next_offset: u64,
    ) -> Result<Option<InstallSnapshotResponse>, RaftError> {
        let node = self
            .nodes
            .get(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        let snapshot_index = request.chunk.meta.last_log_id.index;
        let stale = match node.rejected_apply_index {
            Some(required_index) => snapshot_index < required_index,
            None => snapshot_index <= node.commit_index,
        };
        if !stale {
            return Ok(None);
        }
        let committed_index = node.commit_index;
        self.snapshot_installers.remove(&target);
        Ok(Some(InstallSnapshotResponse {
            term: self.current_term.max(request.term),
            accepted: true,
            next_offset,
            committed_index,
            reason: "stale_snapshot_ignored".to_string(),
        }))
    }

    fn install_or_queue_completed_snapshot(
        &mut self,
        target: NodeId,
        snapshot: RaftSnapshot,
        fence: ApplySnapshotFence,
    ) -> Result<bool, RaftError> {
        match self.install_snapshot_to(target, snapshot.clone(), fence.clone()) {
            Ok(()) => Ok(true),
            Err(error) if Self::snapshot_install_waits_for_apply(&error) => {
                self.pending_snapshots.insert(
                    target,
                    PendingSnapshotInstall { snapshot, fence },
                );
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    fn try_install_pending_snapshot_to(
        &mut self,
        target: NodeId,
    ) -> Result<bool, RaftError> {
        let Some(pending) = self.pending_snapshots.get(&target).cloned() else {
            return Ok(false);
        };
        match self.install_snapshot_to(target, pending.snapshot, pending.fence) {
            Ok(()) => Ok(true),
            Err(error) if Self::snapshot_install_waits_for_apply(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn snapshot_install_waits_for_apply(error: &RaftError) -> bool {
        match error {
            RaftError::InvalidRequest(reason) => {
                reason.contains("has inflight apply tasks")
                    || reason.contains("has unapplied entries")
            }
            _ => false,
        }
    }

    fn reject_snapshot_before_observe(
        &self,
        target: NodeId,
        request: &InstallSnapshotRequest,
        next_offset: u64,
    ) -> Result<Option<InstallSnapshotResponse>, RaftError> {
        let node = self
            .nodes
            .get(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        if request.term < self.current_term {
            return Ok(Some(InstallSnapshotResponse {
                term: node.hard_state.current_term.max(self.current_term),
                accepted: false,
                next_offset,
                committed_index: 0,
                reason: "stale_term".to_string(),
            }));
        }
        match self.nodes.get(&request.leader_id) {
            Some(leader) if leader.replica_role.can_be_leader() && leader.healthy => {
                return Ok(None);
            }
            Some(leader) if leader.replica_role.can_be_leader() => {
                return Ok(Some(InstallSnapshotResponse {
                    term: node.hard_state.current_term.max(self.current_term),
                    accepted: false,
                    next_offset,
                    committed_index: 0,
                    reason: "leader_unavailable".to_string(),
                }));
            }
            _ => {}
        }
        Ok(Some(InstallSnapshotResponse {
            term: node.hard_state.current_term.max(self.current_term),
            accepted: false,
            next_offset,
            committed_index: 0,
            reason: "leader_not_member".to_string(),
        }))
    }

    fn observe_snapshot_leader(&mut self, request: &InstallSnapshotRequest) {
        if request.term < self.current_term {
            return;
        }
        if request.term > self.current_term {
            self.last_index_before_current_term = self.last_log_index;
            self.current_term = request.term;
            self.clear_reorder_queues();
        }
        if self.leader_id != Some(request.leader_id) {
            if let Some(transferee_id) = self
                .leader_transfer
                .as_ref()
                .map(|transfer| transfer.transferee_id)
            {
                if transferee_id == request.leader_id {
                    self.leader_transfer = None;
                } else {
                    self.abort_leader_transfer("snapshot_leader_changed");
                }
            }
            self.leader_id = Some(request.leader_id);
            self.clear_reorder_queues();
            self.reset_leader_lease_epoch();
            self.invalidate_follower_lease();
        }
        for peer in self.nodes.values_mut() {
            peer.hard_state.current_term = peer.hard_state.current_term.max(request.term);
            peer.raft_role = if peer.id == request.leader_id {
                StateRole::Leader
            } else if peer.replica_role == ReplicaRole::Learner {
                StateRole::Learner
            } else {
                StateRole::Follower
            };
        }
    }

    pub fn status(&self, node_id: NodeId) -> Result<StatusSnapshot, RaftError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        Ok(StatusSnapshot {
            group_id: self.group_id,
            node_id,
            role: node.raft_role,
            term: node.hard_state.current_term,
            leader_id: self.leader_id,
            commit_index: node.commit_index,
            applied_index: node.applied_index,
            last_log_index: node.match_index(),
            last_snapshot_index: node
                .installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_log_id.index)
                .unwrap_or_default(),
            peers: self
                .nodes
                .values()
                .filter(|peer| peer.id != node_id)
                .map(|peer| PeerStatus {
                    node_id: peer.id,
                    matched: peer.match_index(),
                    next_index: peer.match_index() + 1,
                    learner: peer.replica_role == ReplicaRole::Learner,
                    healthy: peer.healthy,
                    lag: node.match_index().saturating_sub(peer.match_index()),
                })
                .collect(),
        })
    }

    pub fn cluster_status_report(&self) -> Result<ClusterStatusReport, RaftError> {
        let nodes = self
            .node_ids()
            .into_iter()
            .map(|node_id| self.status(node_id))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(matrixraft_cluster_status_report(
            self.group_id,
            self.leader_id,
            self.leader_transfer.clone(),
            nodes,
        ))
    }

    pub fn snapshot_trigger_in_progress(&self) -> Option<&SnapshotTriggerState> {
        self.snapshot_trigger.as_ref()
    }

    pub fn duplicate_snapshot_trigger_requests(&self) -> u64 {
        self.duplicate_snapshot_trigger_requests
    }

    pub fn snapshot_trigger_status(&self) -> SnapshotTriggerStatus {
        SnapshotTriggerStatus {
            in_progress: self.snapshot_trigger.is_some(),
            snapshot_id: self
                .snapshot_trigger
                .as_ref()
                .map(|trigger| trigger.meta.snapshot_id.clone()),
            last_log_id: self
                .snapshot_trigger
                .as_ref()
                .map(|trigger| trigger.meta.last_log_id.clone()),
            elapsed_ticks: self
                .snapshot_trigger
                .as_ref()
                .map(|trigger| trigger.elapsed_ticks)
                .unwrap_or_default(),
            timeout_ticks: self
                .snapshot_trigger
                .as_ref()
                .map(|trigger| trigger.timeout_ticks)
                .unwrap_or_default(),
            timed_out: self
                .snapshot_trigger
                .as_ref()
                .map(|trigger| trigger.timed_out)
                .unwrap_or_default(),
            duplicate_requests: self.duplicate_snapshot_trigger_requests,
        }
    }

    pub fn tick_snapshot_trigger(&mut self) -> bool {
        let Some(trigger) = self.snapshot_trigger.as_mut() else {
            return false;
        };
        trigger.elapsed_ticks = trigger.elapsed_ticks.saturating_add(1);
        if !trigger.timed_out && trigger.elapsed_ticks >= trigger.timeout_ticks {
            trigger.timed_out = true;
            true
        } else {
            false
        }
    }

    pub fn trigger_snapshot(&mut self) -> Result<SnapshotMetadata, RaftError> {
        if let Some(trigger) = &self.snapshot_trigger {
            self.duplicate_snapshot_trigger_requests += 1;
            return Ok(trigger.meta.clone());
        }
        let meta = SnapshotMetadata {
            snapshot_id: format!("{}-{}", self.group_id, self.commit_index),
            last_log_id: LogId {
                term: self.current_term,
                index: self.commit_index,
            },
            membership: self.node_ids(),
            members: self.snapshot_members(),
        };
        self.snapshot_trigger = Some(SnapshotTriggerState {
            meta: meta.clone(),
            elapsed_ticks: 0,
            timeout_ticks: self.leader_transfer_timeout_ticks,
            timed_out: false,
        });
        Ok(meta)
    }

    pub fn complete_snapshot_trigger(&mut self, snapshot_id: &str) -> Result<(), RaftError> {
        let trigger = self.snapshot_trigger.as_ref().ok_or_else(|| {
            RaftError::InvalidRequest("no snapshot trigger is in progress".to_string())
        })?;
        if trigger.meta.snapshot_id != snapshot_id {
            return Err(RaftError::InvalidRequest(format!(
                "snapshot trigger mismatch: expected {}, got {snapshot_id}",
                trigger.meta.snapshot_id
            )));
        }
        self.snapshot_trigger = None;
        Ok(())
    }

    pub fn handle_snapshot_ready(
        &mut self,
        snapshot_id: &str,
        success: bool,
    ) -> Result<(), RaftError> {
        let Some(trigger) = self.snapshot_trigger.as_ref() else {
            return Ok(());
        };
        if trigger.meta.snapshot_id != snapshot_id {
            return Ok(());
        }
        let meta = trigger.meta.clone();
        self.snapshot_trigger = None;
        if success {
            self.publish_ready_snapshot_to_waiting_peers(meta)?;
        }
        Ok(())
    }

    fn publish_ready_snapshot_to_waiting_peers(
        &mut self,
        meta: SnapshotMetadata,
    ) -> Result<(), RaftError> {
        let Some(leader_id) = self.leader_id else {
            return Ok(());
        };
        let snapshot_index = meta.last_log_id.index;
        if let Some(leader) = self.nodes.get_mut(&leader_id) {
            let current_snapshot_index = leader
                .installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_log_id.index)
                .unwrap_or_default();
            if snapshot_index >= current_snapshot_index {
                leader.installed_snapshot = Some(meta.clone());
            }
        }
        let snapshot_id = meta.snapshot_id;
        let waiting_peers = self
            .peer_pipelines
            .iter()
            .filter_map(|(peer_id, pipeline)| {
                if *peer_id == leader_id {
                    return None;
                }
                let status = pipeline.status();
                (status.required_snapshot_index > 0
                    && status.required_snapshot_index <= snapshot_index
                    && !status.snapshot_sending
                    && !status.snapshot_installing)
                    .then_some(*peer_id)
            })
            .collect::<Vec<_>>();
        for peer_id in waiting_peers {
            self.peer_pipelines
                .get_mut(&peer_id)
                .ok_or(RaftError::NodeNotFound(peer_id))?
                .begin_snapshot_send(snapshot_id.clone(), snapshot_index, 1)?;
        }
        Ok(())
    }

    pub fn peer_pipeline_status(
        &self,
        peer_id: NodeId,
    ) -> Result<PeerProgress, RaftError> {
        self.peer_pipelines
            .get(&peer_id)
            .map(ReplicationPipeline::status)
            .ok_or(RaftError::NodeNotFound(peer_id))
    }

    pub fn peer_pipeline_statuses(&self) -> Vec<PeerProgress> {
        self.peer_pipelines
            .iter()
            .filter(|(peer_id, _)| Some(**peer_id) != self.leader_id)
            .map(|(_, pipeline)| pipeline.status())
            .collect()
    }

    pub fn receive_out_of_order_append_for(
        &mut self,
        peer_id: NodeId,
        entry: LogEntry,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .receive_out_of_order(&entry)
    }

    pub fn expire_peer_reorder_queue(&mut self, peer_id: NodeId) -> Result<u64, RaftError> {
        Ok(self
            .peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .expire_reorder_queue())
    }

    pub fn record_network_error_for(&mut self, peer_id: NodeId) -> Result<(), RaftError> {
        let should_probe = self
            .peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .record_network_error();
        if should_probe
            && self
                .nodes
                .get(&peer_id)
                .map(|node| node.healthy)
                .unwrap_or(false)
        {
            let _ = self.catch_up_peer_with_reason(peer_id, "network_error_probe")?;
        }
        Ok(())
    }

    pub fn record_replication_task_result_for(
        &mut self,
        peer_id: NodeId,
        success: bool,
    ) -> Result<bool, RaftError> {
        self.nodes
            .get(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?;
        if success {
            return Ok(false);
        }
        if self.snapshot_trigger.is_some() {
            return Ok(false);
        }
        let leader_id = self.leader_id.ok_or(RaftError::NoLeader)?;
        let leader_snapshot = self
            .nodes
            .get(&leader_id)
            .and_then(|leader| leader.installed_snapshot.clone());
        let required_snapshot_index = self
            .peer_pipelines
            .get(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .status()
            .required_snapshot_index;
        if let Some(snapshot) = leader_snapshot {
            let snapshot_index = snapshot.last_log_id.index;
            if snapshot_index >= required_snapshot_index {
                let pipeline = self
                    .peer_pipelines
                    .get_mut(&peer_id)
                    .ok_or(RaftError::NodeNotFound(peer_id))?;
                pipeline.begin_snapshot_send(snapshot.snapshot_id, snapshot_index, 1)?;
                return Ok(true);
            }
        }
        let _ = self.trigger_snapshot()?;
        Ok(false)
    }

    pub fn begin_snapshot_send_to(
        &mut self,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .begin_snapshot_send(snapshot_id, snapshot_index, total_chunks)
    }

    pub fn record_snapshot_chunk_sent_to(
        &mut self,
        peer_id: NodeId,
        bytes: u64,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .record_snapshot_chunk_sent(bytes)
    }

    pub fn acknowledge_snapshot_chunk_to(
        &mut self,
        peer_id: NodeId,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .acknowledge_snapshot_chunk()
    }

    pub fn handle_snapshot_finish_from(
        &mut self,
        peer_id: NodeId,
        accepted: bool,
        committed_index: LogIndex,
    ) -> Result<(), RaftError> {
        let pipeline = self
            .peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?;
        let status = pipeline.status();
        if !status.snapshot_sending && !status.snapshot_installing {
            return Ok(());
        }
        pipeline.handle_snapshot_finish(accepted, committed_index)?;
        if accepted {
            if self
                .nodes
                .get(&peer_id)
                .map(|node| node.healthy)
                .unwrap_or(false)
            {
                let _ = self.catch_up_peer_with_reason(peer_id, "snapshot_finish_tail")?;
            }
        } else if self.leader_id.is_some() && !self.snapshot_trigger_status().in_progress {
            let _ = self.trigger_snapshot()?;
        }
        Ok(())
    }

    pub fn update_snapshot_progress_from(
        &mut self,
        peer_id: NodeId,
        remote_receiving: bool,
        elapsed_since_last_receiving_ms: u64,
        send_timeout_ms: u64,
    ) -> Result<bool, RaftError> {
        Ok(self
            .peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .update_snapshot_progress(
                remote_receiving,
                elapsed_since_last_receiving_ms,
                send_timeout_ms,
            ))
    }

    pub fn retry_snapshot_chunk_to(&mut self, peer_id: NodeId) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .retry_snapshot_chunk()
    }

    pub fn begin_snapshot_install_from(
        &mut self,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .begin_snapshot_install(snapshot_id, snapshot_index, total_chunks)
    }

    pub fn receive_snapshot_chunk_from(
        &mut self,
        peer_id: NodeId,
        bytes: u64,
        done: bool,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .receive_snapshot_chunk(bytes, done)
    }

    pub fn cancel_snapshot_send_to(&mut self, peer_id: NodeId) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .cancel_snapshot_send_for_new_snapshot();
        Ok(())
    }

    pub fn rollback_snapshot_install_from(
        &mut self,
        peer_id: NodeId,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .rollback_snapshot_install();
        Ok(())
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    fn snapshot_members(&self) -> Vec<Peer> {
        self.nodes
            .values()
            .map(|node| Peer {
                node_id: node.id,
                raft_addr: String::new(),
                snapshot_addr: String::new(),
                role: node.replica_role,
                auto_promote: node.auto_promote,
            })
            .collect()
    }

    fn refresh_commit_index(&mut self) {
        let mut candidate_indexes: Vec<_> = self
            .nodes
            .values()
            .filter(|node| {
                node.replica_role.participates_in_quorum()
                    && !(self.ignore_witness && node.replica_role == ReplicaRole::Witness)
                    && (node.replica_role != ReplicaRole::Witness
                        || self.count_witness_in_commit_quorum)
            })
            .map(Node::match_index)
            .collect();
        let quorum_size = self.commit_quorum_size();
        if candidate_indexes.len() < quorum_size {
            return;
        }
        candidate_indexes.sort_unstable();
        let quorum_index = candidate_indexes.len() - quorum_size;
        let commit_index = candidate_indexes[quorum_index];
        let Some(leader_id) = self.leader_id else {
            return;
        };
        let Some(leader) = self.nodes.get(&leader_id) else {
            return;
        };
        if leader.log_term_at(commit_index) != Some(self.current_term) {
            return;
        }
        self.commit_index = self.commit_index.max(commit_index);
        for node in self.nodes.values_mut() {
            if node.healthy {
                node.advance_commit(self.commit_index);
            }
        }
        self.refresh_cluster_indexes();
    }

    fn refresh_cluster_indexes(&mut self) {
        self.commit_index = self
            .nodes
            .values()
            .map(|node| node.commit_index)
            .max()
            .unwrap_or_default();
        self.applied_index = self
            .nodes
            .values()
            .filter(|node| node.replica_role.can_serve_data())
            .map(|node| node.safety_applied_index)
            .min()
            .unwrap_or_default();
        self.last_log_index = self
            .nodes
            .values()
            .map(Node::match_index)
            .max()
            .unwrap_or_default();
    }

    fn commit_quorum_size(&self) -> usize {
        let voters = self
            .nodes
            .values()
            .filter(|node| {
                node.replica_role == ReplicaRole::Voter
                    || (!self.ignore_witness
                        && self.count_witness_in_commit_quorum
                        && node.replica_role == ReplicaRole::Witness)
            })
            .count();
        voters / 2 + 1
    }

    fn has_live_quorum(&self) -> bool {
        let live_voters = self
            .nodes
            .values()
            .filter(|node| {
                node.healthy
                    && node.replica_role.participates_in_quorum()
                    && !(self.ignore_witness && node.replica_role == ReplicaRole::Witness)
            })
            .count();
        live_voters
            >= self
                .membership()
                .quorum_size_with_witness_policy(self.ignore_witness)
    }

    fn reset_replication_pipelines_for_leader(&mut self, leader_id: NodeId) {
        let next_index = self.last_log_index + 1;
        for node_id in self.nodes.keys().copied().collect::<Vec<_>>() {
            let pipeline = self.peer_pipelines.entry(node_id).or_insert_with(|| {
                ReplicationPipeline::new(node_id, next_index, PipelineLimits::default())
            });
            let is_leader = node_id == leader_id;
            pipeline.reset_for_leader_transition(
                if is_leader { self.last_log_index } else { 0 },
                next_index,
                if is_leader {
                    ProgressState::Replicate
                } else {
                    ProgressState::Probe
                },
            );
        }
    }

    fn refresh_replication_pipelines(&mut self) {
        for (node_id, node) in &self.nodes {
            self.peer_pipelines.entry(*node_id).or_insert_with(|| {
                ReplicationPipeline::new(
                    *node_id,
                    node.match_index() + 1,
                    PipelineLimits::default(),
                )
            });
        }
        if let Some(leader_id) = self.leader_id {
            if let Some(pipeline) = self.peer_pipelines.get_mut(&leader_id) {
                let response = AppendEntriesResponse {
                    term: self.current_term,
                    success: true,
                    match_index: self.last_log_index,
                    rejection_hint: None,
                    rejected_index: None,
                    require_snapshot: None,
                    snapshot_state: SnapshotState::None,
                    lease_confirmation_epoch: 0,
                    lease_duration_ms: 0,
                };
                let _ = pipeline.handle_append_response(&response);
            }
        }
    }
}

impl Consensus for RaftCluster {
    fn start(&mut self) -> Result<(), RaftError> {
        RaftCluster::start(self)
    }

    fn stop(&mut self) -> Result<(), RaftError> {
        RaftCluster::stop(self)
    }

    fn status(&self) -> Result<StatusSnapshot, RaftError> {
        let node_id = self.leader_id.ok_or(RaftError::NoLeader)?;
        RaftCluster::status(self, node_id)
    }

    fn is_busy(&self) -> Result<bool, RaftError> {
        Ok(RaftCluster::is_busy(self))
    }

    fn step(&mut self, message: Message) -> Result<StepResult, RaftError> {
        RaftCluster::step(self, message)
    }

    fn step_batch(
        &mut self,
        messages: Vec<Message>,
    ) -> Result<Vec<StepResult>, RaftError> {
        RaftCluster::step_batch(self, messages)
    }

    fn propose(
        &mut self,
        payload: Payload,
        options: ProposeOptions,
    ) -> Result<LogId, RaftError> {
        RaftCluster::propose_with_options(self, payload, options)
    }

    fn read_index(
        &self,
        min_commit_index: LogIndex,
    ) -> Result<ReadIndexResponse, RaftError> {
        let requester_id = self.leader_id.ok_or(RaftError::NoLeader)?;
        RaftCluster::read_index(
            self,
            ReadIndexRequest {
                group_id: self.group_id,
                requester_id,
                min_commit_index,
                allow_lease_read: false,
            },
        )
    }

    fn add_peer(&mut self, peer: Peer) -> Result<(), RaftError> {
        RaftCluster::add_peer(self, peer)
    }

    fn add_learner(&mut self, peer: Peer) -> Result<(), RaftError> {
        RaftCluster::add_learner(self, peer)
    }

    fn promote_peer(&mut self, node_id: NodeId) -> Result<(), RaftError> {
        RaftCluster::promote_peer(self, node_id)
    }

    fn add_witness(&mut self, peer: Peer) -> Result<(), RaftError> {
        RaftCluster::add_witness(self, peer)
    }

    fn remove_peer(&mut self, node_id: NodeId) -> Result<(), RaftError> {
        RaftCluster::remove_peer(self, node_id)
    }

    fn transfer_leader(&mut self, target: NodeId) -> Result<(), RaftError> {
        RaftCluster::transfer_leader(self, target)
    }

    fn resign_leader(&mut self, reason: &str) -> Result<bool, RaftError> {
        RaftCluster::resign_leader(self, reason)
    }

    fn campaign(&mut self, forced: bool) -> Result<(), RaftError> {
        let candidate_id = self.leader_id.unwrap_or_else(|| {
            self.nodes
                .values()
                .find(|node| node.replica_role.can_be_leader())
                .map(|node| node.id)
                .unwrap_or_default()
        });
        RaftCluster::campaign(self, candidate_id, forced)
    }

    fn release_memory(&mut self) -> Result<bool, RaftError> {
        RaftCluster::release_memory(self)
    }

    fn trigger_snapshot(&mut self) -> Result<SnapshotMetadata, RaftError> {
        RaftCluster::trigger_snapshot(self)
    }

    fn complete_snapshot_trigger(&mut self, snapshot_id: &str) -> Result<(), RaftError> {
        RaftCluster::complete_snapshot_trigger(self, snapshot_id)
    }
}
