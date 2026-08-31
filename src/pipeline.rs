// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Replication pipeline evidence and BaselineRaft-style backpressure validation.

use std::collections::{BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::{AppendEntriesResponse, LogEntry, LogId, LogIndex, NodeId, RaftError, SnapshotId};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProgressState {
    #[default]
    Probe,
    Replicate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerProgress {
    pub peer_id: u64,
    #[serde(default)]
    pub progress_state: ProgressState,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub old_paused: bool,
    pub match_index: u64,
    pub next_index: u64,
    pub append_requests: u64,
    #[serde(default)]
    pub append_batches: u64,
    #[serde(default)]
    pub max_append_batch_entries: u64,
    #[serde(default)]
    pub max_append_batch_bytes: u64,
    pub append_accepted: u64,
    pub append_rejected: u64,
    #[serde(default)]
    pub retry_attempts: u64,
    #[serde(default)]
    pub backoff_ms: u64,
    #[serde(default)]
    pub next_retry_after_ms: u64,
    pub inflight_entries: u64,
    pub inflight_bytes: u64,
    pub append_queue_depth: u64,
    pub append_queue_limit: u64,
    pub append_queue_max_depth: u64,
    pub inflight_bytes_limit: u64,
    pub apply_inflight_tasks: u64,
    pub apply_inflight_limit: u64,
    pub apply_queue_depth: u64,
    pub apply_queue_max_depth: u64,
    pub apply_batch_bytes_limit: u64,
    pub apply_backpressure_rejections: u64,
    pub memory_backpressure_rejections: u64,
    pub oversized_log_rejections: u64,
    pub reorder_queue_depth: u64,
    pub out_of_order_append_rejections: u64,
    pub reorder_entries_rejected: u64,
    pub reorder_entry_timeouts: u64,
    pub reorder_dropped_packages: u64,
    #[serde(default)]
    pub stale_term_rejections: u64,
    #[serde(default)]
    pub packet_loss_events: u64,
    #[serde(default)]
    pub network_error_probe_transitions: u64,
    pub snapshot_sending: bool,
    pub snapshot_installing: bool,
    pub snapshot_installed_index: u64,
    pub snapshot_send_attempts: u64,
    pub snapshot_install_total_chunks: u64,
    pub snapshot_install_progress_per_mille: u64,
    pub snapshot_backpressure_rejections: u64,
    pub snapshot_rate_limit_rejections: u64,
    pub snapshot_install_rolled_back: u64,
    #[serde(default)]
    pub snapshot_chunk_retry_count: u64,
    #[serde(default)]
    pub snapshot_send_timeouts: u64,
    #[serde(default)]
    pub required_snapshot_index: LogIndex,
    #[serde(default)]
    pub acked_snapshot_index: LogIndex,
    pub snapshot_during_membership_change: bool,
    pub snapshot_rejoin_after_compacted_log: bool,
    pub transfer_leader_target: bool,
    pub transfer_leader_timeouts: u64,
    pub pre_vote_rejections: u64,
    pub election_rejections: u64,
    pub offline_timeout_reached: bool,
    pub offline_timeout_rejections: u64,
    #[serde(default)]
    pub follower_lag: LogIndex,
    #[serde(default)]
    pub learner_catchup_rounds: u64,
    #[serde(default)]
    pub learner_caught_up: bool,
    #[serde(default)]
    pub witness_quorum_required: u64,
    #[serde(default)]
    pub witness_quorum_acked: u64,
    #[serde(default)]
    pub witness_quorum_reached: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObservedPeerPipeline {
    pub peer_id: NodeId,
    #[serde(default)]
    pub match_index: LogIndex,
    #[serde(default)]
    pub next_index: LogIndex,
    #[serde(default)]
    pub append_requests: u64,
    #[serde(default)]
    pub append_accepted: u64,
    #[serde(default)]
    pub append_rejected: u64,
    #[serde(default)]
    pub inflight_entries: u64,
    #[serde(default)]
    pub inflight_bytes: u64,
    #[serde(default)]
    pub append_queue_depth: u64,
    #[serde(default)]
    pub append_queue_limit: u64,
    #[serde(default)]
    pub append_queue_max_depth: u64,
    #[serde(default)]
    pub inflight_bytes_limit: u64,
    #[serde(default)]
    pub apply_inflight_tasks: u64,
    #[serde(default)]
    pub apply_inflight_limit: u64,
    #[serde(default)]
    pub apply_queue_depth: u64,
    #[serde(default)]
    pub apply_queue_max_depth: u64,
    #[serde(default)]
    pub apply_batch_bytes_limit: u64,
    #[serde(default)]
    pub apply_backpressure_rejections: u64,
    #[serde(default)]
    pub memory_backpressure_rejections: u64,
    #[serde(default)]
    pub oversized_log_rejections: u64,
    #[serde(default)]
    pub reorder_queue_depth: u64,
    #[serde(default)]
    pub out_of_order_append_rejections: u64,
    #[serde(default)]
    pub reorder_entries_rejected: u64,
    #[serde(default)]
    pub reorder_entry_timeouts: u64,
    #[serde(default)]
    pub reorder_dropped_packages: u64,
    #[serde(default)]
    pub stale_term_rejections: u64,
    #[serde(default)]
    pub packet_loss_events: u64,
    #[serde(default)]
    pub network_error_probe_transitions: u64,
    #[serde(default)]
    pub snapshot_sending: bool,
    #[serde(default)]
    pub snapshot_installing: bool,
    #[serde(default)]
    pub snapshot_installed_index: LogIndex,
    #[serde(default)]
    pub snapshot_send_attempts: u64,
    #[serde(default)]
    pub snapshot_install_total_chunks: u64,
    #[serde(default)]
    pub snapshot_install_progress_per_mille: u64,
    #[serde(default)]
    pub snapshot_backpressure_rejections: u64,
    #[serde(default)]
    pub snapshot_rate_limit_rejections: u64,
    #[serde(default)]
    pub snapshot_install_rolled_back: u64,
    #[serde(default)]
    pub snapshot_chunk_retry_count: u64,
    #[serde(default)]
    pub snapshot_send_timeouts: u64,
    #[serde(default)]
    pub snapshot_during_membership_change: bool,
    #[serde(default)]
    pub snapshot_rejoin_after_compacted_log: bool,
    #[serde(default)]
    pub transfer_leader_target: bool,
    #[serde(default)]
    pub transfer_leader_timeouts: u64,
    #[serde(default)]
    pub pre_vote_rejections: u64,
    #[serde(default)]
    pub election_rejections: u64,
    #[serde(default)]
    pub offline_timeout_reached: bool,
    #[serde(default)]
    pub offline_timeout_rejections: u64,
    #[serde(default)]
    pub auto_promoted_from_learner: bool,
    #[serde(default)]
    pub witness_quorum_required: u64,
    #[serde(default)]
    pub witness_quorum_acked: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InflightAppend {
    pub first_log_id: LogId,
    pub last_log_id: LogId,
    pub entry_count: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotTransferState {
    pub snapshot_id: SnapshotId,
    pub snapshot_index: LogIndex,
    pub total_chunks: u64,
    pub acknowledged_chunks: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    #[serde(default)]
    pub latest_receiving_elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplyInflightTask {
    pub applied_index: LogIndex,
    pub batch_bytes: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplyBatchStatus {
    Applied,
    NotReady,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplyBatchOutcome {
    pub status: ApplyBatchStatus,
    pub first_log_id: Option<LogId>,
    pub last_log_id: Option<LogId>,
    pub applied_through: LogIndex,
    pub next_index: LogIndex,
    pub applied_entries: Vec<LogEntry>,
    pub pending_entries: Vec<LogEntry>,
}

pub fn matrixraft_apply_batch_outcome(
    entries: &[LogEntry],
    applied_count: usize,
    status: ApplyBatchStatus,
) -> ApplyBatchOutcome {
    let applied_count = applied_count.min(entries.len());
    let applied_entries = entries[..applied_count].to_vec();
    let pending_entries = entries[applied_count..].to_vec();
    let first_log_id = entries.first().map(|entry| entry.log_id.clone());
    let last_log_id = entries.last().map(|entry| entry.log_id.clone());
    let applied_through = applied_entries
        .last()
        .map(|entry| entry.log_id.index)
        .unwrap_or(0);
    let next_index = pending_entries
        .first()
        .map(|entry| entry.log_id.index)
        .or_else(|| {
            last_log_id
                .as_ref()
                .map(|log_id| log_id.index.saturating_add(1))
        })
        .unwrap_or(0);

    ApplyBatchOutcome {
        status,
        first_log_id,
        last_log_id,
        applied_through,
        next_index,
        applied_entries,
        pending_entries,
    }
}

/// What the pipeline needs to remember about a queued append: where it sits in
/// the log and how much it weighs. The payload itself stays in the log, so
/// queueing an entry for N peers no longer copies it N times.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct QueuedAppend {
    log_id: LogId,
    bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationPipeline {
    peer_id: NodeId,
    limits: PipelineLimits,
    status: PeerProgress,
    append_queue: VecDeque<QueuedAppend>,
    append_queue_bytes: u64,
    inflight: VecDeque<InflightAppend>,
    apply_inflight: VecDeque<ApplyInflightTask>,
    reorder_queue: BTreeSet<LogIndex>,
    snapshot_transfer: Option<SnapshotTransferState>,
}

impl ReplicationPipeline {
    pub fn new(peer_id: NodeId, next_index: LogIndex, limits: PipelineLimits) -> Self {
        Self {
            peer_id,
            limits,
            status: PeerProgress::new(peer_id, next_index, limits),
            append_queue: VecDeque::new(),
            append_queue_bytes: 0,
            inflight: VecDeque::new(),
            apply_inflight: VecDeque::new(),
            reorder_queue: BTreeSet::new(),
            snapshot_transfer: None,
        }
    }

    pub fn reset_for_leader_transition(
        &mut self,
        match_index: LogIndex,
        next_index: LogIndex,
        progress_state: ProgressState,
    ) {
        self.status = PeerProgress::new(self.peer_id, next_index, self.limits);
        self.status.match_index = match_index;
        self.status.next_index = next_index;
        self.status.progress_state = progress_state;
        self.append_queue.clear();
        self.append_queue_bytes = 0;
        self.inflight.clear();
        self.apply_inflight.clear();
        self.reorder_queue.clear();
        self.snapshot_transfer = None;
    }

    pub fn peer_id(&self) -> NodeId {
        self.peer_id
    }

    pub fn status(&self) -> PeerProgress {
        self.status.clone()
    }

    pub fn progress_state(&self) -> ProgressState {
        self.status.progress_state
    }

    pub fn set_progress_state(&mut self, progress_state: ProgressState) {
        self.status.progress_state = progress_state;
        self.status.paused = false;
    }

    pub fn pause(&mut self) {
        self.status.paused = true;
    }

    pub fn resume(&mut self) {
        match self.status.progress_state {
            ProgressState::Probe => {
                self.status.paused = false;
            }
            ProgressState::Replicate => {
                if self.inflight_is_full() {
                    self.free_first_inflight();
                    self.status.old_paused = true;
                }
            }
        }
    }

    pub fn is_paused(&self) -> bool {
        match self.status.progress_state {
            ProgressState::Probe => self.status.paused,
            ProgressState::Replicate => self.inflight_is_full(),
        }
    }

    pub fn inflight_is_empty(&self) -> bool {
        match self.status.progress_state {
            ProgressState::Probe => false,
            ProgressState::Replicate => self.inflight.is_empty(),
        }
    }

    pub fn append_queue_bytes(&self) -> u64 {
        self.append_queue_bytes
    }

    pub fn take_empty_append_due_to_old_pause(&mut self) -> bool {
        if self.status.old_paused && self.inflight_is_empty() {
            self.status.old_paused = false;
            true
        } else {
            false
        }
    }

    pub fn queue_append(&mut self, entry: &LogEntry) -> Result<(), RaftError> {
        let bytes = entry.payload.len() as u64;
        if bytes > self.limits.max_apply_batch_bytes {
            self.status.oversized_log_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "append entry exceeds max apply batch bytes".to_string(),
            ));
        }
        if self.append_queue.len() as u64 >= self.status.append_queue_limit {
            self.status.apply_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "append queue backpressure limit reached".to_string(),
            ));
        }
        if self.status.inflight_bytes + self.append_queue_bytes + bytes
            > self.limits.max_memory_replicate_log_bytes
        {
            self.status.memory_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "replication memory backpressure limit reached".to_string(),
            ));
        }
        self.append_queue.push_back(QueuedAppend {
            log_id: entry.log_id.clone(),
            bytes,
        });
        self.append_queue_bytes = self.append_queue_bytes.saturating_add(bytes);
        self.status.append_queue_depth = self.append_queue.len() as u64;
        self.status.append_queue_max_depth = self
            .status
            .append_queue_max_depth
            .max(self.status.append_queue_depth);
        Ok(())
    }

    pub fn flush_append_window(&mut self) -> Vec<InflightAppend> {
        self.flush_append_batch(1, self.limits.max_apply_batch_bytes)
    }

    pub fn flush_append_batch(&mut self, max_entries: u64, max_bytes: u64) -> Vec<InflightAppend> {
        let mut flushed = Vec::new();
        let max_entries = if self.status.progress_state == ProgressState::Probe {
            1
        } else {
            max_entries.max(1)
        };
        let max_bytes = max_bytes.max(1);
        while !self.is_paused()
            && self.status.inflight_entries < self.limits.max_inflights_replicate
        {
            let Some(first_log_id) = self
                .append_queue
                .front()
                .map(|queued| queued.log_id.clone())
            else {
                break;
            };
            let mut entry_count = 0_u64;
            let mut bytes = 0;
            let mut last_log_id = first_log_id.clone();
            while entry_count < max_entries {
                let Some(queued) = self.append_queue.front() else {
                    break;
                };
                let entry_bytes = queued.bytes;
                if entry_count > 0 && bytes + entry_bytes > max_bytes {
                    break;
                }
                if self.status.inflight_bytes + bytes + entry_bytes
                    > self.limits.max_memory_replicate_log_bytes
                {
                    break;
                }
                bytes += entry_bytes;
                entry_count += 1;
                last_log_id = queued.log_id.clone();
                let _ = self.append_queue.pop_front().expect("front exists");
                self.append_queue_bytes = self.append_queue_bytes.saturating_sub(entry_bytes);
            }
            if entry_count == 0 {
                self.status.memory_backpressure_rejections += 1;
                break;
            }
            let inflight = InflightAppend {
                first_log_id,
                last_log_id,
                entry_count,
                bytes,
            };
            self.status.append_requests += 1;
            self.status.append_batches += 1;
            self.status.max_append_batch_entries = self
                .status
                .max_append_batch_entries
                .max(inflight.entry_count);
            self.status.max_append_batch_bytes =
                self.status.max_append_batch_bytes.max(inflight.bytes);
            self.status.inflight_entries += inflight.entry_count;
            self.status.inflight_bytes += inflight.bytes;
            self.inflight.push_back(inflight.clone());
            flushed.push(inflight);
            if self.status.progress_state == ProgressState::Probe {
                self.pause();
                break;
            }
        }
        self.status.append_queue_depth = self.append_queue.len() as u64;
        flushed
    }

    pub fn handle_append_response(
        &mut self,
        response: &AppendEntriesResponse,
    ) -> Result<(), RaftError> {
        if response.term == 0 {
            self.status.stale_term_rejections += 1;
        }
        if response.success {
            self.status.append_accepted += 1;
            self.status.old_paused = self.is_paused();
            let previous_match_index = self.status.match_index;
            self.status.match_index = self.status.match_index.max(response.match_index);
            self.status.next_index = self.status.match_index + 1;
            if self.status.match_index > previous_match_index {
                match self.status.progress_state {
                    ProgressState::Probe => {
                        self.resume();
                        self.status.progress_state = ProgressState::Replicate;
                        self.reset_inflight_window();
                        self.status.next_index = self.status.match_index + 1;
                    }
                    ProgressState::Replicate => {
                        self.release_inflight_through(response.match_index);
                    }
                }
            } else {
                self.release_inflight_through(response.match_index);
            }
            self.drain_reorder_queue();
            self.status.retry_attempts = 0;
            self.status.backoff_ms = 0;
            self.status.next_retry_after_ms = 0;
            if let Some(required_index) = response.require_snapshot {
                self.maybe_require_snapshot(required_index);
            }
            Ok(())
        } else {
            if response.require_snapshot.is_none()
                && response.match_index < self.status.match_index
                && response
                    .rejected_index
                    .map(|rejected| rejected < self.status.match_index)
                    .unwrap_or(true)
            {
                return Ok(());
            }
            if self.status.progress_state == ProgressState::Probe
                && response
                    .rejected_index
                    .is_some_and(|rejected| rejected != self.status.next_index)
            {
                return Ok(());
            }
            self.status.append_rejected += 1;
            self.status.retry_attempts += 1;
            self.status.backoff_ms = next_backoff_ms(self.status.retry_attempts);
            self.status.next_retry_after_ms = self.status.backoff_ms;
            let was_replicating = self.status.progress_state == ProgressState::Replicate;
            if self.status.progress_state == ProgressState::Replicate {
                self.status.progress_state = ProgressState::Probe;
                self.status.paused = false;
            } else {
                self.resume();
            }
            self.reset_inflight_window();
            self.status.next_index = if was_replicating {
                self.status.match_index.saturating_add(1)
            } else {
                response
                    .rejection_hint
                    .unwrap_or_else(|| response.match_index.saturating_add(1))
                    .max(1)
            };
            if response.rejection_hint == response.rejected_index
                && response.rejection_hint.is_some()
            {
                self.pause();
            }
            if let Some(required_index) = response.require_snapshot {
                self.maybe_require_snapshot(required_index);
            }
            Err(RaftError::InvalidRequest(
                "append rejected by peer pipeline".to_string(),
            ))
        }
    }

    pub fn record_retry_backoff_tick(&mut self, elapsed_ms: u64) -> bool {
        self.status.next_retry_after_ms =
            self.status.next_retry_after_ms.saturating_sub(elapsed_ms);
        self.status.next_retry_after_ms == 0 && self.status.retry_attempts > 0
    }

    pub fn record_network_error(&mut self) -> bool {
        self.status.packet_loss_events = self.status.packet_loss_events.saturating_add(1);
        let was_replicating = self.status.progress_state == ProgressState::Replicate;
        if !was_replicating {
            return false;
        }
        self.status.progress_state = ProgressState::Probe;
        self.status.paused = false;
        self.reset_inflight_window();
        self.status.next_index = self.status.match_index + 1;
        self.status.next_retry_after_ms = 0;
        self.status.network_error_probe_transitions = self
            .status
            .network_error_probe_transitions
            .saturating_add(1);
        true
    }

    pub fn record_heartbeat_response(&mut self) {
        if self.is_paused() {
            self.resume();
        }
    }

    pub fn update_follower_lag(&mut self, leader_commit_index: LogIndex) -> LogIndex {
        self.status.follower_lag = leader_commit_index.saturating_sub(self.status.match_index);
        self.status.follower_lag
    }

    pub fn record_learner_catchup_round(&mut self, leader_commit_index: LogIndex) -> bool {
        self.status.learner_catchup_rounds += 1;
        self.update_follower_lag(leader_commit_index);
        self.status.learner_caught_up = self.status.match_index >= leader_commit_index;
        self.status.learner_caught_up
    }

    pub fn record_witness_quorum(&mut self, acknowledged: u64, required: u64) -> bool {
        self.status.witness_quorum_acked = acknowledged;
        self.status.witness_quorum_required = required;
        self.status.witness_quorum_reached = acknowledged >= required;
        self.status.witness_quorum_reached
    }

    pub fn has_apply_inflight_tasks(&self) -> bool {
        !self.apply_inflight.is_empty()
    }

    pub fn can_install_snapshot_now(&self) -> bool {
        !self.has_apply_inflight_tasks()
    }

    pub fn begin_apply_task(
        &mut self,
        applied_index: LogIndex,
        batch_bytes: u64,
    ) -> Result<(), RaftError> {
        if batch_bytes > self.limits.max_apply_batch_bytes {
            self.status.oversized_log_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "apply batch exceeds max apply batch bytes".to_string(),
            ));
        }
        if self.status.apply_inflight_tasks >= self.limits.max_inflights_apply_task {
            self.status.apply_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "apply inflight task limit reached".to_string(),
            ));
        }
        self.apply_inflight.push_back(ApplyInflightTask {
            applied_index,
            batch_bytes,
        });
        self.status.apply_inflight_tasks = self.apply_inflight.len() as u64;
        self.status.apply_queue_depth = self.status.apply_inflight_tasks;
        self.status.apply_queue_max_depth = self
            .status
            .apply_queue_max_depth
            .max(self.status.apply_queue_depth);
        Ok(())
    }

    pub fn complete_apply_through(&mut self, safety_applied_index: LogIndex) -> u64 {
        let mut completed = 0_u64;
        while self
            .apply_inflight
            .front()
            .map(|task| task.applied_index <= safety_applied_index)
            .unwrap_or(false)
        {
            let _ = self.apply_inflight.pop_front();
            completed += 1;
        }
        self.status.apply_inflight_tasks = self.apply_inflight.len() as u64;
        self.status.apply_queue_depth = self.status.apply_inflight_tasks;
        completed
    }

    pub fn record_peer_active(&mut self) {
        self.status.offline_timeout_reached = false;
    }

    pub fn record_offline_timeout(&mut self) {
        if !self.status.offline_timeout_reached {
            self.status.offline_timeout_rejections =
                self.status.offline_timeout_rejections.saturating_add(1);
        }
        self.status.offline_timeout_reached = true;
    }

    pub fn receive_out_of_order(&mut self, entry: &LogEntry) -> Result<(), RaftError> {
        if entry.log_id.index < self.status.next_index {
            self.status.out_of_order_append_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "append is below peer next index".to_string(),
            ));
        }
        if entry.log_id.index == self.status.next_index {
            self.status.match_index = entry.log_id.index;
            self.status.next_index = entry.log_id.index + 1;
            self.drain_reorder_queue();
            return Ok(());
        }
        if !self.limits.enable_reorder_queue {
            self.status.out_of_order_append_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "out-of-order append received while reorder queue is disabled".to_string(),
            ));
        }
        if self.reorder_queue.len() as u64 >= self.limits.reorder_window_size {
            self.status.reorder_entries_rejected += 1;
            return Err(RaftError::InvalidRequest(
                "reorder queue window is full".to_string(),
            ));
        }
        self.reorder_queue.insert(entry.log_id.index);
        self.status.reorder_queue_depth = self.reorder_queue.len() as u64;
        Ok(())
    }

    pub fn expire_reorder_queue(&mut self) -> u64 {
        let dropped = self.reorder_queue.len() as u64;
        if dropped > 0 {
            self.reorder_queue.clear();
            self.status.reorder_queue_depth = 0;
            self.status.reorder_entry_timeouts += dropped;
            self.status.reorder_dropped_packages += dropped;
        }
        dropped
    }

    pub fn begin_snapshot_send(
        &mut self,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        if self.status.snapshot_sending || self.status.snapshot_installing {
            self.status.snapshot_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "snapshot transfer is already active".to_string(),
            ));
        }
        self.status.snapshot_sending = true;
        self.status.snapshot_send_attempts += 1;
        self.status.snapshot_install_total_chunks = total_chunks;
        self.status.snapshot_install_progress_per_mille = 0;
        self.snapshot_transfer = Some(SnapshotTransferState {
            snapshot_id: snapshot_id.into(),
            snapshot_index,
            total_chunks,
            acknowledged_chunks: 0,
            bytes_sent: 0,
            bytes_received: 0,
            latest_receiving_elapsed_ms: 0,
        });
        Ok(())
    }

    pub fn maybe_require_snapshot(&mut self, required_index: LogIndex) -> bool {
        if self.status.required_snapshot_index >= required_index {
            return false;
        }
        self.status.required_snapshot_index = required_index;
        let _ = self.begin_snapshot_send(
            format!("required-snapshot-{}-{required_index}", self.peer_id),
            required_index,
            1,
        );
        true
    }

    pub fn is_snapshot_required(&self) -> bool {
        self.status.required_snapshot_index != self.status.acked_snapshot_index
    }

    pub fn ack_snapshot_require(&mut self) {
        self.status.acked_snapshot_index = self.status.required_snapshot_index;
    }

    pub fn record_snapshot_chunk_sent(&mut self, bytes: u64) -> Result<(), RaftError> {
        let transfer = self
            .snapshot_transfer
            .as_mut()
            .ok_or_else(|| RaftError::InvalidRequest("snapshot send is not active".to_string()))?;
        transfer.bytes_sent += bytes;
        Ok(())
    }

    pub fn cancel_snapshot_send_for_new_snapshot(&mut self) -> bool {
        let was_sending = self.status.snapshot_sending;
        if was_sending {
            self.snapshot_transfer = None;
            self.status.snapshot_sending = false;
            self.status.snapshot_installing = false;
        }
        was_sending
    }

    pub fn acknowledge_snapshot_chunk(&mut self) -> Result<(), RaftError> {
        let transfer = self.snapshot_transfer.as_mut().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot transfer is not active".to_string())
        })?;
        transfer.acknowledged_chunks += 1;
        self.status.snapshot_install_progress_per_mille = (transfer.acknowledged_chunks * 1000)
            .checked_div(transfer.total_chunks)
            .map_or(1000, |per_mille| per_mille.min(1000));
        if transfer.acknowledged_chunks >= transfer.total_chunks {
            self.finish_snapshot_install()?;
        }
        Ok(())
    }

    pub fn handle_snapshot_finish(
        &mut self,
        accepted: bool,
        committed_index: LogIndex,
    ) -> Result<(), RaftError> {
        let transfer = self.snapshot_transfer.take().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot transfer is not active".to_string())
        })?;
        self.status.snapshot_sending = false;
        self.status.snapshot_installing = false;
        if accepted {
            self.status.snapshot_installed_index = self
                .status
                .snapshot_installed_index
                .max(transfer.snapshot_index);
            self.status.snapshot_install_progress_per_mille = 1000;
            self.status.match_index = self.status.match_index.max(committed_index);
            self.status.next_index = self.status.match_index + 1;
            self.ack_snapshot_require();
            return Ok(());
        }

        self.status.snapshot_chunk_retry_count += 1;
        Ok(())
    }

    pub fn update_snapshot_progress(
        &mut self,
        remote_receiving: bool,
        elapsed_since_last_update_ms: u64,
        send_timeout_ms: u64,
    ) -> bool {
        let Some(transfer) = self.snapshot_transfer.as_mut() else {
            return false;
        };
        if !self.status.snapshot_sending {
            return false;
        }
        if remote_receiving {
            transfer.latest_receiving_elapsed_ms = 0;
            return false;
        }
        transfer.latest_receiving_elapsed_ms = transfer
            .latest_receiving_elapsed_ms
            .saturating_add(elapsed_since_last_update_ms);
        if transfer.latest_receiving_elapsed_ms <= send_timeout_ms {
            return false;
        }

        self.snapshot_transfer = None;
        self.status.snapshot_sending = false;
        self.status.snapshot_installing = false;
        self.status.snapshot_install_progress_per_mille = 0;
        self.status.snapshot_send_timeouts += 1;
        true
    }

    pub fn retry_snapshot_chunk(&mut self) -> Result<(), RaftError> {
        if self.snapshot_transfer.is_none() {
            return Err(RaftError::InvalidRequest(
                "snapshot transfer is not active".to_string(),
            ));
        }
        self.status.snapshot_chunk_retry_count += 1;
        Ok(())
    }

    pub fn begin_snapshot_install(
        &mut self,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        if self.status.snapshot_sending || self.status.snapshot_installing {
            self.status.snapshot_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "snapshot transfer is already active".to_string(),
            ));
        }
        self.status.snapshot_installing = true;
        self.status.snapshot_install_total_chunks = total_chunks;
        self.status.snapshot_install_progress_per_mille = 0;
        self.snapshot_transfer = Some(SnapshotTransferState {
            snapshot_id: snapshot_id.into(),
            snapshot_index,
            total_chunks,
            acknowledged_chunks: 0,
            bytes_sent: 0,
            bytes_received: 0,
            latest_receiving_elapsed_ms: 0,
        });
        Ok(())
    }

    pub fn receive_snapshot_chunk(&mut self, bytes: u64, done: bool) -> Result<(), RaftError> {
        let transfer = self.snapshot_transfer.as_mut().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot install is not active".to_string())
        })?;
        transfer.bytes_received += bytes;
        transfer.acknowledged_chunks += 1;
        self.status.snapshot_install_progress_per_mille = (transfer.acknowledged_chunks * 1000)
            .checked_div(transfer.total_chunks)
            .map_or(1000, |per_mille| per_mille.min(1000));
        if done {
            self.finish_snapshot_install()?;
        }
        Ok(())
    }

    pub fn rollback_snapshot_install(&mut self) {
        self.snapshot_transfer = None;
        self.status.snapshot_sending = false;
        self.status.snapshot_installing = false;
        self.status.snapshot_install_rolled_back += 1;
    }

    pub fn mark_snapshot_rejoin_after_compacted_log(&mut self) {
        self.status.snapshot_rejoin_after_compacted_log = true;
    }

    fn release_inflight_through(&mut self, match_index: LogIndex) {
        while self
            .inflight
            .front()
            .map(|inflight| inflight.last_log_id.index <= match_index)
            .unwrap_or(false)
        {
            if let Some(inflight) = self.inflight.pop_front() {
                self.status.inflight_entries = self
                    .status
                    .inflight_entries
                    .saturating_sub(inflight.entry_count);
                self.status.inflight_bytes =
                    self.status.inflight_bytes.saturating_sub(inflight.bytes);
            }
        }
    }

    fn inflight_is_full(&self) -> bool {
        self.status.inflight_entries >= self.limits.max_inflights_replicate
    }

    fn free_first_inflight(&mut self) {
        if let Some(inflight) = self.inflight.pop_front() {
            self.status.inflight_entries = self
                .status
                .inflight_entries
                .saturating_sub(inflight.entry_count);
            self.status.inflight_bytes = self.status.inflight_bytes.saturating_sub(inflight.bytes);
        }
    }

    fn reset_inflight_window(&mut self) {
        self.inflight.clear();
        self.status.inflight_entries = 0;
        self.status.inflight_bytes = 0;
    }

    fn reset_apply_inflight_window(&mut self) {
        self.apply_inflight.clear();
        self.status.apply_inflight_tasks = 0;
        self.status.apply_queue_depth = 0;
    }

    fn drain_reorder_queue(&mut self) {
        while self.reorder_queue.remove(&self.status.next_index) {
            self.status.match_index = self.status.next_index;
            self.status.next_index = self.status.next_index.saturating_add(1);
        }
        self.status.reorder_queue_depth = self.reorder_queue.len() as u64;
    }

    fn finish_snapshot_install(&mut self) -> Result<(), RaftError> {
        let transfer = self.snapshot_transfer.take().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot transfer is not active".to_string())
        })?;
        self.status.snapshot_sending = false;
        self.status.snapshot_installing = false;
        self.status.snapshot_installed_index = self
            .status
            .snapshot_installed_index
            .max(transfer.snapshot_index);
        self.status.snapshot_install_progress_per_mille = 1000;
        self.status.match_index = self.status.match_index.max(transfer.snapshot_index);
        self.status.next_index = self.status.match_index + 1;
        self.reset_apply_inflight_window();
        Ok(())
    }
}

fn next_backoff_ms(retry_attempts: u64) -> u64 {
    let shift = retry_attempts.saturating_sub(1).min(10) as u32;
    10_u64.saturating_mul(1_u64 << shift).min(5_000)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelineLimits {
    pub max_inflights_replicate: u64,
    pub max_memory_replicate_log_bytes: u64,
    pub max_inflights_apply_task: u64,
    pub max_apply_batch_bytes: u64,
    pub enable_reorder_queue: bool,
    pub reorder_window_size: u64,
    pub reorder_timeout_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelineEvidence {
    pub per_peer_pipeline_state_present: bool,
    pub append_backpressure_enforced: bool,
    pub apply_backpressure_enforced: bool,
    pub memory_replicate_bytes_enforced: bool,
    pub oversized_log_rejection_present: bool,
    pub out_of_order_append_handling_present: bool,
    pub reorder_timeout_drop_present: bool,
    pub packet_loss_probe_present: bool,
    #[serde(default)]
    pub packet_loss_recovery_present: bool,
    #[serde(default)]
    pub reorder_convergence_present: bool,
    #[serde(default)]
    pub packet_loss_reorder_same_peer_recovered: bool,
    pub stale_term_rejection_present: bool,
    pub reorder_queue_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationPipelineEvidenceArtifact {
    pub schema: String,
    pub limits: PipelineLimits,
    pub peers: Vec<PeerProgress>,
    pub evidence: PipelineEvidence,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationPipelineEvidenceValidationReport {
    pub valid: bool,
    pub schema_valid: bool,
    pub peer_state_present: bool,
    pub append_backpressure_enforced: bool,
    pub apply_backpressure_enforced: bool,
    pub memory_replicate_bytes_enforced: bool,
    pub oversized_log_rejection_present: bool,
    pub out_of_order_append_handling_present: bool,
    pub reorder_timeout_drop_present: bool,
    pub packet_loss_probe_present: bool,
    pub packet_loss_recovery_present: bool,
    pub reorder_convergence_present: bool,
    #[serde(default)]
    pub packet_loss_reorder_same_peer_recovered: bool,
    pub stale_term_rejection_present: bool,
    pub reorder_queue_enabled: bool,
    #[serde(default)]
    pub missing: Vec<String>,
}

impl PipelineLimits {
    pub fn production_default() -> Self {
        Self {
            max_inflights_replicate: 256,
            max_memory_replicate_log_bytes: 64 * 1024 * 1024,
            max_inflights_apply_task: 1024,
            max_apply_batch_bytes: 8 * 1024 * 1024,
            enable_reorder_queue: true,
            reorder_window_size: 1024,
            reorder_timeout_us: 5_000_000,
        }
    }
}

impl Default for PipelineLimits {
    fn default() -> Self {
        Self::production_default()
    }
}

impl PeerProgress {
    pub fn new(peer_id: NodeId, next_index: LogIndex, limits: PipelineLimits) -> Self {
        Self {
            peer_id,
            progress_state: ProgressState::Probe,
            paused: false,
            old_paused: false,
            match_index: next_index.saturating_sub(1),
            next_index,
            append_requests: 0,
            append_batches: 0,
            max_append_batch_entries: 0,
            max_append_batch_bytes: 0,
            append_accepted: 0,
            append_rejected: 0,
            retry_attempts: 0,
            backoff_ms: 0,
            next_retry_after_ms: 0,
            inflight_entries: 0,
            inflight_bytes: 0,
            append_queue_depth: 0,
            append_queue_limit: limits.max_inflights_replicate,
            append_queue_max_depth: 0,
            inflight_bytes_limit: limits.max_memory_replicate_log_bytes,
            apply_inflight_tasks: 0,
            apply_inflight_limit: limits.max_inflights_apply_task,
            apply_queue_depth: 0,
            apply_queue_max_depth: 0,
            apply_batch_bytes_limit: limits.max_apply_batch_bytes,
            apply_backpressure_rejections: 0,
            memory_backpressure_rejections: 0,
            oversized_log_rejections: 0,
            reorder_queue_depth: 0,
            out_of_order_append_rejections: 0,
            reorder_entries_rejected: 0,
            reorder_entry_timeouts: 0,
            reorder_dropped_packages: 0,
            stale_term_rejections: 0,
            packet_loss_events: 0,
            network_error_probe_transitions: 0,
            snapshot_sending: false,
            snapshot_installing: false,
            snapshot_installed_index: 0,
            snapshot_send_attempts: 0,
            snapshot_install_total_chunks: 0,
            snapshot_install_progress_per_mille: 0,
            snapshot_backpressure_rejections: 0,
            snapshot_rate_limit_rejections: 0,
            snapshot_install_rolled_back: 0,
            snapshot_chunk_retry_count: 0,
            snapshot_send_timeouts: 0,
            required_snapshot_index: 0,
            acked_snapshot_index: 0,
            snapshot_during_membership_change: false,
            snapshot_rejoin_after_compacted_log: false,
            transfer_leader_target: false,
            transfer_leader_timeouts: 0,
            pre_vote_rejections: 0,
            election_rejections: 0,
            offline_timeout_reached: false,
            offline_timeout_rejections: 0,
            follower_lag: 0,
            learner_catchup_rounds: 0,
            learner_caught_up: false,
            witness_quorum_required: 0,
            witness_quorum_acked: 0,
            witness_quorum_reached: false,
        }
    }
}

pub fn matrixraft_peer_pipeline_status_from_observed(
    observed: &ObservedPeerPipeline,
) -> PeerProgress {
    PeerProgress {
        peer_id: observed.peer_id,
        progress_state: if observed.append_accepted > observed.append_rejected
            || observed.match_index.saturating_add(1) >= observed.next_index
        {
            ProgressState::Replicate
        } else {
            ProgressState::Probe
        },
        paused: observed.append_queue_depth >= observed.append_queue_limit
            && observed.append_queue_limit > 0,
        old_paused: observed.inflight_bytes >= observed.inflight_bytes_limit
            && observed.inflight_bytes_limit > 0,
        match_index: observed.match_index,
        next_index: observed.next_index,
        append_requests: observed.append_requests,
        append_batches: observed.append_requests,
        max_append_batch_entries: observed.inflight_entries.max(1),
        max_append_batch_bytes: observed.inflight_bytes,
        append_accepted: observed.append_accepted,
        append_rejected: observed.append_rejected,
        retry_attempts: observed.append_rejected,
        backoff_ms: 0,
        next_retry_after_ms: 0,
        inflight_entries: observed.inflight_entries,
        inflight_bytes: observed.inflight_bytes,
        append_queue_depth: observed.append_queue_depth,
        append_queue_limit: observed.append_queue_limit,
        append_queue_max_depth: observed.append_queue_max_depth,
        inflight_bytes_limit: observed.inflight_bytes_limit,
        apply_inflight_tasks: observed.apply_inflight_tasks,
        apply_inflight_limit: observed.apply_inflight_limit,
        apply_queue_depth: observed.apply_queue_depth,
        apply_queue_max_depth: observed.apply_queue_max_depth,
        apply_batch_bytes_limit: observed.apply_batch_bytes_limit,
        apply_backpressure_rejections: observed.apply_backpressure_rejections,
        memory_backpressure_rejections: observed.memory_backpressure_rejections,
        oversized_log_rejections: observed.oversized_log_rejections,
        reorder_queue_depth: observed.reorder_queue_depth,
        out_of_order_append_rejections: observed.out_of_order_append_rejections,
        reorder_entries_rejected: observed.reorder_entries_rejected,
        reorder_entry_timeouts: observed.reorder_entry_timeouts,
        reorder_dropped_packages: observed.reorder_dropped_packages,
        stale_term_rejections: observed.stale_term_rejections,
        packet_loss_events: observed.packet_loss_events,
        network_error_probe_transitions: observed.network_error_probe_transitions,
        snapshot_sending: observed.snapshot_sending,
        snapshot_installing: observed.snapshot_installing,
        snapshot_installed_index: observed.snapshot_installed_index,
        snapshot_send_attempts: observed.snapshot_send_attempts,
        snapshot_install_total_chunks: observed.snapshot_install_total_chunks,
        snapshot_install_progress_per_mille: observed.snapshot_install_progress_per_mille,
        snapshot_backpressure_rejections: observed.snapshot_backpressure_rejections,
        snapshot_rate_limit_rejections: observed.snapshot_rate_limit_rejections,
        snapshot_install_rolled_back: observed.snapshot_install_rolled_back,
        snapshot_chunk_retry_count: observed.snapshot_chunk_retry_count,
        snapshot_send_timeouts: observed.snapshot_send_timeouts,
        required_snapshot_index: observed.snapshot_installed_index,
        acked_snapshot_index: if observed.snapshot_installing {
            0
        } else {
            observed.snapshot_installed_index
        },
        snapshot_during_membership_change: observed.snapshot_during_membership_change,
        snapshot_rejoin_after_compacted_log: observed.snapshot_rejoin_after_compacted_log,
        transfer_leader_target: observed.transfer_leader_target,
        transfer_leader_timeouts: observed.transfer_leader_timeouts,
        pre_vote_rejections: observed.pre_vote_rejections,
        election_rejections: observed.election_rejections,
        follower_lag: observed
            .next_index
            .saturating_sub(observed.match_index.saturating_add(1)),
        learner_catchup_rounds: u64::from(observed.auto_promoted_from_learner),
        learner_caught_up: observed.auto_promoted_from_learner,
        witness_quorum_required: observed.witness_quorum_required,
        witness_quorum_acked: observed.witness_quorum_acked,
        witness_quorum_reached: observed.witness_quorum_required > 0
            && observed.witness_quorum_acked >= observed.witness_quorum_required,
        offline_timeout_reached: observed.offline_timeout_reached,
        offline_timeout_rejections: observed.offline_timeout_rejections,
    }
}

pub fn matrixraft_pipeline_evidence(
    peers: &[PeerProgress],
    limits: PipelineLimits,
) -> PipelineEvidence {
    PipelineEvidence {
        per_peer_pipeline_state_present: !peers.is_empty(),
        append_backpressure_enforced: peers.iter().any(|peer| {
            peer.append_queue_limit == limits.max_inflights_replicate
                && (peer.append_queue_max_depth >= peer.append_queue_limit
                    || peer.append_queue_depth >= peer.append_queue_limit)
        }),
        apply_backpressure_enforced: peers.iter().any(|peer| {
            peer.apply_inflight_limit == limits.max_inflights_apply_task
                && (peer.apply_backpressure_rejections > 0
                    || peer.apply_queue_max_depth >= peer.apply_inflight_limit)
        }),
        memory_replicate_bytes_enforced: peers.iter().any(|peer| {
            peer.inflight_bytes_limit == limits.max_memory_replicate_log_bytes
                && peer.memory_backpressure_rejections > 0
        }),
        oversized_log_rejection_present: peers.iter().any(|peer| peer.oversized_log_rejections > 0),
        out_of_order_append_handling_present: peers.iter().any(|peer| {
            peer.out_of_order_append_rejections > 0
                || peer.reorder_entries_rejected > 0
                || peer.reorder_entry_timeouts > 0
                || peer.reorder_dropped_packages > 0
        }),
        reorder_timeout_drop_present: peers
            .iter()
            .any(|peer| peer.reorder_entry_timeouts > 0 && peer.reorder_dropped_packages > 0),
        packet_loss_probe_present: peers
            .iter()
            .any(|peer| peer.packet_loss_events > 0 && peer.network_error_probe_transitions > 0),
        packet_loss_recovery_present: peers.iter().any(|peer| {
            peer.packet_loss_events > 0
                && peer.network_error_probe_transitions > 0
                && peer.append_accepted > 0
                && peer.match_index.saturating_add(1) >= peer.next_index
        }),
        reorder_convergence_present: peers.iter().any(|peer| {
            (peer.out_of_order_append_rejections > 0
                || peer.reorder_entries_rejected > 0
                || peer.reorder_entry_timeouts > 0
                || peer.reorder_dropped_packages > 0)
                && peer.append_accepted > 0
                && peer.reorder_queue_depth == 0
                && peer.match_index.saturating_add(1) >= peer.next_index
        }),
        packet_loss_reorder_same_peer_recovered: peers.iter().any(|peer| {
            peer.packet_loss_events > 0
                && peer.network_error_probe_transitions > 0
                && (peer.out_of_order_append_rejections > 0
                    || peer.reorder_entries_rejected > 0
                    || peer.reorder_entry_timeouts > 0
                    || peer.reorder_dropped_packages > 0)
                && peer.append_accepted > 0
                && peer.reorder_queue_depth == 0
                && peer.match_index.saturating_add(1) >= peer.next_index
        }),
        stale_term_rejection_present: peers.iter().any(|peer| peer.stale_term_rejections > 0),
        reorder_queue_enabled: limits.enable_reorder_queue
            && limits.reorder_window_size > 0
            && limits.reorder_timeout_us > 0
            && peers.iter().any(|peer| peer.reorder_queue_depth > 0),
    }
}

pub fn matrixraft_replication_pipeline_evidence_artifact(
    peers: Vec<PeerProgress>,
    limits: PipelineLimits,
) -> ReplicationPipelineEvidenceArtifact {
    let evidence = matrixraft_pipeline_evidence(&peers, limits);
    ReplicationPipelineEvidenceArtifact {
        schema: "rustraft.replication_pipeline_evidence.v1".to_string(),
        limits,
        peers,
        evidence,
    }
}

pub fn matrixraft_validate_replication_pipeline_evidence_artifact(
    artifact: &ReplicationPipelineEvidenceArtifact,
) -> ReplicationPipelineEvidenceValidationReport {
    let schema_valid = artifact.schema == "rustraft.replication_pipeline_evidence.v1";
    let recomputed = matrixraft_pipeline_evidence(&artifact.peers, artifact.limits);
    let peer_state_present =
        !artifact.peers.is_empty() && recomputed.per_peer_pipeline_state_present;
    let append_backpressure_enforced =
        recomputed.append_backpressure_enforced && artifact.evidence.append_backpressure_enforced;
    let apply_backpressure_enforced =
        recomputed.apply_backpressure_enforced && artifact.evidence.apply_backpressure_enforced;
    let memory_replicate_bytes_enforced = recomputed.memory_replicate_bytes_enforced
        && artifact.evidence.memory_replicate_bytes_enforced;
    let oversized_log_rejection_present = recomputed.oversized_log_rejection_present
        && artifact.evidence.oversized_log_rejection_present;
    let out_of_order_append_handling_present = recomputed.out_of_order_append_handling_present
        && artifact.evidence.out_of_order_append_handling_present;
    let reorder_timeout_drop_present =
        recomputed.reorder_timeout_drop_present && artifact.evidence.reorder_timeout_drop_present;
    let packet_loss_probe_present =
        recomputed.packet_loss_probe_present && artifact.evidence.packet_loss_probe_present;
    let packet_loss_recovery_present =
        recomputed.packet_loss_recovery_present && artifact.evidence.packet_loss_recovery_present;
    let reorder_convergence_present =
        recomputed.reorder_convergence_present && artifact.evidence.reorder_convergence_present;
    let packet_loss_reorder_same_peer_recovered = recomputed
        .packet_loss_reorder_same_peer_recovered
        && artifact.evidence.packet_loss_reorder_same_peer_recovered;
    let stale_term_rejection_present =
        recomputed.stale_term_rejection_present && artifact.evidence.stale_term_rejection_present;
    let reorder_queue_enabled = recomputed.reorder_queue_enabled
        && artifact.evidence.reorder_queue_enabled
        && artifact.limits.enable_reorder_queue;

    let mut missing = Vec::new();
    for (present, requirement) in [
        (schema_valid, "schema_valid"),
        (peer_state_present, "peer_state_present"),
        (append_backpressure_enforced, "append_backpressure_enforced"),
        (apply_backpressure_enforced, "apply_backpressure_enforced"),
        (
            memory_replicate_bytes_enforced,
            "memory_replicate_bytes_enforced",
        ),
        (
            oversized_log_rejection_present,
            "oversized_log_rejection_present",
        ),
        (
            out_of_order_append_handling_present,
            "out_of_order_append_handling_present",
        ),
        (reorder_timeout_drop_present, "reorder_timeout_drop_present"),
        (packet_loss_probe_present, "packet_loss_probe_present"),
        (packet_loss_recovery_present, "packet_loss_recovery_present"),
        (reorder_convergence_present, "reorder_convergence_present"),
        (
            packet_loss_reorder_same_peer_recovered,
            "packet_loss_reorder_same_peer_recovered",
        ),
        (stale_term_rejection_present, "stale_term_rejection_present"),
        (reorder_queue_enabled, "reorder_queue_enabled"),
    ] {
        if !present {
            missing.push(requirement.to_string());
        }
    }

    ReplicationPipelineEvidenceValidationReport {
        valid: missing.is_empty(),
        schema_valid,
        peer_state_present,
        append_backpressure_enforced,
        apply_backpressure_enforced,
        memory_replicate_bytes_enforced,
        oversized_log_rejection_present,
        out_of_order_append_handling_present,
        reorder_timeout_drop_present,
        packet_loss_probe_present,
        packet_loss_recovery_present,
        reorder_convergence_present,
        packet_loss_reorder_same_peer_recovered,
        stale_term_rejection_present,
        reorder_queue_enabled,
        missing,
    }
}
