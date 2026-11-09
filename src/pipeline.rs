// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Replication pipeline evidence and BaselineRaft-style backpressure validation.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

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
    #[serde(default)]
    pub reorder_entries_converged: u64,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PeerTelemetry {
    append_requests: u64,
    append_batches: u64,
    max_append_batch_entries: u64,
    max_append_batch_bytes: u64,
    append_accepted: u64,
    append_rejected: u64,
    append_queue_max_depth: u64,
    apply_queue_max_depth: u64,
    apply_backpressure_rejections: u64,
    memory_backpressure_rejections: u64,
    oversized_log_rejections: u64,
    out_of_order_append_rejections: u64,
    reorder_entries_rejected: u64,
    reorder_entries_converged: u64,
    reorder_entry_timeouts: u64,
    reorder_dropped_packages: u64,
    stale_term_rejections: u64,
    packet_loss_events: u64,
    network_error_probe_transitions: u64,
    snapshot_send_attempts: u64,
    snapshot_backpressure_rejections: u64,
    snapshot_rate_limit_rejections: u64,
    snapshot_install_rolled_back: u64,
    snapshot_chunk_retry_count: u64,
    snapshot_send_timeouts: u64,
    snapshot_during_membership_change: bool,
    snapshot_rejoin_after_compacted_log: bool,
    transfer_leader_target: bool,
    transfer_leader_timeouts: u64,
    pre_vote_rejections: u64,
    election_rejections: u64,
    offline_timeout_rejections: u64,
    learner_catchup_rounds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompactPeerProgress {
    progress_state: ProgressState,
    paused: bool,
    old_paused: bool,
    match_index: u64,
    next_index: u64,
    retry_attempts: u64,
    next_retry_after_ms: u64,
    inflight_entries: u64,
    inflight_bytes: u64,
    snapshot_sending: bool,
    snapshot_installing: bool,
    snapshot_installed_index: u64,
    snapshot_install_total_chunks: u64,
    snapshot_install_progress_per_mille: u64,
    required_snapshot_index: LogIndex,
    acked_snapshot_index: LogIndex,
    offline_timeout_reached: bool,
    follower_lag: LogIndex,
    learner_caught_up: bool,
    witness_quorum_required: u64,
    witness_quorum_acked: u64,
    witness_quorum_reached: bool,
    telemetry: Option<Box<PeerTelemetry>>,
}

impl CompactPeerProgress {
    fn new(next_index: LogIndex) -> Self {
        Self {
            progress_state: ProgressState::Probe,
            paused: false,
            old_paused: false,
            match_index: next_index.saturating_sub(1),
            next_index,
            retry_attempts: 0,
            next_retry_after_ms: 0,
            inflight_entries: 0,
            inflight_bytes: 0,
            snapshot_sending: false,
            snapshot_installing: false,
            snapshot_installed_index: 0,
            snapshot_install_total_chunks: 0,
            snapshot_install_progress_per_mille: 0,
            required_snapshot_index: 0,
            acked_snapshot_index: 0,
            offline_timeout_reached: false,
            follower_lag: 0,
            learner_caught_up: false,
            witness_quorum_required: 0,
            witness_quorum_acked: 0,
            witness_quorum_reached: false,
            telemetry: None,
        }
    }

    fn telemetry_mut(&mut self) -> &mut PeerTelemetry {
        self.telemetry
            .get_or_insert_with(|| Box::new(PeerTelemetry::default()))
    }

    fn reset(&mut self, next_index: LogIndex) {
        let telemetry = self.telemetry.take();
        *self = Self::new(next_index);
        if let Some(mut telemetry) = telemetry {
            *telemetry = PeerTelemetry::default();
            self.telemetry = Some(telemetry);
        }
    }

    fn from_public(progress: PeerProgress) -> Self {
        let telemetry = PeerTelemetry {
            append_requests: progress.append_requests,
            append_batches: progress.append_batches,
            max_append_batch_entries: progress.max_append_batch_entries,
            max_append_batch_bytes: progress.max_append_batch_bytes,
            append_accepted: progress.append_accepted,
            append_rejected: progress.append_rejected,
            append_queue_max_depth: progress.append_queue_max_depth,
            apply_queue_max_depth: progress.apply_queue_max_depth,
            apply_backpressure_rejections: progress.apply_backpressure_rejections,
            memory_backpressure_rejections: progress.memory_backpressure_rejections,
            oversized_log_rejections: progress.oversized_log_rejections,
            out_of_order_append_rejections: progress.out_of_order_append_rejections,
            reorder_entries_rejected: progress.reorder_entries_rejected,
            reorder_entries_converged: progress.reorder_entries_converged,
            reorder_entry_timeouts: progress.reorder_entry_timeouts,
            reorder_dropped_packages: progress.reorder_dropped_packages,
            stale_term_rejections: progress.stale_term_rejections,
            packet_loss_events: progress.packet_loss_events,
            network_error_probe_transitions: progress.network_error_probe_transitions,
            snapshot_send_attempts: progress.snapshot_send_attempts,
            snapshot_backpressure_rejections: progress.snapshot_backpressure_rejections,
            snapshot_rate_limit_rejections: progress.snapshot_rate_limit_rejections,
            snapshot_install_rolled_back: progress.snapshot_install_rolled_back,
            snapshot_chunk_retry_count: progress.snapshot_chunk_retry_count,
            snapshot_send_timeouts: progress.snapshot_send_timeouts,
            snapshot_during_membership_change: progress.snapshot_during_membership_change,
            snapshot_rejoin_after_compacted_log: progress.snapshot_rejoin_after_compacted_log,
            transfer_leader_target: progress.transfer_leader_target,
            transfer_leader_timeouts: progress.transfer_leader_timeouts,
            pre_vote_rejections: progress.pre_vote_rejections,
            election_rejections: progress.election_rejections,
            offline_timeout_rejections: progress.offline_timeout_rejections,
            learner_catchup_rounds: progress.learner_catchup_rounds,
        };
        Self {
            progress_state: progress.progress_state,
            paused: progress.paused,
            old_paused: progress.old_paused,
            match_index: progress.match_index,
            next_index: progress.next_index,
            retry_attempts: progress.retry_attempts,
            next_retry_after_ms: progress.next_retry_after_ms,
            inflight_entries: progress.inflight_entries,
            inflight_bytes: progress.inflight_bytes,
            snapshot_sending: progress.snapshot_sending,
            snapshot_installing: progress.snapshot_installing,
            snapshot_installed_index: progress.snapshot_installed_index,
            snapshot_install_total_chunks: progress.snapshot_install_total_chunks,
            snapshot_install_progress_per_mille: progress.snapshot_install_progress_per_mille,
            required_snapshot_index: progress.required_snapshot_index,
            acked_snapshot_index: progress.acked_snapshot_index,
            offline_timeout_reached: progress.offline_timeout_reached,
            follower_lag: progress.follower_lag,
            learner_caught_up: progress.learner_caught_up,
            witness_quorum_required: progress.witness_quorum_required,
            witness_quorum_acked: progress.witness_quorum_acked,
            witness_quorum_reached: progress.witness_quorum_reached,
            telemetry: (telemetry != PeerTelemetry::default()).then(|| Box::new(telemetry)),
        }
    }

    fn to_public(
        &self,
        peer_id: NodeId,
        limits: PipelineLimits,
        append_queue_depth: u64,
        apply_inflight_tasks: u64,
        reorder_queue_depth: u64,
    ) -> PeerProgress {
        let telemetry = self.telemetry.as_deref().cloned().unwrap_or_default();
        PeerProgress {
            peer_id,
            progress_state: self.progress_state,
            paused: self.paused,
            old_paused: self.old_paused,
            match_index: self.match_index,
            next_index: self.next_index,
            append_requests: telemetry.append_requests,
            append_batches: telemetry.append_batches,
            max_append_batch_entries: telemetry.max_append_batch_entries,
            max_append_batch_bytes: telemetry.max_append_batch_bytes,
            append_accepted: telemetry.append_accepted,
            append_rejected: telemetry.append_rejected,
            retry_attempts: self.retry_attempts,
            backoff_ms: next_backoff_ms(self.retry_attempts),
            next_retry_after_ms: self.next_retry_after_ms,
            inflight_entries: self.inflight_entries,
            inflight_bytes: self.inflight_bytes,
            append_queue_depth,
            append_queue_limit: limits.max_inflights_replicate,
            append_queue_max_depth: telemetry.append_queue_max_depth,
            inflight_bytes_limit: limits.max_memory_replicate_log_bytes,
            apply_inflight_tasks,
            apply_inflight_limit: limits.max_inflights_apply_task,
            apply_queue_depth: apply_inflight_tasks,
            apply_queue_max_depth: telemetry.apply_queue_max_depth,
            apply_batch_bytes_limit: limits.max_apply_batch_bytes,
            apply_backpressure_rejections: telemetry.apply_backpressure_rejections,
            memory_backpressure_rejections: telemetry.memory_backpressure_rejections,
            oversized_log_rejections: telemetry.oversized_log_rejections,
            reorder_queue_depth,
            out_of_order_append_rejections: telemetry.out_of_order_append_rejections,
            reorder_entries_rejected: telemetry.reorder_entries_rejected,
            reorder_entries_converged: telemetry.reorder_entries_converged,
            reorder_entry_timeouts: telemetry.reorder_entry_timeouts,
            reorder_dropped_packages: telemetry.reorder_dropped_packages,
            stale_term_rejections: telemetry.stale_term_rejections,
            packet_loss_events: telemetry.packet_loss_events,
            network_error_probe_transitions: telemetry.network_error_probe_transitions,
            snapshot_sending: self.snapshot_sending,
            snapshot_installing: self.snapshot_installing,
            snapshot_installed_index: self.snapshot_installed_index,
            snapshot_send_attempts: telemetry.snapshot_send_attempts,
            snapshot_install_total_chunks: self.snapshot_install_total_chunks,
            snapshot_install_progress_per_mille: self.snapshot_install_progress_per_mille,
            snapshot_backpressure_rejections: telemetry.snapshot_backpressure_rejections,
            snapshot_rate_limit_rejections: telemetry.snapshot_rate_limit_rejections,
            snapshot_install_rolled_back: telemetry.snapshot_install_rolled_back,
            snapshot_chunk_retry_count: telemetry.snapshot_chunk_retry_count,
            snapshot_send_timeouts: telemetry.snapshot_send_timeouts,
            required_snapshot_index: self.required_snapshot_index,
            acked_snapshot_index: self.acked_snapshot_index,
            snapshot_during_membership_change: telemetry.snapshot_during_membership_change,
            snapshot_rejoin_after_compacted_log: telemetry.snapshot_rejoin_after_compacted_log,
            transfer_leader_target: telemetry.transfer_leader_target,
            transfer_leader_timeouts: telemetry.transfer_leader_timeouts,
            pre_vote_rejections: telemetry.pre_vote_rejections,
            election_rejections: telemetry.election_rejections,
            offline_timeout_reached: self.offline_timeout_reached,
            offline_timeout_rejections: telemetry.offline_timeout_rejections,
            follower_lag: self.follower_lag,
            learner_catchup_rounds: telemetry.learner_catchup_rounds,
            learner_caught_up: self.learner_caught_up,
            witness_quorum_required: self.witness_quorum_required,
            witness_quorum_acked: self.witness_quorum_acked,
            witness_quorum_reached: self.witness_quorum_reached,
        }
    }
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
    pub reorder_entries_converged: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveSnapshotProgress {
    acknowledged_chunks: u64,
    bytes_sent: u64,
    bytes_received: u64,
    latest_receiving_elapsed_ms: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct ActiveSnapshotTransferView {
    snapshot_id: Arc<str>,
    snapshot_index: LogIndex,
    total_chunks: u64,
    acknowledged_chunks: u64,
    bytes_sent: u64,
    bytes_received: u64,
    latest_receiving_elapsed_ms: u64,
}

#[derive(Debug)]
enum ActiveSnapshotSlots {
    One {
        peer_id: NodeId,
        progress: ActiveSnapshotProgress,
    },
    Many(Vec<(NodeId, ActiveSnapshotProgress)>),
}

#[derive(Debug)]
pub(crate) struct ActiveSnapshotTransferBatch {
    // Publication starts the same immutable snapshot for many peers. Keep that
    // descriptor once and pack only their mutable progress into one allocation.
    snapshot_id: Arc<str>,
    snapshot_index: LogIndex,
    total_chunks: u64,
    slots: Mutex<ActiveSnapshotSlots>,
}

impl ActiveSnapshotTransferBatch {
    fn one(
        snapshot_id: Arc<str>,
        peer_id: NodeId,
        snapshot_index: LogIndex,
        total_chunks: u64,
        progress: ActiveSnapshotProgress,
    ) -> Arc<Self> {
        Arc::new(Self {
            snapshot_id,
            snapshot_index,
            total_chunks,
            slots: Mutex::new(ActiveSnapshotSlots::One { peer_id, progress }),
        })
    }

    fn many(
        snapshot_id: Arc<str>,
        peers: impl IntoIterator<Item = NodeId>,
        peer_count: usize,
        snapshot_index: LogIndex,
        total_chunks: u64,
        progress: ActiveSnapshotProgress,
    ) -> Arc<Self> {
        let mut slots = Vec::with_capacity(peer_count);
        slots.extend(peers.into_iter().map(|peer_id| (peer_id, progress)));
        slots.sort_unstable_by_key(|(peer_id, _)| *peer_id);
        Arc::new(Self {
            snapshot_id,
            snapshot_index,
            total_chunks,
            slots: Mutex::new(ActiveSnapshotSlots::Many(slots)),
        })
    }

    fn with_progress<T>(
        &self,
        peer_id: NodeId,
        update: impl FnOnce(&mut ActiveSnapshotProgress) -> T,
    ) -> Result<T, RaftError> {
        let mut slots = self.slots.lock().map_err(|_| {
            RaftError::Storage("snapshot transfer state mutex poisoned".to_string())
        })?;
        let progress = match &mut *slots {
            ActiveSnapshotSlots::One {
                peer_id: slot_peer_id,
                progress,
            } if *slot_peer_id == peer_id => progress,
            ActiveSnapshotSlots::Many(slots) => {
                let index = slots
                    .binary_search_by_key(&peer_id, |(slot_peer_id, _)| *slot_peer_id)
                    .map_err(|_| RaftError::NodeNotFound(peer_id))?;
                &mut slots[index].1
            }
            _ => return Err(RaftError::NodeNotFound(peer_id)),
        };
        Ok(update(progress))
    }

    fn snapshot_view(&self, peer_id: NodeId) -> Result<ActiveSnapshotTransferView, RaftError> {
        self.with_progress(peer_id, |progress| ActiveSnapshotTransferView {
            snapshot_id: Arc::clone(&self.snapshot_id),
            snapshot_index: self.snapshot_index,
            total_chunks: self.total_chunks,
            acknowledged_chunks: progress.acknowledged_chunks,
            bytes_sent: progress.bytes_sent,
            bytes_received: progress.bytes_received,
            latest_receiving_elapsed_ms: progress.latest_receiving_elapsed_ms,
        })
    }

    fn clone_for_peer(&self, peer_id: NodeId) -> Result<Arc<Self>, RaftError> {
        let progress = self.with_progress(peer_id, |progress| *progress)?;
        Ok(Self::one(
            Arc::clone(&self.snapshot_id),
            peer_id,
            self.snapshot_index,
            self.total_chunks,
            progress,
        ))
    }
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

/// A deque that costs one pointer until it first carries work.
///
/// Once allocated, `clear` keeps the deque and its buffer so a leader reset
/// does not turn steady replication into repeated allocation. The custom serde
/// implementation preserves the ordinary array shape.
#[derive(Debug, Clone)]
struct SparseDeque<T> {
    #[allow(clippy::box_collection)]
    inner: Option<Box<VecDeque<T>>>,
}

impl<T: PartialEq> PartialEq for SparseDeque<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self.inner.as_deref(), other.inner.as_deref()) {
            (Some(left), Some(right)) => left == right,
            (Some(queue), None) | (None, Some(queue)) => queue.is_empty(),
            (None, None) => true,
        }
    }
}

impl<T: Eq> Eq for SparseDeque<T> {}

impl<T> Default for SparseDeque<T> {
    fn default() -> Self {
        Self { inner: None }
    }
}

impl<T> SparseDeque<T> {
    fn clear(&mut self) {
        if let Some(queue) = self.inner.as_deref_mut() {
            queue.clear();
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.as_deref().is_none_or(VecDeque::is_empty)
    }

    fn len(&self) -> usize {
        self.inner.as_deref().map_or(0, VecDeque::len)
    }

    fn front(&self) -> Option<&T> {
        self.inner.as_deref()?.front()
    }

    fn push_back(&mut self, value: T) {
        self.inner
            .get_or_insert_with(|| Box::new(VecDeque::new()))
            .push_back(value);
    }

    fn pop_front(&mut self) -> Option<T> {
        self.inner.as_deref_mut()?.pop_front()
    }
}

impl<T: Serialize> Serialize for SparseDeque<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.inner.as_deref() {
            Some(queue) => queue.serialize(serializer),
            None => VecDeque::<T>::new().serialize(serializer),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for SparseDeque<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let queue = VecDeque::deserialize(deserializer)?;
        Ok(Self {
            inner: (!queue.is_empty()).then(|| Box::new(queue)),
        })
    }
}

/// A map with an ordinary map wire shape and no allocation while empty.
#[derive(Debug, Clone)]
struct SparseBTreeMap<K, V> {
    #[allow(clippy::box_collection)]
    inner: Option<Box<BTreeMap<K, V>>>,
}

impl<K: Ord, V: PartialEq> PartialEq for SparseBTreeMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        match (self.inner.as_deref(), other.inner.as_deref()) {
            (Some(left), Some(right)) => left == right,
            (Some(map), None) | (None, Some(map)) => map.is_empty(),
            (None, None) => true,
        }
    }
}

impl<K: Ord, V: Eq> Eq for SparseBTreeMap<K, V> {}

impl<K, V> Default for SparseBTreeMap<K, V> {
    fn default() -> Self {
        Self { inner: None }
    }
}

impl<K: Ord, V> SparseBTreeMap<K, V> {
    fn clear(&mut self) {
        self.inner = None;
    }

    fn is_empty(&self) -> bool {
        self.inner.as_deref().is_none_or(BTreeMap::is_empty)
    }

    fn len(&self) -> usize {
        self.inner.as_deref().map_or(0, BTreeMap::len)
    }

    fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.inner
            .get_or_insert_with(|| Box::new(BTreeMap::new()))
            .insert(key, value)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let map = self.inner.as_deref_mut()?;
        let removed = map.remove(key);
        if map.is_empty() {
            self.inner = None;
        }
        removed
    }

    fn retain<F>(&mut self, mut keep: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        let Some(map) = self.inner.as_deref_mut() else {
            return;
        };
        map.retain(|key, value| keep(key, value));
        if map.is_empty() {
            self.inner = None;
        }
    }
}

impl<K, V> Serialize for SparseBTreeMap<K, V>
where
    K: Ord + Serialize,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.inner.as_deref() {
            Some(map) => map.serialize(serializer),
            None => BTreeMap::<K, V>::new().serialize(serializer),
        }
    }
}

impl<'de, K, V> Deserialize<'de> for SparseBTreeMap<K, V>
where
    K: Ord + Deserialize<'de>,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let map = BTreeMap::deserialize(deserializer)?;
        Ok(Self {
            inner: (!map.is_empty()).then(|| Box::new(map)),
        })
    }
}

pub struct ReplicationPipeline {
    peer_id: NodeId,
    limits: Option<Arc<PipelineLimits>>,
    status: CompactPeerProgress,
    append_queue: SparseDeque<QueuedAppend>,
    append_queue_bytes: u64,
    inflight: SparseDeque<InflightAppend>,
    apply_inflight: SparseDeque<ApplyInflightTask>,
    /// Index -> how long it has waited, in microseconds.
    ///
    /// It was a `BTreeSet<LogIndex>`, which is why `reorder_timeout_us` had
    /// nothing to be compared against. The age is 8 bytes an entry and the
    /// queue is bounded by `reorder_window_size`, so a full window costs twice
    /// what it did -- and only while it is full of out-of-order appends.
    reorder_queue: SparseBTreeMap<LogIndex, u64>,
    snapshot_transfer: Option<Arc<ActiveSnapshotTransferBatch>>,
}

#[derive(Serialize)]
struct ReplicationPipelineRef<'a> {
    peer_id: NodeId,
    limits: &'a PipelineLimits,
    status: PeerProgress,
    append_queue: &'a SparseDeque<QueuedAppend>,
    append_queue_bytes: u64,
    inflight: &'a SparseDeque<InflightAppend>,
    apply_inflight: &'a SparseDeque<ApplyInflightTask>,
    reorder_queue: &'a SparseBTreeMap<LogIndex, u64>,
    snapshot_transfer: Option<ActiveSnapshotTransferView>,
}

#[derive(Deserialize)]
struct ReplicationPipelineOwned {
    peer_id: NodeId,
    limits: PipelineLimits,
    status: PeerProgress,
    append_queue: SparseDeque<QueuedAppend>,
    append_queue_bytes: u64,
    inflight: SparseDeque<InflightAppend>,
    apply_inflight: SparseDeque<ApplyInflightTask>,
    reorder_queue: SparseBTreeMap<LogIndex, u64>,
    snapshot_transfer: Option<SnapshotTransferState>,
}

impl Serialize for ReplicationPipeline {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        ReplicationPipelineRef {
            peer_id: self.peer_id,
            limits: self.limits(),
            status: self.status(),
            append_queue: &self.append_queue,
            append_queue_bytes: self.append_queue_bytes,
            inflight: &self.inflight,
            apply_inflight: &self.apply_inflight,
            reorder_queue: &self.reorder_queue,
            snapshot_transfer: self.snapshot_transfer_view(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ReplicationPipeline {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let pipeline = ReplicationPipelineOwned::deserialize(deserializer)?;
        let snapshot_transfer = pipeline.snapshot_transfer.map(|transfer| {
            ActiveSnapshotTransferBatch::one(
                Arc::<str>::from(transfer.snapshot_id),
                pipeline.peer_id,
                transfer.snapshot_index,
                transfer.total_chunks,
                ActiveSnapshotProgress {
                    acknowledged_chunks: transfer.acknowledged_chunks,
                    bytes_sent: transfer.bytes_sent,
                    bytes_received: transfer.bytes_received,
                    latest_receiving_elapsed_ms: transfer.latest_receiving_elapsed_ms,
                },
            )
        });
        Ok(Self {
            peer_id: pipeline.peer_id,
            limits: Self::limit_handle(pipeline.limits),
            status: CompactPeerProgress::from_public(pipeline.status),
            append_queue: pipeline.append_queue,
            append_queue_bytes: pipeline.append_queue_bytes,
            inflight: pipeline.inflight,
            apply_inflight: pipeline.apply_inflight,
            reorder_queue: pipeline.reorder_queue,
            snapshot_transfer,
        })
    }
}

impl fmt::Debug for ReplicationPipeline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplicationPipeline")
            .field("peer_id", &self.peer_id)
            .field("limits", self.limits())
            .field("status", &self.status())
            .field("append_queue", &self.append_queue)
            .field("append_queue_bytes", &self.append_queue_bytes)
            .field("inflight", &self.inflight)
            .field("apply_inflight", &self.apply_inflight)
            .field("reorder_queue", &self.reorder_queue)
            .field("snapshot_transfer", &self.snapshot_transfer_view())
            .finish()
    }
}

impl PartialEq for ReplicationPipeline {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
            && self.limits() == other.limits()
            && self.status() == other.status()
            && self.append_queue == other.append_queue
            && self.append_queue_bytes == other.append_queue_bytes
            && self.inflight == other.inflight
            && self.apply_inflight == other.apply_inflight
            && self.reorder_queue == other.reorder_queue
            && self.snapshot_transfer_view() == other.snapshot_transfer_view()
    }
}

impl Eq for ReplicationPipeline {}

impl Clone for ReplicationPipeline {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id,
            limits: self.limits.clone(),
            status: self.status.clone(),
            append_queue: self.append_queue.clone(),
            append_queue_bytes: self.append_queue_bytes,
            inflight: self.inflight.clone(),
            apply_inflight: self.apply_inflight.clone(),
            reorder_queue: self.reorder_queue.clone(),
            snapshot_transfer: self.snapshot_transfer.as_ref().map(|batch| {
                batch
                    .clone_for_peer(self.peer_id)
                    .expect("active snapshot transfer must retain its peer slot")
            }),
        }
    }
}

impl ReplicationPipeline {
    pub fn new(peer_id: NodeId, next_index: LogIndex, limits: PipelineLimits) -> Self {
        Self::new_with_limit_handle(peer_id, next_index, Self::limit_handle(limits))
    }

    pub(crate) fn limit_handle(limits: PipelineLimits) -> Option<Arc<PipelineLimits>> {
        (limits != DEFAULT_PIPELINE_LIMITS).then(|| Arc::new(limits))
    }

    pub(crate) fn cloned_limit_handle_for(
        &self,
        limits: PipelineLimits,
    ) -> Option<Arc<PipelineLimits>> {
        if self.limits() == &limits {
            self.limits.clone()
        } else {
            Self::limit_handle(limits)
        }
    }

    #[cfg(test)]
    pub(crate) fn shares_limit_allocation_with(&self, other: &Self) -> bool {
        match (&self.limits, &other.limits) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        }
    }

    pub(crate) fn new_with_limit_handle(
        peer_id: NodeId,
        next_index: LogIndex,
        limits: Option<Arc<PipelineLimits>>,
    ) -> Self {
        Self {
            peer_id,
            limits,
            status: CompactPeerProgress::new(next_index),
            append_queue: SparseDeque::default(),
            append_queue_bytes: 0,
            inflight: SparseDeque::default(),
            apply_inflight: SparseDeque::default(),
            reorder_queue: SparseBTreeMap::default(),
            snapshot_transfer: None,
        }
    }

    fn limits(&self) -> &PipelineLimits {
        self.limits.as_deref().unwrap_or(&DEFAULT_PIPELINE_LIMITS)
    }

    pub fn reset_for_leader_transition(
        &mut self,
        match_index: LogIndex,
        next_index: LogIndex,
        progress_state: ProgressState,
    ) {
        self.status.reset(next_index);
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
        self.status.to_public(
            self.peer_id,
            *self.limits(),
            self.append_queue.len() as u64,
            self.apply_inflight.len() as u64,
            self.reorder_queue.len() as u64,
        )
    }

    fn snapshot_transfer_view(&self) -> Option<ActiveSnapshotTransferView> {
        self.snapshot_transfer.as_ref().map(|batch| {
            batch
                .snapshot_view(self.peer_id)
                .expect("active snapshot transfer must retain its peer slot")
        })
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
        if bytes > self.limits().max_apply_batch_bytes {
            self.status.telemetry_mut().oversized_log_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "append entry exceeds max apply batch bytes".to_string(),
            ));
        }
        if self.append_queue.len() as u64 >= self.limits().max_inflights_replicate {
            self.status.telemetry_mut().apply_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "append queue backpressure limit reached".to_string(),
            ));
        }
        if self.status.inflight_bytes + self.append_queue_bytes + bytes
            > self.limits().max_memory_replicate_log_bytes
        {
            self.status.telemetry_mut().memory_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "replication memory backpressure limit reached".to_string(),
            ));
        }
        self.append_queue.push_back(QueuedAppend {
            log_id: entry.log_id.clone(),
            bytes,
        });
        self.append_queue_bytes = self.append_queue_bytes.saturating_add(bytes);
        let depth = self.append_queue.len() as u64;
        let telemetry = self.status.telemetry_mut();
        telemetry.append_queue_max_depth = telemetry.append_queue_max_depth.max(depth);
        Ok(())
    }

    pub fn flush_append_window(&mut self) -> Vec<InflightAppend> {
        self.flush_append_batch(1, self.limits().max_apply_batch_bytes)
    }

    /// Flushes into a vector this pipeline does not own.
    ///
    /// The replication path calls this once per follower per proposal and
    /// **discards the result**, so the vector it used to return was an
    /// allocation per follower for a value nobody read. Lending one buffer
    /// across the followers makes that one allocation per proposal, and none
    /// at all once the buffer has earned its capacity.
    ///
    /// Appends rather than replaces, so a caller can accumulate across peers.
    /// Clear it yourself if you want only this flush -- that is cheaper than
    /// this deciding, because clearing keeps the capacity and a fresh vector
    /// does not.
    ///
    /// Returns how many batches were appended.
    pub fn flush_append_batch_into(
        &mut self,
        max_entries: u64,
        max_bytes: u64,
        flushed: &mut Vec<InflightAppend>,
    ) -> usize {
        self.flush_append_batch_core(max_entries, max_bytes, Some(flushed))
    }

    /// Flushes without collecting anything, and reports how many batches moved.
    ///
    /// For the caller that only wants the flush to **happen**. The replication
    /// path in `propose` is that caller: it discards the batches because what
    /// matters is the pipeline's own state. Lending it one buffer across the
    /// followers removed the allocation per follower, but the buffer itself
    /// still cost one: a `Vec<InflightAppend>` growing from empty takes its
    /// minimum capacity of four, which at 48 bytes an element is **192 bytes
    /// per proposal** for batches nobody reads.
    pub fn flush_append_batch_counted(&mut self, max_entries: u64, max_bytes: u64) -> usize {
        self.flush_append_batch_core(max_entries, max_bytes, None)
    }

    fn flush_append_batch_core(
        &mut self,
        max_entries: u64,
        max_bytes: u64,
        mut flushed: Option<&mut Vec<InflightAppend>>,
    ) -> usize {
        let mut moved = 0_usize;
        let max_entries = if self.status.progress_state == ProgressState::Probe {
            1
        } else {
            max_entries.max(1)
        };
        let max_bytes = max_bytes.max(1);
        while !self.is_paused()
            && self.status.inflight_entries < self.limits().max_inflights_replicate
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
                    > self.limits().max_memory_replicate_log_bytes
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
                self.status.telemetry_mut().memory_backpressure_rejections += 1;
                break;
            }
            let inflight = InflightAppend {
                first_log_id,
                last_log_id,
                entry_count,
                bytes,
            };
            let telemetry = self.status.telemetry_mut();
            telemetry.append_requests += 1;
            telemetry.append_batches += 1;
            telemetry.max_append_batch_entries =
                telemetry.max_append_batch_entries.max(inflight.entry_count);
            telemetry.max_append_batch_bytes = telemetry.max_append_batch_bytes.max(inflight.bytes);
            self.status.inflight_entries += inflight.entry_count;
            self.status.inflight_bytes += inflight.bytes;
            self.inflight.push_back(inflight.clone());
            if let Some(flushed) = flushed.as_deref_mut() {
                flushed.push(inflight);
            }
            moved += 1;
            if self.status.progress_state == ProgressState::Probe {
                self.pause();
                break;
            }
        }
        moved
    }

    /// Flushes into a fresh vector.
    ///
    /// Kept for callers that want the batches: it is
    /// [`ReplicationPipeline::flush_append_batch_into`] with a vector of its
    /// own, which costs the allocation that one avoids.
    pub fn flush_append_batch(&mut self, max_entries: u64, max_bytes: u64) -> Vec<InflightAppend> {
        let mut flushed = Vec::new();
        self.flush_append_batch_into(max_entries, max_bytes, &mut flushed);
        flushed
    }

    pub fn handle_append_response(
        &mut self,
        response: &AppendEntriesResponse,
    ) -> Result<(), RaftError> {
        if response.term == 0 {
            self.status.telemetry_mut().stale_term_rejections += 1;
        }
        if response.success {
            self.status.telemetry_mut().append_accepted += 1;
            self.status.old_paused = self.is_paused();
            // `response.match_index` is what a FOLLOWER said, taken with
            // `.max()` and no upper bound, and the next line used to add one to
            // it. A follower answering `success` with `match_index = u64::MAX`
            // panicked its leader here -- "attempt to add with overflow" --
            // through the public `handle_append_entries_response`, in any build
            // with overflow checks.
            //
            // The claim is still unbounded, and that is deliberate rather than
            // overlooked: it paces replication and does not decide the commit.
            // Measured -- two followers each claiming 9,000,000 left
            // `commit_index` where it was, because the commit is computed from
            // the leader's own node model and not from this.
            let previous_match_index = self.status.match_index;
            self.status.match_index = self.status.match_index.max(response.match_index);
            self.status.next_index = self.status.match_index.saturating_add(1);
            if self.status.match_index > previous_match_index {
                match self.status.progress_state {
                    ProgressState::Probe => {
                        self.resume();
                        self.status.progress_state = ProgressState::Replicate;
                        self.reset_inflight_window();
                        self.status.next_index = self.status.match_index.saturating_add(1);
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
            self.status.telemetry_mut().append_rejected += 1;
            self.status.retry_attempts += 1;
            self.status.next_retry_after_ms = next_backoff_ms(self.status.retry_attempts);
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
        let telemetry = self.status.telemetry_mut();
        telemetry.packet_loss_events = telemetry.packet_loss_events.saturating_add(1);
        let was_replicating = self.status.progress_state == ProgressState::Replicate;
        if !was_replicating {
            return false;
        }
        self.status.progress_state = ProgressState::Probe;
        self.status.paused = false;
        self.reset_inflight_window();
        self.status.next_index = self.status.match_index.saturating_add(1);
        self.status.next_retry_after_ms = 0;
        let telemetry = self.status.telemetry_mut();
        telemetry.network_error_probe_transitions =
            telemetry.network_error_probe_transitions.saturating_add(1);
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
        self.status.telemetry_mut().learner_catchup_rounds += 1;
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
        if batch_bytes > self.limits().max_apply_batch_bytes {
            self.status.telemetry_mut().oversized_log_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "apply batch exceeds max apply batch bytes".to_string(),
            ));
        }
        if self.apply_inflight.len() as u64 >= self.limits().max_inflights_apply_task {
            self.status.telemetry_mut().apply_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "apply inflight task limit reached".to_string(),
            ));
        }
        self.apply_inflight.push_back(ApplyInflightTask {
            applied_index,
            batch_bytes,
        });
        let depth = self.apply_inflight.len() as u64;
        let telemetry = self.status.telemetry_mut();
        telemetry.apply_queue_max_depth = telemetry.apply_queue_max_depth.max(depth);
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
        completed
    }

    pub fn record_peer_active(&mut self) {
        self.status.offline_timeout_reached = false;
    }

    pub fn record_offline_timeout(&mut self) {
        if !self.status.offline_timeout_reached {
            let telemetry = self.status.telemetry_mut();
            telemetry.offline_timeout_rejections =
                telemetry.offline_timeout_rejections.saturating_add(1);
        }
        self.status.offline_timeout_reached = true;
    }

    pub fn receive_out_of_order(&mut self, entry: &LogEntry) -> Result<(), RaftError> {
        if entry.log_id.index < self.status.next_index {
            self.status.telemetry_mut().out_of_order_append_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "append is below peer next index".to_string(),
            ));
        }
        if entry.log_id.index == self.status.next_index {
            // A peer's append response can leave `next_index` at the ceiling
            // (that claim is stored deliberately), so an entry arriving at it
            // would have no representable successor. Refusing is the only
            // honest answer: clamping would leave the next index pointing at
            // an entry already taken.
            let Some(next_index) = entry.log_id.index.checked_add(1) else {
                self.status.telemetry_mut().out_of_order_append_rejections += 1;
                return Err(RaftError::InvalidRequest(
                    "append index is the largest representable index".to_string(),
                ));
            };
            self.status.match_index = entry.log_id.index;
            self.status.next_index = next_index;
            self.drain_reorder_queue();
            return Ok(());
        }
        if !self.limits().enable_reorder_queue {
            self.status.telemetry_mut().out_of_order_append_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "out-of-order append received while reorder queue is disabled".to_string(),
            ));
        }
        if self.reorder_queue.len() as u64 >= self.limits().reorder_window_size {
            self.status.telemetry_mut().reorder_entries_rejected += 1;
            return Err(RaftError::InvalidRequest(
                "reorder queue window is full".to_string(),
            ));
        }
        // The timeout path records what it drops. Earn that counter storage
        // while this path is already allocating its first queue node, not on
        // a later sweep that otherwise needs no heap work.
        let _ = self.status.telemetry_mut();
        self.reorder_queue.insert(entry.log_id.index, 0);
        Ok(())
    }

    /// Ages every queued entry by `elapsed_us` and drops the ones that have
    /// waited longer than `reorder_timeout_us`.
    ///
    /// This is what makes the timeout a timeout. The pipeline keeps no clock --
    /// the host drives time here as it does for the leader lease -- so the age
    /// is what the caller has handed over, accumulated per entry since it was
    /// queued. An entry queued after a tick starts at zero and is not swept out
    /// by its neighbours' waiting.
    ///
    /// **A timeout of zero is off**, which is the convention the byte and task
    /// limits in this file already use. Without that, a default-constructed
    /// limit set would expire every out-of-order append on the first tick.
    ///
    /// Dropping is counted in `reorder_entry_timeouts` and
    /// `reorder_dropped_packages`, the same counters
    /// [`ReplicationPipeline::expire_reorder_queue`] moves -- that call remains
    /// the operator's all-or-nothing hammer, and this is the policy.
    ///
    /// The comparable engine drains from the front of an arrival-ordered list
    /// while the front is old enough. Ageing each entry is the same decision
    /// made per entry rather than per prefix: it does not depend on the queue
    /// being in arrival order, which a `BTreeMap` keyed by index is not.
    pub fn tick_reorder_queue(&mut self, elapsed_us: u64) -> u64 {
        if self.reorder_queue.is_empty() {
            return 0;
        }
        let timeout_us = self.limits().reorder_timeout_us;
        let mut dropped = 0_u64;
        self.reorder_queue.retain(|_, age_us| {
            *age_us = age_us.saturating_add(elapsed_us);
            let keep = timeout_us == 0 || *age_us < timeout_us;
            if !keep {
                dropped = dropped.saturating_add(1);
            }
            keep
        });
        if dropped == 0 {
            return 0;
        }
        let telemetry = self.status.telemetry_mut();
        telemetry.reorder_entry_timeouts = telemetry.reorder_entry_timeouts.saturating_add(dropped);
        telemetry.reorder_dropped_packages =
            telemetry.reorder_dropped_packages.saturating_add(dropped);
        dropped
    }

    pub fn expire_reorder_queue(&mut self) -> u64 {
        let dropped = self.reorder_queue.len() as u64;
        if dropped > 0 {
            self.reorder_queue.clear();
            let telemetry = self.status.telemetry_mut();
            telemetry.reorder_entry_timeouts += dropped;
            telemetry.reorder_dropped_packages += dropped;
        }
        dropped
    }

    pub fn begin_snapshot_send(
        &mut self,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        self.begin_snapshot_send_shared(
            Arc::<str>::from(snapshot_id.into()),
            snapshot_index,
            total_chunks,
        )
    }

    pub(crate) fn begin_snapshot_send_shared(
        &mut self,
        snapshot_id: Arc<str>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        let batch = ActiveSnapshotTransferBatch::one(
            snapshot_id,
            self.peer_id,
            snapshot_index,
            total_chunks,
            ActiveSnapshotProgress {
                acknowledged_chunks: 0,
                bytes_sent: 0,
                bytes_received: 0,
                latest_receiving_elapsed_ms: 0,
            },
        );
        self.begin_snapshot_send_batch(batch)
    }

    pub(crate) fn can_begin_snapshot_send_for(&self, snapshot_index: LogIndex) -> bool {
        self.status.required_snapshot_index > 0
            && self.status.required_snapshot_index <= snapshot_index
            && !self.status.snapshot_sending
            && !self.status.snapshot_installing
    }

    pub(crate) fn snapshot_send_batch(
        snapshot_id: Arc<str>,
        peer_ids: impl IntoIterator<Item = NodeId>,
        peer_count: usize,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Arc<ActiveSnapshotTransferBatch> {
        ActiveSnapshotTransferBatch::many(
            snapshot_id,
            peer_ids,
            peer_count,
            snapshot_index,
            total_chunks,
            ActiveSnapshotProgress {
                acknowledged_chunks: 0,
                bytes_sent: 0,
                bytes_received: 0,
                latest_receiving_elapsed_ms: 0,
            },
        )
    }

    pub(crate) fn begin_snapshot_send_batch(
        &mut self,
        batch: Arc<ActiveSnapshotTransferBatch>,
    ) -> Result<(), RaftError> {
        if self.status.snapshot_sending || self.status.snapshot_installing {
            self.status.telemetry_mut().snapshot_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "snapshot transfer is already active".to_string(),
            ));
        }
        batch.with_progress(self.peer_id, |_| ())?;
        self.status.snapshot_sending = true;
        self.status.telemetry_mut().snapshot_send_attempts += 1;
        self.status.snapshot_install_total_chunks = batch.total_chunks;
        self.status.snapshot_install_progress_per_mille = 0;
        self.snapshot_transfer = Some(batch);
        Ok(())
    }

    pub fn maybe_require_snapshot(&mut self, required_index: LogIndex) -> bool {
        // This is a public entry point, so the transport validator is not
        // necessarily in the path -- the same reason the append ceiling is
        // enforced here as well as there.
        //
        // The ratchet below only ever moves up, which is what stops a snapshot
        // being re-sent for a requirement already recorded. That also means a
        // value it can never act on is permanent rather than merely ignored.
        // Measured: one `require_snapshot` of `LogIndex::MAX` from a peer left
        // `required_snapshot_index` at the ceiling, and the peer's own later
        // request at index 40 was then refused, so it could never be given the
        // snapshot it actually needed.
        //
        // The ceiling is the representable, unrecoverable case and is refused
        // here. An overclaim that is merely too high for the leader's log is
        // not judgeable from inside the pipeline, which does not know where the
        // leader's log ends; that one belongs to the caller.
        if required_index == LogIndex::MAX {
            return false;
        }
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
        let batch = self
            .snapshot_transfer
            .as_ref()
            .ok_or_else(|| RaftError::InvalidRequest("snapshot send is not active".to_string()))?;
        batch.with_progress(self.peer_id, |progress| progress.bytes_sent += bytes)?;
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
        let batch = self.snapshot_transfer.as_ref().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot transfer is not active".to_string())
        })?;
        let total_chunks = batch.total_chunks;
        let (per_mille, complete) = batch.with_progress(self.peer_id, |progress| {
            progress.acknowledged_chunks += 1;
            let per_mille = (progress.acknowledged_chunks * 1000)
                .checked_div(total_chunks)
                .map_or(1000, |per_mille| per_mille.min(1000));
            (per_mille, progress.acknowledged_chunks >= total_chunks)
        })?;
        self.status.snapshot_install_progress_per_mille = per_mille;
        if complete {
            self.finish_snapshot_install()?;
        }
        Ok(())
    }

    pub fn handle_snapshot_finish(
        &mut self,
        accepted: bool,
        committed_index: LogIndex,
    ) -> Result<(), RaftError> {
        let batch = self.snapshot_transfer.take().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot transfer is not active".to_string())
        })?;
        batch.with_progress(self.peer_id, |_| ())?;
        let snapshot_index = batch.snapshot_index;
        self.status.snapshot_sending = false;
        self.status.snapshot_installing = false;
        if accepted {
            self.status.snapshot_installed_index =
                self.status.snapshot_installed_index.max(snapshot_index);
            self.status.snapshot_install_progress_per_mille = 1000;
            self.status.match_index = self.status.match_index.max(committed_index);
            self.status.next_index = self.status.match_index.saturating_add(1);
            self.ack_snapshot_require();
            return Ok(());
        }

        self.status.telemetry_mut().snapshot_chunk_retry_count += 1;
        Ok(())
    }

    pub fn update_snapshot_progress(
        &mut self,
        remote_receiving: bool,
        elapsed_since_last_update_ms: u64,
        send_timeout_ms: u64,
    ) -> bool {
        let Some(batch) = self.snapshot_transfer.as_ref() else {
            return false;
        };
        if !self.status.snapshot_sending {
            return false;
        }
        let Ok(timed_out) = batch.with_progress(self.peer_id, |progress| {
            if remote_receiving {
                progress.latest_receiving_elapsed_ms = 0;
                return false;
            }
            progress.latest_receiving_elapsed_ms = progress
                .latest_receiving_elapsed_ms
                .saturating_add(elapsed_since_last_update_ms);
            progress.latest_receiving_elapsed_ms > send_timeout_ms
        }) else {
            return false;
        };
        if !timed_out {
            return false;
        }

        self.snapshot_transfer = None;
        self.status.snapshot_sending = false;
        self.status.snapshot_installing = false;
        self.status.snapshot_install_progress_per_mille = 0;
        self.status.telemetry_mut().snapshot_send_timeouts += 1;
        true
    }

    pub fn retry_snapshot_chunk(&mut self) -> Result<(), RaftError> {
        if self.snapshot_transfer.is_none() {
            return Err(RaftError::InvalidRequest(
                "snapshot transfer is not active".to_string(),
            ));
        }
        self.status.telemetry_mut().snapshot_chunk_retry_count += 1;
        Ok(())
    }

    pub fn begin_snapshot_install(
        &mut self,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        if self.status.snapshot_sending || self.status.snapshot_installing {
            self.status.telemetry_mut().snapshot_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "snapshot transfer is already active".to_string(),
            ));
        }
        self.status.snapshot_installing = true;
        self.status.snapshot_install_total_chunks = total_chunks;
        self.status.snapshot_install_progress_per_mille = 0;
        self.snapshot_transfer = Some(ActiveSnapshotTransferBatch::one(
            Arc::<str>::from(snapshot_id.into()),
            self.peer_id,
            snapshot_index,
            total_chunks,
            ActiveSnapshotProgress {
                acknowledged_chunks: 0,
                bytes_sent: 0,
                bytes_received: 0,
                latest_receiving_elapsed_ms: 0,
            },
        ));
        Ok(())
    }

    pub fn receive_snapshot_chunk(&mut self, bytes: u64, done: bool) -> Result<(), RaftError> {
        let batch = self.snapshot_transfer.as_ref().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot install is not active".to_string())
        })?;
        let total_chunks = batch.total_chunks;
        self.status.snapshot_install_progress_per_mille =
            batch.with_progress(self.peer_id, |progress| {
                progress.bytes_received += bytes;
                progress.acknowledged_chunks += 1;
                (progress.acknowledged_chunks * 1000)
                    .checked_div(total_chunks)
                    .map_or(1000, |per_mille| per_mille.min(1000))
            })?;
        if done {
            self.finish_snapshot_install()?;
        }
        Ok(())
    }

    pub fn rollback_snapshot_install(&mut self) {
        self.snapshot_transfer = None;
        self.status.snapshot_sending = false;
        self.status.snapshot_installing = false;
        self.status.telemetry_mut().snapshot_install_rolled_back += 1;
    }

    pub fn mark_snapshot_rejoin_after_compacted_log(&mut self) {
        self.status
            .telemetry_mut()
            .snapshot_rejoin_after_compacted_log = true;
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
        self.status.inflight_entries >= self.limits().max_inflights_replicate
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
    }

    fn drain_reorder_queue(&mut self) {
        let mut converged = 0_u64;
        while self.reorder_queue.remove(&self.status.next_index).is_some() {
            self.status.match_index = self.status.next_index;
            self.status.next_index = self.status.next_index.saturating_add(1);
            converged = converged.saturating_add(1);
        }
        if converged > 0 {
            let telemetry = self.status.telemetry_mut();
            telemetry.reorder_entries_converged = telemetry
                .reorder_entries_converged
                .saturating_add(converged);
        }
    }

    fn finish_snapshot_install(&mut self) -> Result<(), RaftError> {
        let batch = self.snapshot_transfer.take().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot transfer is not active".to_string())
        })?;
        batch.with_progress(self.peer_id, |_| ())?;
        let snapshot_index = batch.snapshot_index;
        self.status.snapshot_sending = false;
        self.status.snapshot_installing = false;
        self.status.snapshot_installed_index =
            self.status.snapshot_installed_index.max(snapshot_index);
        self.status.snapshot_install_progress_per_mille = 1000;
        self.status.match_index = self.status.match_index.max(snapshot_index);
        self.status.next_index = self.status.match_index.saturating_add(1);
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
    /// How long an out-of-order append may wait for the hole before it to be
    /// filled, in microseconds. **Zero is off.**
    ///
    /// Its magnitude used to be unread: the queue was a `BTreeSet<LogIndex>`
    /// that recorded no arrival time, so 1 and 5,000,000 behaved alike and the
    /// only use of the field was `reorder_timeout_us > 0` as a liveness
    /// predicate in `matrixraft_pipeline_evidence`. The queue carries an age
    /// per entry now and [`ReplicationPipeline::tick_reorder_queue`] is what
    /// compares the two.
    ///
    /// Entries leave the queue three ways: drained when the hole before them is
    /// filled, dropped by a tick once they have waited this long, or dropped
    /// all at once when an embedder calls `expire_reorder_queue` -- reachable
    /// as `AdminCommand::ExpirePeerReorderQueue`. All three of the latter
    /// increment `reorder_entry_timeouts`.
    pub reorder_timeout_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelineEvidence {
    pub per_peer_pipeline_state_present: bool,
    #[serde(default)]
    pub peer_pipeline_observed_peer_count: u64,
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
    #[serde(default)]
    pub packet_loss_reorder_faulted_peer_count: u64,
    #[serde(default)]
    pub packet_loss_reorder_recovered_peer_count: u64,
    #[serde(default)]
    pub packet_loss_reorder_all_faulted_peers_recovered: bool,
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
    #[serde(default)]
    pub peer_pipeline_observed_peer_count: u64,
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
    #[serde(default)]
    pub packet_loss_reorder_faulted_peer_count: u64,
    #[serde(default)]
    pub packet_loss_reorder_recovered_peer_count: u64,
    #[serde(default)]
    pub packet_loss_reorder_all_faulted_peers_recovered: bool,
    pub stale_term_rejection_present: bool,
    pub reorder_queue_enabled: bool,
    #[serde(default)]
    pub missing: Vec<String>,
}

const DEFAULT_PIPELINE_LIMITS: PipelineLimits = PipelineLimits {
    max_inflights_replicate: 256,
    max_memory_replicate_log_bytes: 64 * 1024 * 1024,
    max_inflights_apply_task: 1024,
    max_apply_batch_bytes: 8 * 1024 * 1024,
    enable_reorder_queue: true,
    reorder_window_size: 1024,
    reorder_timeout_us: 5_000_000,
};

impl PipelineLimits {
    pub fn production_default() -> Self {
        DEFAULT_PIPELINE_LIMITS
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
            reorder_entries_converged: 0,
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
        reorder_entries_converged: observed.reorder_entries_converged,
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
    let packet_loss_reorder_faulted_peer_count = peers
        .iter()
        .filter(|peer| peer.packet_loss_events > 0 && peer_has_reorder_fault(peer))
        .count() as u64;
    let packet_loss_reorder_recovered_peer_count = peers
        .iter()
        .filter(|peer| peer_has_packet_loss_reorder_recovery(peer))
        .count() as u64;
    PipelineEvidence {
        per_peer_pipeline_state_present: !peers.is_empty(),
        peer_pipeline_observed_peer_count: peers.len() as u64,
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
                || peer.reorder_entries_converged > 0
                || peer.reorder_entry_timeouts > 0
                || peer.reorder_dropped_packages > 0
        }),
        reorder_timeout_drop_present: peers
            .iter()
            .any(|peer| peer.reorder_entry_timeouts > 0 && peer.reorder_dropped_packages > 0),
        packet_loss_probe_present: peers
            .iter()
            .any(|peer| peer.packet_loss_events > 0 && peer.network_error_probe_transitions > 0),
        packet_loss_recovery_present: peers.iter().any(peer_has_packet_loss_recovery),
        reorder_convergence_present: peers.iter().any(|peer| {
            peer_has_reorder_fault(peer)
                && peer.append_accepted > 0
                && peer.reorder_queue_depth == 0
                && peer.match_index.saturating_add(1) >= peer.next_index
        }),
        packet_loss_reorder_same_peer_recovered: packet_loss_reorder_recovered_peer_count > 0,
        packet_loss_reorder_faulted_peer_count,
        packet_loss_reorder_recovered_peer_count,
        packet_loss_reorder_all_faulted_peers_recovered: packet_loss_reorder_faulted_peer_count > 0
            && packet_loss_reorder_faulted_peer_count == packet_loss_reorder_recovered_peer_count,
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
    let peer_pipeline_observed_peer_count = artifact.peers.len() as u64;
    let packet_loss_reorder_faulted_peer_count = recomputed.packet_loss_reorder_faulted_peer_count;
    let packet_loss_reorder_recovered_peer_count =
        recomputed.packet_loss_reorder_recovered_peer_count;
    let packet_loss_reorder_all_faulted_peers_recovered = recomputed
        .packet_loss_reorder_all_faulted_peers_recovered
        && artifact
            .evidence
            .packet_loss_reorder_all_faulted_peers_recovered
        && artifact.evidence.packet_loss_reorder_faulted_peer_count
            == packet_loss_reorder_faulted_peer_count
        && artifact.evidence.packet_loss_reorder_recovered_peer_count
            == packet_loss_reorder_recovered_peer_count
        && artifact.evidence.peer_pipeline_observed_peer_count == peer_pipeline_observed_peer_count;
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
        (
            packet_loss_reorder_all_faulted_peers_recovered,
            "packet_loss_reorder_all_faulted_peers_recovered",
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
        peer_pipeline_observed_peer_count,
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
        packet_loss_reorder_faulted_peer_count,
        packet_loss_reorder_recovered_peer_count,
        packet_loss_reorder_all_faulted_peers_recovered,
        stale_term_rejection_present,
        reorder_queue_enabled,
        missing,
    }
}

fn peer_has_packet_loss_recovery(peer: &PeerProgress) -> bool {
    peer.packet_loss_events > 0
        && peer.network_error_probe_transitions > 0
        && peer.append_accepted > 0
        && peer.match_index.saturating_add(1) >= peer.next_index
}

fn peer_has_reorder_fault(peer: &PeerProgress) -> bool {
    peer.out_of_order_append_rejections > 0
        || peer.reorder_entries_rejected > 0
        || peer.reorder_entries_converged > 0
        || peer.reorder_entry_timeouts > 0
        || peer.reorder_dropped_packages > 0
}

fn peer_has_packet_loss_reorder_recovery(peer: &PeerProgress) -> bool {
    peer_has_packet_loss_recovery(peer) && peer_has_reorder_fault(peer)
}
