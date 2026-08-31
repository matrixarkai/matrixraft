// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Node lifecycle API for embedding RustRaft as a standalone library.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    LogId, LogIndex, Message, NodeId, Payload, Peer, ProposeOptions, RaftError, ReadIndexResponse,
    SnapshotMetadata, StatusSnapshot, StepResult,
};

pub use crate::{
    NodeOptions, NodeRuntime, NodeRuntimeState, NodeRuntimeStatus, PeerRuntimeState,
    RuntimeTimerStatus, SnapshotTriggerStatus,
};

/// Public consensus lifecycle and command API implemented by production Raft runtimes.
pub trait Consensus {
    fn start(&mut self) -> Result<(), RaftError>;
    fn stop(&mut self) -> Result<(), RaftError>;
    fn status(&self) -> Result<StatusSnapshot, RaftError>;
    fn is_busy(&self) -> Result<bool, RaftError>;
    fn step(&mut self, message: Message) -> Result<StepResult, RaftError>;
    fn step_batch(&mut self, messages: Vec<Message>) -> Result<Vec<StepResult>, RaftError>;
    fn propose(&mut self, payload: Payload, options: ProposeOptions) -> Result<LogId, RaftError>;
    fn read_index(&self, min_commit_index: LogIndex) -> Result<ReadIndexResponse, RaftError>;
    fn add_peer(&mut self, peer: Peer) -> Result<(), RaftError>;
    fn add_learner(&mut self, peer: Peer) -> Result<(), RaftError>;
    fn promote_peer(&mut self, node_id: NodeId) -> Result<(), RaftError>;
    fn add_witness(&mut self, peer: Peer) -> Result<(), RaftError>;
    fn remove_peer(&mut self, node_id: NodeId) -> Result<(), RaftError>;
    fn transfer_leader(&mut self, target: NodeId) -> Result<(), RaftError>;
    fn resign_leader(&mut self, reason: &str) -> Result<bool, RaftError>;
    fn campaign(&mut self, forced: bool) -> Result<(), RaftError>;
    fn release_memory(&mut self) -> Result<bool, RaftError>;
    fn trigger_snapshot(&mut self) -> Result<SnapshotMetadata, RaftError>;
    fn complete_snapshot_trigger(&mut self, _snapshot_id: &str) -> Result<(), RaftError> {
        Ok(())
    }
}

pub const MATRIXRAFT_REQUEST_TIMER_MAX_TIMEOUT_MS: u64 = u64::MAX;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimerTask {
    pub node_id: NodeId,
    pub request_id: u64,
    pub deadline_ms: u64,
    pub start_at_ms: u64,
}

impl TimerTask {
    pub fn new(node_id: NodeId, request_id: u64, deadline_ms: u64, start_at_ms: u64) -> Self {
        Self {
            node_id,
            request_id,
            deadline_ms,
            start_at_ms,
        }
    }

    fn timeout_key(&self) -> TimerKey {
        TimerKey {
            deadline_ms: self.deadline_ms,
            node_id: self.node_id,
            request_id: self.request_id,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct TimerKey {
    deadline_ms: u64,
    node_id: NodeId,
    request_id: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestTimer {
    tasks: BTreeMap<(NodeId, u64), TimerTask>,
    pending_timeouts: BTreeSet<TimerKey>,
}

impl RequestTimer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn watch(
        &mut self,
        node_id: NodeId,
        request_id: u64,
        deadline_ms: u64,
        start_at_ms: u64,
    ) -> Option<TimerTask> {
        let handler = (node_id, request_id);
        let task = TimerTask::new(node_id, request_id, deadline_ms, start_at_ms);
        let previous = self.tasks.insert(handler, task.clone());
        if let Some(previous_task) = previous.as_ref() {
            if previous_task.deadline_ms != 0 {
                self.pending_timeouts.remove(&previous_task.timeout_key());
            }
        }
        if deadline_ms != 0 {
            self.pending_timeouts.insert(task.timeout_key());
        }
        previous
    }

    pub fn cancel(&mut self, node_id: NodeId, request_id: u64) -> Option<TimerTask> {
        let task = self.tasks.remove(&(node_id, request_id))?;
        if task.deadline_ms != 0 {
            self.pending_timeouts.remove(&task.timeout_key());
        }
        Some(task)
    }

    pub fn notify(&mut self, node_id: NodeId, request_id: u64) -> Option<TimerTask> {
        self.cancel(node_id, request_id)
    }

    pub fn lapsed(&mut self, now_ms: u64, limit: usize) -> Vec<TimerTask> {
        let mut timeout_tasks = Vec::new();
        while timeout_tasks.len() < limit {
            let Some(timeout_key) = self.pending_timeouts.iter().next().copied() else {
                break;
            };
            if timeout_key.deadline_ms >= now_ms {
                break;
            }
            self.pending_timeouts.remove(&timeout_key);
            if let Some(task) = self
                .tasks
                .remove(&(timeout_key.node_id, timeout_key.request_id))
            {
                timeout_tasks.push(task);
            }
        }
        timeout_tasks
    }

    pub fn next_timeout_ms(&self, now_ms: u64) -> u64 {
        let Some(timeout_key) = self.pending_timeouts.iter().next() else {
            return MATRIXRAFT_REQUEST_TIMER_MAX_TIMEOUT_MS;
        };
        if timeout_key.deadline_ms < now_ms {
            return 0;
        }
        timeout_key.deadline_ms - now_ms
    }

    pub fn remove_node_tasks(&mut self, node_id: NodeId) -> Vec<TimerTask> {
        let handlers: Vec<_> = self
            .tasks
            .range((node_id, 0)..=(node_id, u64::MAX))
            .map(|(handler, _)| *handler)
            .collect();

        let mut node_tasks = Vec::with_capacity(handlers.len());
        for (_, request_id) in handlers {
            if let Some(task) = self.cancel(node_id, request_id) {
                node_tasks.push(task);
            }
        }
        node_tasks
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn timed_len(&self) -> usize {
        self.pending_timeouts.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TickAdmission {
    pub accepted: bool,
    pub pending_ticks: u64,
    pub max_pending_ticks: u64,
    pub rejected_ticks: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TickBackpressure {
    pub pending_ticks: u64,
    pub max_pending_ticks: u64,
    pub accepted_ticks: u64,
    pub rejected_ticks: u64,
    pub completed_ticks: u64,
}

impl TickBackpressure {
    pub fn new(max_pending_ticks: u64) -> Self {
        Self {
            pending_ticks: 0,
            max_pending_ticks: max_pending_ticks.max(1),
            accepted_ticks: 0,
            rejected_ticks: 0,
            completed_ticks: 0,
        }
    }

    pub fn admit_tick(&mut self) -> TickAdmission {
        if self.pending_ticks < self.max_pending_ticks {
            self.pending_ticks += 1;
            self.accepted_ticks += 1;
            return TickAdmission {
                accepted: true,
                pending_ticks: self.pending_ticks,
                max_pending_ticks: self.max_pending_ticks,
                rejected_ticks: self.rejected_ticks,
                reason: "tick_admitted".to_string(),
            };
        }

        self.rejected_ticks += 1;
        TickAdmission {
            accepted: false,
            pending_ticks: self.pending_ticks,
            max_pending_ticks: self.max_pending_ticks,
            rejected_ticks: self.rejected_ticks,
            reason: "pending_tick_limit_reached".to_string(),
        }
    }

    pub fn complete_tick(&mut self) -> bool {
        if self.pending_ticks == 0 {
            return false;
        }
        self.pending_ticks -= 1;
        self.completed_ticks += 1;
        true
    }

    pub fn reset(&mut self) {
        self.pending_ticks = 0;
    }
}
