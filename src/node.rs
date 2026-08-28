// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Node lifecycle API for embedding RustRaft as a standalone library.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    RustRaftError, RustRaftLogId, RustRaftLogIndex, RustRaftMessage, RustRaftNodeId,
    RustRaftPayload, RustRaftPeer, RustRaftProposeOptions, RustRaftReadIndexResponse,
    RustRaftSnapshotMeta, RustRaftStatusSnapshot, RustRaftStepResult,
};

pub use crate::{
    RaftNodeRuntime, RaftNodeRuntimeState, RaftNodeRuntimeStatus, RaftPeerRuntimeState,
    RaftRuntimeTimerStatus, RustRaftNodeOptions, RustRaftSnapshotTriggerStatus,
};

/// Public consensus lifecycle and command API implemented by production Raft runtimes.
pub trait RustRaftConsensus {
    fn start(&mut self) -> Result<(), RustRaftError>;
    fn stop(&mut self) -> Result<(), RustRaftError>;
    fn status(&self) -> Result<RustRaftStatusSnapshot, RustRaftError>;
    fn is_busy(&self) -> Result<bool, RustRaftError>;
    fn step(&mut self, message: RustRaftMessage) -> Result<RustRaftStepResult, RustRaftError>;
    fn step_batch(
        &mut self,
        messages: Vec<RustRaftMessage>,
    ) -> Result<Vec<RustRaftStepResult>, RustRaftError>;
    fn propose(
        &mut self,
        payload: RustRaftPayload,
        options: RustRaftProposeOptions,
    ) -> Result<RustRaftLogId, RustRaftError>;
    fn read_index(
        &self,
        min_commit_index: RustRaftLogIndex,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError>;
    fn add_peer(&mut self, peer: RustRaftPeer) -> Result<(), RustRaftError>;
    fn add_learner(&mut self, peer: RustRaftPeer) -> Result<(), RustRaftError>;
    fn promote_peer(&mut self, node_id: RustRaftNodeId) -> Result<(), RustRaftError>;
    fn add_witness(&mut self, peer: RustRaftPeer) -> Result<(), RustRaftError>;
    fn remove_peer(&mut self, node_id: RustRaftNodeId) -> Result<(), RustRaftError>;
    fn transfer_leader(&mut self, target: RustRaftNodeId) -> Result<(), RustRaftError>;
    fn resign_leader(&mut self, reason: &str) -> Result<bool, RustRaftError>;
    fn campaign(&mut self, forced: bool) -> Result<(), RustRaftError>;
    fn release_memory(&mut self) -> Result<bool, RustRaftError>;
    fn trigger_snapshot(&mut self) -> Result<RustRaftSnapshotMeta, RustRaftError>;
    fn complete_snapshot_trigger(&mut self, _snapshot_id: &str) -> Result<(), RustRaftError> {
        Ok(())
    }
}

pub const MATRIXRAFT_REQUEST_TIMER_MAX_TIMEOUT_MS: u64 = u64::MAX;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftTimerTask {
    pub node_id: RustRaftNodeId,
    pub request_id: u64,
    pub deadline_ms: u64,
    pub start_at_ms: u64,
}

impl RustRaftTimerTask {
    pub fn new(
        node_id: RustRaftNodeId,
        request_id: u64,
        deadline_ms: u64,
        start_at_ms: u64,
    ) -> Self {
        Self {
            node_id,
            request_id,
            deadline_ms,
            start_at_ms,
        }
    }

    fn timeout_key(&self) -> RustRaftTimerKey {
        RustRaftTimerKey {
            deadline_ms: self.deadline_ms,
            node_id: self.node_id,
            request_id: self.request_id,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct RustRaftTimerKey {
    deadline_ms: u64,
    node_id: RustRaftNodeId,
    request_id: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftRequestTimer {
    tasks: BTreeMap<(RustRaftNodeId, u64), RustRaftTimerTask>,
    pending_timeouts: BTreeSet<RustRaftTimerKey>,
}

impl RustRaftRequestTimer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn watch(
        &mut self,
        node_id: RustRaftNodeId,
        request_id: u64,
        deadline_ms: u64,
        start_at_ms: u64,
    ) -> Option<RustRaftTimerTask> {
        let handler = (node_id, request_id);
        let task = RustRaftTimerTask::new(node_id, request_id, deadline_ms, start_at_ms);
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

    pub fn cancel(
        &mut self,
        node_id: RustRaftNodeId,
        request_id: u64,
    ) -> Option<RustRaftTimerTask> {
        let task = self.tasks.remove(&(node_id, request_id))?;
        if task.deadline_ms != 0 {
            self.pending_timeouts.remove(&task.timeout_key());
        }
        Some(task)
    }

    pub fn notify(
        &mut self,
        node_id: RustRaftNodeId,
        request_id: u64,
    ) -> Option<RustRaftTimerTask> {
        self.cancel(node_id, request_id)
    }

    pub fn lapsed(&mut self, now_ms: u64, limit: usize) -> Vec<RustRaftTimerTask> {
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

    pub fn remove_node_tasks(&mut self, node_id: RustRaftNodeId) -> Vec<RustRaftTimerTask> {
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
pub struct RustRaftTickAdmission {
    pub accepted: bool,
    pub pending_ticks: u64,
    pub max_pending_ticks: u64,
    pub rejected_ticks: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftTickBackpressure {
    pub pending_ticks: u64,
    pub max_pending_ticks: u64,
    pub accepted_ticks: u64,
    pub rejected_ticks: u64,
    pub completed_ticks: u64,
}

impl RustRaftTickBackpressure {
    pub fn new(max_pending_ticks: u64) -> Self {
        Self {
            pending_ticks: 0,
            max_pending_ticks: max_pending_ticks.max(1),
            accepted_ticks: 0,
            rejected_ticks: 0,
            completed_ticks: 0,
        }
    }

    pub fn admit_tick(&mut self) -> RustRaftTickAdmission {
        if self.pending_ticks < self.max_pending_ticks {
            self.pending_ticks += 1;
            self.accepted_ticks += 1;
            return RustRaftTickAdmission {
                accepted: true,
                pending_ticks: self.pending_ticks,
                max_pending_ticks: self.max_pending_ticks,
                rejected_ticks: self.rejected_ticks,
                reason: "tick_admitted".to_string(),
            };
        }

        self.rejected_ticks += 1;
        RustRaftTickAdmission {
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
