// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style scheduler task queue contracts.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::{
    RaftError, RustRaftHardState, RustRaftLogEntry, RustRaftLogId, RustRaftLogIndex,
    RustRaftMailBox, RustRaftMailBoxFetchPolicy, RustRaftMailPriority, RustRaftMessage,
    RustRaftNodeId, RustRaftSnapshotMeta,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftApplyTask {
    pub entries: Vec<RustRaftLogEntry>,
    pub snapshot: Option<RustRaftSnapshotMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftFlushTaskDesc {
    pub first_index: Option<RustRaftLogIndex>,
    pub last_index: Option<RustRaftLogIndex>,
    pub unstable_config_change_index: Option<RustRaftLogIndex>,
    pub delay_apply_task: Option<RustRaftApplyTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftFlushTask {
    pub desc: RustRaftFlushTaskDesc,
    pub committed_index: RustRaftLogIndex,
    pub should_flush_meta: bool,
    pub members: Vec<RustRaftNodeId>,
    pub hard_state: Option<RustRaftHardState>,
    pub entries: Vec<RustRaftLogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftResetTask {
    pub initial_state: RustRaftLogId,
    pub members: Vec<RustRaftNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadTask {
    pub target_id: RustRaftNodeId,
    pub from_index: RustRaftLogIndex,
    pub to_index: RustRaftLogIndex,
    pub limit_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftApplySnapshotTask {
    pub snapshot: RustRaftSnapshotMeta,
    pub target_id: RustRaftNodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftTriggerSnapshotTask {
    pub request_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RustRaftSchedulerTask {
    TriggerSnapshot(RustRaftTriggerSnapshotTask),
    ApplySnapshot(RustRaftApplySnapshotTask),
    Read(RustRaftReadTask),
    Apply(RustRaftApplyTask),
    Reset(RustRaftResetTask),
    Flush(RustRaftFlushTask),
    Message(RustRaftMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftApplyResult {
    pub applied_index: RustRaftLogIndex,
    pub rejected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftStepDownSignal {
    pub transferee: Option<RustRaftNodeId>,
}

#[derive(Debug)]
pub struct RustRaftScheduler {
    tasks: RustRaftMailBox<RustRaftSchedulerTask>,
    apply_results: Mutex<Vec<RustRaftApplyResult>>,
    step_downs: Mutex<Vec<RustRaftStepDownSignal>>,
}

impl RustRaftScheduler {
    pub fn new(high_watermark: usize) -> Self {
        Self {
            tasks: RustRaftMailBox::new(high_watermark),
            apply_results: Mutex::new(Vec::new()),
            step_downs: Mutex::new(Vec::new()),
        }
    }

    // The rejected task is returned by value (channel `try_send` semantics) so the
    // caller can re-schedule it; boxing it would complicate that retry contract.
    #[allow(clippy::result_large_err)]
    pub fn try_schedule(
        &self,
        priority: RustRaftMailPriority,
        task: RustRaftSchedulerTask,
    ) -> Result<(), RustRaftSchedulerTask> {
        if self.tasks.try_send(priority, task.clone()) {
            Ok(())
        } else {
            Err(task)
        }
    }

    pub fn schedule(&self, priority: RustRaftMailPriority, task: RustRaftSchedulerTask) {
        self.tasks.send(priority, task);
    }

    pub fn wait_and_schedule(&self, priority: RustRaftMailPriority, task: RustRaftSchedulerTask) {
        self.tasks.wait_and_send(priority, task);
    }

    pub fn schedule_message(&self, message: RustRaftMessage) {
        self.schedule(
            RustRaftMailPriority::Normal,
            RustRaftSchedulerTask::Message(message),
        );
    }

    pub fn fetch(&self, policy: RustRaftMailBoxFetchPolicy) -> Vec<RustRaftSchedulerTask> {
        self.tasks.fetch(policy)
    }

    pub fn clear(&self) {
        self.tasks.clear();
    }

    pub fn queued_tasks(&self) -> usize {
        self.tasks.total_len()
    }

    pub fn send_apply_result(&self, applied_index: RustRaftLogIndex, rejected: bool) {
        self.apply_results
            .lock()
            .expect("scheduler apply result mutex poisoned")
            .push(RustRaftApplyResult {
                applied_index,
                rejected,
            });
    }

    pub fn drain_apply_results(&self) -> Vec<RustRaftApplyResult> {
        let mut results = self
            .apply_results
            .lock()
            .expect("scheduler apply result mutex poisoned");
        std::mem::take(&mut *results)
    }

    pub fn step_down(&self, transferee: Option<RustRaftNodeId>) {
        self.step_downs
            .lock()
            .expect("scheduler step-down mutex poisoned")
            .push(RustRaftStepDownSignal { transferee });
    }

    pub fn drain_step_downs(&self) -> Vec<RustRaftStepDownSignal> {
        let mut signals = self
            .step_downs
            .lock()
            .expect("scheduler step-down mutex poisoned");
        std::mem::take(&mut *signals)
    }

    pub fn validate_flush_task(task: &RustRaftFlushTask) -> Result<(), RaftError> {
        if task.should_flush_meta && task.hard_state.is_none() {
            return Err(RaftError::InvalidRequest(
                "flush task that flushes metadata must include hard_state".to_string(),
            ));
        }
        if let (Some(first), Some(last)) = (task.desc.first_index, task.desc.last_index) {
            if first == 0 || last < first {
                return Err(RaftError::InvalidRequest(format!(
                    "invalid flush task range: {first}..={last}"
                )));
            }
            if !task.entries.is_empty() {
                let entry_first = task.entries.first().expect("entry").log_id.index;
                let entry_last = task.entries.last().expect("entry").log_id.index;
                if entry_first != first || entry_last != last {
                    return Err(RaftError::InvalidRequest(format!(
                        "flush task entries cover {entry_first}..={entry_last}, expected {first}..={last}"
                    )));
                }
            }
        }
        Ok(())
    }
}
