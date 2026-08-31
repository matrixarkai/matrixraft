// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style scheduler task queue contracts.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::{
    HardState, LogEntry, LogId, LogIndex, MailBox, MailBoxFetchPolicy, MailPriority, Message,
    NodeId, RaftError, SnapshotMetadata,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplyTask {
    pub entries: Vec<LogEntry>,
    pub snapshot: Option<SnapshotMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlushTaskDesc {
    pub first_index: Option<LogIndex>,
    pub last_index: Option<LogIndex>,
    pub unstable_config_change_index: Option<LogIndex>,
    pub delay_apply_task: Option<ApplyTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlushTask {
    pub desc: FlushTaskDesc,
    pub committed_index: LogIndex,
    pub should_flush_meta: bool,
    pub members: Vec<NodeId>,
    pub hard_state: Option<HardState>,
    pub entries: Vec<LogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResetTask {
    pub initial_state: LogId,
    pub members: Vec<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadTask {
    pub target_id: NodeId,
    pub from_index: LogIndex,
    pub to_index: LogIndex,
    pub limit_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplySnapshotTask {
    pub snapshot: SnapshotMetadata,
    pub target_id: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TriggerSnapshotTask {
    pub request_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchedulerTask {
    TriggerSnapshot(TriggerSnapshotTask),
    ApplySnapshot(ApplySnapshotTask),
    Read(ReadTask),
    Apply(ApplyTask),
    Reset(ResetTask),
    Flush(FlushTask),
    Message(Message),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplyResult {
    pub applied_index: LogIndex,
    pub rejected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepDownSignal {
    pub transferee: Option<NodeId>,
}

#[derive(Debug)]
pub struct Scheduler {
    tasks: MailBox<SchedulerTask>,
    apply_results: Mutex<Vec<ApplyResult>>,
    step_downs: Mutex<Vec<StepDownSignal>>,
}

impl Scheduler {
    pub fn new(high_watermark: usize) -> Self {
        Self {
            tasks: MailBox::new(high_watermark),
            apply_results: Mutex::new(Vec::new()),
            step_downs: Mutex::new(Vec::new()),
        }
    }

    // The rejected task is returned by value (channel `try_send` semantics) so the
    // caller can re-schedule it; boxing it would complicate that retry contract.
    #[allow(clippy::result_large_err)]
    pub fn try_schedule(
        &self,
        priority: MailPriority,
        task: SchedulerTask,
    ) -> Result<(), SchedulerTask> {
        if self.tasks.try_send(priority, task.clone()) {
            Ok(())
        } else {
            Err(task)
        }
    }

    pub fn schedule(&self, priority: MailPriority, task: SchedulerTask) {
        self.tasks.send(priority, task);
    }

    pub fn wait_and_schedule(&self, priority: MailPriority, task: SchedulerTask) {
        self.tasks.wait_and_send(priority, task);
    }

    pub fn schedule_message(&self, message: Message) {
        self.schedule(MailPriority::Normal, SchedulerTask::Message(message));
    }

    pub fn fetch(&self, policy: MailBoxFetchPolicy) -> Vec<SchedulerTask> {
        self.tasks.fetch(policy)
    }

    pub fn clear(&self) {
        self.tasks.clear();
    }

    pub fn queued_tasks(&self) -> usize {
        self.tasks.total_len()
    }

    pub fn send_apply_result(&self, applied_index: LogIndex, rejected: bool) {
        self.apply_results
            .lock()
            .expect("scheduler apply result mutex poisoned")
            .push(ApplyResult {
                applied_index,
                rejected,
            });
    }

    pub fn drain_apply_results(&self) -> Vec<ApplyResult> {
        let mut results = self
            .apply_results
            .lock()
            .expect("scheduler apply result mutex poisoned");
        std::mem::take(&mut *results)
    }

    pub fn step_down(&self, transferee: Option<NodeId>) {
        self.step_downs
            .lock()
            .expect("scheduler step-down mutex poisoned")
            .push(StepDownSignal { transferee });
    }

    pub fn drain_step_downs(&self) -> Vec<StepDownSignal> {
        let mut signals = self
            .step_downs
            .lock()
            .expect("scheduler step-down mutex poisoned");
        std::mem::take(&mut *signals)
    }

    pub fn validate_flush_task(task: &FlushTask) -> Result<(), RaftError> {
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
