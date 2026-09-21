// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Applying many groups' committed entries on a shared pool, and letting a
//! read wait for its group to catch up.
//!
//! Apply is the slow half of a raft group: it touches the state machine and the
//! disk. A store hosting many groups cannot give each one a thread for it, so
//! this runs every group's apply work on a [`DriverWorkerPool`] — `applier_num`
//! threads, batches bounded by `apply_max_batch_count`.
//!
//! It also answers a question the runtime could not. `docs/read_index_safety_review.md`
//! records that a follower read is served only when `applied_index >= read_index`
//! and otherwise reports `follower_apply_pending`, and notes that apply-wait —
//! blocking until the group catches up — was missing. [`Applier::wait_for_applied`]
//! is that wait: it returns when the group's applied index reaches the committed
//! index the read needs, and never before.
//!
//! The applier does not decide what applying means. A host implements
//! [`ApplyHandler`] and returns the index it reached, so this owns the
//! scheduling and the waiting and nothing about the state machine.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::{
    ApplyTask, DriverGroupKey, DriverMailHandler, DriverOptions, DriverWorkerPool, LogIndex,
    MailPriority, RaftError,
};

/// How an applier is sized.
///
/// The names match `MatrixRaftGroupContext`, which has carried them as a record
/// of intended wiring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplierOptions {
    /// Threads applying entries. Shared by every registered group.
    pub applier_num: usize,
    /// Most entries handed to a group's handler in one call.
    pub apply_max_batch_count: usize,
    /// Most apply tasks a group may have queued before more are refused.
    pub max_queue_depth: usize,
}

impl Default for ApplierOptions {
    fn default() -> Self {
        Self {
            applier_num: 2,
            apply_max_batch_count: 64,
            max_queue_depth: 4096,
        }
    }
}

impl ApplierOptions {
    fn validate(&self) -> Result<(), RaftError> {
        if self.applier_num == 0 {
            return Err(RaftError::InvalidRequest(
                "applier requires at least one thread".to_string(),
            ));
        }
        if self.apply_max_batch_count == 0 {
            return Err(RaftError::InvalidRequest(
                "apply batch count must be at least 1".to_string(),
            ));
        }
        Ok(())
    }

    fn to_driver_options(self) -> DriverOptions {
        DriverOptions {
            worker_num: self.applier_num,
            max_messages_each_poll: self.apply_max_batch_count,
            max_queue_depth: self.max_queue_depth,
            ..DriverOptions::default()
        }
    }
}

/// The index an apply task carries its group up to.
///
/// The last entry's, or zero for a task that carries none -- a snapshot-only
/// task, which moves the group by installing rather than by applying.
pub fn matrixraft_apply_task_through_index(task: &ApplyTask) -> LogIndex {
    task.entries
        .last()
        .map(|entry| entry.log_id.index)
        .unwrap_or(0)
}

/// What a host does when its group has entries to apply.
///
/// Returns the index the group has reached. Returning a lower index than the
/// batch carried means the group did not finish it, and the applied index does
/// not move past what was reported — a read waiting on a later index keeps
/// waiting, which is the conservative direction.
pub trait ApplyHandler: Send + Sync {
    fn apply(&self, tasks: Vec<ApplyTask>) -> LogIndex;
}

/// Where a group has got to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupApplyProgress {
    pub applied_index: LogIndex,
    pub batches_applied: u64,
    pub tasks_applied: u64,
}

#[derive(Debug, Default)]
struct AppliedState {
    applied_index: LogIndex,
    batches: u64,
    tasks: u64,
    /// Set when the group is cancelled, so a waiter stops waiting for a group
    /// that is gone instead of sitting out its whole timeout.
    cancelled: bool,
}

struct GroupState {
    applied: Mutex<AppliedState>,
    advanced: Condvar,
}

impl GroupState {
    fn new() -> Self {
        Self {
            applied: Mutex::new(AppliedState::default()),
            advanced: Condvar::new(),
        }
    }
}

/// Runs a group's apply work and reports how far it got.
struct GroupApplier {
    handler: Arc<dyn ApplyHandler>,
    state: Arc<GroupState>,
    batch_limit: usize,
}

impl DriverMailHandler<ApplyTask> for GroupApplier {
    fn handle_mail(&self, tasks: Vec<ApplyTask>) {
        // The pool already bounds a batch by `max_messages_each_poll`, which is
        // set from `apply_max_batch_count`. This second bound is not redundant:
        // it holds even if a caller builds the pool differently, and it is what
        // the option name promises.
        for batch in tasks.chunks(self.batch_limit.max(1)) {
            let count = batch.len() as u64;
            let reached = self.handler.apply(batch.to_vec());

            let mut applied = self.state.applied.lock().expect("applied mutex poisoned");
            // Never moves backwards: a handler that reports an older index than
            // the group already reached does not un-apply anything.
            if reached > applied.applied_index {
                applied.applied_index = reached;
            }
            applied.batches += 1;
            applied.tasks += count;
            self.state.advanced.notify_all();
        }
    }
}

/// Why a wait for an applied index ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyWait {
    /// The group reached the index. A read may be served.
    Reached { applied_index: LogIndex },
    /// It had not reached the index in time. A read must not be served.
    TimedOut { applied_index: LogIndex },
    /// The group was cancelled while the wait was in progress. A read must not
    /// be served, and retrying will not help -- which is why this is not a
    /// timeout.
    GroupGone { applied_index: LogIndex },
}

impl ApplyWait {
    /// True only when the group is at or past the index that was waited for.
    pub fn reached(&self) -> bool {
        matches!(self, ApplyWait::Reached { .. })
    }

    pub fn applied_index(&self) -> LogIndex {
        match self {
            ApplyWait::Reached { applied_index }
            | ApplyWait::TimedOut { applied_index }
            | ApplyWait::GroupGone { applied_index } => *applied_index,
        }
    }
}

/// Applies many groups' entries on a fixed pool.
pub struct Applier {
    options: ApplierOptions,
    pool: DriverWorkerPool<ApplyTask>,
    groups: Mutex<BTreeMap<DriverGroupKey, Arc<GroupState>>>,
    submitted: AtomicU64,
    refused: AtomicU64,
}

impl std::fmt::Debug for Applier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Applier")
            .field("options", &self.options)
            .field(
                "registered_groups",
                &self.groups.lock().map(|g| g.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl Applier {
    pub fn start(options: ApplierOptions) -> Result<Self, RaftError> {
        options.validate()?;
        Ok(Self {
            options,
            pool: DriverWorkerPool::start(options.to_driver_options())?,
            groups: Mutex::new(BTreeMap::new()),
            submitted: AtomicU64::new(0),
            refused: AtomicU64::new(0),
        })
    }

    pub fn options(&self) -> &ApplierOptions {
        &self.options
    }

    /// Threads this applier owns, which is `applier_num` whatever the group
    /// count.
    pub fn thread_count(&self) -> usize {
        self.pool.thread_count()
    }

    pub fn register_group(
        &self,
        key: DriverGroupKey,
        handler: Arc<dyn ApplyHandler>,
    ) -> Result<(), RaftError> {
        let state = Arc::new(GroupState::new());
        {
            let mut groups = self.groups.lock().expect("applier groups mutex poisoned");
            if groups.contains_key(&key) {
                return Err(RaftError::InvalidRequest(format!(
                    "applier already holds group {} node {}",
                    key.group_id, key.node_id
                )));
            }
            groups.insert(key, Arc::clone(&state));
        }
        let group_applier = GroupApplier {
            handler,
            state,
            batch_limit: self.options.apply_max_batch_count,
        };
        self.pool.register_group(
            key,
            Arc::new(group_applier) as Arc<dyn DriverMailHandler<ApplyTask>>,
        )
    }

    /// Stops applying for a group and wakes anything waiting on it, so a
    /// waiter cannot outlive the group it is waiting for.
    pub fn cancel_group(&self, key: DriverGroupKey) -> bool {
        let removed = {
            let mut groups = self.groups.lock().expect("applier groups mutex poisoned");
            groups.remove(&key)
        };
        let Some(state) = removed else {
            return false;
        };
        {
            let mut applied = state.applied.lock().expect("applied mutex poisoned");
            applied.cancelled = true;
        }
        state.advanced.notify_all();
        self.pool.cancel_group(key)
    }

    pub fn group_count(&self) -> usize {
        self.groups
            .lock()
            .expect("applier groups mutex poisoned")
            .len()
    }

    /// Queues apply work for a group.
    pub fn submit(&self, key: DriverGroupKey, task: ApplyTask) -> Result<(), RaftError> {
        match self.pool.send(key, MailPriority::Normal, task) {
            Ok(()) => {
                self.submitted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                self.refused.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    pub fn progress(&self, key: DriverGroupKey) -> Option<GroupApplyProgress> {
        let state = {
            let groups = self.groups.lock().expect("applier groups mutex poisoned");
            Arc::clone(groups.get(&key)?)
        };
        let applied = state.applied.lock().expect("applied mutex poisoned");
        Some(GroupApplyProgress {
            applied_index: applied.applied_index,
            batches_applied: applied.batches,
            tasks_applied: applied.tasks,
        })
    }

    /// Waits until a group has applied through `index`, or the timeout passes.
    ///
    /// This is the apply-wait a follower read needs: a read at `index` may be
    /// served when this returns [`ApplyWait::Reached`] and must not be when it
    /// returns [`ApplyWait::TimedOut`]. It never reports reached early — the
    /// applied index it carries is the group's own, taken under the same lock
    /// the applier advances it with.
    pub fn wait_for_applied(
        &self,
        key: DriverGroupKey,
        index: LogIndex,
        timeout: Duration,
    ) -> Result<ApplyWait, RaftError> {
        let state = {
            let groups = self.groups.lock().expect("applier groups mutex poisoned");
            let Some(state) = groups.get(&key) else {
                return Err(RaftError::NodeNotFound(key.node_id));
            };
            Arc::clone(state)
        };

        let deadline = Instant::now() + timeout;
        let mut applied = state.applied.lock().expect("applied mutex poisoned");
        while applied.applied_index < index {
            if applied.cancelled {
                return Ok(ApplyWait::GroupGone {
                    applied_index: applied.applied_index,
                });
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(ApplyWait::TimedOut {
                    applied_index: applied.applied_index,
                });
            }
            let (next, _) = state
                .advanced
                .wait_timeout(applied, deadline - now)
                .expect("applied mutex poisoned");
            applied = next;
        }
        Ok(ApplyWait::Reached {
            applied_index: applied.applied_index,
        })
    }

    pub fn submitted_tasks(&self) -> u64 {
        self.submitted.load(Ordering::Relaxed)
    }

    pub fn refused_tasks(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    pub fn stop(&mut self) {
        self.pool.stop();
    }
}
