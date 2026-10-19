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

/// How far a group got, and whether it can go further.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyReport {
    /// Reached this index. Keep sending.
    Reached { applied_index: LogIndex },
    /// Reached this index and cannot go past it until the group is resumed.
    ///
    /// The state machine refused an entry rather than failing to keep up: a
    /// deserialisation it cannot perform, a schema it does not know, a
    /// corruption. Retrying the same entry will do the same thing, so the
    /// applier stops handing this group tasks and refuses what is submitted
    /// for it, until a host that has installed a snapshot past the entry calls
    /// [`Applier::resume_group`].
    ///
    /// The distinction from a plain low `applied_index` is the one that
    /// matters: that means "not finished, send it again", and this means
    /// "sending it again will not help".
    Stalled {
        applied_index: LogIndex,
        reason: String,
    },
}

impl ApplyReport {
    pub fn reached(applied_index: LogIndex) -> Self {
        Self::Reached { applied_index }
    }

    pub fn applied_index(&self) -> LogIndex {
        match self {
            Self::Reached { applied_index } | Self::Stalled { applied_index, .. } => *applied_index,
        }
    }

    pub fn is_stalled(&self) -> bool {
        matches!(self, Self::Stalled { .. })
    }
}

/// Why a group is not applying, and where it stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyStall {
    pub applied_index: LogIndex,
    pub reason: String,
}

/// What a host does when its group has entries to apply.
///
/// Returns the index the group has reached. Returning a lower index than the
/// batch carried means the group did not finish it, and the applied index does
/// not move past what was reported — a read waiting on a later index keeps
/// waiting, which is the conservative direction.
pub trait ApplyHandler: Send + Sync {
    fn apply(&self, tasks: Vec<ApplyTask>) -> LogIndex;

    /// The same work, saying whether the group can continue.
    ///
    /// Defaults to [`ApplyReport::Reached`] with whatever `apply` reported, so
    /// a host that has nothing to stall for implements nothing. A host whose
    /// state machine can refuse an entry overrides this instead of `apply`,
    /// and then a refusal stops the applier rather than being retried forever.
    fn apply_reporting(&self, tasks: Vec<ApplyTask>) -> ApplyReport {
        ApplyReport::reached(self.apply(tasks))
    }
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
    /// Set when the handler reported a stall, and cleared by `resume_group`.
    stall: Option<ApplyStall>,
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
        // A group that has already stalled is not handed what is left: the
        // chunks after the one that stalled are the entries past the one the
        // state machine refused, and offering them would be applying out of
        // order.
        if self
            .state
            .applied
            .lock()
            .expect("applied mutex poisoned")
            .stall
            .is_some()
        {
            return;
        }
        for batch in tasks.chunks(self.batch_limit.max(1)) {
            let count = batch.len() as u64;
            let report = self.handler.apply_reporting(batch.to_vec());
            let reached = report.applied_index();

            let mut applied = self.state.applied.lock().expect("applied mutex poisoned");
            // Never moves backwards: a handler that reports an older index than
            // the group already reached does not un-apply anything.
            if reached > applied.applied_index {
                applied.applied_index = reached;
            }
            applied.batches += 1;
            applied.tasks += count;
            if let ApplyReport::Stalled {
                applied_index,
                reason,
            } = report
            {
                applied.stall = Some(ApplyStall {
                    applied_index,
                    reason,
                });
                // Woken, because a waiter on a later index will never be
                // reached now and should find that out from `wait_for_applied`
                // rather than by sitting out its timeout.
                self.state.advanced.notify_all();
                return;
            }
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
    /// The group has stalled below the index waited for. A read must not be
    /// served, and retrying will not help until the group is resumed -- the
    /// same reason `GroupGone` is not a timeout.
    ///
    /// Carries no reason, so this enum stays `Copy` and a wait does not clone a
    /// string. [`Applier::apply_stall`] has the reason for a caller that wants
    /// to log it.
    Stalled { applied_index: LogIndex },
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
            | ApplyWait::GroupGone { applied_index }
            | ApplyWait::Stalled { applied_index } => *applied_index,
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
    stalled_refusals: AtomicU64,
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
            stalled_refusals: AtomicU64::new(0),
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
    /// Queues apply work for a group.
    ///
    /// Refused while the group is stalled, rather than queued for a handler
    /// that has said it cannot go further. Without that, every task submitted
    /// after a refusal would be handed over, do the same thing and fail the
    /// same way -- a stuck state machine turned into a spin, with the queue
    /// filling behind it.
    pub fn submit(&self, key: DriverGroupKey, task: ApplyTask) -> Result<(), RaftError> {
        if let Some(stall) = self.apply_stall(key) {
            self.stalled_refusals.fetch_add(1, Ordering::Relaxed);
            self.refused.fetch_add(1, Ordering::Relaxed);
            return Err(RaftError::InvalidRequest(format!(
                "group {} node {} has stalled at applied index {}: {}",
                key.group_id, key.node_id, stall.applied_index, stall.reason
            )));
        }
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

    /// Why a group is not applying, or `None` while it is.
    pub fn apply_stall(&self, key: DriverGroupKey) -> Option<ApplyStall> {
        let state = {
            let groups = self.groups.lock().expect("applier groups mutex poisoned");
            Arc::clone(groups.get(&key)?)
        };
        // Bound rather than returned straight out: as a tail expression the
        // guard borrows `state` past the point `state` is dropped.
        let stall = state
            .applied
            .lock()
            .expect("applied mutex poisoned")
            .stall
            .clone();
        stall
    }

    /// Lets a stalled group apply again, and says whether it was stalled.
    ///
    /// For a host that has installed a snapshot past the entry its state
    /// machine refused. Nothing here checks that, because nothing here can:
    /// the applier does not know what the state machine will accept, only what
    /// it refused. A host that resumes a group without moving it past the
    /// entry gets the same stall on the next batch, which is the safe way for
    /// this to be wrong.
    ///
    /// Returns false for a group that is not stalled, including one that does
    /// not exist -- resuming is idempotent and this is not an error.
    pub fn resume_group(&self, key: DriverGroupKey) -> bool {
        let Some(state) = ({
            let groups = self.groups.lock().expect("applier groups mutex poisoned");
            groups.get(&key).map(Arc::clone)
        }) else {
            return false;
        };
        let mut applied = state.applied.lock().expect("applied mutex poisoned");
        if applied.stall.take().is_none() {
            return false;
        }
        // Woken, so a waiter that saw the stall can look again.
        state.advanced.notify_all();
        true
    }

    /// Tasks refused because their group had stalled. A subset of
    /// [`Self::refused_tasks`].
    pub fn stalled_refusals(&self) -> u64 {
        self.stalled_refusals.load(Ordering::Relaxed)
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
            if applied.stall.is_some() {
                return Ok(ApplyWait::Stalled {
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
