// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A driver that ticks many raft groups from one thread.
//!
//! A `NodeRuntime` owns a thread per group, which is the simplest thing that
//! works and costs exactly one OS thread per group — `examples/group_scaling.rs`
//! measures 1.00 at every size from 1 to 1024. A store that hosts ten thousand
//! groups cannot pay that.
//!
//! The shape here is the one multi-group stores converge on: groups register
//! with a driver, and the driver's own threads do the work. This module is the
//! tick half of that. One ticker thread holds a due-time heap keyed by
//! `(deadline, group)`, sleeps a millisecond, advances a logical clock, and
//! fires every group whose deadline has arrived. Thread count is a property of
//! the driver's options, not of how many groups are registered.
//!
//! Each group keeps its own interval, so a driver can carry a group ticking
//! every 10ms beside one ticking every second without either of them paying for
//! the other.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::{
    ChannelSelector, ChannelSelectorPolicy, GroupId, MailChannel, MailPriority, NodeId, RaftError,
    MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS,
};

/// The clock the ticker advances, in milliseconds per step.
const DRIVER_CLOCK_STEP_MS: u64 = 1;

/// Which group a driver entry belongs to.
///
/// A store hosts one replica of each of many groups, so a group id alone does
/// not identify an entry — two replicas of the same group in one process (which
/// the tests do) need distinct keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DriverGroupKey {
    pub group_id: GroupId,
    pub node_id: NodeId,
}

impl DriverGroupKey {
    pub fn new(group_id: GroupId, node_id: NodeId) -> Self {
        Self { group_id, node_id }
    }
}

/// What the driver calls when a group's tick comes due.
///
/// Implementors are called from the ticker thread and should return promptly;
/// the driver drops its lock before calling, so a slow implementation delays
/// only itself and whatever is behind it in the same millisecond, not the
/// clock.
pub trait DriverTickReceiver: Send + Sync {
    fn fire_tick(&self);
}

/// How a driver is sized.
///
/// The names match `MatrixRaftGroupContext`, which has carried them as a record
/// of intended wiring; this is the type that reads them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverOptions {
    /// Worker threads for group mail. The ticker is separate and there is
    /// always exactly one of it.
    pub worker_num: usize,
    /// Most channels a worker takes from one selection.
    pub max_messages_each_poll: usize,
    /// Most mails a group's channel holds before sends are refused.
    pub max_queue_depth: usize,
    /// Byte budget for one worker pass over a group.
    pub driver_batch_bytes: usize,
    /// Default tick interval for a group that registers without one.
    pub tick_interval_ms: u64,
}

impl Default for DriverOptions {
    fn default() -> Self {
        Self {
            worker_num: 1,
            max_messages_each_poll: 64,
            max_queue_depth: 4096,
            driver_batch_bytes: 1024 * 1024,
            tick_interval_ms: 100,
        }
    }
}

impl DriverOptions {
    fn validate(&self) -> Result<(), RaftError> {
        if self.worker_num == 0 {
            return Err(RaftError::InvalidRequest(
                "driver requires at least one worker".to_string(),
            ));
        }
        if self.tick_interval_ms == 0 {
            return Err(RaftError::InvalidRequest(
                "driver tick interval must be at least 1ms".to_string(),
            ));
        }
        Ok(())
    }
}

/// What a driver is doing, for anyone who has to operate one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverStats {
    pub registered_groups: usize,
    pub ticks_fired: u64,
    /// Ticks skipped because the group unregistered between being scheduled
    /// and coming due. Expected during shutdown; a climbing count while groups
    /// are stable is not.
    pub ticks_dropped: u64,
    pub clock_ms: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct Deadline {
    at_ms: u64,
    key: DriverGroupKey,
}

impl Ord for Deadline {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // By time first, then by key, so the order is total and a run is
        // reproducible rather than depending on heap luck.
        self.at_ms.cmp(&other.at_ms).then(self.key.cmp(&other.key))
    }
}

impl PartialOrd for Deadline {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct Registration {
    receiver: Arc<dyn DriverTickReceiver>,
    interval_ms: u64,
}

struct TickState {
    clock_ms: u64,
    /// `Reverse` because `BinaryHeap` is a max-heap and the earliest deadline
    /// is the one wanted.
    queue: BinaryHeap<Reverse<Deadline>>,
    groups: BTreeMap<DriverGroupKey, Registration>,
}

struct DriverInner {
    options: DriverOptions,
    state: Mutex<TickState>,
    exit: AtomicBool,
    ticks_fired: AtomicU64,
    ticks_dropped: AtomicU64,
}

impl DriverInner {
    /// One step of the clock: take what is due, reschedule it, then fire it
    /// with the lock dropped.
    fn advance_once(&self) {
        let due = {
            let mut state = self.state.lock().expect("driver state mutex poisoned");
            state.clock_ms = state.clock_ms.saturating_add(DRIVER_CLOCK_STEP_MS);
            let now = state.clock_ms;

            let mut due: Vec<Arc<dyn DriverTickReceiver>> = Vec::new();
            while let Some(Reverse(next)) = state.queue.peek() {
                if next.at_ms > now {
                    break;
                }
                let Some(Reverse(entry)) = state.queue.pop() else {
                    break;
                };
                let Some(registration) = state.groups.get(&entry.key) else {
                    // Unregistered between being scheduled and coming due.
                    self.ticks_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                due.push(Arc::clone(&registration.receiver));
                let interval = registration.interval_ms.max(1);
                // From the deadline rather than from now, so a group that fell
                // behind catches up to its schedule instead of drifting.
                let next_at = entry.at_ms.saturating_add(interval).max(now + 1);
                state.queue.push(Reverse(Deadline {
                    at_ms: next_at,
                    key: entry.key,
                }));
            }
            due
        };

        for receiver in due {
            receiver.fire_tick();
            self.ticks_fired.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Ticks many groups from one thread.
///
/// Dropping a driver stops its ticker and joins it.
pub struct Driver {
    inner: Arc<DriverInner>,
    ticker: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for Driver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Driver")
            .field("options", &self.inner.options)
            .field("registered_groups", &self.group_count())
            .finish()
    }
}

impl Driver {
    pub fn start(options: DriverOptions) -> Result<Self, RaftError> {
        options.validate()?;
        let inner = Arc::new(DriverInner {
            options,
            state: Mutex::new(TickState {
                clock_ms: 0,
                queue: BinaryHeap::new(),
                groups: BTreeMap::new(),
            }),
            exit: AtomicBool::new(false),
            ticks_fired: AtomicU64::new(0),
            ticks_dropped: AtomicU64::new(0),
        });

        let ticker_inner = Arc::clone(&inner);
        let ticker = thread::Builder::new()
            .name("rustraft-driver-ticker".to_string())
            .spawn(move || {
                while !ticker_inner.exit.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(DRIVER_CLOCK_STEP_MS));
                    ticker_inner.advance_once();
                }
            })
            .map_err(|err| RaftError::Transport(format!("failed to spawn driver ticker: {err}")))?;

        Ok(Self {
            inner,
            ticker: Some(ticker),
        })
    }

    pub fn options(&self) -> &DriverOptions {
        &self.inner.options
    }

    /// Registers a group, ticking at the driver's default interval.
    pub fn register_group(
        &self,
        key: DriverGroupKey,
        receiver: Arc<dyn DriverTickReceiver>,
    ) -> Result<(), RaftError> {
        self.register_group_every(key, receiver, self.inner.options.tick_interval_ms)
    }

    /// Registers a group with an interval of its own.
    ///
    /// Zero means the driver's default rather than a busy loop.
    pub fn register_group_every(
        &self,
        key: DriverGroupKey,
        receiver: Arc<dyn DriverTickReceiver>,
        interval_ms: u64,
    ) -> Result<(), RaftError> {
        let interval_ms = if interval_ms == 0 {
            self.inner.options.tick_interval_ms
        } else {
            interval_ms
        }
        .max(1);

        let mut state = self
            .inner
            .state
            .lock()
            .expect("driver state mutex poisoned");
        if state.groups.contains_key(&key) {
            return Err(RaftError::InvalidRequest(format!(
                "driver already holds group {} node {}",
                key.group_id, key.node_id
            )));
        }
        let at_ms = state.clock_ms.saturating_add(interval_ms);
        state.groups.insert(
            key,
            Registration {
                receiver,
                interval_ms,
            },
        );
        state.queue.push(Reverse(Deadline { at_ms, key }));
        Ok(())
    }

    /// Stops ticking a group. Its next scheduled deadline is dropped when it
    /// comes due, which `DriverStats::ticks_dropped` counts.
    pub fn cancel_group(&self, key: DriverGroupKey) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("driver state mutex poisoned");
        state.groups.remove(&key).is_some()
    }

    pub fn group_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("driver state mutex poisoned")
            .groups
            .len()
    }

    /// Threads this driver owns: its workers plus the one ticker.
    ///
    /// Deliberately a function of the options and nothing else — it does not
    /// move when groups register, which is the whole point of the driver.
    pub fn thread_count(&self) -> usize {
        self.inner.options.worker_num + 1
    }

    pub fn stats(&self) -> DriverStats {
        let state = self
            .inner
            .state
            .lock()
            .expect("driver state mutex poisoned");
        DriverStats {
            registered_groups: state.groups.len(),
            ticks_fired: self.inner.ticks_fired.load(Ordering::Relaxed),
            ticks_dropped: self.inner.ticks_dropped.load(Ordering::Relaxed),
            clock_ms: state.clock_ms,
        }
    }

    pub fn stop(&mut self) {
        self.inner.exit.store(true, Ordering::Relaxed);
        if let Some(ticker) = self.ticker.take() {
            let _ = ticker.join();
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.stop();
    }
}

/// What the driver hands a group when its mail is ready.
///
/// Called from a worker thread, one batch at a time for a given group, so an
/// implementation sees its own mail in order and never concurrently with
/// itself.
pub trait DriverMailHandler<Mail>: Send + Sync {
    fn handle_mail(&self, mails: Vec<Mail>);
}

/// Wraps a host's mail so the pool has something of its own to send.
///
/// Shutdown needs to wake workers that are blocked on the selector, and a
/// sentinel cannot be synthesised from an arbitrary `Mail`. Wrapping gives the
/// pool one, which is why workers can block indefinitely instead of polling a
/// timeout — an idle driver costs nothing.
enum Envelope<Mail> {
    Mail(Mail),
    Stop,
}

struct PoolGroup<Mail> {
    channel: Arc<MailChannel<Envelope<Mail>>>,
    slot: NodeId,
}

/// Tells the pool how many bytes a mail is, so `driver_batch_bytes` can bound a
/// batch. Supplied by the host, because only the host knows.
pub type DriverMailSize<Mail> = Arc<dyn Fn(&Mail) -> usize + Send + Sync>;

struct PoolInner<Mail> {
    options: DriverOptions,
    mail_size: Option<DriverMailSize<Mail>>,
    selector: ChannelSelector<Envelope<Mail>>,
    groups: Mutex<BTreeMap<DriverGroupKey, PoolGroup<Mail>>>,
    handlers: Mutex<BTreeMap<NodeId, Arc<dyn DriverMailHandler<Mail>>>>,
    next_slot: AtomicU64,
    exit: AtomicBool,
    batches_handled: AtomicU64,
    mails_handled: AtomicU64,
    mails_refused: AtomicU64,
}

/// What a worker pool is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverWorkerStats {
    pub registered_groups: usize,
    /// Batches handed to a group. One batch can carry many mails.
    pub batches_handled: u64,
    pub mails_handled: u64,
    /// Sends refused because a group's queue was at `max_queue_depth`.
    pub mails_refused: u64,
    pub queued_mails: i64,
}

/// Runs many groups' mail on a fixed pool of threads.
///
/// `NodeRuntime` gives each group a thread, so mail for a thousand groups needs
/// a thousand threads. Here `worker_num` threads select across every registered
/// group's channel and hand each group its mail in batches.
///
/// Dropping the pool stops its workers and joins them.
pub struct DriverWorkerPool<Mail> {
    inner: Arc<PoolInner<Mail>>,
    workers: Vec<JoinHandle<()>>,
}

impl<Mail> std::fmt::Debug for DriverWorkerPool<Mail> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverWorkerPool")
            .field("worker_num", &self.inner.options.worker_num)
            .field(
                "registered_groups",
                // Read directly rather than through group_count(), which lives
                // on the `Mail: Send` impl; Debug carries no bounds.
                &self
                    .inner
                    .groups
                    .lock()
                    .map(|groups| groups.len())
                    .unwrap_or(0),
            )
            .finish()
    }
}

impl<Mail: Send + 'static> DriverWorkerPool<Mail> {
    /// Starts a pool whose batches are bounded by `max_messages_each_poll`
    /// alone. `driver_batch_bytes` needs a size function; see
    /// [`DriverWorkerPool::start_with_mail_size`].
    pub fn start(options: DriverOptions) -> Result<Self, RaftError> {
        Self::start_inner(options, None)
    }

    /// Starts a pool that also bounds a batch by `driver_batch_bytes`, using
    /// `mail_size` to measure each mail.
    ///
    /// The budget is a floor of one: a mail larger than the whole budget is
    /// still delivered, on its own, rather than stalling its group forever.
    pub fn start_with_mail_size(
        options: DriverOptions,
        mail_size: DriverMailSize<Mail>,
    ) -> Result<Self, RaftError> {
        Self::start_inner(options, Some(mail_size))
    }

    fn start_inner(
        options: DriverOptions,
        mail_size: Option<DriverMailSize<Mail>>,
    ) -> Result<Self, RaftError> {
        options.validate()?;
        let inner = Arc::new(PoolInner {
            options,
            mail_size,
            selector: ChannelSelector::new(),
            groups: Mutex::new(BTreeMap::new()),
            handlers: Mutex::new(BTreeMap::new()),
            next_slot: AtomicU64::new(1),
            exit: AtomicBool::new(false),
            batches_handled: AtomicU64::new(0),
            mails_handled: AtomicU64::new(0),
            mails_refused: AtomicU64::new(0),
        });

        let mut workers = Vec::with_capacity(options.worker_num);
        for index in 0..options.worker_num {
            let worker_inner = Arc::clone(&inner);
            let handle = thread::Builder::new()
                .name(format!("rustraft-driver-worker-{index}"))
                .spawn(move || worker_inner.run_worker())
                .map_err(|err| {
                    RaftError::Transport(format!("failed to spawn driver worker: {err}"))
                })?;
            workers.push(handle);
        }

        Ok(Self { inner, workers })
    }

    /// Registers a group and returns nothing: mail goes in through `send`.
    pub fn register_group(
        &self,
        key: DriverGroupKey,
        handler: Arc<dyn DriverMailHandler<Mail>>,
    ) -> Result<(), RaftError> {
        let mut groups = self
            .inner
            .groups
            .lock()
            .expect("driver groups mutex poisoned");
        if groups.contains_key(&key) {
            return Err(RaftError::InvalidRequest(format!(
                "driver already holds group {} node {}",
                key.group_id, key.node_id
            )));
        }
        // A channel is keyed by one id, and two replicas of different groups
        // can share a node id, so the pool hands out its own dense slot rather
        // than reusing the node id.
        let slot = self.inner.next_slot.fetch_add(1, Ordering::Relaxed);
        let channel = MailChannel::new(slot, self.inner.options.max_queue_depth);
        groups.insert(key, PoolGroup { channel, slot });
        self.inner
            .handlers
            .lock()
            .expect("driver handlers mutex poisoned")
            .insert(slot, handler);
        Ok(())
    }

    pub fn cancel_group(&self, key: DriverGroupKey) -> bool {
        let mut groups = self
            .inner
            .groups
            .lock()
            .expect("driver groups mutex poisoned");
        let Some(group) = groups.remove(&key) else {
            return false;
        };
        self.inner
            .handlers
            .lock()
            .expect("driver handlers mutex poisoned")
            .remove(&group.slot);
        true
    }

    /// Queues mail for a group, refusing it when the group is at
    /// `max_queue_depth` rather than letting one slow group grow without bound.
    pub fn send(
        &self,
        key: DriverGroupKey,
        priority: MailPriority,
        mail: Mail,
    ) -> Result<(), RaftError> {
        let channel = {
            let groups = self
                .inner
                .groups
                .lock()
                .expect("driver groups mutex poisoned");
            let Some(group) = groups.get(&key) else {
                return Err(RaftError::NodeNotFound(key.node_id));
            };
            Arc::clone(&group.channel)
        };
        // The depth is checked here rather than left to the channel. A
        // channel's own limit is compared against a selector-wide count that is
        // only refreshed when a worker fetches, so a group whose worker is busy
        // -- exactly the group a depth bound is for -- would never be refused.
        if channel.queued_len() >= self.inner.options.max_queue_depth {
            self.inner.mails_refused.fetch_add(1, Ordering::Relaxed);
            return Err(RaftError::InvalidRequest(format!(
                "group {} node {} is at its queue depth of {}",
                key.group_id, key.node_id, self.inner.options.max_queue_depth
            )));
        }
        if self
            .inner
            .selector
            .try_send_to_channel(channel, priority, Envelope::Mail(mail))
            .is_err()
        {
            self.inner.mails_refused.fetch_add(1, Ordering::Relaxed);
            return Err(RaftError::InvalidRequest(format!(
                "group {} node {} is at its queue depth of {}",
                key.group_id, key.node_id, self.inner.options.max_queue_depth
            )));
        }
        Ok(())
    }

    pub fn group_count(&self) -> usize {
        self.inner
            .groups
            .lock()
            .expect("driver groups mutex poisoned")
            .len()
    }

    /// Threads this pool owns. A function of the options, not of the group
    /// count — which is the whole point.
    pub fn thread_count(&self) -> usize {
        self.inner.options.worker_num
    }

    pub fn stats(&self) -> DriverWorkerStats {
        DriverWorkerStats {
            registered_groups: self.group_count(),
            batches_handled: self.inner.batches_handled.load(Ordering::Relaxed),
            mails_handled: self.inner.mails_handled.load(Ordering::Relaxed),
            mails_refused: self.inner.mails_refused.load(Ordering::Relaxed),
            queued_mails: self.inner.selector.total_mail_count(),
        }
    }

    pub fn stop(&mut self) {
        self.shutdown();
    }
}

impl<Mail> DriverWorkerPool<Mail> {
    /// Stops the workers and joins them. Idempotent.
    ///
    /// Lives on the unbounded impl so `Drop` can call it: `Drop` cannot carry
    /// bounds the struct does not, and `stop` is public on the `Mail: Send`
    /// impl where callers are.
    fn shutdown(&mut self) {
        if self.workers.is_empty() {
            return;
        }
        self.inner.exit.store(true, Ordering::Relaxed);
        // One is enough: whoever receives it relays it before returning.
        self.inner.selector.send_global(Envelope::Stop);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl<Mail> Drop for DriverWorkerPool<Mail> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl<Mail: Send + 'static> PoolInner<Mail> {
    fn run_worker(&self) {
        let policy = ChannelSelectorPolicy {
            limit: self.options.max_messages_each_poll.max(1),
            timeout_ms: MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS,
        };
        while !self.exit.load(Ordering::Relaxed) {
            let selection = self.selector.select(policy, &[]);
            // Global mail is only ever the stop sentinel.
            //
            // `select` drains EVERY global mail, so one worker can take all the
            // sentinels that were posted and leave its colleagues parked for
            // good -- a join that never returns. Each worker relays one on its
            // way out instead, so the wake-up walks the pool however the drain
            // happened to split them.
            if selection
                .global_mails
                .iter()
                .any(|mail| matches!(mail, Envelope::Stop))
            {
                self.selector.send_global(Envelope::Stop);
                return;
            }
            for channel in selection.channels {
                if self.exit.load(Ordering::Relaxed) {
                    return;
                }
                self.drain_channel(&channel);
            }
        }
    }

    /// Cuts a drained batch into pieces no larger than `driver_batch_bytes`.
    ///
    /// Without a size function there is nothing to measure, so the whole batch
    /// goes as one and only the count bound applies.
    fn split_on_byte_budget(&self, mails: Vec<Mail>) -> Vec<Vec<Mail>> {
        let Some(mail_size) = self.mail_size.as_ref() else {
            return vec![mails];
        };
        let budget = self.options.driver_batch_bytes.max(1);
        let mut batches: Vec<Vec<Mail>> = Vec::new();
        let mut current: Vec<Mail> = Vec::new();
        let mut current_bytes = 0_usize;
        for mail in mails {
            let bytes = mail_size(&mail);
            // A single oversized mail goes on its own rather than never.
            if !current.is_empty() && current_bytes.saturating_add(bytes) > budget {
                batches.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current_bytes = current_bytes.saturating_add(bytes);
            current.push(mail);
        }
        if !current.is_empty() {
            batches.push(current);
        }
        batches
    }

    fn drain_channel(&self, channel: &Arc<MailChannel<Envelope<Mail>>>) {
        let slot = channel.replica_id();
        let envelopes = channel.fetch(&self.selector);
        if envelopes.is_empty() {
            return;
        }
        let mut mails = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            match envelope {
                Envelope::Mail(mail) => mails.push(mail),
                Envelope::Stop => return,
            }
        }
        if mails.is_empty() {
            return;
        }
        let handler = {
            let handlers = self
                .handlers
                .lock()
                .expect("driver handlers mutex poisoned");
            handlers.get(&slot).map(Arc::clone)
        };
        // A group cancelled between selection and drain has no handler; its
        // mail is dropped rather than held for a group that is gone.
        let Some(handler) = handler else {
            return;
        };
        for batch in self.split_on_byte_budget(mails) {
            let count = batch.len() as u64;
            handler.handle_mail(batch);
            self.batches_handled.fetch_add(1, Ordering::Relaxed);
            self.mails_handled.fetch_add(count, Ordering::Relaxed);
        }
    }
}
