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

use crate::{GroupId, NodeId, RaftError};

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
