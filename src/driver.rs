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

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle, ThreadId};
use std::time::{Duration, Instant};

use crate::{
    ChannelSelector, ChannelSelectorPolicy, GroupId, MailChannel, MailPriority, NodeId, RaftError,
    MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS,
};

/// The clock the ticker advances, in milliseconds per step.
///
/// Still the granularity of the logical clock. It is no longer how often the
/// ticker wakes: the ticker waits until the earliest deadline in its heap, so a
/// store on a 100ms interval wakes about ten times a second rather than a
/// thousand.
const DRIVER_CLOCK_STEP_MS: u64 = 1;

/// Longest the ticker will wait, however far away the next deadline is.
///
/// A backstop, not the cadence. `exit` is read between waits and a registration
/// notifies, so neither depends on this; it bounds how long a ticker with an
/// empty heap can sit before noticing anything at all.
const DRIVER_MAX_SLEEP_MS: u64 = 50;

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
    ///
    /// There is a floor, and it is not the 1ms that `validate` accepts. The
    /// ticker advances its clock after sleeping `DRIVER_CLOCK_STEP_MS`, and
    /// `thread::sleep` guarantees *at least* the duration asked for, so a
    /// reading lands every two milliseconds or so. A group is fired at most
    /// once per reading -- a backlog is deliberately not replayed, see
    /// `a_stalled_clock_fires_a_group_once_rather_than_storming_it` -- so an
    /// interval near the step cannot be held. Measured: 1024 groups on a 1ms
    /// interval deliver about 46% of the ticks the interval asks for, and
    /// [`DriverStats::ticks_skipped`] counts the rest.
    ///
    /// Heartbeats, leader leases and election timeouts are all counted in
    /// ticks, so an interval that cannot be held makes all three last longer
    /// than they were configured to. Read 1ms as "as fast as this will go"
    /// rather than as a rate; 10ms is held in full at every size measured.
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
    /// Ticks the schedule owed a group and did not deliver.
    ///
    /// A group whose deadline is already more than one interval in the past has
    /// been owed every interval in between. Those are not delivered -- see
    /// `a_stalled_clock_fires_a_group_once_rather_than_storming_it`, which
    /// refuses to hand a group five hundred ticks at once -- and until this
    /// counter existed nothing recorded that they had been owed.
    ///
    /// It is the shortfall a host otherwise needs an external probe to see.
    /// Measured with one: 1024 groups at a 1ms interval deliver 41.5% of the
    /// ticks asked for on 4 shards and 71.5% on 16, with the worker pool idle
    /// at 0.0% handed over throughout, because an in-place tick runs the raft
    /// work on the ticker thread. See [`crate::SharedRuntimeStats::ticks_skipped`] for
    /// the whole sweep. Heartbeats, leases and election timeouts are
    /// counted in ticks, so a shortfall here lengthens all three.
    pub ticks_skipped: u64,
    /// Times a registration woke the ticker because the group it registered
    /// came due before the deadline the ticker was waiting on.
    ///
    /// Registering woke the ticker unconditionally, which is a thread wake and a
    /// lock handoff per group. A host registers its groups in a loop and they
    /// land in the same millisecond, so all but the first asked the ticker to
    /// wake for a deadline it already held: 65,536 groups cost 824ns each to
    /// register, against 190ns once the wake became conditional.
    ///
    /// It is reported rather than merely fixed because the condition is not
    /// provable from the outside -- the ticker never sleeps longer than
    /// `DRIVER_MAX_SLEEP_MS`, so a wake that is skipped when it should not be
    /// costs latency and nothing fails. This counter is what makes that
    /// visible, and `driver_registration_wakes_only_when_the_deadline_moves`
    /// is what holds it.
    pub registration_wakes: u64,
    /// Times `cancel_group` had to wait for a tick that was already firing.
    ///
    /// A cancel takes the state lock and the ticker gathers what is due under
    /// the same lock, so a cancel either wins -- and the group is simply not
    /// gathered -- or arrives after the gather, holding a receiver the ticker
    /// is about to call. Only the second case waits, which is why this counts
    /// rather than being inferable from the cancel count: on a quiet driver it
    /// stays at zero, and a host that sees it climbing is cancelling into its
    /// own tick storm.
    pub cancel_quiesce_waits: u64,
    pub clock_ms: u64,
}

/// A group waiting for its millisecond: 8 bytes, and no time of its own,
/// because the bucket it sits in carries that. One per registered group,
/// appended and taken in runs rather than sifted one at a time.
#[derive(Clone, Copy)]
struct Scheduled {
    /// Where the registration lives, so coming due is an array index rather
    /// than a search. Paired with the generation it was scheduled under: a slot
    /// is handed out again after a cancel, and a group still scheduled across
    /// that reuse must not fire whoever took the slot over.
    slot: u32,
    generation: u32,
}

struct Registration {
    receiver: Arc<dyn DriverTickReceiver>,
    interval_ms: u64,
}

/// One registration's home. Vacant slots are kept rather than compacted, so a
/// slot index stays valid for as long as a deadline can be outstanding against
/// it.
struct Slot {
    registration: Option<Registration>,
    /// Bumped on every cancel, so a deadline scheduled before the cancel can be
    /// told from one scheduled after a reuse.
    generation: u32,
}

struct TickState {
    clock_ms: u64,
    /// Keyed by the millisecond they come due, so the earliest is the first key
    /// and groups that share an interval share a bucket. One entry per distinct
    /// deadline rather than one per group, which is what a heap held: it sifted
    /// an entry per group on every pop and every push, and cost 71.4ns a tick
    /// at 65,536 groups against 17.8ns here, on one interval, measured in the
    /// same run on the same box.
    ///
    /// The cost stops growing with the group count: 17.4, 16.7 and 17.8ns at
    /// 1,024, 8,192 and 65,536 groups, where the heap read 44.6, 59.0 and 71.4.
    /// What it grows with instead is how many distinct deadlines are in flight,
    /// which is bounded by the intervals in use rather than by the groups.
    ///
    /// Within a bucket the order is the order groups were put there. They all
    /// come due at the same logical time, so there is nothing to sort by; what
    /// the old tie-break was for was determinism, and an append-ordered run is
    /// deterministic.
    buckets: BTreeMap<u64, Vec<Scheduled>>,
    /// Bucket vectors that have been emptied, kept for the next bucket that
    /// needs one. A bucket holding a single group would otherwise free its
    /// vector and allocate another on every tick of that group -- measured at
    /// 2.25 allocations a tick against the 1.25 a heap managed, by
    /// `tests/tick_window.rs`, which is what caught it.
    spare: Vec<Vec<Scheduled>>,
    /// Key to slot. Consulted when a group is registered or cancelled, never
    /// when one comes due: a `BTreeMap` lookup on the tick path cost at least
    /// 55ns of the 138ns a tick took at 65,536 groups, which is where the
    /// slot table comes from.
    groups: BTreeMap<DriverGroupKey, u32>,
    slots: Vec<Slot>,
    /// Slots whose group was cancelled, to be handed out again.
    free: Vec<u32>,
}

struct DriverInner {
    options: DriverOptions,
    state: Mutex<TickState>,
    /// Wakes the ticker when the thing it is waiting for has changed: a group
    /// registered with a deadline earlier than the one it went to sleep on, or
    /// the driver shutting down.
    wake: Condvar,
    exit: AtomicBool,
    ticks_fired: AtomicU64,
    ticks_dropped: AtomicU64,
    ticks_skipped: AtomicU64,
    registration_wakes: AtomicU64,
    cancel_quiesce_waits: AtomicU64,
    /// Which firing pass the ticker is in, and whether it is in one: bumped
    /// once when a pass of due receivers has been gathered and once when they
    /// have all been called, so an odd value means receivers are in flight.
    ///
    /// Per pass, not per tick. A mark on each tick would put a lock or a fence
    /// on the path that costs 17.8ns at 65,536 groups, to serve a cancel that
    /// a host performs once per group in its life.
    fire_pass: AtomicU64,
    /// Cancels currently waiting for a pass to finish. Read by the ticker so
    /// the common case -- nobody cancelling -- pays one load per pass and no
    /// lock at all.
    cancel_waiters: AtomicUsize,
    /// Notified when a firing pass ends. Paired with `state`, like `wake`.
    quiesced: Condvar,
    /// The ticker's own thread, so a receiver that cancels its own group from
    /// inside `fire_tick` is not made to wait for itself.
    ticker_thread: OnceLock<ThreadId>,
}

impl DriverInner {
    /// Waits until the earliest deadline in the heap, or until something changes.
    ///
    /// The ticker used to sleep a flat `DRIVER_CLOCK_STEP_MS` and look again,
    /// which is a thousand wake-ups a second per shard whatever the interval. On
    /// a 100ms interval that is a hundred wake-ups for each one that had work,
    /// and it was the larger part of what an idle store cost: 33 threads with
    /// almost nothing due burned 0.196 cores doing this and nothing else.
    ///
    /// Taking the lock to read the next deadline and then handing that same guard
    /// to `wait_timeout` is what makes it safe: a registration cannot slip
    /// between the two and be missed, because it needs the lock to change the
    /// buckets and the notify happens under it.
    fn wait_for_next_deadline(&self, now_ms: u64) {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let wait_ms = match state.buckets.first_key_value() {
            // Already due: go round and fire it rather than sleeping on it.
            Some((&at_ms, _)) if at_ms <= now_ms => return,
            Some((&at_ms, _)) => (at_ms - now_ms).min(DRIVER_MAX_SLEEP_MS),
            // Nothing registered yet. A registration will notify.
            None => DRIVER_MAX_SLEEP_MS,
        };
        let _ = self.wake.wait_timeout(
            state,
            Duration::from_millis(wait_ms.max(DRIVER_CLOCK_STEP_MS)),
        );
    }

    /// Advances the clock to `now_ms`, then takes what is due, reschedules it,
    /// and fires it with the lock dropped.
    ///
    /// The caller passes elapsed wall time rather than this counting its own
    /// sleeps. `thread::sleep` guarantees *at least* the duration asked for, so
    /// a clock that added a millisecond per iteration fell behind by the
    /// overshoot -- 7.7% at 100 groups, where there is no capacity pressure at
    /// all. Leases and election timeouts are counted in these ticks, so the
    /// drift lengthened both.
    /// The caller lends the vector the due groups are gathered into, because
    /// there is one ticker thread and it can keep the same one for the life of
    /// the driver. Making a fresh one here was **one allocation per tick** for
    /// every group that came due together -- 1.250 allocations a tick in
    /// `tests/tick_window.rs`, which is where it showed.
    fn advance_to(&self, now_ms: u64, due: &mut Vec<Arc<dyn DriverTickReceiver>>) {
        debug_assert!(due.is_empty(), "the lent vector arrives drained");
        {
            let mut state = self.state.lock().expect("driver state mutex poisoned");
            // Never goes backwards, and a late wake-up catches up instead of
            // losing the time for good.
            if now_ms > state.clock_ms {
                state.clock_ms = now_ms;
            }
            let now = state.clock_ms;

            while let Some((&next_due, _)) = state.buckets.first_key_value() {
                if next_due > now {
                    break;
                }
                let Some((at_ms, mut scheduled)) = state.buckets.pop_first() else {
                    break;
                };
                for entry in scheduled.drain(..) {
                    // An array index, not a search. The generation check is
                    // what used to be a lookup failing: unregistered between
                    // being scheduled and coming due, or the slot handed to
                    // another group since.
                    let slot = &state.slots[entry.slot as usize];
                    let Some(registration) = slot
                        .registration
                        .as_ref()
                        .filter(|_| slot.generation == entry.generation)
                    else {
                        self.ticks_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    };
                    due.push(Arc::clone(&registration.receiver));
                    let interval = registration.interval_ms.max(1);
                    // Every whole interval between the deadline and now is a
                    // tick this group was owed and will not get, because
                    // `max(now + 1)` below pulls the deadline forward instead
                    // of delivering the backlog. Counted rather than delivered,
                    // so the shortfall is visible without an external probe.
                    let owed = now.saturating_sub(at_ms) / interval;
                    if owed > 0 {
                        self.ticks_skipped.fetch_add(owed, Ordering::Relaxed);
                    }
                    // From the deadline rather than from now, so a group that
                    // fell behind catches up to its schedule instead of
                    // drifting. Always past `at_ms`, because the interval is at
                    // least 1, so a bucket can never reschedule into itself.
                    let next_at = at_ms.saturating_add(interval).max(now + 1);
                    // `get_mut` before `insert` so the common case -- the rest
                    // of a bucket landing where the first of it did -- is one
                    // lookup, and so a new bucket can take a vector back from
                    // the pool rather than allocating one.
                    if let Some(bucket) = state.buckets.get_mut(&next_at) {
                        bucket.push(entry);
                    } else {
                        let mut fresh = state.spare.pop().unwrap_or_default();
                        fresh.push(entry);
                        state.buckets.insert(next_at, fresh);
                    }
                }
                // Emptied by the drain, and keeping its capacity.
                state.spare.push(scheduled);
            }

            // Marked while the lock is still held, which is what makes a
            // cancel's reading of it meaningful: a cancel holding this lock
            // sees either an even value -- nothing gathered, so nothing of its
            // group is in flight -- or the odd value of the pass that may be
            // holding its receiver. There is no window in between.
            if !due.is_empty() {
                self.fire_pass.fetch_add(1, Ordering::SeqCst);
            }
        }

        if due.is_empty() {
            return;
        }

        // Outside the lock, and drained rather than consumed so the vector goes
        // back to the caller empty and keeping its capacity.
        for receiver in due.drain(..) {
            receiver.fire_tick();
            self.ticks_fired.fetch_add(1, Ordering::Relaxed);
        }

        // Ends the pass before looking for waiters, so a cancel that increments
        // `cancel_waiters` after this read has already seen the new pass value
        // and will not wait. The load is what keeps a quiet driver off the
        // lock: one atomic read per pass when nobody is cancelling.
        self.fire_pass.fetch_add(1, Ordering::SeqCst);
        if self.cancel_waiters.load(Ordering::SeqCst) > 0 {
            let _state = self.state.lock().expect("driver state mutex poisoned");
            self.quiesced.notify_all();
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
                buckets: BTreeMap::new(),
                spare: Vec::new(),
                groups: BTreeMap::new(),
                slots: Vec::new(),
                free: Vec::new(),
            }),
            wake: Condvar::new(),
            exit: AtomicBool::new(false),
            ticks_fired: AtomicU64::new(0),
            ticks_dropped: AtomicU64::new(0),
            ticks_skipped: AtomicU64::new(0),
            registration_wakes: AtomicU64::new(0),
            cancel_quiesce_waits: AtomicU64::new(0),
            fire_pass: AtomicU64::new(0),
            cancel_waiters: AtomicUsize::new(0),
            quiesced: Condvar::new(),
            ticker_thread: OnceLock::new(),
        });

        let ticker_inner = Arc::clone(&inner);
        let ticker = thread::Builder::new()
            .name("rustraft-driver-ticker".to_string())
            .spawn(move || {
                // Recorded by the ticker itself rather than taken from the
                // JoinHandle, because `Thread::id` of a handle is available
                // before the thread runs but this is read on the cancel path
                // and has to be the id of whoever calls `fire_tick`.
                let _ = ticker_inner.ticker_thread.set(thread::current().id());
                let started = Instant::now();
                // One vector for the life of the ticker, lent to every call.
                let mut due: Vec<Arc<dyn DriverTickReceiver>> = Vec::new();
                while !ticker_inner.exit.load(Ordering::Relaxed) {
                    // Elapsed, not a count of iterations: see `advance_to`. That
                    // matters more now than it did, because the waits are no
                    // longer uniform.
                    ticker_inner.advance_to(started.elapsed().as_millis() as u64, &mut due);
                    ticker_inner.wait_for_next_deadline(started.elapsed().as_millis() as u64);
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
        // Read before the insert, because the insert is what may change it.
        let earliest_before = state.buckets.keys().next().copied();
        let registration = Registration {
            receiver,
            interval_ms,
        };
        let slot = match state.free.pop() {
            Some(slot) => {
                let reused = &mut state.slots[slot as usize];
                reused.registration = Some(registration);
                slot
            }
            None => {
                state.slots.push(Slot {
                    registration: Some(registration),
                    generation: 0,
                });
                (state.slots.len() - 1) as u32
            }
        };
        let generation = state.slots[slot as usize].generation;
        state.groups.insert(key, slot);
        state
            .buckets
            .entry(at_ms)
            .or_default()
            .push(Scheduled { slot, generation });
        // The ticker has to be woken when this deadline lands before the one it
        // went to sleep on, and nothing else would wake it in time. When it does
        // not -- which is every registration after the first at a given
        // millisecond -- the ticker is already due to wake then, so a notify
        // buys nothing and costs a thread wake and a lock handoff each time.
        //
        // That was the whole of the registration cost: a store registering the
        // groups it hosts does so in a loop, and the groups land in the same
        // bucket, so all but a handful of the wakes were for a deadline the
        // ticker already held. Strictly earlier is the condition -- an equal
        // deadline is one the ticker will already serve.
        let wake_ticker = earliest_before.is_none_or(|earliest| at_ms < earliest);
        // Dropped before the notify so the woken thread is not immediately
        // blocked on the lock we still hold.
        drop(state);
        if wake_ticker {
            self.inner
                .registration_wakes
                .fetch_add(1, Ordering::Relaxed);
            self.inner.wake.notify_all();
        }
        Ok(())
    }

    /// Stops ticking a group, and does not return while a tick of it is still
    /// running.
    ///
    /// Both halves matter to a caller that is about to drop whatever the
    /// receiver borrows. Removing the registration is what stops further ticks:
    /// any deadline still outstanding was scheduled under the old generation,
    /// so it is dropped when it comes due rather than fired, which
    /// `DriverStats::ticks_dropped` counts. But the ticker gathers the
    /// receivers it is about to call under the state lock and then calls them
    /// with the lock dropped, so a cancel that arrives after the gather can
    /// take the lock, remove the registration and return while `fire_tick` is
    /// running on the `Arc` the ticker already cloned. That is the window this
    /// waits out, and `DriverStats::cancel_quiesce_waits` counts how often it
    /// is hit.
    ///
    /// It waits for the whole pass rather than for this group's tick, which is
    /// a stronger guarantee than asked for and the reason it costs nothing when
    /// it is not needed: the alternative is marking each tick as it is fired,
    /// on a path measured in tens of nanoseconds.
    ///
    /// A receiver may cancel its own group from inside `fire_tick`. That is not
    /// made to wait -- it would be waiting for itself -- and it does not need
    /// to: the registration is gone before the wait would start, so the only
    /// tick of this group that can still be running is the caller's own.
    pub fn cancel_group(&self, key: DriverGroupKey) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("driver state mutex poisoned");
        let Some(slot) = state.groups.remove(&key) else {
            return false;
        };
        let vacated = &mut state.slots[slot as usize];
        vacated.registration = None;
        // Any deadline still outstanding for this group was scheduled under the
        // old generation, so it will be dropped rather than fired -- including
        // after the slot is handed to someone else.
        vacated.generation = vacated.generation.wrapping_add(1);
        state.free.push(slot);

        // Read under the lock the ticker marks the pass under, so this is
        // either the pass that may hold this group's receiver or an even value
        // meaning there is none.
        let pass = self.inner.fire_pass.load(Ordering::SeqCst);
        let on_the_ticker = self
            .inner
            .ticker_thread
            .get()
            .is_some_and(|ticker| *ticker == thread::current().id());
        if pass % 2 == 1 && !on_the_ticker {
            self.inner
                .cancel_quiesce_waits
                .fetch_add(1, Ordering::Relaxed);
            self.inner.cancel_waiters.fetch_add(1, Ordering::SeqCst);
            while self.inner.fire_pass.load(Ordering::SeqCst) == pass {
                // Bounded, so a notify that is somehow missed costs this caller
                // latency rather than wedging it -- the same reason the ticker's
                // own wait is bounded.
                let (next, _) = self
                    .inner
                    .quiesced
                    .wait_timeout(state, Duration::from_millis(DRIVER_MAX_SLEEP_MS))
                    .expect("driver state mutex poisoned");
                state = next;
            }
            self.inner.cancel_waiters.fetch_sub(1, Ordering::SeqCst);
        }
        true
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
            ticks_skipped: self.inner.ticks_skipped.load(Ordering::Relaxed),
            registration_wakes: self.inner.registration_wakes.load(Ordering::Relaxed),
            cancel_quiesce_waits: self.inner.cancel_quiesce_waits.load(Ordering::Relaxed),
            clock_ms: state.clock_ms,
        }
    }

    pub fn stop(&mut self) {
        self.inner.exit.store(true, Ordering::Relaxed);
        // Without this the join waits for the ticker's current sleep to run out,
        // which is now up to `DRIVER_MAX_SLEEP_MS` rather than a millisecond.
        self.inner.wake.notify_all();
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
    /// Identifies the channel. Distinct from the worker it is pinned to.
    channel_id: NodeId,
    /// The worker that serves this group, chosen once at registration.
    worker: usize,
}

/// One worker's own queue.
///
/// Separate selectors are the whole point: with one shared between workers,
/// every select, fire and send contends on a single mutex, and the pool gets
/// slower as threads are added rather than faster.
struct WorkerSlot<Mail> {
    selector: ChannelSelector<Envelope<Mail>>,
    /// The channel this worker is inside a handler for, or zero for none.
    ///
    /// Written while `handlers` is held, which is what makes a cancel's
    /// reading of it meaningful: the handler is taken out of that map under the
    /// same lock, so a cancel holding it sees either a handler that has not
    /// been taken -- and so cannot be called, because it is gone -- or the
    /// channel of a call that is already under way.
    ///
    /// A channel id is never zero and never reused: they are handed out from
    /// `next_slot`, which starts at one and only goes up. So a cancel waiting
    /// for this to stop being its own channel cannot be fooled by the same
    /// value arriving again for a different group.
    in_flight: AtomicU64,
    /// This worker's thread, so a handler that cancels its own group is not
    /// made to wait for itself.
    thread: OnceLock<ThreadId>,
}

/// Tells the pool how many bytes a mail is, so `driver_batch_bytes` can bound a
/// batch. Supplied by the host, because only the host knows.
pub type DriverMailSize<Mail> = Arc<dyn Fn(&Mail) -> usize + Send + Sync>;

struct PoolInner<Mail> {
    options: DriverOptions,
    mail_size: Option<DriverMailSize<Mail>>,
    slots: Vec<WorkerSlot<Mail>>,
    groups: Mutex<BTreeMap<DriverGroupKey, PoolGroup<Mail>>>,
    handlers: Mutex<BTreeMap<NodeId, Arc<dyn DriverMailHandler<Mail>>>>,
    next_slot: AtomicU64,
    exit: AtomicBool,
    batches_handled: AtomicU64,
    mails_handled: AtomicU64,
    mails_refused: AtomicU64,
    cancel_quiesce_waits: AtomicU64,
    /// Cancels waiting for a handler to return, so a worker that has just
    /// finished one pays a single load to find out that nobody cares.
    cancel_waiters: AtomicUsize,
    /// Notified when a handler returns. Paired with `handlers`.
    quiesced: Condvar,
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
    /// Times `cancel_group` had to wait for a handler that was already running.
    ///
    /// Zero on a pool whose groups are cancelled while idle. A host that sees
    /// it climbing is cancelling groups that are busy, which is worth knowing
    /// because that is the case where the cancel costs it a handler call.
    pub cancel_quiesce_waits: u64,
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
            slots: (0..options.worker_num)
                .map(|_| WorkerSlot {
                    selector: ChannelSelector::new(),
                    in_flight: AtomicU64::new(0),
                    thread: OnceLock::new(),
                })
                .collect(),
            groups: Mutex::new(BTreeMap::new()),
            handlers: Mutex::new(BTreeMap::new()),
            next_slot: AtomicU64::new(1),
            exit: AtomicBool::new(false),
            batches_handled: AtomicU64::new(0),
            mails_handled: AtomicU64::new(0),
            mails_refused: AtomicU64::new(0),
            cancel_quiesce_waits: AtomicU64::new(0),
            cancel_waiters: AtomicUsize::new(0),
            quiesced: Condvar::new(),
        });

        let mut workers = Vec::with_capacity(options.worker_num);
        for index in 0..options.worker_num {
            let worker_inner = Arc::clone(&inner);
            let handle = thread::Builder::new()
                .name(format!("rustraft-driver-worker-{index}"))
                .spawn(move || worker_inner.run_worker(index))
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
        // can share a node id, so the pool hands out its own dense id rather
        // than reusing the node id.
        let channel_id = self.inner.next_slot.fetch_add(1, Ordering::Relaxed);
        // Pinned by the group's own key, so the same group always lands on the
        // same worker and two groups rarely share one. Round-robin on
        // registration order would pile a burst of consecutive group ids onto
        // one worker whenever the count divides.
        let worker = self.inner.worker_for(&key);
        let channel = MailChannel::new(channel_id, self.inner.options.max_queue_depth);
        groups.insert(
            key,
            PoolGroup {
                channel,
                channel_id,
                worker,
            },
        );
        self.inner
            .handlers
            .lock()
            .expect("driver handlers mutex poisoned")
            .insert(channel_id, handler);
        Ok(())
    }

    /// Stops a group taking mail, and does not return while its handler is
    /// still running.
    ///
    /// Both halves matter to a caller that is about to drop what the handler
    /// borrows. Removing the group stops further mail reaching it and removing
    /// the handler stops a drain already in flight from finding anything to
    /// call -- but a worker takes the handler out of the map and calls it with
    /// the lock dropped, so a cancel arriving in between removed the handler
    /// and returned `true` while `handle_mail` was running on the `Arc` the
    /// worker had already cloned.
    ///
    /// The worker records which channel it is inside a handler for while it
    /// still holds that lock, so the two cases are now distinguishable and the
    /// second one waits. `DriverWorkerStats::cancel_quiesce_waits` counts it.
    ///
    /// This waits for *this group's* handler rather than for the worker to go
    /// idle: one worker serves many groups, and a group cancelled while its
    /// neighbour is mid-handler has nothing to wait for.
    ///
    /// Mail that was queued and not yet drained is dropped rather than
    /// delivered, which is unchanged and deliberate -- see `drain_channel`.
    ///
    /// A handler may cancel its own group from inside `handle_mail`. That is
    /// not made to wait, which would be a wait for itself: the pool records
    /// each worker's thread and skips the wait for it. Sound rather than
    /// merely convenient, because the handler is out of the map before the
    /// wait would begin, so the only call that can still be running is the
    /// caller's own.
    pub fn cancel_group(&self, key: DriverGroupKey) -> bool {
        let channel_id = {
            let mut groups = self
                .inner
                .groups
                .lock()
                .expect("driver groups mutex poisoned");
            let Some(group) = groups.remove(&key) else {
                return false;
            };
            group.channel_id
        };
        // The group is out before the handlers lock is taken, so nothing new
        // can be sent to it while this waits, and two locks are never held at
        // once.
        let worker = self.inner.worker_for(&key);
        let mut handlers = self
            .inner
            .handlers
            .lock()
            .expect("driver handlers mutex poisoned");
        handlers.remove(&channel_id);

        let slot = &self.inner.slots[worker];
        let on_this_worker = slot
            .thread
            .get()
            .is_some_and(|worker_thread| *worker_thread == thread::current().id());
        if slot.in_flight.load(Ordering::Acquire) == channel_id && !on_this_worker {
            self.inner
                .cancel_quiesce_waits
                .fetch_add(1, Ordering::Relaxed);
            self.inner.cancel_waiters.fetch_add(1, Ordering::SeqCst);
            while slot.in_flight.load(Ordering::Acquire) == channel_id {
                // Bounded, so a notify that is somehow missed costs this caller
                // latency rather than wedging it.
                let (next, _) = self
                    .inner
                    .quiesced
                    .wait_timeout(handlers, Duration::from_millis(DRIVER_MAX_SLEEP_MS))
                    .expect("driver handlers mutex poisoned");
                handlers = next;
            }
            self.inner.cancel_waiters.fetch_sub(1, Ordering::SeqCst);
        }
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
        let (channel, worker) = {
            let groups = self
                .inner
                .groups
                .lock()
                .expect("driver groups mutex poisoned");
            let Some(group) = groups.get(&key) else {
                return Err(RaftError::NodeNotFound(key.node_id));
            };
            (Arc::clone(&group.channel), group.worker)
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
        if self.inner.slots[worker]
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

    /// Queues many mails for one group at once.
    ///
    /// A host that has several messages for the same group -- a batch of append
    /// responses, or whatever one network read carried -- can hand them over
    /// together. `send` costs a lock on the group table, a lock on the worker's
    /// selector and a wake for **each** mail; this costs one of each for the
    /// batch, and the mails land in the channel in the order given.
    ///
    /// All or nothing. The batch is refused whole when it would take the group
    /// past `max_queue_depth`, rather than part of it going and the rest being
    /// dropped: a caller that has to retry needs to know what to retry, and
    /// "some prefix" is not something this could report. The refusal counts
    /// every mail in the batch, because none of them arrived.
    ///
    /// An empty batch is nothing to do, and in particular does not wake a
    /// worker to look at a channel with nothing in it.
    ///
    /// **Not free, and not always a win.** The batch is a vector the caller
    /// built, and at a batch of one that vector costs more than the locks it
    /// saves. Measured by `examples/pool_batch_send.rs` on this library's own
    /// pool, nanoseconds and allocations per mail, medians of seven:
    ///
    /// | mails per call | `send` | `send_many` |
    /// |---|---|---|
    /// | 1 | 301.1 ns, 0.217 allocations | 344.4 ns, 1.254 allocations |
    /// | 4 | 269.8 ns, 0.185 | 99.4 ns, 0.326 |
    /// | 8 | 262.2 ns, 0.176 | 45.3 ns, 0.154 |
    /// | 64 | 247.6 ns, 0.176 | 10.2 ns, 0.017 |
    ///
    /// So: faster from about four mails, and cheaper in allocations from about
    /// eight. A caller with one mail should use `send`, and a caller whose
    /// batch already exists -- because a network read or a replication round
    /// produced it -- gets the time without paying for the vector at all.
    pub fn send_many(
        &self,
        key: DriverGroupKey,
        priority: MailPriority,
        mails: Vec<Mail>,
    ) -> Result<(), RaftError> {
        if mails.is_empty() {
            return Ok(());
        }
        let (channel, worker) = {
            let groups = self
                .inner
                .groups
                .lock()
                .expect("driver groups mutex poisoned");
            let Some(group) = groups.get(&key) else {
                return Err(RaftError::NodeNotFound(key.node_id));
            };
            (Arc::clone(&group.channel), group.worker)
        };
        // Checked here for the reason `send` explains: the channel's own limit
        // is compared against a selector-wide count that is only refreshed when
        // a worker fetches, so a group whose worker is busy would never be
        // refused. Counting the batch in is what makes the check mean the same
        // thing for a batch as for one mail.
        let refused = mails.len();
        if channel.queued_len().saturating_add(refused) > self.inner.options.max_queue_depth {
            self.inner
                .mails_refused
                .fetch_add(refused as u64, Ordering::Relaxed);
            return Err(RaftError::InvalidRequest(format!(
                "group {} node {} cannot take {} more: its queue depth is {}",
                key.group_id, key.node_id, refused, self.inner.options.max_queue_depth
            )));
        }
        // Wrapped on the way in rather than collected first: a vector of
        // envelopes would be an allocation per batch for something the channel
        // consumes immediately. `map` over a vector's iterator keeps the exact
        // length, which the depth check inside needs.
        if self.inner.slots[worker]
            .selector
            .try_send_iter_to_channel_checked(
                channel,
                priority,
                mails.into_iter().map(Envelope::Mail),
            )?
            .is_err()
        {
            self.inner
                .mails_refused
                .fetch_add(refused as u64, Ordering::Relaxed);
            return Err(RaftError::InvalidRequest(format!(
                "group {} node {} cannot take {} more: its queue depth is {}",
                key.group_id, key.node_id, refused, self.inner.options.max_queue_depth
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
            cancel_quiesce_waits: self.inner.cancel_quiesce_waits.load(Ordering::Relaxed),
            queued_mails: self
                .inner
                .slots
                .iter()
                .map(|slot| slot.selector.total_mail_count())
                .sum(),
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
        // Each worker waits on its own slot, so each slot needs its own
        // sentinel. One posted to a single selector would wake one worker and
        // leave the rest on a join that never returns.
        for slot in &self.inner.slots {
            slot.selector.send_global(Envelope::Stop);
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

/// Clears a worker's in-flight mark when a drain leaves the handler.
///
/// A guard rather than a call at each return, so a panic inside a host's
/// handler cannot leave a cancel waiting on a mark that nothing will clear.
struct InFlightGuard<'pool, Mail> {
    inner: &'pool PoolInner<Mail>,
    worker: usize,
}

impl<Mail> Drop for InFlightGuard<'_, Mail> {
    fn drop(&mut self) {
        self.inner.finished_with(self.worker);
    }
}

impl<Mail> PoolInner<Mail> {
    /// Says the worker is out of the handler, and wakes a cancel if one is
    /// waiting for exactly that.
    ///
    /// The mark is cleared before the waiter count is read, so a cancel that
    /// registers itself after that read has already seen the mark cleared and
    /// will not wait. With nobody cancelling -- the whole life of a pool, minus
    /// a few microseconds -- this is one store and one load, and no lock.
    fn finished_with(&self, worker: usize) {
        self.slots[worker].in_flight.store(0, Ordering::Release);
        if self.cancel_waiters.load(Ordering::SeqCst) > 0 {
            let _handlers = self
                .handlers
                .lock()
                .expect("driver handlers mutex poisoned");
            self.quiesced.notify_all();
        }
    }
}

impl<Mail> Drop for DriverWorkerPool<Mail> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl<Mail: Send + 'static> PoolInner<Mail> {
    /// Which worker serves a group.
    ///
    /// Hashed rather than round-robin so a run of consecutive group ids spreads
    /// instead of landing on one worker whenever the count divides them.
    fn worker_for(&self, key: &DriverGroupKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % self.slots.len().max(1) as u64) as usize
    }

    fn run_worker(&self, worker: usize) {
        // Recorded by the worker itself, because this has to be the thread that
        // calls a handler -- it is read to tell a handler cancelling its own
        // group from anyone else cancelling it.
        let _ = self.slots[worker].thread.set(thread::current().id());
        let policy = ChannelSelectorPolicy {
            limit: self.options.max_messages_each_poll.max(1),
            timeout_ms: MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS,
        };
        let selector = &self.slots[worker].selector;
        // Three buffers for the life of this worker, lent to every pass. A
        // fresh vector per pass was an allocation per pass for the selection
        // and one per group for the drain.
        let mut envelopes: Vec<Envelope<Mail>> = Vec::new();
        let mut channels: Vec<Arc<MailChannel<Envelope<Mail>>>> = Vec::new();
        let mut global_mails: Vec<Envelope<Mail>> = Vec::new();
        while !self.exit.load(Ordering::Relaxed) {
            selector.select_into(policy, &[], &mut channels, &mut global_mails);
            // Global mail is only ever the stop sentinel.
            //
            // `select` drains EVERY global mail, so one worker can take all the
            // sentinels that were posted and leave its colleagues parked for
            // good -- a join that never returns. Each worker relays one on its
            // way out instead, so the wake-up walks the pool however the drain
            // happened to split them.
            if global_mails
                .iter()
                .any(|mail| matches!(mail, Envelope::Stop))
            {
                // One worker per slot now, so the sentinel has nobody to relay
                // to -- it is put back only so a second reader of the same slot
                // would still see it.
                selector.send_global(Envelope::Stop);
                return;
            }
            // The mail buffer is lent to the next pass, which needs it empty.
            // Nothing clears it here because there is nothing in it: global
            // mail is only ever the sentinel, and the sentinel returned above.
            // Asserted rather than assumed, because the assumption is what
            // makes the lending sound.
            debug_assert!(
                global_mails.is_empty(),
                "global mail is only ever the stop sentinel"
            );
            // Drained rather than consumed, so the buffer comes back empty
            // for the next pass and keeps the capacity it has earned. Breaking
            // out part way still empties it: dropping the `Drain` removes
            // whatever is left.
            for channel in channels.drain(..) {
                if self.exit.load(Ordering::Relaxed) {
                    return;
                }
                self.drain_channel(worker, &channel, &mut envelopes);
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

    /// Drains one channel into the handler, borrowing the worker's buffer.
    ///
    /// `envelopes` arrives empty and leaves empty; it is the worker's, kept for
    /// the life of the worker so a drain does not allocate one. It leaves empty
    /// even on the `Stop` path, because breaking out of a `drain` still drops
    /// the `Drain` guard and that removes whatever is left -- the `debug_assert`
    /// is what says so out loud, since the next channel in the same pass is
    /// drained with the same buffer.
    fn drain_channel(
        &self,
        worker: usize,
        channel: &Arc<MailChannel<Envelope<Mail>>>,
        envelopes: &mut Vec<Envelope<Mail>>,
    ) {
        debug_assert!(envelopes.is_empty(), "the lent buffer arrives drained");
        let channel_id = channel.replica_id();
        channel.fetch_into(&self.slots[worker].selector, envelopes);
        if envelopes.is_empty() {
            return;
        }
        let mut mails = Vec::with_capacity(envelopes.len());
        let mut stopping = false;
        for envelope in envelopes.drain(..) {
            match envelope {
                Envelope::Mail(mail) => mails.push(mail),
                Envelope::Stop => {
                    stopping = true;
                    break;
                }
            }
        }
        if stopping || mails.is_empty() {
            return;
        }
        let handler = {
            let handlers = self
                .handlers
                .lock()
                .expect("driver handlers mutex poisoned");
            let handler = handlers.get(&channel_id).map(Arc::clone);
            // Marked here, under the same lock the handler was taken from, so a
            // cancel holding that lock cannot see this call as not started.
            // Marking after the lock was dropped would leave exactly the window
            // the cancel is waiting to close.
            if handler.is_some() {
                self.slots[worker]
                    .in_flight
                    .store(channel_id, Ordering::Release);
            }
            handler
        };
        // A group cancelled between selection and drain has no handler; its
        // mail is dropped rather than held for a group that is gone.
        let Some(handler) = handler else {
            return;
        };
        // Without a mail sizer there is exactly one batch, and it is the vector
        // already in hand. `split_on_byte_budget` wrapped it in a second vector
        // to return it, which was an allocation per drain for a cut that was
        // never made.
        // Clears the mark however this leaves the handler, a panic in a
        // host's handler included: a cancel waiting on the mark would
        // otherwise wait for a call that is never coming back.
        let _in_flight = InFlightGuard {
            inner: self,
            worker,
        };
        if self.mail_size.is_none() {
            let count = mails.len() as u64;
            handler.handle_mail(mails);
            self.batches_handled.fetch_add(1, Ordering::Relaxed);
            self.mails_handled.fetch_add(count, Ordering::Relaxed);
            return;
        }
        for batch in self.split_on_byte_budget(mails) {
            let count = batch.len() as u64;
            handler.handle_mail(batch);
            self.batches_handled.fetch_add(1, Ordering::Relaxed);
            self.mails_handled.fetch_add(count, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Counts the ticks it is sent, so a test can say exactly how many a given
    /// clock move produced.
    #[derive(Debug, Default)]
    struct Counter(AtomicU64);

    impl Counter {
        fn count(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    impl DriverTickReceiver for Counter {
        fn fire_tick(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A driver's insides with **no ticker thread**, so the clock is driven by
    /// hand.
    ///
    /// `tests/driver.rs` exercises the running driver and must poll to a
    /// deadline to stay green on a loaded machine. That makes it the wrong
    /// place to assert a tick *rate*: a slow clock and a busy box look the
    /// same from outside. Driving the clock makes the counts exact and the
    /// assertions independent of the machine.
    fn hand_driven(groups: &[(u64, u64)]) -> (DriverInner, Arc<Counter>) {
        let counter = Arc::new(Counter::default());
        let mut buckets: BTreeMap<u64, Vec<Scheduled>> = BTreeMap::new();
        let mut registered = BTreeMap::new();
        let mut slots = Vec::new();
        for &(group_id, interval_ms) in groups {
            let key = DriverGroupKey::new(group_id, 1);
            slots.push(Slot {
                registration: Some(Registration {
                    receiver: Arc::clone(&counter) as Arc<dyn DriverTickReceiver>,
                    interval_ms,
                }),
                generation: 0,
            });
            let slot = (slots.len() - 1) as u32;
            registered.insert(key, slot);
            buckets.entry(interval_ms).or_default().push(Scheduled {
                slot,
                generation: 0,
            });
        }
        let inner = DriverInner {
            options: DriverOptions::default(),
            state: Mutex::new(TickState {
                clock_ms: 0,
                buckets,
                spare: Vec::new(),
                groups: registered,
                slots,
                free: Vec::new(),
            }),
            wake: Condvar::new(),
            exit: AtomicBool::new(false),
            ticks_fired: AtomicU64::new(0),
            ticks_dropped: AtomicU64::new(0),
            ticks_skipped: AtomicU64::new(0),
            registration_wakes: AtomicU64::new(0),
            cancel_quiesce_waits: AtomicU64::new(0),
            fire_pass: AtomicU64::new(0),
            cancel_waiters: AtomicUsize::new(0),
            quiesced: Condvar::new(),
            ticker_thread: OnceLock::new(),
        };
        (inner, counter)
    }

    impl DriverInner {
        /// Lends `advance_to` a vector of its own. The ticker keeps one for its
        /// whole life; a test that only cares which groups fired need not.
        fn advance_to_once(&self, now_ms: u64) {
            let mut due = Vec::new();
            self.advance_to(now_ms, &mut due);
        }

        /// The logical clock, for tests that assert on it directly rather
        /// than inferring it from which groups came due.
        fn clock(&self) -> u64 {
            self.state
                .lock()
                .expect("driver state mutex poisoned")
                .clock_ms
        }
    }

    #[test]
    fn a_tick_follows_the_clocks_value_not_how_often_it_is_asked() {
        // The ticker used to count its own sleeps, adding a millisecond to the
        // logical clock per loop. `thread::sleep` guarantees *at least* the
        // duration asked for, so every iteration overshot and the clock fell
        // behind wall time -- measured at 7.7% with 100 groups registered,
        // which is nowhere near any capacity limit.
        //
        // Heartbeats, leader leases and election timeouts are all counted in
        // these ticks, so the drift made leases last longer than they were
        // configured to, which is the dangerous direction.
        //
        // Asking five times at the same instant must therefore answer once.
        let (inner, counter) = hand_driven(&[(1, 10)]);

        for _ in 0..5 {
            inner.advance_to_once(10);
        }
        assert_eq!(
            counter.count(),
            1,
            "five calls at one instant fired {} ticks, so the clock is being \
             advanced by the call and not read from it",
            counter.count()
        );

        inner.advance_to_once(20);
        assert_eq!(counter.count(), 2, "the next interval is due at 20ms");
    }

    #[test]
    fn the_clock_does_not_run_backwards() {
        // Wall time is read rather than accumulated, so this guards against a
        // reading that goes backwards.
        //
        // The clock is read directly instead of being inferred from ticks. An
        // earlier version of this test watched the tick count alone and a
        // mutant that dropped the guard outright survived it: the group's next
        // deadline sat beyond both the forward and the backward reading, so
        // neither delivered a tick and the test passed on broken code.
        let (inner, counter) = hand_driven(&[(1, 10)]);

        inner.advance_to_once(100);
        assert_eq!(counter.count(), 1, "the group was due and fired once");
        assert_eq!(inner.clock(), 100, "the clock took the reading");

        inner.advance_to_once(50);
        assert_eq!(
            inner.clock(),
            100,
            "a backwards reading moved the clock back to {}",
            inner.clock()
        );
        assert_eq!(counter.count(), 1, "and nothing came due again");

        // Visible in ticks too: a group registered now is scheduled off the
        // clock, so a clock that went back would make it due early.
        inner.advance_to_once(101);
        assert_eq!(counter.count(), 2, "the schedule resumes from where it was");
    }

    #[test]
    fn a_stalled_clock_fires_a_group_once_rather_than_storming_it() {
        // Five seconds of a ten-millisecond interval is five hundred missed
        // ticks. Delivering them all would hand every group its whole backlog
        // at once, on the single ticker thread. The deadline is pulled forward
        // instead: one tick, then the ordinary cadence.
        let (inner, counter) = hand_driven(&[(1, 10)]);

        inner.advance_to_once(5_000);
        assert_eq!(
            counter.count(),
            1,
            "a five-second stall delivered {} ticks to one group",
            counter.count()
        );

        assert_eq!(
            inner.ticks_skipped.load(Ordering::Relaxed),
            499,
            "the five hundred ticks it was owed were refused but not recorded"
        );

        inner.advance_to_once(5_001);
        assert_eq!(counter.count(), 2, "and then it resumes");
        assert_eq!(
            inner.ticks_skipped.load(Ordering::Relaxed),
            499,
            "an on-time tick added to the shortfall"
        );
    }

    #[test]
    fn a_clock_read_less_often_than_the_interval_skips_the_difference() {
        // The machine-independent form of a measurement: 1024 groups at a 1ms
        // interval deliver about 46% of the ticks asked for, because the ticker
        // sleeps a millisecond and `thread::sleep` guarantees *at least* that.
        // A clock that can only be read every 2ms with a 1ms interval is the
        // same arithmetic with the overshoot made exact.
        let (inner, counter) = hand_driven(&[(1, 1)]);

        for ms in (2..=20).step_by(2) {
            inner.advance_to_once(ms);
        }

        assert_eq!(
            counter.count(),
            10,
            "ten readings delivered {} ticks",
            counter.count()
        );
        assert_eq!(
            inner.ticks_skipped.load(Ordering::Relaxed),
            10,
            "20ms at a 1ms interval owes twenty ticks and delivered ten, so ten \
             are missing: the counter says {}",
            inner.ticks_skipped.load(Ordering::Relaxed)
        );
        // Not a dropped tick: nothing was unregistered.
        assert_eq!(inner.ticks_dropped.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_deadline_outstanding_across_a_slot_reuse_does_not_fire_its_successor() {
        // A registration lives in a slot and a deadline names that slot, so a
        // cancel followed by a registration that takes the slot over leaves a
        // deadline in the heap pointing at the wrong group. The generation is
        // what tells them apart; without it the newcomer would be ticked on the
        // departed group-s schedule.
        let (inner, departed) = hand_driven(&[(1, 10)]);
        let newcomer = Arc::new(Counter::default());

        {
            let mut state = inner.state.lock().expect("driver state mutex poisoned");
            // Cancel, exactly as `cancel_group` does.
            let slot = state
                .groups
                .remove(&DriverGroupKey::new(1, 1))
                .expect("registered");
            let vacated = &mut state.slots[slot as usize];
            vacated.registration = None;
            vacated.generation = vacated.generation.wrapping_add(1);
            state.free.push(slot);

            // A new group takes the freed slot, as registration would give it.
            let slot = state.free.pop().expect("one free slot");
            let generation = state.slots[slot as usize].generation;
            state.slots[slot as usize].registration = Some(Registration {
                receiver: Arc::clone(&newcomer) as Arc<dyn DriverTickReceiver>,
                interval_ms: 1_000,
            });
            state.groups.insert(DriverGroupKey::new(2, 1), slot);
            state
                .buckets
                .entry(1_000)
                .or_default()
                .push(Scheduled { slot, generation });
            // The departed group-s deadline at 10ms is still in the heap.
        }

        inner.advance_to_once(10);

        assert_eq!(
            newcomer.count(),
            0,
            "the newcomer was ticked on the departed group-s 10ms deadline"
        );
        assert_eq!(departed.count(), 0, "the departed group must not be ticked");
        assert_eq!(
            inner.ticks_dropped.load(Ordering::Relaxed),
            1,
            "the stale deadline must be counted as dropped, not silently ignored"
        );

        // And the newcomer still gets its own.
        inner.advance_to_once(1_000);
        assert_eq!(newcomer.count(), 1, "the newcomer keeps its own schedule");
    }

    #[test]
    fn the_tick_count_is_exact_at_every_scale_and_the_cost_is_reported() {
        // Asserts the counts, which are exact arithmetic, and only *prints* the
        // cost. A timing assertion here would be a flaky test rather than a
        // finding -- `tests/tick_locality.rs` carries the scars of one.
        //
        // The print is for reproducing the figures the scheduler was built on.
        // Release profile, minimum of four, arms interleaved, one box, at 65,536
        // groups on a single interval: a heap of one entry per group read
        // 71.4 ns a tick and these buckets read 17.8. Levels move between boxes
        // and between days, so only compare figures from one run with each other.
        //
        // The spread rows are there because one bucket per distinct deadline is
        // only cheap while deadlines are shared. Across the six rows the gain
        // ran from 1.53x, at 1,024 groups spread over 64 intervals where there
        // is barely a group per bucket, to 5.90x at 65,536 groups over 64, where
        // a heap degrades and buckets do not. It was not slower anywhere
        // measured, and a group per interval is the case to watch: there the two
        // converge.
        use std::time::Instant;
        const TICKS_EACH: u64 = 10;
        println!();
        for &n in &[1_024u64, 8_192, 65_536] {
            for &spread in &[1u64, 64] {
                // `spread` distinct intervals, dealt round-robin.
                let interval = |g: u64| 10 + (g % spread);
                let groups: Vec<(u64, u64)> = (1..=n).map(|g| (g, interval(g - 1))).collect();
                let span = TICKS_EACH * 10;
                let owed: u64 = (0..n).map(|g| span / interval(g)).sum();

                let (inner, counter) = hand_driven(&groups);
                let began = Instant::now();
                for ms in 1..=span {
                    inner.advance_to_once(ms);
                }
                let elapsed_ns = began.elapsed().as_nanos() as f64;

                assert_eq!(
                    counter.count(),
                    owed,
                    "{n} groups over {span} ms at {spread} distinct intervals owe \
                     exactly one tick per whole interval each"
                );
                assert_eq!(
                    inner.ticks_dropped.load(Ordering::Relaxed),
                    0,
                    "nothing was cancelled, so nothing may be dropped"
                );
                assert_eq!(
                    inner.ticks_skipped.load(Ordering::Relaxed),
                    0,
                    "the clock is driven a millisecond at a time, so nothing is owed"
                );
                println!(
                    "    {n:>6} groups  {spread:>3} interval(s)  {:>7.1} ns a tick",
                    elapsed_ns / counter.count() as f64
                );
            }
        }
        println!();
    }

    #[test]
    fn every_group_is_ticked_once_per_interval_of_the_clock() {
        // The exact-count version of `one_ticker_carries_every_group`: that
        // test polls to a deadline because a real thread is ticking, so it can
        // only assert "enough". Here the arithmetic is closed.
        let groups: Vec<(u64, u64)> = (1..=50).map(|group_id| (group_id, 10)).collect();
        let (inner, counter) = hand_driven(&groups);

        for ms in 1..=100 {
            inner.advance_to_once(ms);
        }

        assert_eq!(
            counter.count(),
            50 * 10,
            "100ms at a 10ms interval is ten ticks for each of fifty groups"
        );
        assert_eq!(
            inner.ticks_fired.load(Ordering::Relaxed),
            counter.count(),
            "the driver's own count disagrees with what the groups received"
        );
        assert_eq!(
            inner.ticks_dropped.load(Ordering::Relaxed),
            0,
            "no group was unregistered, so nothing should have been dropped"
        );
        assert_eq!(
            inner.ticks_skipped.load(Ordering::Relaxed),
            0,
            "the clock was read every millisecond, so no interval was missed"
        );
    }
}
