// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style multi-channel selector for per-replica work queues.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::{MailPriority, NodeId, RaftError};

pub const MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS: u64 = i64::MAX as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelSelectorPolicy {
    pub limit: usize,
    pub timeout_ms: u64,
}

impl Default for ChannelSelectorPolicy {
    fn default() -> Self {
        Self {
            limit: 1,
            timeout_ms: MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS,
        }
    }
}

#[derive(Debug)]
struct MailChannelInner<Mail> {
    size: usize,
    previous_channel_mail_count: i64,
    selector_total_mail_count: i64,
    max_depth: usize,
    rejected_send_count: u64,
    channels: [VecDeque<Mail>; 3],
    buffered: [VecDeque<Mail>; 3],
}

impl<Mail> MailChannelInner<Mail> {
    fn new() -> Self {
        Self {
            size: 0,
            previous_channel_mail_count: 0,
            selector_total_mail_count: 0,
            max_depth: 0,
            rejected_send_count: 0,
            channels: std::array::from_fn(|_| VecDeque::new()),
            buffered: std::array::from_fn(|_| VecDeque::new()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct MailChannelPressureStats {
    pub replica_id: NodeId,
    pub num_mail_limit: usize,
    pub queued_len: usize,
    pub selector_total_mail_count: i64,
    pub max_depth: usize,
    pub rejected_send_count: u64,
}

#[derive(Debug)]
pub struct MailChannel<Mail> {
    replica_id: NodeId,
    num_mail_limit: usize,
    inner: Mutex<MailChannelInner<Mail>>,
}

impl<Mail> MailChannel<Mail> {
    pub fn new(replica_id: NodeId, num_mail_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            replica_id,
            num_mail_limit: num_mail_limit.max(1),
            inner: Mutex::new(MailChannelInner::new()),
        })
    }

    pub fn replica_id(&self) -> NodeId {
        self.replica_id
    }

    pub fn try_send(&self, priority: MailPriority, mail: Mail) -> Result<(), Mail> {
        self.try_send_checked(priority, mail)
            .expect("mail channel mutex poisoned")
    }

    pub fn try_send_checked(
        &self,
        priority: MailPriority,
        mail: Mail,
    ) -> Result<Result<(), Mail>, RaftError> {
        let mut inner = self.inner.lock().map_err(mail_channel_poisoned)?;
        if self.overflow(&inner) {
            inner.rejected_send_count = inner.rejected_send_count.saturating_add(1);
            return Ok(Err(mail));
        }
        inner.channels[priority_index(priority)].push_back(mail);
        inner.size += 1;
        inner.max_depth = inner.max_depth.max(inner.size);
        Ok(Ok(()))
    }

    pub fn try_send_many(&self, priority: MailPriority, mails: Vec<Mail>) -> Result<(), Vec<Mail>> {
        self.try_send_many_checked(priority, mails)
            .expect("mail channel mutex poisoned")
    }

    pub fn try_send_many_checked(
        &self,
        priority: MailPriority,
        mails: Vec<Mail>,
    ) -> Result<Result<(), Vec<Mail>>, RaftError> {
        let mut inner = self.inner.lock().map_err(mail_channel_poisoned)?;
        if self.overflow(&inner) || inner.size.saturating_add(mails.len()) > self.num_mail_limit {
            inner.rejected_send_count = inner
                .rejected_send_count
                .saturating_add(mails.len().try_into().unwrap_or(u64::MAX));
            return Ok(Err(mails));
        }
        inner.size += mails.len();
        inner.max_depth = inner.max_depth.max(inner.size);
        inner.channels[priority_index(priority)].extend(mails);
        Ok(Ok(()))
    }

    pub fn send(&self, priority: MailPriority, mail: Mail) {
        self.send_checked(priority, mail)
            .expect("mail channel mutex poisoned");
    }

    pub fn send_checked(&self, priority: MailPriority, mail: Mail) -> Result<(), RaftError> {
        let mut inner = self.inner.lock().map_err(mail_channel_poisoned)?;
        inner.channels[priority_index(priority)].push_back(mail);
        inner.size += 1;
        inner.max_depth = inner.max_depth.max(inner.size);
        Ok(())
    }

    pub fn fetch(&self, selector: &ChannelSelector<Mail>) -> Vec<Mail> {
        self.fetch_checked(selector)
            .expect("mail channel mutex poisoned")
    }

    pub fn fetch_checked(&self, selector: &ChannelSelector<Mail>) -> Result<Vec<Mail>, RaftError> {
        let mut mails = Vec::new();
        self.fetch_into_checked(selector, &mut mails)?;
        Ok(mails)
    }

    /// Fetches into a buffer the caller keeps, appending to whatever is in it.
    ///
    /// A worker drains a channel on every pass of its loop and hands the mail
    /// on, so the vector it drained into is free again immediately. Returning a
    /// fresh one each time made that an allocation per drain, per group; lending
    /// one costs nothing after the first pass. The ticker's due list is kept the
    /// same way and for the same reason.
    ///
    /// The buffer is not cleared here. The caller owns what is already in it,
    /// and a drain that appends is what lets one pass gather from more than one
    /// channel.
    pub fn fetch_into(&self, selector: &ChannelSelector<Mail>, out: &mut Vec<Mail>) {
        self.fetch_into_checked(selector, out)
            .expect("mail channel mutex poisoned")
    }

    pub fn fetch_into_checked(
        &self,
        selector: &ChannelSelector<Mail>,
        out: &mut Vec<Mail>,
    ) -> Result<(), RaftError> {
        self.consume_checked(selector)?;
        let mut inner = self.inner.lock().map_err(mail_channel_poisoned)?;
        // Reserved once from what is staged. Extending from three deques in turn
        // reallocated once per non-empty priority, and the count is known before
        // anything is moved.
        let staged: usize = inner.buffered.iter().map(VecDeque::len).sum();
        out.reserve(staged);
        for channel in &mut inner.buffered {
            out.extend(channel.drain(..));
        }
        Ok(())
    }

    pub fn queued_len(&self) -> usize {
        self.queued_len_checked()
            .expect("mail channel mutex poisoned")
    }

    pub fn queued_len_checked(&self) -> Result<usize, RaftError> {
        Ok(self.inner.lock().map_err(mail_channel_poisoned)?.size)
    }

    pub fn selector_total_mail_count(&self) -> i64 {
        self.selector_total_mail_count_checked()
            .expect("mail channel mutex poisoned")
    }

    pub fn selector_total_mail_count_checked(&self) -> Result<i64, RaftError> {
        Ok(self
            .inner
            .lock()
            .map_err(mail_channel_poisoned)?
            .selector_total_mail_count)
    }

    pub fn pressure_stats(&self) -> MailChannelPressureStats {
        self.pressure_stats_checked()
            .expect("mail channel mutex poisoned")
    }

    pub fn pressure_stats_checked(&self) -> Result<MailChannelPressureStats, RaftError> {
        let inner = self.inner.lock().map_err(mail_channel_poisoned)?;
        Ok(MailChannelPressureStats {
            replica_id: self.replica_id,
            num_mail_limit: self.num_mail_limit,
            queued_len: inner.size,
            selector_total_mail_count: inner.selector_total_mail_count,
            max_depth: inner.max_depth,
            rejected_send_count: inner.rejected_send_count,
        })
    }

    fn consume_checked(&self, selector: &ChannelSelector<Mail>) -> Result<(), RaftError> {
        let mut inner = self.inner.lock().map_err(mail_channel_poisoned)?;
        let num_mails = inner.size as i64;
        let diff = num_mails - inner.previous_channel_mail_count;
        inner.previous_channel_mail_count = num_mails;
        inner.selector_total_mail_count = selector.add_total_mail_count_checked(diff)?;
        inner.size = 0;

        // Appended, not assigned. `fetch` is consume-then-drain under two
        // separate lock holds, and a channel can be selected again in between
        // when a sender fires it, so two workers can be inside `fetch` for the
        // same channel at once. Assigning here made the second consume discard
        // whatever the first had staged and not yet drained -- mail accepted by
        // `send` and then silently dropped.
        //
        // Drained in place rather than swapped out. Swapping three fresh deques
        // in left the channel at zero capacity, so the next send had to grow the
        // buffer again -- every cycle, for every group on every worker. Draining
        // moves exactly the same mail and keeps the allocation the channel had
        // already earned: a cycle of 64 mails cost 6 allocations and now costs 1,
        // and the one left is the vector `fetch` returns.
        //
        // The two fields are borrowed separately on purpose; they are disjoint,
        // which is what lets this take both at once.
        let MailChannelInner {
            channels, buffered, ..
        } = &mut *inner;
        for (buffered, channel) in buffered.iter_mut().zip(channels.iter_mut()) {
            buffered.extend(channel.drain(..));
        }
        Ok(())
    }

    /// Whether this channel is at its depth.
    ///
    /// Its OWN queue against its own limit. It used to ask whether the
    /// *selector's* total mail count exceeded this channel's limit, and that
    /// is wrong in both directions.
    ///
    /// It refused mail for an empty channel whenever the other groups on the
    /// same worker were busy -- and `selector_total_mail_count` is a copy
    /// refreshed only when this channel is drained, so once it was stale and
    /// high the channel refused everything, received nothing, was never
    /// drained, and never corrected itself. It stayed shut. A store of 4096
    /// groups over 4 workers hit exactly that: a worker serving more than
    /// `max_queue_depth` groups crosses the total, and in the failing runs
    /// all four workers sat parked with nothing to do while the groups they
    /// owned went unticked and stopped answering.
    ///
    /// It also let a channel that really was at its depth take more, because
    /// one busy group on a quiet worker never moves a selector-wide count
    /// far. `DriverWorkerPool::send` had grown its own `queued_len` check to
    /// work around that; this is the bound that check was standing in for.
    fn overflow(&self, inner: &MailChannelInner<Mail>) -> bool {
        inner.size >= self.num_mail_limit
    }
}

#[derive(Debug)]
struct ChannelSelectorInner<Mail> {
    total_mail_count: i64,
    active_channels: BTreeSet<NodeId>,
    channel_list: VecDeque<Arc<MailChannel<Mail>>>,
    global_mails: VecDeque<Mail>,
}

impl<Mail> ChannelSelectorInner<Mail> {
    fn new() -> Self {
        Self {
            total_mail_count: 0,
            active_channels: BTreeSet::new(),
            channel_list: VecDeque::new(),
            global_mails: VecDeque::new(),
        }
    }

    fn has_ready_work(&self) -> bool {
        !self.channel_list.is_empty() || !self.global_mails.is_empty()
    }
}

#[derive(Debug)]
pub struct ChannelSelector<Mail> {
    inner: Mutex<ChannelSelectorInner<Mail>>,
    readable: Condvar,
}

#[derive(Debug)]
pub struct ChannelSelection<Mail> {
    pub channels: Vec<Arc<MailChannel<Mail>>>,
    pub global_mails: Vec<Mail>,
    pub has_active_channels_left: bool,
}

impl<Mail> Default for ChannelSelector<Mail> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Mail> ChannelSelector<Mail> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ChannelSelectorInner::new()),
            readable: Condvar::new(),
        }
    }

    pub fn send_global(&self, mail: Mail) {
        self.send_global_checked(mail)
            .expect("channel selector mutex poisoned");
    }

    pub fn send_global_checked(&self, mail: Mail) -> Result<(), RaftError> {
        let mut inner = self.inner.lock().map_err(channel_selector_poisoned)?;
        inner.global_mails.push_back(mail);
        self.readable.notify_one();
        Ok(())
    }

    pub fn send_to_channel(
        &self,
        channel: Arc<MailChannel<Mail>>,
        priority: MailPriority,
        mail: Mail,
    ) {
        self.send_to_channel_checked(channel, priority, mail)
            .expect("channel selector mutex poisoned");
    }

    pub fn send_to_channel_checked(
        &self,
        channel: Arc<MailChannel<Mail>>,
        priority: MailPriority,
        mail: Mail,
    ) -> Result<(), RaftError> {
        channel.send_checked(priority, mail)?;
        let _ = self.fire_checked(channel)?;
        Ok(())
    }

    pub fn try_send_to_channel(
        &self,
        channel: Arc<MailChannel<Mail>>,
        priority: MailPriority,
        mail: Mail,
    ) -> Result<(), Mail> {
        self.try_send_to_channel_checked(channel, priority, mail)
            .expect("channel selector mutex poisoned")
    }

    pub fn try_send_to_channel_checked(
        &self,
        channel: Arc<MailChannel<Mail>>,
        priority: MailPriority,
        mail: Mail,
    ) -> Result<Result<(), Mail>, RaftError> {
        if let Err(mail) = channel.try_send_checked(priority, mail)? {
            return Ok(Err(mail));
        }
        let _ = self.fire_checked(channel)?;
        Ok(Ok(()))
    }

    pub fn try_send_many_to_channel_checked(
        &self,
        channel: Arc<MailChannel<Mail>>,
        priority: MailPriority,
        mails: Vec<Mail>,
    ) -> Result<Result<(), Vec<Mail>>, RaftError> {
        if let Err(mails) = channel.try_send_many_checked(priority, mails)? {
            return Ok(Err(mails));
        }
        let _ = self.fire_checked(channel)?;
        Ok(Ok(()))
    }

    pub fn fire(&self, channel: Arc<MailChannel<Mail>>) -> bool {
        self.fire_checked(channel)
            .expect("channel selector mutex poisoned")
    }

    pub fn fire_checked(&self, channel: Arc<MailChannel<Mail>>) -> Result<bool, RaftError> {
        let replica_id = channel.replica_id();
        let mut inner = self.inner.lock().map_err(channel_selector_poisoned)?;
        if !inner.active_channels.insert(replica_id) {
            return Ok(false);
        }
        inner.channel_list.push_back(channel);
        self.readable.notify_one();
        Ok(true)
    }

    pub fn select(
        &self,
        policy: ChannelSelectorPolicy,
        input: &[Arc<MailChannel<Mail>>],
    ) -> ChannelSelection<Mail> {
        self.select_checked(policy, input)
            .expect("channel selector mutex poisoned")
    }

    pub fn select_checked(
        &self,
        policy: ChannelSelectorPolicy,
        input: &[Arc<MailChannel<Mail>>],
    ) -> Result<ChannelSelection<Mail>, RaftError> {
        let mut inner = self.inner.lock().map_err(channel_selector_poisoned)?;
        self.rearrange(&mut inner, input);
        if !inner.has_ready_work()
            && policy.timeout_ms == MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS
        {
            while !inner.has_ready_work() {
                inner = self
                    .readable
                    .wait(inner)
                    .map_err(channel_selector_poisoned)?;
            }
        } else if !inner.has_ready_work() && policy.timeout_ms != 0 {
            let deadline = Instant::now() + Duration::from_millis(policy.timeout_ms);
            while !inner.has_ready_work() {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let wait_for = deadline.saturating_duration_since(now);
                let (next_inner, timeout) = self
                    .readable
                    .wait_timeout(inner, wait_for)
                    .map_err(channel_selector_poisoned)?;
                inner = next_inner;
                if timeout.timed_out() {
                    break;
                }
            }
        }

        let global_mails = inner.global_mails.drain(..).collect::<Vec<_>>();
        let count = policy.limit.min(inner.active_channels.len());
        let mut channels = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(channel) = inner.channel_list.pop_front() else {
                break;
            };
            inner.active_channels.remove(&channel.replica_id());
            channels.push(channel);
        }
        Ok(ChannelSelection {
            channels,
            global_mails,
            has_active_channels_left: !inner.active_channels.is_empty(),
        })
    }

    pub fn total_mail_count(&self) -> i64 {
        self.total_mail_count_checked()
            .expect("channel selector mutex poisoned")
    }

    pub fn total_mail_count_checked(&self) -> Result<i64, RaftError> {
        Ok(self
            .inner
            .lock()
            .map_err(channel_selector_poisoned)?
            .total_mail_count)
    }

    pub fn active_channel_count(&self) -> usize {
        self.active_channel_count_checked()
            .expect("channel selector mutex poisoned")
    }

    pub fn active_channel_count_checked(&self) -> Result<usize, RaftError> {
        Ok(self
            .inner
            .lock()
            .map_err(channel_selector_poisoned)?
            .active_channels
            .len())
    }

    fn add_total_mail_count_checked(&self, diff: i64) -> Result<i64, RaftError> {
        let mut inner = self.inner.lock().map_err(channel_selector_poisoned)?;
        inner.total_mail_count += diff;
        Ok(inner.total_mail_count)
    }

    fn rearrange(&self, inner: &mut ChannelSelectorInner<Mail>, input: &[Arc<MailChannel<Mail>>]) {
        for channel in input {
            if inner.active_channels.insert(channel.replica_id()) {
                inner.channel_list.push_back(Arc::clone(channel));
            }
        }
    }
}

fn mail_channel_poisoned<T>(_error: T) -> RaftError {
    RaftError::Storage("mail channel mutex poisoned".to_string())
}

fn channel_selector_poisoned<T>(_error: T) -> RaftError {
    RaftError::Storage("channel selector mutex poisoned".to_string())
}

fn priority_index(priority: MailPriority) -> usize {
    match priority {
        MailPriority::Urgent => 0,
        MailPriority::Normal => 1,
        MailPriority::Slowly => 2,
    }
}
