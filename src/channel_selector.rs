// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style multi-channel selector for per-replica work queues.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::{RustRaftMailPriority, RustRaftNodeId};

pub const MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS: u64 = i64::MAX as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RustRaftChannelSelectorPolicy {
    pub limit: usize,
    pub timeout_ms: u64,
}

impl Default for RustRaftChannelSelectorPolicy {
    fn default() -> Self {
        Self {
            limit: 1,
            timeout_ms: MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS,
        }
    }
}

#[derive(Debug)]
struct RustRaftMailChannelInner<Mail> {
    size: usize,
    previous_channel_mail_count: i64,
    selector_total_mail_count: i64,
    channels: [VecDeque<Mail>; 3],
    buffered: [VecDeque<Mail>; 3],
}

impl<Mail> RustRaftMailChannelInner<Mail> {
    fn new() -> Self {
        Self {
            size: 0,
            previous_channel_mail_count: 0,
            selector_total_mail_count: 0,
            channels: std::array::from_fn(|_| VecDeque::new()),
            buffered: std::array::from_fn(|_| VecDeque::new()),
        }
    }
}

#[derive(Debug)]
pub struct RustRaftMailChannel<Mail> {
    replica_id: RustRaftNodeId,
    num_mail_limit: usize,
    inner: Mutex<RustRaftMailChannelInner<Mail>>,
}

impl<Mail> RustRaftMailChannel<Mail> {
    pub fn new(replica_id: RustRaftNodeId, num_mail_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            replica_id,
            num_mail_limit: num_mail_limit.max(1),
            inner: Mutex::new(RustRaftMailChannelInner::new()),
        })
    }

    pub fn replica_id(&self) -> RustRaftNodeId {
        self.replica_id
    }

    pub fn try_send(&self, priority: RustRaftMailPriority, mail: Mail) -> Result<(), Mail> {
        let mut inner = self.inner.lock().expect("mail channel mutex poisoned");
        if self.overflow(&inner) {
            return Err(mail);
        }
        inner.channels[priority_index(priority)].push_back(mail);
        inner.size += 1;
        Ok(())
    }

    pub fn try_send_many(
        &self,
        priority: RustRaftMailPriority,
        mails: Vec<Mail>,
    ) -> Result<(), Vec<Mail>> {
        let mut inner = self.inner.lock().expect("mail channel mutex poisoned");
        if self.overflow(&inner) {
            return Err(mails);
        }
        inner.size += mails.len();
        inner.channels[priority_index(priority)].extend(mails);
        Ok(())
    }

    pub fn send(&self, priority: RustRaftMailPriority, mail: Mail) {
        let mut inner = self.inner.lock().expect("mail channel mutex poisoned");
        inner.channels[priority_index(priority)].push_back(mail);
        inner.size += 1;
    }

    pub fn fetch(&self, selector: &RustRaftChannelSelector<Mail>) -> Vec<Mail> {
        self.consume(selector);
        let mut inner = self.inner.lock().expect("mail channel mutex poisoned");
        let mut mails = Vec::new();
        for channel in &mut inner.buffered {
            mails.extend(channel.drain(..));
        }
        mails
    }

    pub fn queued_len(&self) -> usize {
        self.inner.lock().expect("mail channel mutex poisoned").size
    }

    pub fn selector_total_mail_count(&self) -> i64 {
        self.inner
            .lock()
            .expect("mail channel mutex poisoned")
            .selector_total_mail_count
    }

    fn consume(&self, selector: &RustRaftChannelSelector<Mail>) {
        let mut inner = self.inner.lock().expect("mail channel mutex poisoned");
        let num_mails = inner.size as i64;
        let diff = num_mails - inner.previous_channel_mail_count;
        inner.previous_channel_mail_count = num_mails;
        inner.selector_total_mail_count = selector.add_total_mail_count(diff);
        inner.size = 0;

        let mut channels = std::array::from_fn(|_| VecDeque::new());
        std::mem::swap(&mut inner.channels, &mut channels);
        inner.buffered = channels;
    }

    fn overflow(&self, inner: &RustRaftMailChannelInner<Mail>) -> bool {
        inner.selector_total_mail_count as usize > self.num_mail_limit
    }
}

#[derive(Debug)]
struct RustRaftChannelSelectorInner<Mail> {
    total_mail_count: i64,
    active_channels: BTreeSet<RustRaftNodeId>,
    channel_list: VecDeque<Arc<RustRaftMailChannel<Mail>>>,
    global_mails: VecDeque<Mail>,
}

impl<Mail> RustRaftChannelSelectorInner<Mail> {
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
pub struct RustRaftChannelSelector<Mail> {
    inner: Mutex<RustRaftChannelSelectorInner<Mail>>,
    readable: Condvar,
}

#[derive(Debug)]
pub struct RustRaftChannelSelection<Mail> {
    pub channels: Vec<Arc<RustRaftMailChannel<Mail>>>,
    pub global_mails: Vec<Mail>,
    pub has_active_channels_left: bool,
}

impl<Mail> Default for RustRaftChannelSelector<Mail> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Mail> RustRaftChannelSelector<Mail> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(RustRaftChannelSelectorInner::new()),
            readable: Condvar::new(),
        }
    }

    pub fn send_global(&self, mail: Mail) {
        let mut inner = self.inner.lock().expect("channel selector mutex poisoned");
        inner.global_mails.push_back(mail);
        self.readable.notify_one();
    }

    pub fn send_to_channel(
        &self,
        channel: Arc<RustRaftMailChannel<Mail>>,
        priority: RustRaftMailPriority,
        mail: Mail,
    ) {
        channel.send(priority, mail);
        self.fire(channel);
    }

    pub fn try_send_to_channel(
        &self,
        channel: Arc<RustRaftMailChannel<Mail>>,
        priority: RustRaftMailPriority,
        mail: Mail,
    ) -> Result<(), Mail> {
        channel.try_send(priority, mail)?;
        self.fire(channel);
        Ok(())
    }

    pub fn fire(&self, channel: Arc<RustRaftMailChannel<Mail>>) -> bool {
        let replica_id = channel.replica_id();
        let mut inner = self.inner.lock().expect("channel selector mutex poisoned");
        if !inner.active_channels.insert(replica_id) {
            return false;
        }
        inner.channel_list.push_back(channel);
        self.readable.notify_one();
        true
    }

    pub fn select(
        &self,
        policy: RustRaftChannelSelectorPolicy,
        input: &[Arc<RustRaftMailChannel<Mail>>],
    ) -> RustRaftChannelSelection<Mail> {
        let mut inner = self.inner.lock().expect("channel selector mutex poisoned");
        self.rearrange(&mut inner, input);
        if !inner.has_ready_work()
            && policy.timeout_ms == MATRIXRAFT_CHANNEL_SELECTOR_MAX_TIMEOUT_MS
        {
            while !inner.has_ready_work() {
                inner = self
                    .readable
                    .wait(inner)
                    .expect("channel selector mutex poisoned");
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
                    .expect("channel selector mutex poisoned");
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
        RustRaftChannelSelection {
            channels,
            global_mails,
            has_active_channels_left: !inner.active_channels.is_empty(),
        }
    }

    pub fn total_mail_count(&self) -> i64 {
        self.inner
            .lock()
            .expect("channel selector mutex poisoned")
            .total_mail_count
    }

    pub fn active_channel_count(&self) -> usize {
        self.inner
            .lock()
            .expect("channel selector mutex poisoned")
            .active_channels
            .len()
    }

    fn add_total_mail_count(&self, diff: i64) -> i64 {
        let mut inner = self.inner.lock().expect("channel selector mutex poisoned");
        inner.total_mail_count += diff;
        inner.total_mail_count
    }

    fn rearrange(
        &self,
        inner: &mut RustRaftChannelSelectorInner<Mail>,
        input: &[Arc<RustRaftMailChannel<Mail>>],
    ) {
        for channel in input {
            if inner.active_channels.insert(channel.replica_id()) {
                inner.channel_list.push_back(Arc::clone(channel));
            }
        }
    }
}

fn priority_index(priority: RustRaftMailPriority) -> usize {
    match priority {
        RustRaftMailPriority::Urgent => 0,
        RustRaftMailPriority::Normal => 1,
        RustRaftMailPriority::Slowly => 2,
    }
}
