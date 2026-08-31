// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style priority mailbox for scheduler and transport work queues.

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

pub const MATRIXRAFT_MAILBOX_MAX_TIMEOUT_MS: u64 = i64::MAX as u64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MailPriority {
    Urgent,
    Normal,
    Slowly,
}

impl MailPriority {
    fn index(self) -> usize {
        match self {
            Self::Urgent => 0,
            Self::Normal => 1,
            Self::Slowly => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct MailBoxFetchPolicy {
    pub limit: usize,
    pub timeout_ms: u64,
    pub include_until: MailPriority,
}

impl Default for MailBoxFetchPolicy {
    fn default() -> Self {
        Self {
            limit: 1,
            timeout_ms: MATRIXRAFT_MAILBOX_MAX_TIMEOUT_MS,
            include_until: MailPriority::Urgent,
        }
    }
}

#[derive(Debug)]
struct MailBoxInner<Mail> {
    channels: [VecDeque<Mail>; 3],
}

impl<Mail> MailBoxInner<Mail> {
    fn new() -> Self {
        Self {
            channels: std::array::from_fn(|_| VecDeque::new()),
        }
    }

    fn has_new_mail(&self) -> bool {
        self.channels.iter().any(|channel| !channel.is_empty())
    }
}

#[derive(Debug)]
pub struct MailBox<Mail> {
    high_watermark: usize,
    inner: Mutex<MailBoxInner<Mail>>,
    readable: Condvar,
    writable: Condvar,
}

impl<Mail> MailBox<Mail> {
    pub fn new(high_watermark: usize) -> Self {
        Self {
            high_watermark: high_watermark.max(1),
            inner: Mutex::new(MailBoxInner::new()),
            readable: Condvar::new(),
            writable: Condvar::new(),
        }
    }

    pub fn try_send(&self, priority: MailPriority, mail: Mail) -> bool {
        let mut inner = self.inner.lock().expect("mailbox mutex poisoned");
        let channel = &mut inner.channels[priority.index()];
        if channel.len() >= self.high_watermark {
            return false;
        }

        channel.push_back(mail);
        self.readable.notify_one();
        true
    }

    pub fn wait_and_send(&self, priority: MailPriority, mail: Mail) {
        let mut inner = self.inner.lock().expect("mailbox mutex poisoned");
        while inner.channels[priority.index()].len() >= self.high_watermark {
            inner = self.writable.wait(inner).expect("mailbox mutex poisoned");
        }

        inner.channels[priority.index()].push_back(mail);
        self.readable.notify_one();
    }

    pub fn send(&self, priority: MailPriority, mail: Mail) {
        let mut inner = self.inner.lock().expect("mailbox mutex poisoned");
        inner.channels[priority.index()].push_back(mail);
        self.readable.notify_one();
    }

    pub fn fetch(&self, policy: MailBoxFetchPolicy) -> Vec<Mail> {
        let mut inner = self.inner.lock().expect("mailbox mutex poisoned");
        if !inner.has_new_mail() && policy.timeout_ms == MATRIXRAFT_MAILBOX_MAX_TIMEOUT_MS {
            while !inner.has_new_mail() {
                inner = self.readable.wait(inner).expect("mailbox mutex poisoned");
            }
        } else if !inner.has_new_mail() && policy.timeout_ms != 0 {
            let deadline = Instant::now() + Duration::from_millis(policy.timeout_ms);
            loop {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let wait_for = deadline.saturating_duration_since(now);
                let (next_inner, timeout) = self
                    .readable
                    .wait_timeout(inner, wait_for)
                    .expect("mailbox mutex poisoned");
                inner = next_inner;
                if inner.has_new_mail() || timeout.timed_out() {
                    break;
                }
            }
        }

        let mut output = Vec::new();
        let mut lower_priority_limit = policy.limit;
        let all_include_until = policy.include_until.index();
        for priority_index in 0..inner.channels.len() {
            let channel_len = inner.channels[priority_index].len();
            let take = if priority_index >= all_include_until {
                channel_len.min(lower_priority_limit)
            } else {
                channel_len
            };

            for _ in 0..take {
                if let Some(mail) = inner.channels[priority_index].pop_front() {
                    output.push(mail);
                }
            }
            if priority_index >= all_include_until {
                lower_priority_limit = lower_priority_limit.saturating_sub(take);
            }
        }
        self.writable.notify_all();
        output
    }

    pub fn clear(&self) {
        let mut inner = self.inner.lock().expect("mailbox mutex poisoned");
        for channel in &mut inner.channels {
            channel.clear();
        }
        self.writable.notify_all();
    }

    pub fn len(&self, priority: MailPriority) -> usize {
        self.inner.lock().expect("mailbox mutex poisoned").channels[priority.index()].len()
    }

    pub fn total_len(&self) -> usize {
        self.inner
            .lock()
            .expect("mailbox mutex poisoned")
            .channels
            .iter()
            .map(VecDeque::len)
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.total_len() == 0
    }
}
