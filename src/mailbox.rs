// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style priority mailbox for scheduler and transport work queues.

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::RaftError;

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
    max_channel_depth: usize,
    rejected_send_count: u64,
}

impl<Mail> MailBoxInner<Mail> {
    fn new() -> Self {
        Self {
            channels: std::array::from_fn(|_| VecDeque::new()),
            max_channel_depth: 0,
            rejected_send_count: 0,
        }
    }

    fn has_new_mail(&self) -> bool {
        self.channels.iter().any(|channel| !channel.is_empty())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct MailBoxPressureStats {
    pub high_watermark: usize,
    pub total_len: usize,
    pub max_channel_depth: usize,
    pub rejected_send_count: u64,
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
        self.try_send_checked(priority, mail)
            .expect("mailbox mutex poisoned")
    }

    pub fn try_send_checked(&self, priority: MailPriority, mail: Mail) -> Result<bool, RaftError> {
        let mut inner = self.inner.lock().map_err(mailbox_poisoned)?;
        let channel = &mut inner.channels[priority.index()];
        if channel.len() >= self.high_watermark {
            inner.rejected_send_count = inner.rejected_send_count.saturating_add(1);
            return Ok(false);
        }

        channel.push_back(mail);
        let depth = channel.len();
        inner.max_channel_depth = inner.max_channel_depth.max(depth);
        self.readable.notify_one();
        Ok(true)
    }

    pub fn try_send_many(&self, priority: MailPriority, mails: Vec<Mail>) -> Result<(), Vec<Mail>> {
        self.try_send_many_checked(priority, mails)
            .expect("mailbox mutex poisoned")
    }

    pub fn try_send_many_checked(
        &self,
        priority: MailPriority,
        mails: Vec<Mail>,
    ) -> Result<Result<(), Vec<Mail>>, RaftError> {
        let mut inner = self.inner.lock().map_err(mailbox_poisoned)?;
        let channel = &mut inner.channels[priority.index()];
        if channel.len().saturating_add(mails.len()) > self.high_watermark {
            inner.rejected_send_count = inner
                .rejected_send_count
                .saturating_add(mails.len().try_into().unwrap_or(u64::MAX));
            return Ok(Err(mails));
        }

        channel.extend(mails);
        let depth = channel.len();
        inner.max_channel_depth = inner.max_channel_depth.max(depth);
        self.readable.notify_one();
        Ok(Ok(()))
    }

    pub fn wait_and_send(&self, priority: MailPriority, mail: Mail) {
        self.wait_and_send_checked(priority, mail)
            .expect("mailbox mutex poisoned");
    }

    pub fn wait_and_send_checked(
        &self,
        priority: MailPriority,
        mail: Mail,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.lock().map_err(mailbox_poisoned)?;
        while inner.channels[priority.index()].len() >= self.high_watermark {
            inner = self.writable.wait(inner).map_err(mailbox_poisoned)?;
        }

        inner.channels[priority.index()].push_back(mail);
        inner.max_channel_depth = inner
            .max_channel_depth
            .max(inner.channels[priority.index()].len());
        self.readable.notify_one();
        Ok(())
    }

    pub fn send(&self, priority: MailPriority, mail: Mail) {
        self.send_checked(priority, mail)
            .expect("mailbox mutex poisoned");
    }

    pub fn send_checked(&self, priority: MailPriority, mail: Mail) -> Result<(), RaftError> {
        let mut inner = self.inner.lock().map_err(mailbox_poisoned)?;
        inner.channels[priority.index()].push_back(mail);
        inner.max_channel_depth = inner
            .max_channel_depth
            .max(inner.channels[priority.index()].len());
        self.readable.notify_one();
        Ok(())
    }

    pub fn fetch(&self, policy: MailBoxFetchPolicy) -> Vec<Mail> {
        self.fetch_checked(policy).expect("mailbox mutex poisoned")
    }

    pub fn fetch_checked(&self, policy: MailBoxFetchPolicy) -> Result<Vec<Mail>, RaftError> {
        let mut inner = self.inner.lock().map_err(mailbox_poisoned)?;
        if !inner.has_new_mail() && policy.timeout_ms == MATRIXRAFT_MAILBOX_MAX_TIMEOUT_MS {
            while !inner.has_new_mail() {
                inner = self.readable.wait(inner).map_err(mailbox_poisoned)?;
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
                    .map_err(mailbox_poisoned)?;
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
        Ok(output)
    }

    pub fn clear(&self) {
        self.clear_checked().expect("mailbox mutex poisoned");
    }

    pub fn clear_checked(&self) -> Result<(), RaftError> {
        let mut inner = self.inner.lock().map_err(mailbox_poisoned)?;
        for channel in &mut inner.channels {
            channel.clear();
        }
        self.writable.notify_all();
        Ok(())
    }

    pub fn len(&self, priority: MailPriority) -> usize {
        self.len_checked(priority).expect("mailbox mutex poisoned")
    }

    pub fn len_checked(&self, priority: MailPriority) -> Result<usize, RaftError> {
        Ok(self.inner.lock().map_err(mailbox_poisoned)?.channels[priority.index()].len())
    }

    pub fn total_len(&self) -> usize {
        self.total_len_checked().expect("mailbox mutex poisoned")
    }

    pub fn total_len_checked(&self) -> Result<usize, RaftError> {
        Ok(self
            .inner
            .lock()
            .map_err(mailbox_poisoned)?
            .channels
            .iter()
            .map(VecDeque::len)
            .sum())
    }

    pub fn is_empty(&self) -> bool {
        self.total_len() == 0
    }

    pub fn is_empty_checked(&self) -> Result<bool, RaftError> {
        Ok(self.total_len_checked()? == 0)
    }

    pub fn pressure_stats(&self) -> MailBoxPressureStats {
        self.pressure_stats_checked()
            .expect("mailbox mutex poisoned")
    }

    pub fn pressure_stats_checked(&self) -> Result<MailBoxPressureStats, RaftError> {
        let inner = self.inner.lock().map_err(mailbox_poisoned)?;
        Ok(MailBoxPressureStats {
            high_watermark: self.high_watermark,
            total_len: inner.channels.iter().map(VecDeque::len).sum(),
            max_channel_depth: inner.max_channel_depth,
            rejected_send_count: inner.rejected_send_count,
        })
    }
}

fn mailbox_poisoned<T>(_error: T) -> RaftError {
    RaftError::Storage("mailbox mutex poisoned".to_string())
}
