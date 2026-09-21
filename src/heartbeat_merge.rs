// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style heartbeat merge queue for multi-group store transports.

use crate::{
    AppendEntriesRequest, AppendEntriesResponse, DriverTickReceiver, Message, NodeId, RaftError,
};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const MATRIXRAFT_HEARTBEAT_MERGE_BUCKETS: usize = 16;

pub trait HeartbeatAddressResolver {
    fn resolve_raft_addr(&self, from: NodeId, to: NodeId) -> Result<String, RaftError>;
}

impl<F> HeartbeatAddressResolver for F
where
    F: Fn(NodeId, NodeId) -> Result<String, RaftError>,
{
    fn resolve_raft_addr(&self, from: NodeId, to: NodeId) -> Result<String, RaftError> {
        self(from, to)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum HeartbeatMergeMessage {
    AppendEntriesRequest {
        target: NodeId,
        request: AppendEntriesRequest,
    },
    AppendEntriesResponse {
        local_node_id: NodeId,
        peer_id: NodeId,
        response: AppendEntriesResponse,
    },
}

impl HeartbeatMergeMessage {
    pub fn from_node_id(&self) -> NodeId {
        match self {
            Self::AppendEntriesRequest { request, .. } => request.leader_id,
            Self::AppendEntriesResponse { local_node_id, .. } => *local_node_id,
        }
    }

    pub fn to_node_id(&self) -> NodeId {
        match self {
            Self::AppendEntriesRequest { target, .. } => *target,
            Self::AppendEntriesResponse { peer_id, .. } => *peer_id,
        }
    }

    pub fn into_raft_message(self) -> Message {
        match self {
            Self::AppendEntriesRequest { target, request } => {
                Message::AppendEntries { target, request }
            }
            Self::AppendEntriesResponse {
                local_node_id,
                peer_id,
                response,
            } => Message::AppendEntriesResponse {
                local_node_id,
                peer_id,
                response,
            },
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatMergeStats {
    pub queued_requests: u64,
    pub queued_responses: u64,
    pub flushed_requests: u64,
    pub flushed_responses: u64,
    pub bypassed_messages: u64,
    pub resolver_failures: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergedHeartbeatBatch {
    pub raft_addr: String,
    pub messages: Vec<HeartbeatMergeMessage>,
}

#[derive(Debug, Clone)]
pub struct HeartbeatMerger {
    enabled: bool,
    buckets: Vec<BTreeMap<String, Vec<HeartbeatMergeMessage>>>,
    stats: HeartbeatMergeStats,
}

impl HeartbeatMerger {
    pub fn new(enabled: bool) -> Self {
        Self::with_bucket_count(enabled, MATRIXRAFT_HEARTBEAT_MERGE_BUCKETS)
    }

    pub fn enabled() -> Self {
        Self::new(true)
    }

    pub fn disabled() -> Self {
        Self::new(false)
    }

    pub fn with_bucket_count(enabled: bool, bucket_count: usize) -> Self {
        let bucket_count = bucket_count.max(1);
        Self {
            enabled,
            buckets: vec![BTreeMap::new(); bucket_count],
            stats: HeartbeatMergeStats::default(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn stats(&self) -> &HeartbeatMergeStats {
        &self.stats
    }

    pub fn pending_len(&self) -> usize {
        self.buckets
            .iter()
            .flat_map(BTreeMap::values)
            .map(Vec::len)
            .sum()
    }

    pub fn maybe_merge<R>(
        &mut self,
        message: Message,
        resolver: &R,
    ) -> Result<Option<Message>, RaftError>
    where
        R: HeartbeatAddressResolver,
    {
        if !self.enabled {
            self.stats.bypassed_messages = self.stats.bypassed_messages.saturating_add(1);
            return Ok(Some(message));
        }

        let heartbeat = match message {
            Message::AppendEntries { target, request } if request.entries.is_empty() => {
                HeartbeatMergeMessage::AppendEntriesRequest { target, request }
            }
            other => {
                self.stats.bypassed_messages = self.stats.bypassed_messages.saturating_add(1);
                return Ok(Some(other));
            }
        };

        self.queue_heartbeat(heartbeat, resolver)?;
        Ok(None)
    }

    pub fn merge_heartbeat_response<R>(
        &mut self,
        local_node_id: NodeId,
        peer_id: NodeId,
        response: AppendEntriesResponse,
        resolver: &R,
    ) -> Result<(), RaftError>
    where
        R: HeartbeatAddressResolver,
    {
        if !self.enabled {
            self.stats.bypassed_messages = self.stats.bypassed_messages.saturating_add(1);
            return Ok(());
        }
        self.queue_heartbeat(
            HeartbeatMergeMessage::AppendEntriesResponse {
                local_node_id,
                peer_id,
                response,
            },
            resolver,
        )
    }

    fn queue_heartbeat<R>(
        &mut self,
        heartbeat: HeartbeatMergeMessage,
        resolver: &R,
    ) -> Result<(), RaftError>
    where
        R: HeartbeatAddressResolver,
    {
        let from = heartbeat.from_node_id();
        let to = heartbeat.to_node_id();
        let raft_addr = match resolver.resolve_raft_addr(from, to) {
            Ok(raft_addr) => raft_addr,
            Err(error) => {
                self.stats.resolver_failures = self.stats.resolver_failures.saturating_add(1);
                return Err(error);
            }
        };
        let bucket = bucket_for_addr(&raft_addr, self.buckets.len());
        match heartbeat {
            HeartbeatMergeMessage::AppendEntriesRequest { .. } => {
                self.stats.queued_requests = self.stats.queued_requests.saturating_add(1);
                self.buckets[bucket]
                    .entry(raft_addr)
                    .or_default()
                    .push(heartbeat);
            }
            HeartbeatMergeMessage::AppendEntriesResponse { .. } => {
                self.stats.queued_responses = self.stats.queued_responses.saturating_add(1);
                self.buckets[bucket]
                    .entry(raft_addr)
                    .or_default()
                    .push(heartbeat);
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Vec<MergedHeartbeatBatch> {
        let mut batches = Vec::new();
        for bucket in &mut self.buckets {
            let drained = std::mem::take(bucket);
            for (raft_addr, messages) in drained {
                for message in &messages {
                    match message {
                        HeartbeatMergeMessage::AppendEntriesRequest { .. } => {
                            self.stats.flushed_requests =
                                self.stats.flushed_requests.saturating_add(1);
                        }
                        HeartbeatMergeMessage::AppendEntriesResponse { .. } => {
                            self.stats.flushed_responses =
                                self.stats.flushed_responses.saturating_add(1);
                        }
                    }
                }
                batches.push(MergedHeartbeatBatch {
                    raft_addr,
                    messages,
                });
            }
        }
        batches
    }

    pub fn flush_messages(&mut self) -> Vec<Message> {
        self.flush()
            .into_iter()
            .flat_map(|batch| batch.messages)
            .map(HeartbeatMergeMessage::into_raft_message)
            .collect()
    }
}

impl HeartbeatAddressResolver for HashMap<(NodeId, NodeId), String> {
    fn resolve_raft_addr(&self, from: NodeId, to: NodeId) -> Result<String, RaftError> {
        self.get(&(from, to)).cloned().ok_or_else(|| {
            RaftError::Transport(format!(
                "raft address for heartbeat {from}->{to} was not found"
            ))
        })
    }
}

fn bucket_for_addr(raft_addr: &str, bucket_count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    raft_addr.hash(&mut hasher);
    (hasher.finish() as usize) % bucket_count.max(1)
}

/// Where merged heartbeats go once a flush has gathered them.
///
/// One call carries every heartbeat bound for one address, which is the whole
/// point: many groups sharing a peer send one batch rather than one message
/// each.
pub trait MergedHeartbeatSender: Send + Sync {
    fn send_merged(&self, batches: Vec<MergedHeartbeatBatch>);
}

/// Holds heartbeats back so many groups' heartbeats to one peer travel
/// together, and flushes them on the driver's tick.
///
/// `HeartbeatMerger` buckets by destination address and can absorb a heartbeat,
/// but something has to decide *when* to let the buckets go. That is the
/// `merge_heartbeat_interval_milli` setting, and this is the piece that spends
/// it: register a flusher with a [`Driver`] at that interval and every tick
/// drains the merger.
///
/// ```ignore
/// let flusher = HeartbeatFlusher::new(HeartbeatMerger::enabled(), sender);
/// driver.register_group_every(key, flusher.clone(), merge_heartbeat_interval_milli)?;
/// ```
///
/// A merger that is disabled absorbs nothing: `maybe_merge` hands every message
/// straight back, so a host can leave the call in place and turn the behaviour
/// off with the setting rather than with a branch of its own.
pub struct HeartbeatFlusher {
    merger: Mutex<HeartbeatMerger>,
    sender: Arc<dyn MergedHeartbeatSender>,
    flushes: AtomicU64,
    batches_sent: AtomicU64,
    messages_sent: AtomicU64,
}

impl std::fmt::Debug for HeartbeatFlusher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeartbeatFlusher")
            .field("flushes", &self.flushes.load(Ordering::Relaxed))
            .field("batches_sent", &self.batches_sent.load(Ordering::Relaxed))
            .field("messages_sent", &self.messages_sent.load(Ordering::Relaxed))
            .finish()
    }
}

/// What a flusher has done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatFlushStats {
    /// Flushes attempted, including ones that found nothing.
    pub flushes: u64,
    /// Batches handed to the sender. One per destination address per flush.
    pub batches_sent: u64,
    /// Heartbeats inside those batches. The ratio to `batches_sent` is what the
    /// merging buys.
    pub messages_sent: u64,
    /// Still held, waiting for the next flush.
    pub pending: usize,
    pub merger: HeartbeatMergeStats,
}

impl HeartbeatFlusher {
    pub fn new(merger: HeartbeatMerger, sender: Arc<dyn MergedHeartbeatSender>) -> Arc<Self> {
        Arc::new(Self {
            merger: Mutex::new(merger),
            sender,
            flushes: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
        })
    }

    /// Offers a message to the merger.
    ///
    /// `None` means it was absorbed and will go out with the next flush.
    /// `Some(message)` means it was not a heartbeat, or merging is off, and the
    /// caller should send it itself.
    pub fn maybe_merge<R>(
        &self,
        message: Message,
        resolver: &R,
    ) -> Result<Option<Message>, RaftError>
    where
        R: HeartbeatAddressResolver,
    {
        self.merger
            .lock()
            .expect("heartbeat merger mutex poisoned")
            .maybe_merge(message, resolver)
    }

    /// Sends everything held, and reports how many batches went.
    ///
    /// The sender is called with the lock released, so a slow transport delays
    /// the next flush rather than every group trying to queue a heartbeat.
    pub fn flush_now(&self) -> usize {
        let batches = {
            let mut merger = self.merger.lock().expect("heartbeat merger mutex poisoned");
            merger.flush()
        };
        self.flushes.fetch_add(1, Ordering::Relaxed);
        if batches.is_empty() {
            return 0;
        }
        let batch_count = batches.len();
        let message_count: usize = batches.iter().map(|batch| batch.messages.len()).sum();
        self.batches_sent
            .fetch_add(batch_count as u64, Ordering::Relaxed);
        self.messages_sent
            .fetch_add(message_count as u64, Ordering::Relaxed);
        self.sender.send_merged(batches);
        batch_count
    }

    pub fn stats(&self) -> HeartbeatFlushStats {
        let merger = self.merger.lock().expect("heartbeat merger mutex poisoned");
        HeartbeatFlushStats {
            flushes: self.flushes.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            pending: merger.pending_len(),
            merger: merger.stats().clone(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.merger
            .lock()
            .expect("heartbeat merger mutex poisoned")
            .is_enabled()
    }
}

impl DriverTickReceiver for HeartbeatFlusher {
    fn fire_tick(&self) {
        self.flush_now();
    }
}
