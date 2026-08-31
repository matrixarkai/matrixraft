// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style heartbeat merge queue for multi-group store transports.

use crate::{AppendEntriesRequest, AppendEntriesResponse, Message, NodeId, RaftError};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

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
