// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style heartbeat merge queue for multi-group store transports.

use crate::{
    AppendEntriesRequest, AppendEntriesResponse, RaftError, RustRaftMessage, RustRaftNodeId,
};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

pub const MATRIXRAFT_HEARTBEAT_MERGE_BUCKETS: usize = 16;

pub trait RustRaftHeartbeatAddressResolver {
    fn resolve_raft_addr(
        &self,
        from: RustRaftNodeId,
        to: RustRaftNodeId,
    ) -> Result<String, RaftError>;
}

impl<F> RustRaftHeartbeatAddressResolver for F
where
    F: Fn(RustRaftNodeId, RustRaftNodeId) -> Result<String, RaftError>,
{
    fn resolve_raft_addr(
        &self,
        from: RustRaftNodeId,
        to: RustRaftNodeId,
    ) -> Result<String, RaftError> {
        self(from, to)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum RustRaftHeartbeatMergeMessage {
    AppendEntriesRequest {
        target: RustRaftNodeId,
        request: AppendEntriesRequest,
    },
    AppendEntriesResponse {
        local_node_id: RustRaftNodeId,
        peer_id: RustRaftNodeId,
        response: AppendEntriesResponse,
    },
}

impl RustRaftHeartbeatMergeMessage {
    pub fn from_node_id(&self) -> RustRaftNodeId {
        match self {
            Self::AppendEntriesRequest { request, .. } => request.leader_id,
            Self::AppendEntriesResponse { local_node_id, .. } => *local_node_id,
        }
    }

    pub fn to_node_id(&self) -> RustRaftNodeId {
        match self {
            Self::AppendEntriesRequest { target, .. } => *target,
            Self::AppendEntriesResponse { peer_id, .. } => *peer_id,
        }
    }

    pub fn into_raft_message(self) -> RustRaftMessage {
        match self {
            Self::AppendEntriesRequest { target, request } => {
                RustRaftMessage::AppendEntries { target, request }
            }
            Self::AppendEntriesResponse {
                local_node_id,
                peer_id,
                response,
            } => RustRaftMessage::AppendEntriesResponse {
                local_node_id,
                peer_id,
                response,
            },
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftHeartbeatMergeStats {
    pub queued_requests: u64,
    pub queued_responses: u64,
    pub flushed_requests: u64,
    pub flushed_responses: u64,
    pub bypassed_messages: u64,
    pub resolver_failures: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMergedHeartbeatBatch {
    pub raft_addr: String,
    pub messages: Vec<RustRaftHeartbeatMergeMessage>,
}

#[derive(Debug, Clone)]
pub struct RustRaftHeartbeatMerger {
    enabled: bool,
    buckets: Vec<BTreeMap<String, Vec<RustRaftHeartbeatMergeMessage>>>,
    stats: RustRaftHeartbeatMergeStats,
}

impl RustRaftHeartbeatMerger {
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
            stats: RustRaftHeartbeatMergeStats::default(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn stats(&self) -> &RustRaftHeartbeatMergeStats {
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
        message: RustRaftMessage,
        resolver: &R,
    ) -> Result<Option<RustRaftMessage>, RaftError>
    where
        R: RustRaftHeartbeatAddressResolver,
    {
        if !self.enabled {
            self.stats.bypassed_messages = self.stats.bypassed_messages.saturating_add(1);
            return Ok(Some(message));
        }

        let heartbeat = match message {
            RustRaftMessage::AppendEntries { target, request } if request.entries.is_empty() => {
                RustRaftHeartbeatMergeMessage::AppendEntriesRequest { target, request }
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
        local_node_id: RustRaftNodeId,
        peer_id: RustRaftNodeId,
        response: AppendEntriesResponse,
        resolver: &R,
    ) -> Result<(), RaftError>
    where
        R: RustRaftHeartbeatAddressResolver,
    {
        if !self.enabled {
            self.stats.bypassed_messages = self.stats.bypassed_messages.saturating_add(1);
            return Ok(());
        }
        self.queue_heartbeat(
            RustRaftHeartbeatMergeMessage::AppendEntriesResponse {
                local_node_id,
                peer_id,
                response,
            },
            resolver,
        )
    }

    fn queue_heartbeat<R>(
        &mut self,
        heartbeat: RustRaftHeartbeatMergeMessage,
        resolver: &R,
    ) -> Result<(), RaftError>
    where
        R: RustRaftHeartbeatAddressResolver,
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
            RustRaftHeartbeatMergeMessage::AppendEntriesRequest { .. } => {
                self.stats.queued_requests = self.stats.queued_requests.saturating_add(1);
                self.buckets[bucket]
                    .entry(raft_addr)
                    .or_default()
                    .push(heartbeat);
            }
            RustRaftHeartbeatMergeMessage::AppendEntriesResponse { .. } => {
                self.stats.queued_responses = self.stats.queued_responses.saturating_add(1);
                self.buckets[bucket]
                    .entry(raft_addr)
                    .or_default()
                    .push(heartbeat);
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Vec<RustRaftMergedHeartbeatBatch> {
        let mut batches = Vec::new();
        for bucket in &mut self.buckets {
            let drained = std::mem::take(bucket);
            for (raft_addr, messages) in drained {
                for message in &messages {
                    match message {
                        RustRaftHeartbeatMergeMessage::AppendEntriesRequest { .. } => {
                            self.stats.flushed_requests =
                                self.stats.flushed_requests.saturating_add(1);
                        }
                        RustRaftHeartbeatMergeMessage::AppendEntriesResponse { .. } => {
                            self.stats.flushed_responses =
                                self.stats.flushed_responses.saturating_add(1);
                        }
                    }
                }
                batches.push(RustRaftMergedHeartbeatBatch {
                    raft_addr,
                    messages,
                });
            }
        }
        batches
    }

    pub fn flush_messages(&mut self) -> Vec<RustRaftMessage> {
        self.flush()
            .into_iter()
            .flat_map(|batch| batch.messages)
            .map(RustRaftHeartbeatMergeMessage::into_raft_message)
            .collect()
    }
}

impl RustRaftHeartbeatAddressResolver for HashMap<(RustRaftNodeId, RustRaftNodeId), String> {
    fn resolve_raft_addr(
        &self,
        from: RustRaftNodeId,
        to: RustRaftNodeId,
    ) -> Result<String, RaftError> {
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
