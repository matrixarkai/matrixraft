// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Raft configuration and validation API.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftConfig {
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub leader_lease_ms: u64,
    #[serde(default)]
    pub last_follower_lease_ms: u64,
    pub max_payload_bytes: u64,
    #[serde(default = "default_max_log_buffer_bytes")]
    pub max_log_buffer_bytes: u64,
    pub snapshot_threshold_entries: u64,
    pub max_segment_bytes: u64,
    pub min_keep_segment_num: u64,
    pub enable_pre_vote: bool,
    pub enable_lease_read: bool,
    #[serde(default = "default_assume_lease_when_start")]
    pub assume_lease_when_start: bool,
}

fn default_assume_lease_when_start() -> bool {
    true
}

fn default_max_log_buffer_bytes() -> u64 {
    64 * 1024 * 1024
}

impl Default for RustRaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_ms: 1_000,
            heartbeat_interval_ms: 100,
            leader_lease_ms: 500,
            last_follower_lease_ms: 0,
            max_payload_bytes: 8 * 1024 * 1024,
            max_log_buffer_bytes: default_max_log_buffer_bytes(),
            snapshot_threshold_entries: 10_000,
            max_segment_bytes: 64 * 1024 * 1024,
            min_keep_segment_num: 2,
            enable_pre_vote: true,
            enable_lease_read: true,
            assume_lease_when_start: true,
        }
    }
}

impl RustRaftConfig {
    pub fn validate(&self) -> Result<(), RaftConfigError> {
        if self.election_timeout_ms == 0 {
            return Err(RaftConfigError::ZeroElectionTimeout);
        }
        if self.heartbeat_interval_ms == 0 {
            return Err(RaftConfigError::ZeroHeartbeatInterval);
        }
        if self.leader_lease_ms == 0 {
            return Err(RaftConfigError::ZeroLeaderLease);
        }
        if self.heartbeat_interval_ms >= self.election_timeout_ms {
            return Err(RaftConfigError::HeartbeatNotLessThanElection {
                heartbeat_interval_ms: self.heartbeat_interval_ms,
                election_timeout_ms: self.election_timeout_ms,
            });
        }
        if self.leader_lease_ms >= self.election_timeout_ms {
            return Err(RaftConfigError::LeaseNotLessThanElection {
                leader_lease_ms: self.leader_lease_ms,
                election_timeout_ms: self.election_timeout_ms,
            });
        }
        if self.max_payload_bytes == 0 {
            return Err(RaftConfigError::ZeroMaxPayloadBytes);
        }
        if self.max_log_buffer_bytes == 0 {
            return Err(RaftConfigError::ZeroMaxLogBufferBytes);
        }
        if self.snapshot_threshold_entries == 0 {
            return Err(RaftConfigError::ZeroSnapshotThreshold);
        }
        if self.max_segment_bytes == 0 {
            return Err(RaftConfigError::ZeroMaxSegmentBytes);
        }
        Ok(())
    }
}

pub type RaftConfig = RustRaftConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Error)]
pub enum RaftConfigError {
    #[error("election_timeout_ms must be greater than zero")]
    ZeroElectionTimeout,
    #[error("heartbeat_interval_ms must be greater than zero")]
    ZeroHeartbeatInterval,
    #[error("leader_lease_ms must be greater than zero")]
    ZeroLeaderLease,
    #[error(
        "heartbeat_interval_ms ({heartbeat_interval_ms}) must be less than election_timeout_ms ({election_timeout_ms})"
    )]
    HeartbeatNotLessThanElection {
        heartbeat_interval_ms: u64,
        election_timeout_ms: u64,
    },
    #[error(
        "leader_lease_ms ({leader_lease_ms}) must be less than election_timeout_ms ({election_timeout_ms})"
    )]
    LeaseNotLessThanElection {
        leader_lease_ms: u64,
        election_timeout_ms: u64,
    },
    #[error("max_payload_bytes must be greater than zero")]
    ZeroMaxPayloadBytes,
    #[error("max_log_buffer_bytes must be greater than zero")]
    ZeroMaxLogBufferBytes,
    #[error("snapshot_threshold_entries must be greater than zero")]
    ZeroSnapshotThreshold,
    #[error("max_segment_bytes must be greater than zero")]
    ZeroMaxSegmentBytes,
}
