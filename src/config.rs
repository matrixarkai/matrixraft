// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Raft configuration and validation API.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::pipeline::PipelineLimits;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub leader_lease_ms: u64,
    #[serde(default)]
    pub last_follower_lease_ms: u64,
    pub max_payload_bytes: u64,
    #[serde(default = "default_max_log_buffer_bytes")]
    pub max_log_buffer_bytes: u64,
    /// How many retained log entries the leader may hold before a snapshot is
    /// due.
    ///
    /// Read by `RaftCluster::snapshot_due`, which the node runtime consults
    /// on its heartbeat tick and acts on by calling
    /// `RaftCluster::trigger_snapshot`. Gated by `can_trigger_snapshot`.
    ///
    /// It used to be consulted by nothing: declared, defaulted to 10,000,
    /// rejected when zero, and read nowhere, so there was no log-size snapshot
    /// trigger at all.
    ///
    /// `MatrixRaftOptions::max_applied_log_bytes` is the **byte**-valued knob
    /// for the same idea and is still unread, defaulting to `u64::MAX`. It is
    /// not wired here because a second threshold expressing the same decision
    /// would cost another eight bytes on every group -- the entry threshold is
    /// the one `Config` already carries.
    pub snapshot_threshold_entries: u64,
    pub max_segment_bytes: u64,
    pub min_keep_segment_num: u64,
    pub enable_pre_vote: bool,
    pub enable_lease_read: bool,
    #[serde(default = "default_assume_lease_when_start")]
    pub assume_lease_when_start: bool,
    /// Whether the engine may decide on its own that the log is long enough to
    /// warrant a snapshot.
    ///
    /// The comparable engine's own flag, with its own warning attached: it is
    /// safe to turn on and **not** safe to turn back off, because a snapshot
    /// already decided on will still be taken.
    ///
    /// With this off, `snapshot_threshold_entries` decides nothing and a
    /// snapshot happens only when a host asks for one.
    #[serde(default = "default_can_trigger_snapshot")]
    pub can_trigger_snapshot: bool,
    /// The replication pipeline's limits for every peer a cluster built from
    /// this config holds.
    ///
    /// `RaftCluster` used to build them with `PipelineLimits::default()` at
    /// four sites and `Config` carried none of them, so the reorder window,
    /// the reorder timeout, the in-flight bounds and the apply batch size were
    /// not reachable from a cluster's own configuration at all -- only the
    /// defaults were. The compat layer had these as options and converted them
    /// for its own node type, which is why they looked configurable.
    ///
    /// `#[serde(default)]` is what keeps an older configuration working: a
    /// document with no `pipeline` key gets `PipelineLimits::default()`, which
    /// is the production default this used to hardcode. Serde treats a `Box<T>`
    /// as a `T`, so the document shape is the same either way.
    ///
    /// **Boxed because `RaftCluster` holds a `Config` by value.** Plain, these
    /// 56 bytes took the cluster from 520 to 576 -- +10.8% on a struct a shard
    /// walks once per group per visit, which is what
    /// `tests/cluster_footprint.rs` is there to make someone think about.
    /// Boxed it is 8 bytes and the cluster is 528. The cost is one allocation
    /// per cluster: `Config` is cloned exactly once in this crate, when a node
    /// runtime is built. Use [`Config::with_pipeline_limits`] rather than
    /// writing the `Box` yourself.
    #[serde(default)]
    pub pipeline: Box<PipelineLimits>,
}

fn default_assume_lease_when_start() -> bool {
    true
}

fn default_can_trigger_snapshot() -> bool {
    true
}

fn default_max_log_buffer_bytes() -> u64 {
    64 * 1024 * 1024
}

impl Default for Config {
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
            can_trigger_snapshot: true,
            pipeline: Box::default(),
        }
    }
}

impl Config {
    /// This config with `pipeline` set, so a caller never writes the `Box`.
    pub fn with_pipeline_limits(mut self, limits: PipelineLimits) -> Self {
        self.pipeline = Box::new(limits);
        self
    }

    /// The pipeline limits a cluster should build its peers with.
    ///
    /// Zero is sanitised to one for every limit **except
    /// `reorder_timeout_us`**, where zero means the timeout is off -- so
    /// raising it to one would turn "never expire" into "expire on the first
    /// tick", which is the opposite. The other five describe a capacity, and a
    /// capacity of zero is a pipeline that can do nothing rather than a
    /// pipeline without a limit.
    ///
    /// Sanitising rather than rejecting is what the compat layer's
    /// `to_pipeline_limits` already does, and `Config::validate` says nothing
    /// about these fields for the same reason: a host that asks for a window of
    /// zero gets the smallest working window, not a cluster that will not
    /// start.
    pub fn pipeline_limits(&self) -> PipelineLimits {
        PipelineLimits {
            max_inflights_replicate: self.pipeline.max_inflights_replicate.max(1),
            max_memory_replicate_log_bytes: self.pipeline.max_memory_replicate_log_bytes.max(1),
            max_inflights_apply_task: self.pipeline.max_inflights_apply_task.max(1),
            max_apply_batch_bytes: self.pipeline.max_apply_batch_bytes.max(1),
            enable_reorder_queue: self.pipeline.enable_reorder_queue,
            reorder_window_size: self.pipeline.reorder_window_size.max(1),
            reorder_timeout_us: self.pipeline.reorder_timeout_us,
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.election_timeout_ms == 0 {
            return Err(ConfigError::ZeroElectionTimeout);
        }
        if self.heartbeat_interval_ms == 0 {
            return Err(ConfigError::ZeroHeartbeatInterval);
        }
        if self.leader_lease_ms == 0 {
            return Err(ConfigError::ZeroLeaderLease);
        }
        if self.heartbeat_interval_ms >= self.election_timeout_ms {
            return Err(ConfigError::HeartbeatNotLessThanElection {
                heartbeat_interval_ms: self.heartbeat_interval_ms,
                election_timeout_ms: self.election_timeout_ms,
            });
        }
        if self.leader_lease_ms >= self.election_timeout_ms {
            return Err(ConfigError::LeaseNotLessThanElection {
                leader_lease_ms: self.leader_lease_ms,
                election_timeout_ms: self.election_timeout_ms,
            });
        }
        if self.max_payload_bytes == 0 {
            return Err(ConfigError::ZeroMaxPayloadBytes);
        }
        if self.max_log_buffer_bytes == 0 {
            return Err(ConfigError::ZeroMaxLogBufferBytes);
        }
        if self.snapshot_threshold_entries == 0 {
            return Err(ConfigError::ZeroSnapshotThreshold);
        }
        if self.max_segment_bytes == 0 {
            return Err(ConfigError::ZeroMaxSegmentBytes);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Error)]
pub enum ConfigError {
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
