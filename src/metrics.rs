// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Metric-name contract for RustRaft observability.

use serde::{Deserialize, Serialize};

pub use crate::rustraft_baseline_raft_runtime_capability_prometheus;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMetricNames {
    pub ready: String,
    pub append_latency_ms: String,
    pub vote_latency_ms: String,
    pub pre_vote_latency_ms: String,
    pub read_index_latency_ms: String,
    pub snapshot_install_latency_ms: String,
    pub peer_append_queue_depth: String,
    pub peer_reorder_queue_depth: String,
    pub peer_snapshot_installed_index: String,
    pub wal_segment_count: String,
    pub blocker_total: String,
    pub fatal_total: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPrometheusMetricSet {
    pub format: String,
    pub metric_count: u64,
    pub text: String,
}

pub fn rustraft_metric_names() -> RustRaftMetricNames {
    RustRaftMetricNames {
        ready: "rustraft_ready".to_string(),
        append_latency_ms: "rustraft_append_latency_ms".to_string(),
        vote_latency_ms: "rustraft_vote_latency_ms".to_string(),
        pre_vote_latency_ms: "rustraft_pre_vote_latency_ms".to_string(),
        read_index_latency_ms: "rustraft_read_index_latency_ms".to_string(),
        snapshot_install_latency_ms: "rustraft_snapshot_install_latency_ms".to_string(),
        peer_append_queue_depth: "rustraft_peer_append_queue_depth".to_string(),
        peer_reorder_queue_depth: "rustraft_peer_reorder_queue_depth".to_string(),
        peer_snapshot_installed_index: "rustraft_peer_snapshot_installed_index".to_string(),
        wal_segment_count: "rustraft_wal_segment_count".to_string(),
        blocker_total: "rustraft_blocker_total".to_string(),
        fatal_total: "rustraft_fatal_total".to_string(),
    }
}
