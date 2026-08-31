// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Snapshot-send probe: what does chunking a snapshot cost before a byte moves?
//!
//! `SnapshotLifecycle::checkpoint` builds every chunk up front, so beginning a
//! send holds a second copy of the payload as well as the snapshot itself. Each
//! chunk also clones the metadata, and that metadata carries a `Vec<Peer>` whose
//! peers each hold two `String`s -- so the clone count grows with the payload
//! even though the metadata never changes.
//!
//! Sending a snapshot is exactly when a lagging follower makes the leader spend
//! memory, so the multiple matters. One payload size per process.
//!
//!     snapshot_checkpoint_cost <payload_mib> <chunk_kib>

use matrixraft::{LogId, Peer, RaftSnapshot, ReplicaRole, SnapshotLifecycle, SnapshotMetadata};

fn rss_bytes() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            if let Ok(kb) = rest.trim().trim_end_matches(" kB").trim().parse::<u64>() {
                return kb * 1024;
            }
        }
    }
    0
}

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("10.0.0.{node_id}:9000"),
        snapshot_addr: format!("10.0.0.{node_id}:10000"),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn main() {
    let payload_mib: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(64);
    let chunk_kib: u64 = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(64);

    let payload_bytes = payload_mib * 1024 * 1024;
    let snapshot = RaftSnapshot {
        group_id: 9,
        meta: SnapshotMetadata {
            snapshot_id: "snapshot-7".to_string(),
            last_log_id: LogId {
                term: 3,
                index: 1_000,
            },
            membership: vec![1, 2, 3],
            members: vec![peer(1), peer(2), peer(3)],
        },
        payload: vec![7u8; payload_bytes],
    };

    let before = rss_bytes();
    let chunks = SnapshotLifecycle::checkpoint(&snapshot, chunk_kib * 1024).expect("checkpoint");
    let after = rss_bytes();
    let growth = after.saturating_sub(before);

    println!(
        "payload={payload_mib} MiB chunk={chunk_kib} KiB chunks={} rss_growth={:.1} MiB \
         multiple_of_payload={:.2}",
        chunks.len(),
        growth as f64 / (1024.0 * 1024.0),
        growth as f64 / payload_bytes as f64,
    );
    // Keep the chunks alive so the measurement is of what a send holds.
    assert!(!chunks.is_empty());
}
