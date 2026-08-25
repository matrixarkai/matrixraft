// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Measures how many WAL bytes one proposal costs, mirroring what the node
//! runtime does per proposal: propose, then append `wal_record_for` to the WAL.

use std::time::Instant;

use matrixraft::{
    PersistentRaftWal, PersistentRaftWalOptions, RaftCluster, RaftConfig, RustRaftPeer,
    RustRaftReplicaRole,
};

fn peer(node_id: u64, role: RustRaftReplicaRole) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn dir_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

fn run(entries: usize, payload_bytes: usize) -> (u64, f64) {
    let dir =
        std::env::temp_dir().join(format!("mraft-wal-probe-{}-{entries}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut wal = PersistentRaftWal::open(PersistentRaftWalOptions {
        dir: dir.clone(),
        max_records_per_segment: 1_000_000,
        max_segment_bytes: u64::MAX,
        min_keep_segments: 1,
        fsync_on_append: false,
    })
    .expect("open wal");

    let mut cluster = RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("start");
    cluster.campaign(1, true).expect("campaign");

    let payload = vec![7_u8; payload_bytes];
    let started = Instant::now();
    for _ in 0..entries {
        cluster.propose(payload.clone()).expect("propose");
        wal.append_built(|coverage| cluster.wal_record_for_coverage(1, coverage))
            .expect("append");
    }
    let seconds = started.elapsed().as_secs_f64();
    let bytes = dir_bytes(&dir);
    let _ = std::fs::remove_dir_all(&dir);
    (bytes, seconds)
}

fn main() {
    let payload_bytes: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(64);
    println!("payload_bytes={payload_bytes}  (fsync disabled: this measures bytes, not syscalls)");
    println!(
        "{:>8}  {:>14}  {:>14}  {:>12}  {:>10}",
        "proposals", "wal_bytes", "bytes/proposal", "amplification", "seconds"
    );
    for entries in [250_usize, 500, 1000, 2000] {
        let (bytes, seconds) = run(entries, payload_bytes);
        let per = bytes as f64 / entries as f64;
        let amplification = per / payload_bytes as f64;
        println!("{entries:>8}  {bytes:>14}  {per:>14.0}  {amplification:>12.1}x  {seconds:>10.3}");
    }
}
