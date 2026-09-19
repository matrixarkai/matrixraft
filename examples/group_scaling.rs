// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What does one more raft group cost a store process, when the group is idle?
//!
//! `MatrixRaftMultiRaftServer` lets one process host many groups: it keys its
//! nodes by `(group_id, node_id)`, so a store that owns a replica of each of a
//! thousand groups holds a thousand entries. Every entry carries its own
//! `NodeRuntime`, and a `NodeRuntime` is an OS thread plus a command channel.
//! That is a real design choice with a real price, and the price is what an
//! operator needs before deciding how finely to shard.
//!
//! The other probes in this directory sweep the number of nodes *inside* one
//! group. This one sweeps the number of *groups*, which is the axis the
//! benchmark harness does not touch -- `BenchmarkOptions` has a `node_count`
//! and no group count, and every workload there runs as a single group.
//!
//! Reported per group: resident bytes, threads, and the wall time the creation
//! itself took. Threads are the number to trust -- it is read exactly from
//! `/proc/self/status`, where resident memory moves for reasons that have
//! nothing to do with this process.
//!
//! Linux only, for `/proc/self/status`.
//!
//! ```bash
//! cargo run --release --example group_scaling            # 16 groups
//! cargo run --release --example group_scaling -- 64      # 64 groups
//! ```

use matrixraft::{
    MatrixRaftGroupContextBuilder, MatrixRaftMultiRaftServer, MatrixRaftOptions,
    MatrixRaftTransportBuilder, Peer, ReplicaRole,
};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// A value from `/proc/self/status`, in its own units.
fn status_field(name: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(name) {
            return rest.trim().trim_end_matches(" kB").trim().parse().ok();
        }
    }
    None
}

fn rss_bytes() -> Option<u64> {
    status_field("VmRSS:").map(|kb| kb * 1024)
}

fn thread_count() -> Option<u64> {
    status_field("Threads:")
}

fn peer(group_id: u64, node_id: u64) -> Peer {
    Peer {
        node_id,
        // Nothing dials these: the probe never starts the transport. They only
        // have to be distinct and well formed.
        raft_addr: format!("127.0.0.1:{}", 30_000 + (group_id % 1_000) * 8 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 40_000 + (group_id % 1_000) * 8 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn options(group_id: u64, wal_dir: &Path, snapshot_dir: &Path) -> MatrixRaftOptions {
    MatrixRaftOptions {
        group_id,
        peer_id: 1,
        raft_addr: peer(group_id, 1).raft_addr,
        snapshot_addr: peer(group_id, 1).snapshot_addr,
        wal_dir: wal_dir.display().to_string(),
        snapshot_dir: snapshot_dir.display().to_string(),
        peers: vec![peer(group_id, 1), peer(group_id, 2), peer(group_id, 3)],
        role: ReplicaRole::Voter,
        // The probe measures an idle group, so it must not pay for durability
        // it is not exercising, and must not let a tick fire mid-measurement.
        wal_sync: false,
        election_cycle_tick: 4,
        transfer_timeout_tick: 3,
        offline_timeout_tick: 10,
        tick_interval_ms: 3_600_000,
        lease_duration_ms: 20,
        last_lease_duration_ms: 10,
        assume_lease_when_start: false,
        max_memory_replicate_log_bytes: 64 * 1024,
        max_disk_replicate_log_num: 64,
        max_cache_memory_bytes: 16 * 1024 * 1024,
        max_apply_batch_bytes: 64 * 1024,
        enable_reorder_queue: true,
        reorder_timeout_us: 3_000,
        reorder_window_size: 128,
        max_inflights_apply_task: 5,
        max_inflights_replicate: 128,
        enable_pre_vote: true,
        max_segment_bytes: 4 * 1024 * 1024,
        min_keep_segment_num: 2,
        can_trigger_snapshot: true,
        max_applied_log_bytes: u64::MAX,
        send_snapshot_timeout_ms: 60_000,
    }
}

fn probe_root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-group-scaling-{}-{nonce}",
        std::process::id()
    ))
}

fn main() {
    let groups: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(16);

    if rss_bytes().is_none() || thread_count().is_none() {
        println!("/proc/self/status unavailable: this probe needs Linux");
        return;
    }

    let root = probe_root();
    // Create every directory first. Otherwise the sweep measures directory
    // creation as though it were the cost of a raft group.
    let dirs: Vec<(PathBuf, PathBuf)> = (1..=groups)
        .map(|group_id| {
            let wal = root.join(format!("g{group_id}/wal"));
            let snapshot = root.join(format!("g{group_id}/snapshot"));
            std::fs::create_dir_all(&wal).expect("wal dir");
            std::fs::create_dir_all(&snapshot).expect("snapshot dir");
            (wal, snapshot)
        })
        .collect();

    // One transport for the whole server, which is the shape a store uses: the
    // groups it hosts share it rather than each opening their own.
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(1)
        .set_num_connection_group(1)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .tick_interval(3_600_000)
        .build()
        .expect("group context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let rss_before = rss_bytes().expect("VmRSS");
    let threads_before = thread_count().expect("Threads");
    let started = Instant::now();

    for (index, (wal, snapshot)) in dirs.iter().enumerate() {
        let group_id = index as u64 + 1;
        server
            .create_node(options(group_id, wal, snapshot), 0)
            .expect("create node");
    }

    let elapsed = started.elapsed();
    let rss_after = rss_bytes().expect("VmRSS");
    let threads_after = thread_count().expect("Threads");
    // Hold the server across the readings, or the runtimes may be torn down
    // before the numbers are taken.
    std::hint::black_box(&server);

    let hosted = server.group_count() as u64;
    assert_eq!(
        hosted, groups,
        "probe measured {hosted} groups but was asked for {groups}"
    );

    let rss_delta = rss_after.saturating_sub(rss_before);
    let thread_delta = threads_after.saturating_sub(threads_before);
    println!(
        "groups={groups:<5} rss_delta={:.1} MiB  per_group={:.0} KiB  \
         threads={thread_delta} ({:.2}/group)  create={:?} ({:?}/group)",
        rss_delta as f64 / 1048576.0,
        rss_delta as f64 / groups as f64 / 1024.0,
        thread_delta as f64 / groups as f64,
        elapsed,
        elapsed / groups as u32
    );

    // One group count per process, deliberately. A second sweep inside the same
    // process reads a smaller delta, because the pages the first sweep returned
    // are still mapped and ready to be reused -- which reports as a flat memory
    // profile and looks like a result rather than a broken measurement.
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}
