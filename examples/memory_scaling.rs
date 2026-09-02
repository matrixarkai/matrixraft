// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Memory probe: what does a proposal cost in resident memory, and how does
//! that scale with group size?
//!
//! `RaftCluster` models every node of a group inside one object, and each node
//! keeps its own log. So the memory a group costs here is expected to grow with
//! the number of nodes as well as the number of entries. This measures how
//! much, because "expected to grow" is not a number.
//!
//! Reads `VmRSS` from `/proc/self/status`, so Linux only. RSS moves for reasons
//! other than this process's allocations, so the probe proposes in large blocks
//! and reports bytes per entry rather than trusting any single reading.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// Resident set size in bytes, or `None` off Linux.
fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

fn measure(nodes: u64, entries: usize, payload_bytes: usize) -> (u64, u64) {
    let before = rss_bytes().expect("VmRSS");
    let peers: Vec<Peer> = (1..=nodes).map(peer).collect();
    let mut cluster = RaftCluster::new(7, Config::default(), peers).expect("valid cluster");
    cluster.start().expect("start");
    cluster.campaign(1, true).expect("campaign");
    let payload = vec![7u8; payload_bytes];
    for _ in 0..entries {
        cluster.propose(payload.clone()).expect("propose");
    }
    let after = rss_bytes().expect("VmRSS");
    // Keep the cluster alive across the reading, or the allocator may have
    // handed the pages back before we look.
    std::hint::black_box(&cluster);
    (before, after)
}

fn main() {
    let entries: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(20_000);
    let payload_bytes: usize = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(256);

    if rss_bytes().is_none() {
        println!("VmRSS unavailable: this probe needs Linux");
        return;
    }

    // One group size per process, on purpose. Measuring several in a row inside
    // one process reads a smaller delta for each later size, because dropping
    // the previous cluster leaves pages mapped for the next one to reuse -- at
    // which point a flat memory profile reports as ~0 bytes and looks like a
    // spectacular result rather than a broken measurement.
    let nodes: u64 = std::env::args()
        .nth(3)
        .and_then(|a| a.parse().ok())
        .unwrap_or(3);

    let logical = entries as f64 * payload_bytes as f64;
    let (before, after) = measure(nodes, entries, payload_bytes);
    let delta = after.saturating_sub(before);
    let per_entry = delta as f64 / entries as f64;
    let per_entry_node = per_entry / nodes as f64;
    let amplification = delta as f64 / logical;
    println!(
        "nodes={nodes:<3} entries={entries} payload={payload_bytes}B  logical={:.1} MiB  \
         rss_delta={:.1} MiB  bytes/entry={per_entry:.0}  per_node={per_entry_node:.0}  \
         amplification={amplification:.2}x",
        logical / 1048576.0,
        delta as f64 / 1048576.0
    );
}
