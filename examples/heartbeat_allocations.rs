// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one heartbeat round costs the allocator.
//!
//! A heartbeat is the highest-frequency thing a raft leader does: every tick,
//! to every peer, whether or not there is anything to replicate. So a cost per
//! peer here is multiplied by the tick rate as well as the fan-out, which
//! makes it the biggest multiplier in the system.
//!
//! Varying the peer count at a fixed configuration says whether the cost
//! follows the peers.
//!
//! ```text
//! cargo run --release --example heartbeat_allocations
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 41_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 42_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn cluster(peers: usize) -> RaftCluster {
    let members = (1..=peers as u64).map(peer).collect::<Vec<_>>();
    let mut cluster = RaftCluster::new(7, Config::default(), members).expect("cluster");
    cluster.start().expect("start");
    cluster
}

/// `(allocs, bytes)` per heartbeat round, and the peers it reached.
///
/// `settle` proposes once before measuring. That matters: after `start` the
/// leader's campaign no-op may be ahead of what the followers have matched, and
/// a heartbeat that sees `match_index < last_log_index` probes the tail. So the
/// two rows answer different questions -- what a heartbeat costs while the
/// followers are behind, and what it costs once they are not.
fn measure(peers: usize, rounds: usize, settle: bool) -> (f64, f64, u64) {
    let mut cluster = cluster(peers);
    if settle {
        cluster
            .propose(b"settle the followers".to_vec())
            .expect("propose");
    }

    // Warm, and check the round is doing the work: a broadcast that reached
    // nobody would report a very cheap heartbeat.
    let reached = cluster.broadcast_heartbeat().expect("heartbeat");
    for _ in 0..8 {
        cluster.broadcast_heartbeat().expect("warm");
    }

    let before_allocs = ALLOCATIONS.load(Ordering::Relaxed);
    let before_bytes = BYTES.load(Ordering::Relaxed);
    for _ in 0..rounds {
        let sent = cluster.broadcast_heartbeat().expect("heartbeat");
        assert_eq!(
            sent, reached,
            "every round must reach the same peers, or the rows are not comparable"
        );
    }
    let allocs = ALLOCATIONS.load(Ordering::Relaxed) - before_allocs;
    let bytes = BYTES.load(Ordering::Relaxed) - before_bytes;
    (
        allocs as f64 / rounds as f64,
        bytes as f64 / rounds as f64,
        reached,
    )
}

fn row(label: &str, peers: usize, rounds: usize, settle: bool) {
    let (allocs, bytes, reached) = measure(peers, rounds, settle);
    let per_peer = if reached == 0 {
        0.0
    } else {
        allocs / reached as f64
    };
    let bytes_per_peer = if reached == 0 {
        0.0
    } else {
        bytes / reached as f64
    };
    println!(
        "{label:<22} {peers:>5}  {reached:>7}  {allocs:>12.2}  {bytes:>11.0}  {per_peer:>11.2}  {bytes_per_peer:>10.1}"
    );
}

fn main() {
    println!(
        "{:<22} {:>5}  {:>7}  {:>12}  {:>11}  {:>11}  {:>10}",
        "", "peers", "reached", "allocs/round", "bytes/round", "allocs/peer", "bytes/peer"
    );
    for peers in [1usize, 3, 5, 7, 9] {
        row("followers behind", peers, 500, false);
    }
    println!();
    for peers in [1usize, 3, 5, 7, 9] {
        row("followers caught up", peers, 500, true);
    }
    println!();
    println!("`reached` is asserted equal across every round of a row, so a");
    println!("cheap figure cannot come from a broadcast that reached nobody.");
}
