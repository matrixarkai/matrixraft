// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one proposal costs the allocator, and whether that cost follows the
//! peer count.
//!
//! The question this is built to answer: a proposal has to reach every node, so
//! does the engine pay per node or once? Varying the peer count at a fixed
//! payload answers it directly -- a cost that climbs with peers is a per-node
//! copy, and one that is flat is a shared entry doing its job. The payload
//! sweep then says whether what is copied is the payload itself or only the
//! handles around it.
//!
//! ```text
//! cargo run --release --example propose_allocations
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
        raft_addr: format!("127.0.0.1:{}", 7_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 8_000 + node_id),
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

/// Allocations and bytes per proposal.
fn measure(peers: usize, payload_bytes: usize, proposals: usize) -> (f64, f64) {
    let mut cluster = cluster(peers);
    let payload = vec![7u8; payload_bytes];

    // Warm outside the window: the first proposal of a fresh cluster grows
    // whatever it grows, and the question is the steady state.
    for _ in 0..8 {
        cluster.propose(payload.clone()).expect("warm");
    }

    let before_allocs = ALLOCATIONS.load(Ordering::Relaxed);
    let before_bytes = BYTES.load(Ordering::Relaxed);
    for _ in 0..proposals {
        cluster.propose(payload.clone()).expect("propose");
    }
    let allocs = ALLOCATIONS.load(Ordering::Relaxed) - before_allocs;
    let bytes = BYTES.load(Ordering::Relaxed) - before_bytes;
    (
        allocs as f64 / proposals as f64,
        bytes as f64 / proposals as f64,
    )
}

fn main() {
    // One proposal's payload is cloned by the caller each time, which is a
    // fixed cost in every row and is noted so the figures are not read as if
    // the engine did it.
    println!("== peers swept at a 256-byte payload");
    println!("peers  allocs/propose  bytes/propose");
    for peers in [1usize, 3, 5, 7, 9] {
        let (allocs, bytes) = measure(peers, 256, 200);
        println!("{peers:>5}  {allocs:>14.2}  {bytes:>13.0}");
    }

    println!();
    println!("== payload swept at 5 peers");
    println!("payload_b  allocs/propose  bytes/propose");
    for payload in [0usize, 64, 256, 1024, 4096, 16384] {
        let (allocs, bytes) = measure(5, payload, 200);
        println!("{payload:>9}  {allocs:>14.2}  {bytes:>13.0}");
    }

    println!();
    println!("Each row includes one caller-side clone of the payload, so the");
    println!("payload column has that as its floor.");
}
