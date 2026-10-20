// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Where the bytes a proposal allocates actually go.
//!
//! `propose_allocations` answers *how many* and *how much*; it does not say
//! what. The count is flat in the payload and the byte total is flat too --
//! about a kilobyte per proposal that is not the payload -- so the cost is
//! structural, and the next question is which allocation is which.
//!
//! This records the **size of every allocation** made during a small number of
//! proposals and prints the distinct sizes with their counts. A size histogram
//! identifies sites the way a profile would: a 16-byte string, a 24-byte
//! vector header, a 512-byte buffer each look like themselves.
//!
//! The payload is 0 bytes, so nothing in the output is the payload, and the
//! harness clones nothing.
//!
//! ```text
//! cargo run --release --example propose_allocation_sizes
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

static RECORDING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static SIZES: Mutex<Option<BTreeMap<usize, usize>>> = Mutex::new(None);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if RECORDING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            // The lock itself must not recurse into the recorder, so the flag
            // is cleared around it. One proposal at a time, single threaded.
            RECORDING.store(false, Ordering::Relaxed);
            if let Ok(mut guard) = SIZES.lock() {
                *guard
                    .get_or_insert_with(BTreeMap::new)
                    .entry(layout.size())
                    .or_insert(0) += 1;
            }
            RECORDING.store(true, Ordering::Relaxed);
        }
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
        raft_addr: format!("127.0.0.1:{}", 31_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 32_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn main() {
    let peers: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(5);
    let proposals: usize = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(50);

    let members = (1..=peers).map(peer).collect::<Vec<_>>();
    let mut cluster = RaftCluster::new(7, Config::default(), members).expect("cluster");
    cluster.start().expect("start");

    // Warm outside the window: the first proposals grow what they grow.
    for _ in 0..16 {
        cluster.propose(Vec::new()).expect("warm");
    }

    RECORDING.store(true, Ordering::Relaxed);
    for _ in 0..proposals {
        cluster.propose(Vec::new()).expect("propose");
    }
    RECORDING.store(false, Ordering::Relaxed);

    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    let sizes = SIZES.lock().expect("sizes").take().unwrap_or_default();

    println!("peers={peers} proposals={proposals} payload=0");
    println!(
        "  {:.2} allocations/proposal, {:.1} bytes/proposal",
        allocations as f64 / proposals as f64,
        bytes as f64 / proposals as f64
    );
    println!();
    println!(
        "  {:>10}  {:>10}  {:>12}  {:>12}",
        "size", "count", "per proposal", "bytes/propose"
    );
    for (size, count) in &sizes {
        println!(
            "  {:>10}  {:>10}  {:>12.2}  {:>12.1}",
            size,
            count,
            *count as f64 / proposals as f64,
            (size * count) as f64 / proposals as f64
        );
    }
    let counted: usize = sizes.iter().map(|(size, count)| size * count).sum();
    assert_eq!(
        counted, bytes,
        "the histogram has to account for every byte counted"
    );
    assert!(
        allocations > 0,
        "nothing was recorded, so this measured an empty window"
    );
}
