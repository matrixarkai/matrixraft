// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Automatic heartbeat catch-up should not build heap bookkeeping that is
//! discarded as soon as the recovered peer has been visited.
//!
//! Across 64 one-entry recoveries, the old path allocated 253 times: a peer-ID
//! list, a discarded reason, and exact follower-log growth on nearly every
//! round. Stack chunks, an empty internal reason, and amortized log growth leave
//! five allocations in the same run, or 0.078 per recovered follower.

use matrixraft::{Config, NodeOptions, NodeRuntime, Peer, ReplicaRole};
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 32_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 33_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn probe_root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-heartbeat-catchup-{}-{nonce}",
        std::process::id()
    ))
}

#[test]
fn repeated_heartbeat_catchup_bounds_heap_bookkeeping() {
    let root = probe_root();
    let config = Config {
        heartbeat_interval_ms: 5,
        election_timeout_ms: 1_000,
        leader_lease_ms: 500,
        ..Config::default()
    };
    let mut runtime = NodeRuntime::create(NodeOptions {
        group_id: 71,
        node_id: 1,
        raft_addr: "127.0.0.1:32001".to_string(),
        snapshot_addr: "127.0.0.1:33001".to_string(),
        wal_dir: root.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: root.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config,
        peers: vec![peer(1), peer(2), peer(3)],
    })
    .expect("create runtime");
    runtime.start().expect("start runtime");

    const ROUNDS: usize = 64;
    ALLOCATIONS.store(0, Ordering::Relaxed);
    for round in 0..ROUNDS {
        runtime.set_node_healthy(2, false).expect("pause follower");
        runtime
            .propose(vec![round as u8])
            .expect("append while follower is paused");
        runtime.set_node_healthy(2, true).expect("resume follower");

        COUNTING.store(true, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(25));
        COUNTING.store(false, Ordering::Relaxed);

        let status = runtime.status().expect("runtime status");
        let follower = status
            .peer_runtime
            .iter()
            .find(|peer| peer.node_id == 2)
            .expect("follower status");
        assert_eq!(
            follower.lag, 0,
            "round {round} did not recover the follower"
        );
    }

    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let per_round = allocations as f64 / ROUNDS as f64;
    println!("heartbeat catch-up: {per_round:.3} allocations per recovered follower");
    assert!(
        allocations <= 8,
        "heartbeat catch-up used {allocations} allocations across {ROUNDS} recovered followers ({per_round:.3} per follower)"
    );

    drop(runtime);
    let _ = std::fs::remove_dir_all(root);
}
