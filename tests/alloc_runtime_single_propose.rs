// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A lone durable runtime proposal should not pay for batch bookkeeping when
//! no second proposal is waiting in the mailbox.
//!
//! Over 64 durable proposals, eagerly constructing the batch vector used 1,949
//! allocations, or 30.453 per call. Keeping the first proposal on the stack
//! until a second one is present removes exactly 64 allocations: 1,885 total,
//! or 29.453 per call.

use matrixraft::{Config, NodeOptions, NodeRuntime, Peer, ReplicaRole};
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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
        raft_addr: format!("127.0.0.1:{}", 34_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 35_000 + node_id),
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
        "matrixraft-runtime-propose-{}-{nonce}",
        std::process::id()
    ))
}

#[test]
fn lone_runtime_proposals_bound_heap_bookkeeping() {
    let root = probe_root();
    let config = Config {
        heartbeat_interval_ms: 1_000,
        election_timeout_ms: 5_000,
        leader_lease_ms: 2_000,
        ..Config::default()
    };
    let mut runtime = NodeRuntime::create(NodeOptions {
        group_id: 72,
        node_id: 1,
        raft_addr: "127.0.0.1:34001".to_string(),
        snapshot_addr: "127.0.0.1:35001".to_string(),
        wal_dir: root.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: root.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config,
        peers: vec![peer(1), peer(2), peer(3)],
    })
    .expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(vec![0; 8]).expect("warm proposal");

    const ROUNDS: usize = 64;
    let payloads = (1..=ROUNDS)
        .map(|round| vec![round as u8; 8])
        .collect::<Vec<_>>();
    ALLOCATIONS.store(0, Ordering::Relaxed);
    for payload in payloads {
        COUNTING.store(true, Ordering::Relaxed);
        runtime.propose(payload).expect("durable proposal");
        COUNTING.store(false, Ordering::Relaxed);
    }

    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let per_proposal = allocations as f64 / ROUNDS as f64;
    println!("lone runtime proposal: {per_proposal:.3} allocations per call");
    assert!(
        per_proposal < 30.0,
        "lone runtime proposals used {per_proposal:.3} allocations per call"
    );

    drop(runtime);
    let _ = std::fs::remove_dir_all(root);
}
