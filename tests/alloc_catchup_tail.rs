// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Catch-up shares retained log entries instead of copying every payload.
//!
//! The leader already owns immutable entries behind `Arc`. Recovering a
//! lagging follower should clone those handles in bounded chunks, leaving only
//! follower-log growth and the owned report reason to allocate.
//!
//! Measured with 32 missing entries carrying 4 KiB each:
//!
//! | before | after |
//! |---:|---:|
//! | 71 allocations / 135640 B | 3 allocations / 288 B |

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = TRACK_ALLOCATIONS.try_with(|tracking| {
            if tracking.get() {
                ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
                BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn voter(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 66_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 67_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

#[test]
fn lagging_follower_catchup_does_not_copy_payloads() {
    let mut cluster =
        RaftCluster::new(20, Config::default(), (1..=3).map(voter).collect()).expect("cluster");
    cluster.start().expect("start");
    cluster.set_node_healthy(2, false).expect("isolate peer");
    for index in 0..32_u8 {
        cluster
            .propose(vec![index; 4 * 1024])
            .expect("propose catch-up entry");
    }
    let leader_index = cluster.status(1).expect("leader status").last_log_index;
    assert!(cluster.status(2).expect("peer status").last_log_index < leader_index);
    cluster.set_node_healthy(2, true).expect("heal peer");

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    let report = cluster.catch_up_peer(2).expect("catch up peer");
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    assert!(report.caught_up);
    assert_eq!(report.learner_match_index_after, leader_index);
    assert_eq!(
        cluster.status(2).expect("caught-up status").last_log_index,
        leader_index
    );
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    println!("32-entry catch-up: {allocations} allocations and {bytes} bytes");
    assert!(
        allocations <= 3,
        "catch-up should allocate its follower log once and retain its report, got {allocations} allocations"
    );
    assert!(
        bytes <= 400,
        "catch-up copied retained payloads or kept oversized scratch: {bytes} bytes"
    );
}
