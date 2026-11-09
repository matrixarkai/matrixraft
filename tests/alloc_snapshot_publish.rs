// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Publishing a completed snapshot moves its metadata into the leader.
//!
//! Each started transfer shares one completed snapshot id and one transfer
//! batch. The completed trigger already owns the metadata installed on the
//! leader, so publication should allocate a constant number of times while its
//! contiguous progress storage grows only with the peers that need it.
//!
//! Measured before and after moving the completed metadata through publication:
//!
//! | voters | boxed transfers | shared batch |
//! |---|---:|---:|
//! | 3 | 3 allocations / 152 B | 3 / 208 B |
//! | 33 | 33 allocations / 2072 B | 3 / 1408 B |

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use matrixraft::{AppendEntriesResponse, Config, Peer, RaftCluster, ReplicaRole, SnapshotState};

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
        raft_addr: format!("127.0.0.1:{}", 64_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 65_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn publication_cost(voters: u64) -> (usize, usize) {
    let mut cluster = RaftCluster::new(19, Config::default(), (1..=voters).map(voter).collect())
        .expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"snapshot-base".to_vec()).expect("propose");
    let leader = cluster.leader_id().expect("leader");
    let term = cluster.status(leader).expect("leader status").term;

    for peer_id in 2..=voters {
        cluster
            .handle_append_entries_response(
                leader,
                peer_id,
                AppendEntriesResponse {
                    term,
                    success: false,
                    match_index: 1,
                    rejection_hint: Some(1),
                    rejected_index: None,
                    require_snapshot: Some(2),
                    snapshot_state: SnapshotState::None,
                    lease_confirmation_epoch: 0,
                    lease_duration_ms: 0,
                },
            )
            .expect("snapshot request");
    }
    let snapshot_id = cluster
        .snapshot_trigger_status()
        .snapshot_id
        .expect("snapshot trigger");

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    cluster
        .handle_snapshot_ready(&snapshot_id, true)
        .expect("publish snapshot");
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    for peer_id in 2..=voters {
        assert!(
            cluster
                .peer_pipeline_status(peer_id)
                .expect("peer pipeline")
                .snapshot_sending,
            "peer {peer_id} did not start its snapshot transfer"
        );
    }
    (
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

fn publication_cost_without_waiters() -> (usize, usize) {
    let mut cluster =
        RaftCluster::new(19, Config::default(), (1..=3).map(voter).collect()).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"snapshot-base".to_vec()).expect("propose");
    let snapshot_id = cluster.trigger_snapshot().expect("trigger").snapshot_id;

    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    cluster
        .handle_snapshot_ready(&snapshot_id, true)
        .expect("publish snapshot");
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));

    (
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

#[test]
fn completed_snapshot_publication_allocates_one_shared_transfer_batch() {
    assert_eq!(
        publication_cost_without_waiters(),
        (0, 0),
        "publication without waiting peers must not allocate a transfer batch"
    );
    let mut measured = Vec::new();
    for voters in [3, 33] {
        let (allocations, bytes) = publication_cost(voters);
        println!("{voters} voters: {allocations} allocations and {bytes} bytes per publication");
        measured.push((voters, allocations, bytes));
    }
    for (voters, allocations, bytes) in measured {
        assert_eq!(
            allocations, 3,
            "a {voters}-voter publication should allocate one id, one batch, and one contiguous slot block, got {allocations} allocations and {bytes} bytes"
        );
        let expected_bytes = 128 + (voters as usize - 1) * 40;
        assert_eq!(
            bytes, expected_bytes,
            "a {voters}-voter publication retained unexpected transfer storage"
        );
    }
}
