// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Publishing a completed snapshot does not allocate a waiting-peer list.
//!
//! Each started transfer owns its snapshot id, so one string allocation per
//! waiting follower is intentional. Selecting those followers should not also
//! materialize every peer id before the transfers begin.
//!
//! Measured before and after direct pipeline iteration:
//!
//! | voters | before | after |
//! |---|---:|---:|
//! | 3 | 9 allocations / 480 B | 8 / 448 B |
//! | 33 | 42 allocations / 5368 B | 38 / 4888 B |

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

#[test]
fn completed_snapshot_publication_avoids_peer_list_scratch() {
    let mut measured = Vec::new();
    for voters in [3, 33] {
        let (allocations, bytes) = publication_cost(voters);
        println!("{voters} voters: {allocations} allocations and {bytes} bytes per publication");
        measured.push((voters, allocations, bytes));
    }
    for (voters, allocations, bytes) in measured {
        let expected_allocations = voters as usize + 5;
        let expected_bytes = if voters == 3 { 448 } else { 4_888 };
        assert_eq!(
            allocations, expected_allocations,
            "a {voters}-voter publication should retain only owned snapshot state, got {allocations} allocations and {bytes} bytes"
        );
        assert_eq!(
            bytes, expected_bytes,
            "a {voters}-voter publication retained unexpected heap scratch"
        );
    }
}
