// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A cached out-of-order append does not survive the leader changing *within a
//! term*.
//!
//! The reorder queue holds whole `AppendEntriesRequest`s, and
//! `drain_reordered_appends` replays each one through the full append handler
//! -- which re-validates the term. So a cached append from an **earlier term**
//! is refused on replay whether or not the queue was emptied, and the thirteen
//! `clear_reorder_queues()` calls are a memory tidy-up on every path that
//! advances the term.
//!
//! The case where the clear is load-bearing is a leader change that keeps the
//! term: the cached entry's term still matches, so the replay's term check
//! cannot refuse it, and only the explicit clear drops it. Two sites do that --
//! the append path's own, which
//! `append_entries_reorder_queue_is_cleared_on_leader_change` covers, and the
//! snapshot path's, which nothing covered.
//!
//! This was worth getting wrong once. Deleting each of the thirteen calls one
//! at a time left twelve green, and my first five tests for them bumped the
//! term -- so all five passed with the clear deleted and asserted nothing about
//! it. An "undefended" site can mean the guarantee is held somewhere else
//! instead; the way to tell is to delete the call and watch the test that is
//! supposed to care.

use matrixraft::{
    AppendEntriesRequest, Config, InstallSnapshotRequest, LogEntry, LogId, Peer, RaftCluster,
    ReplicaRole, SnapshotChunk, SnapshotMetadata,
};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn entry(term: u64, index: u64, payload: &str) -> LogEntry {
    LogEntry {
        log_id: LogId { term, index },
        payload: payload.as_bytes().to_vec(),
        is_command: true,
    }
}

fn append(
    term: u64,
    leader_id: u64,
    prev: Option<LogId>,
    entries: Vec<LogEntry>,
) -> AppendEntriesRequest {
    let leader_commit = entries.last().map(|e| e.log_id.index).unwrap_or(0);
    AppendEntriesRequest {
        group_id: 7,
        term,
        leader_id,
        prev_log_id: prev,
        entries,
        leader_commit,
        lease_epoch: 0,
    }
}

/// A follower holding index 1, with index 3 cached waiting for index 2.
///
/// Both halves are asserted: a change that stopped caching altogether would
/// fail here rather than making the tests below pass for the wrong reason.
fn follower_with_a_cached_append() -> RaftCluster {
    let mut cluster = RaftCluster::new(7, Config::default(), vec![peer(1), peer(2), peer(3)])
        .expect("valid cluster");
    cluster.start().expect("cluster starts");

    let base = cluster
        .append_entries_to(2, append(1, 1, None, vec![entry(1, 1, "one")]))
        .expect("base append");
    assert!(base.success, "the follower has to take index 1");

    let queued = cluster
        .append_entries_to(
            2,
            append(
                1,
                1,
                Some(LogId { term: 1, index: 2 }),
                vec![entry(1, 3, "cached-three")],
            ),
        )
        .expect("out-of-order append");
    assert!(
        !queued.success,
        "index 3 arriving before index 2 has to be refused and cached"
    );
    assert_eq!(
        cluster.status(2).expect("status").last_log_index,
        1,
        "and must not have been appended"
    );
    cluster
}

/// Deliver the missing index 2 in the same term and report where the log ends.
fn deliver_the_missing_index(cluster: &mut RaftCluster, leader_id: u64) -> u64 {
    let filled = cluster
        .append_entries_to(
            2,
            append(
                1,
                leader_id,
                Some(LogId { term: 1, index: 1 }),
                vec![entry(1, 2, "two")],
            ),
        )
        .expect("the missing append");
    assert!(filled.success, "index 2 has to be accepted");
    cluster.status(2).expect("status").last_log_index
}

#[test]
fn a_snapshot_from_a_new_leader_in_the_same_term_drops_the_cached_append() {
    let mut cluster = follower_with_a_cached_append();
    let term_before = cluster.status(2).expect("status").term;

    // A snapshot from node 3 while the term is unchanged. This is the leader
    // changing without the term moving, which is the only case the explicit
    // clear is load-bearing for: the cached entry's term still matches, so the
    // replay's own term check cannot refuse it.
    let installed = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 7,
                term: term_before,
                leader_id: 3,
                chunk: SnapshotChunk {
                    meta: SnapshotMetadata {
                        snapshot_id: "new-leader-same-term".to_string(),
                        last_log_id: LogId { term: 1, index: 1 },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"snapshot".to_vec(),
                    done: true,
                },
            },
        )
        .expect("snapshot from a new leader");
    let _ = installed;
    assert_eq!(
        cluster.status(2).expect("status").term,
        term_before,
        "the term must NOT have moved, or this test is about the replay's own \
         term check rather than about the clear"
    );

    assert_eq!(
        deliver_the_missing_index(&mut cluster, 3),
        2,
        "the entry cached under the old leader replayed under the new one"
    );
}
