// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the log still holds, and the question that answers.
//!
//! The comparable engine's meta manager exposes the half-open range of indexes
//! still held. Nothing here did, and two things followed: a host could not tell
//! whether a lagging follower was catchable from the log or needed a snapshot,
//! and #186 could not test that the release floor stays at or below what the
//! host has confirmed applied -- compaction removes from the FRONT, so
//! `last_log_index` never moves and nothing exposed the floor. That survivor was
//! documented as having no observable. These tests are that observable.

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

const PAYLOAD_BYTES: usize = 900;

fn cluster(max_log_buffer_bytes: u64) -> RaftCluster {
    let mut cluster = RaftCluster::new(
        7,
        Config {
            max_log_buffer_bytes,
            max_payload_bytes: 1024,
            ..Config::default()
        },
        vec![peer(1), peer(2), peer(3)],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
}

#[test]
fn the_retained_range_says_what_the_log_holds() {
    let mut cluster = cluster(64 * 1024 * 1024);
    let leader = cluster.leader_id().expect("leader");
    let payload = vec![b'x'; PAYLOAD_BYTES];
    for _ in 0..5 {
        cluster.propose(payload.clone()).expect("propose");
    }

    let range = cluster
        .retained_log_range(leader)
        .expect("the leader has a range");
    let last = cluster.status(leader).expect("status").last_log_index;
    assert_eq!(
        range.last_log_index, last,
        "the range ends at the log's end"
    );
    assert!(
        range.first_log_index >= 1 && range.first_log_index <= range.last_log_index,
        "and starts at a real index: {range:?}"
    );

    // The predicate the range exists for. Both bounds, because checking only
    // the upper one passes even when the lower is read from the wrong end --
    // the range collapses to a single index and the probes still answer
    // correctly.
    assert!(
        range.last_log_index > range.first_log_index,
        "the range has to span more than one index to tell the bounds apart: \
         {range:?}"
    );
    assert!(
        range.serves(range.first_log_index),
        "the first index is served from the log"
    );
    assert!(
        range.serves(range.last_log_index),
        "the last index is served from the log"
    );
    assert!(
        !range.serves(range.first_log_index - 1),
        "an index below the floor is not"
    );
    assert!(
        !range.serves(range.last_log_index + 1),
        "an index past the end is not"
    );
    assert!(
        !range.serves(0),
        "and index zero is not a real entry, however empty the range looks"
    );
}

#[test]
fn an_empty_log_serves_nothing() {
    // Both bounds are zero for an empty log, so the emptiness check cannot be
    // folded into the comparison -- `0 >= 0 && 0 <= 0` would say it serves
    // index zero.
    //
    // A cluster that has not started holds no entries, which is how the empty
    // case is reached. The first version of this wrapped the assertion in
    // `if range.first_log_index == 0` against a STARTED cluster, where the
    // leader already holds its noop -- so the guard was never taken and the
    // test silently asserted nothing.
    let cluster = RaftCluster::new(7, Config::default(), vec![peer(1), peer(2), peer(3)])
        .expect("valid cluster");
    let range = cluster.retained_log_range(1).expect("range");
    assert_eq!(
        range.first_log_index, 0,
        "an unstarted cluster has to hold no entries for this to be about \
         anything: {range:?}"
    );
    assert_eq!(range.last_log_index, 0, "{range:?}");
    assert!(!range.serves(0), "an empty log serves nothing: {range:?}");
    assert!(!range.serves(1), "{range:?}");
}

#[test]
fn the_catch_up_predicate_follows_the_floor() {
    let mut cluster = cluster(64 * 1024 * 1024);
    let payload = vec![b'x'; PAYLOAD_BYTES];
    cluster.propose(payload.clone()).expect("propose");

    // A peer at the frontier needs nothing, which is catchable rather than a
    // reason to send a snapshot. Getting this backwards was the first version
    // of the predicate.
    assert!(
        cluster
            .peer_can_catch_up_from_log(2)
            .expect("peer 2 answers"),
        "a peer that needs nothing is catchable"
    );

    // And a peer genuinely behind the floor is not. Node 3 goes away, the log
    // moves on, and the retained range is checked directly rather than trusting
    // the predicate to agree with itself.
    cluster.set_node_healthy(3, false).expect("away");
    for _ in 0..4 {
        let _ = cluster.propose(payload.clone());
    }
    cluster.set_node_healthy(3, true).expect("back, behind");
    let leader = cluster.leader_id().expect("leader");
    let range = cluster.retained_log_range(leader).expect("range");
    let behind_next = cluster
        .peer_pipeline_status(3)
        .expect("peer 3")
        .match_index
        .saturating_add(1);
    assert_eq!(
        cluster
            .peer_can_catch_up_from_log(3)
            .expect("peer 3 answers"),
        range.serves(behind_next) || behind_next > range.last_log_index,
        "the predicate has to agree with the range it is built on: \
         needs {behind_next}, range {range:?}"
    );

    // A peer that is not a member has no answer.
    assert!(
        cluster.peer_can_catch_up_from_log(99).is_err(),
        "a peer that is not a member is an error, not a false"
    );
}
