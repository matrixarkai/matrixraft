// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A crafted snapshot chunk is refused, not fatal.
//!
//! Both install entry points computed `chunk.offset + chunk.data.len()` as the
//! first thing they did, before anything had validated the offset -- and the
//! offset comes off the wire. `offset = u64::MAX` with one byte of data was
//! "attempt to add with overflow" at `cluster_runtime.rs:5569`, and the same
//! again at `:5661` for `snapshot_index + 1`, where the snapshot index is the
//! peer's own `chunk.meta.last_log_id.index`. One crafted chunk, one downed
//! node, in any build with overflow checks on -- which is the default debug
//! profile, and release too wherever `overflow-checks` is set as a hardening
//! measure.
//!
//! `SnapshotInstallState::install_chunk` validates the offset properly; it just
//! did not get to run first.
//!
//! Saturating is the whole fix: the offset then fails the checks that were
//! always there, and the peer is told what is wrong with its chunk instead of
//! the node going down. The comparable engine does the same job at the wire,
//! refusing a chunk whose declared size is 1MB or more rather than reading it.
//!
//! **There are four of these, two in each entry point**, so each entry point
//! is tested separately. A fix applied to one of them passes the other's tests.

use matrixraft::{
    Config, InstallSnapshotRequest, LogId, Peer, RaftCluster, ReplicaRole, SnapshotChunk,
    SnapshotLifecycle, SnapshotMetadata,
};

const GROUP_ID: u64 = 95;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 23_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 23_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started() -> RaftCluster {
    let mut cluster =
        RaftCluster::new(GROUP_ID, Config::default(), vec![peer(1), peer(2), peer(3)])
            .expect("build the cluster");
    cluster.start().expect("start the cluster");
    cluster
}

fn request(offset: u64, snapshot_index: u64, data: Vec<u8>, done: bool) -> InstallSnapshotRequest {
    InstallSnapshotRequest {
        group_id: GROUP_ID,
        term: 1,
        leader_id: 1,
        chunk: SnapshotChunk {
            meta: SnapshotMetadata {
                snapshot_id: "crafted".to_string(),
                last_log_id: LogId {
                    term: 1,
                    index: snapshot_index,
                },
                membership: vec![1, 2, 3],
                members: Vec::new(),
                payload_bytes: 0,
            },
            offset,
            data,
            done,
        },
    }
}

#[test]
fn a_chunk_offset_at_the_top_of_the_range_is_refused() {
    let mut cluster = started();
    let refused = cluster
        .install_snapshot_chunk_to(2, request(u64::MAX, 5, vec![1], true))
        .expect_err("an offset at the top of the range cannot be installed");
    assert!(
        refused.to_string().contains("arrived before offset 0"),
        "the peer has to be told what is wrong with its chunk: {refused}"
    );
}

#[test]
fn a_snapshot_index_at_the_top_of_the_range_is_refused() {
    let mut cluster = started();
    let refused = cluster
        .install_snapshot_chunk_to(2, request(0, u64::MAX, Vec::new(), true))
        .expect_err("a snapshot index at the top of the range cannot be installed");
    assert!(
        !refused.to_string().is_empty(),
        "and it is an error rather than a panic: {refused}"
    );
}

#[test]
fn the_lifecycle_entry_point_refuses_them_too() {
    // The same two sites exist in `install_snapshot_lifecycle_request_to`, and
    // nothing about fixing one fixes the other.
    let mut cluster = started();
    let mut lifecycle = SnapshotLifecycle::new(Default::default()).expect("a lifecycle");

    let refused_offset = cluster
        .install_snapshot_lifecycle_request_to(
            2,
            &mut lifecycle,
            request(u64::MAX, 5, vec![1], true),
        )
        .expect_err("the offset is refused through this path as well");
    assert!(
        !refused_offset.to_string().is_empty(),
        "an error, not a panic: {refused_offset}"
    );

    let mut cluster = started();
    let mut lifecycle = SnapshotLifecycle::new(Default::default()).expect("a lifecycle");
    let refused_index = cluster.install_snapshot_lifecycle_request_to(
        2,
        &mut lifecycle,
        request(0, u64::MAX, Vec::new(), true),
    );
    assert!(
        refused_index.is_err(),
        "and so is the snapshot index: {refused_index:?}"
    );
}

#[test]
fn an_ordinary_chunk_sequence_still_installs() {
    // The positive control. Saturating arithmetic that refused everything
    // would satisfy every assertion above.
    let mut cluster = started();
    let first = cluster
        .install_snapshot_chunk_to(2, request(0, 9, vec![b'a'; 8], false))
        .expect("the first chunk is accepted");
    assert!(first.accepted, "reason: {}", first.reason);
    assert_eq!(
        first.next_offset, 8,
        "the offset advances by the bytes sent"
    );

    let second = cluster
        .install_snapshot_chunk_to(2, request(8, 9, vec![b'b'; 4], true))
        .expect("the last chunk is accepted");
    assert!(second.accepted, "reason: {}", second.reason);
    assert_eq!(second.next_offset, 12);
    assert_eq!(
        second.reason, "snapshot_installed",
        "and a complete sequence installs rather than stalling"
    );
}
