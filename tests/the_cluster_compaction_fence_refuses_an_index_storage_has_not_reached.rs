// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Compacting past what storage has durably taken would discard log records
//! that are not yet on disk, so a fenced compaction refuses a requested index
//! storage has not reached.
//!
//! That rule is enforced at **two sites**, with the same two clauses and the
//! same blocker text: once where the WAL compacts itself, and once at the
//! cluster level in `compact_logs_with_storage_fence`. Only the WAL copy had a
//! test. Both clauses of the cluster copy could be deleted with 1175 tests
//! green.
//!
//! The reason is uniform across every test that drives the cluster-level call:
//! each one builds a fence whose `durable_applied_index` and
//! `storage_flushed_index` both already equal the requested index, and asserts
//! `fence_valid`. The valid path is covered several times over; **the refusal
//! never is.** A rule enforced at many sites and tested at one is the shape
//! worth naming here -- the duplicate reads as covered because its twin is.
//!
//! ## On the second clause
//!
//! The condition is `durable_applied_index < log_index || storage_flushed_index
//! < log_index`, and the second half cannot decide anything at either site: the
//! validator that runs immediately before it already requires flush at or after
//! durable apply, so with the first clause false that forces flush at or above
//! the requested index. That ordering is pinned on its own elsewhere, as a
//! property of the validator rather than of either call site, so this file does
//! not restate it. What it covers is the clause that *is* reachable here.

use matrixraft::{Peer, RaftCluster, ReplicaRole, StorageApplyFence};

const GROUP: u64 = 55;
const COMPACT_THROUGH: u64 = 4;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 18_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 19_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// A started cluster holding six committed writes, so there is something a
/// compaction could remove and something a refusal could protect.
fn cluster_with_writes() -> RaftCluster {
    let mut cluster = RaftCluster::new(GROUP, Default::default(), vec![peer(1), peer(2), peer(3)])
        .expect("cluster");
    cluster.start().expect("start");
    for index in 1..=6 {
        cluster
            .propose(format!("write-{index}").into_bytes())
            .expect("propose");
    }
    cluster
}

/// A fence that passes the validator: applied at or below committed, durable
/// apply at or below applied, flush at or after durable apply.
fn fence(durable_applied_index: u64, storage_flushed_index: u64) -> StorageApplyFence {
    StorageApplyFence {
        group_id: GROUP,
        node_id: 1,
        committed_index: 7,
        applied_index: 7,
        durable_applied_index,
        storage_flushed_index,
        installed_snapshot_index: 0,
        first_retained_log_index: 1,
    }
}

#[test]
fn a_durable_apply_behind_the_requested_index_refuses_and_releases_nothing() {
    let mut cluster = cluster_with_writes();

    let blocked = cluster
        .compact_logs_with_storage_fence(COMPACT_THROUGH, fence(COMPACT_THROUGH - 2, 7))
        .expect("the call answers with a report rather than failing");

    assert!(
        !blocked.fence_valid,
        "a durable apply two indexes behind the request is not a valid fence"
    );
    assert_eq!(
        blocked.blocker.as_deref(),
        Some("compaction fence is behind requested log index"),
        "the report must name the fence as the blocker, not the validator -- a refusal from one layer up would read the same to a caller checking only fence_valid"
    );
    assert_eq!(
        blocked.released_segments, 0,
        "a refused compaction must release nothing"
    );
    assert_eq!(
        blocked.requested_log_index, COMPACT_THROUGH,
        "the report is about the index that was asked for"
    );

    // `released_segments == 0` on its own would also hold if there had been
    // nothing to release. This is what rules that out: the entries the refusal
    // protected are still there to be removed once the fence catches up.
    let allowed = cluster
        .compact_logs_with_storage_fence(COMPACT_THROUGH, fence(COMPACT_THROUGH, 7))
        .expect("the call answers");
    assert!(
        allowed.fence_valid,
        "a durable apply that has reached the requested index is a valid fence"
    );
    assert!(
        allowed.released_segments > 0,
        "the refusal above protected real entries, which this now removes"
    );
}

#[test]
fn a_fence_that_has_caught_up_compacts() {
    // The control for the whole file: if the cluster refused every fenced
    // compaction, the test above would pass for the wrong reason.
    let mut cluster = cluster_with_writes();

    let report = cluster
        .compact_logs_with_storage_fence(COMPACT_THROUGH, fence(COMPACT_THROUGH, COMPACT_THROUGH))
        .expect("the call answers");

    assert!(report.fence_valid, "both clauses are false for this fence");
    assert_eq!(report.blocker, None, "a valid fence carries no blocker");
    assert!(
        report.released_segments > 0,
        "a valid fence at index 4 releases the entries below it"
    );
}
