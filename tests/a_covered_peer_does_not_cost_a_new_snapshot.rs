// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A peer that can no longer be caught up from the log is served from the
//! snapshot the leader already has, whenever that snapshot reaches far enough.
//!
//! An append response carrying a required snapshot index says "the log no
//! longer holds what I need". The leader owes that peer a snapshot covering
//! the index -- not a *new* snapshot. Building one unconditionally costs twice
//! over: a snapshot the leader did not need, and the cancellation of whatever
//! transfer was already in flight to that same peer, which may have been the
//! very snapshot that would have fixed it.
//!
//! Two rules sit on that path and they are asserted separately here, because
//! either one going missing is a different defect:
//!
//!   1. an already-covered requirement is answered with nothing at all, and
//!   2. a build that is already running is not restarted.
//!
//! The third test is the control for both: a leader whose snapshot really is
//! behind the requirement must cancel the transfer and build, or a peer past
//! the log floor could never be caught up again.

use matrixraft::{AppendEntriesResponse, Config, Peer, RaftCluster, ReplicaRole, SnapshotState};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn three_node_cluster() -> RaftCluster {
    RaftCluster::new(
        7,
        Config::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("valid cluster")
}

/// Replicate to `peer_id` and acknowledge up to `match_index`, which is what
/// moves the leader's commit index and so the index a snapshot would carry.
fn acknowledge(cluster: &mut RaftCluster, leader: u64, peer_id: u64, term: u64, match_index: u64) {
    cluster.catch_up_peer(peer_id).expect("replicate to peer");
    cluster
        .handle_append_entries_response(
            leader,
            peer_id,
            AppendEntriesResponse {
                term,
                success: true,
                match_index,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: SnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            },
        )
        .expect("quorum acknowledgement");
}

/// A leader with a committed entry, so a snapshot it takes reaches a real
/// index rather than 0. Returns the leader id, its term and that commit index.
fn leader_with_a_commit(cluster: &mut RaftCluster) -> (u64, u64, u64) {
    cluster.start().expect("cluster starts");
    let leader = cluster.leader_id().expect("a leader");
    let term = cluster.status(leader).expect("leader status").term;
    let proposed = cluster.propose(b"payload".to_vec()).expect("propose");
    acknowledge(cluster, leader, 2, term, proposed.index);
    acknowledge(cluster, leader, 3, term, proposed.index);
    let commit_index = cluster.status(leader).expect("leader status").commit_index;
    assert_eq!(
        commit_index, proposed.index,
        "the proposal must be committed before a snapshot can cover it"
    );
    (leader, term, commit_index)
}

/// The append response a peer sends when the log can no longer catch it up.
fn requires_snapshot(term: u64, required_index: u64) -> AppendEntriesResponse {
    AppendEntriesResponse {
        term,
        success: false,
        match_index: 0,
        rejection_hint: None,
        rejected_index: None,
        require_snapshot: Some(required_index),
        // Receiving, because this peer is being sent a snapshot already. It
        // also keeps the send-timeout accounting from retiring the transfer
        // for an unrelated reason, which would make the assertions below pass
        // or fail for something other than the rule under test.
        snapshot_state: SnapshotState::Receiving,
        lease_confirmation_epoch: 0,
        lease_duration_ms: 0,
    }
}

#[test]
fn a_requirement_the_leader_snapshot_already_covers_costs_nothing() {
    let mut cluster = three_node_cluster();
    let (leader, term, commit_index) = leader_with_a_commit(&mut cluster);

    let meta = cluster.trigger_snapshot().expect("take a snapshot");
    cluster
        .handle_snapshot_ready(&meta.snapshot_id, true)
        .expect("the snapshot finishes");
    assert_eq!(
        meta.last_log_id.index, commit_index,
        "the leader snapshot reaches its commit index"
    );
    assert!(
        !cluster.snapshot_trigger_status().in_progress,
        "no build is running once the snapshot is ready"
    );

    cluster
        .begin_snapshot_send_to(2, "transfer-already-in-flight", 1, 4)
        .expect("a transfer to that peer is under way");
    assert!(
        cluster
            .peer_pipeline_status(2)
            .expect("peer status")
            .snapshot_sending,
        "the premise: there is something to lose"
    );

    // Index 1 is behind the leader's snapshot, so the snapshot it already
    // holds carries everything this peer is missing.
    cluster
        .handle_append_entries_response(leader, 2, requires_snapshot(term, 1))
        .expect("the requirement is accepted");

    assert!(
        cluster
            .peer_pipeline_status(2)
            .expect("peer status")
            .snapshot_sending,
        "a covered requirement must not cancel the transfer that is already serving it"
    );
    assert!(
        !cluster.snapshot_trigger_status().in_progress,
        "a covered requirement must not provoke a second snapshot"
    );
}

#[test]
fn a_snapshot_exactly_at_the_required_index_is_enough() {
    let mut cluster = three_node_cluster();
    let (leader, term, commit_index) = leader_with_a_commit(&mut cluster);

    let meta = cluster.trigger_snapshot().expect("take a snapshot");
    cluster
        .handle_snapshot_ready(&meta.snapshot_id, true)
        .expect("the snapshot finishes");
    cluster
        .begin_snapshot_send_to(2, "transfer-already-in-flight", 1, 4)
        .expect("a transfer to that peer is under way");

    // The boundary: the requirement names the leader's snapshot index exactly.
    // A snapshot at index N includes index N, so this is covered -- the test
    // exists because reading that comparison as strict would make it stale.
    cluster
        .handle_append_entries_response(leader, 2, requires_snapshot(term, commit_index))
        .expect("the requirement is accepted");

    assert!(
        cluster
            .peer_pipeline_status(2)
            .expect("peer status")
            .snapshot_sending,
        "a snapshot at the required index covers it, so the transfer stands"
    );
    assert!(
        !cluster.snapshot_trigger_status().in_progress,
        "a snapshot at the required index covers it, so nothing is rebuilt"
    );
}

#[test]
fn a_leader_snapshot_behind_the_requirement_is_rebuilt() {
    let mut cluster = three_node_cluster();
    let (leader, term, commit_index) = leader_with_a_commit(&mut cluster);

    // No snapshot was ever taken, so the leader holds none and counts as being
    // at index 0 -- genuinely behind any requirement at all.
    cluster
        .begin_snapshot_send_to(2, "transfer-of-a-snapshot-too-old", 1, 4)
        .expect("a transfer to that peer is under way");

    cluster
        .handle_append_entries_response(leader, 2, requires_snapshot(term, commit_index))
        .expect("the requirement is accepted");

    assert!(
        !cluster
            .peer_pipeline_status(2)
            .expect("peer status")
            .snapshot_sending,
        "a transfer of a snapshot that cannot cover the peer is abandoned"
    );
    let status = cluster.snapshot_trigger_status();
    assert!(
        status.in_progress,
        "a requirement past every snapshot the leader has must provoke one"
    );
    assert_eq!(
        status
            .last_log_id
            .expect("the new build names its index")
            .index,
        commit_index,
        "the snapshot being built reaches the commit index, so it will cover the peer"
    );
}

#[test]
fn a_build_already_running_is_not_restarted() {
    let mut cluster = three_node_cluster();
    let (leader, term, commit_index) = leader_with_a_commit(&mut cluster);

    // A build is running and has NOT finished, so the leader still holds no
    // snapshot: the requirement below is past what it has, which is what gets
    // past the coverage rule and reaches the second one.
    let running = cluster.trigger_snapshot().expect("a build starts");
    assert!(
        cluster.snapshot_trigger_status().in_progress,
        "the premise: a build is already under way"
    );
    let duplicates_before = cluster.duplicate_snapshot_trigger_requests();

    cluster
        .begin_snapshot_send_to(2, "transfer-of-a-snapshot-too-old", 1, 4)
        .expect("a transfer to that peer is under way");
    cluster
        .handle_append_entries_response(leader, 2, requires_snapshot(term, commit_index))
        .expect("the requirement is accepted");

    assert_eq!(
        cluster.duplicate_snapshot_trigger_requests(),
        duplicates_before,
        "asking for a snapshot while one is building must not even reach the trigger"
    );
    assert_eq!(
        cluster.snapshot_trigger_status().snapshot_id,
        Some(running.snapshot_id),
        "the build that was already running is the one still running"
    );
    assert!(
        !cluster
            .peer_pipeline_status(2)
            .expect("peer status")
            .snapshot_sending,
        "the stale transfer is still abandoned -- only the rebuild is skipped"
    );
}
