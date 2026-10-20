// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A follower's append response cannot fault its leader.
//!
//! `ReplicationPipeline::handle_append_response` took `response.match_index`
//! with `.max()` -- no upper bound -- and then added one to it. The response
//! comes from a follower, so a follower answering `success` with
//! `match_index = u64::MAX` panicked its **leader**:
//!
//! ```text
//! panicked at src/pipeline.rs:597: attempt to add with overflow
//! PANICKED through the public response API
//! ```
//!
//! Reachable through `RaftCluster::handle_append_entries_response`, which is
//! the documented way to hand a leader a response that arrived over the
//! network, in any build with overflow checks -- the default debug profile,
//! and release wherever `overflow-checks` is set as hardening.
//!
//! **The claim itself is still unbounded, and that is deliberate.** It paces
//! replication and does not decide the commit, which is computed from the
//! leader's own node model. The third test here measures that rather than
//! assuming it, so that nobody bounds the claim believing it is a commit hole,
//! and so the saturation cannot quietly turn into a commit change.

use matrixraft::{
    AppendEntriesResponse, Config, Peer, PipelineLimits, RaftCluster, ReplicaRole,
    ReplicationPipeline, SnapshotState,
};

const GROUP_ID: u64 = 97;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 25_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 25_500 + node_id),
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

fn response(term: u64, match_index: u64) -> AppendEntriesResponse {
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
    }
}

#[test]
fn a_follower_claiming_the_top_of_the_range_does_not_fault_its_leader() {
    let mut cluster = started();
    let leader = cluster.leader_id().expect("a leader");
    cluster.propose(b"one".to_vec()).expect("an entry");
    let term = cluster.status(leader).expect("status").term;

    // The test completing at all is the assertion: this used to abort the
    // process rather than return.
    cluster
        .handle_append_entries_response(leader, 2, response(term, u64::MAX))
        .expect("a claim at the top of the range is handled, not fatal");
}

#[test]
fn the_pipeline_handles_the_same_claim_directly() {
    // An embedder driving replication itself reaches the pipeline without
    // going through the cluster, so the bound has to hold there too.
    let mut pipeline = ReplicationPipeline::new(2, 1, PipelineLimits::default());
    pipeline
        .handle_append_response(&response(1, u64::MAX))
        .expect("handled");
    assert_eq!(
        pipeline.status().next_index,
        u64::MAX,
        "the next index saturates rather than wrapping to zero, which would \
         restart replication from the beginning of the log"
    );
}

#[test]
fn an_inflated_claim_does_not_move_the_commit_index() {
    // Measured, not assumed. Two followers claiming far more than the leader
    // ever wrote is a quorum of inflated claims, and the commit index has to
    // stay where the leader's own model puts it.
    let mut cluster = started();
    let leader = cluster.leader_id().expect("a leader");
    cluster.propose(b"one".to_vec()).expect("an entry");
    let term = cluster.status(leader).expect("status").term;
    let before = cluster.status(leader).expect("status").commit_index;

    for peer_id in [2u64, 3] {
        cluster
            .handle_append_entries_response(leader, peer_id, response(term, 9_000_000))
            .expect("handled");
    }

    assert_eq!(
        cluster.status(leader).expect("status").commit_index,
        before,
        "the commit index is computed from the leader's own node model, so a \
         follower cannot talk it forward"
    );
}

#[test]
fn an_ordinary_response_still_advances_the_peer() {
    // The positive control: saturating arithmetic that pinned the next index
    // would satisfy the assertions above.
    let mut pipeline = ReplicationPipeline::new(2, 1, PipelineLimits::default());
    pipeline
        .handle_append_response(&response(1, 4))
        .expect("handled");
    assert_eq!(pipeline.status().match_index, 4);
    assert_eq!(
        pipeline.status().next_index,
        5,
        "an ordinary claim still moves the next index one past it"
    );
}

#[test]
fn a_later_step_cannot_fault_on_the_claim_either() {
    // The response path is not the only place that adds one to the stored
    // match index -- five sites do, all reading the same field. Saturating only
    // the response site would let `u64::MAX` be STORED and move the fault one
    // step later instead of removing it, which is what this sequence shows:
    // a claim at the top of the range, then an ordinary network error.
    //
    // `record_network_error` is the cheapest of those later steps to reach.
    // The two snapshot-install sites are the same shape behind a transfer.
    let mut pipeline = ReplicationPipeline::new(2, 1, PipelineLimits::default());
    pipeline
        .handle_append_response(&response(1, u64::MAX))
        .expect("the claim is handled");
    assert_eq!(pipeline.status().match_index, u64::MAX, "and it is stored");

    // Has to actually transition for the assertion below to mean anything: it
    // returns false and touches nothing unless the peer was replicating.
    let transitioned = pipeline.record_network_error();
    assert!(
        transitioned,
        "the peer has to have been replicating for this path to run at all"
    );
    assert_eq!(
        pipeline.status().next_index,
        u64::MAX,
        "the later step saturates too rather than wrapping to zero"
    );
}
