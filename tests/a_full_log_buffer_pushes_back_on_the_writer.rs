// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a leader does when its log buffer fills: it refuses the write.
//!
//! `max_log_buffer_bytes` bounds the payload bytes a leader holds, and
//! `propose_with_options` enforces it in two tiers:
//!
//! ```text
//! if self.should_release_memory() { let _ = self.release_memory()?; }   // 90%
//! if self.is_busy() && !self.release_memory()? {                       // 100%
//!     return Err(RaftError::InvalidRequest("out of memory buffer"));
//! }
//! ```
//!
//! At nine tenths of the limit a proposal **tries a release first**, so an
//! ordinary workload is absorbed rather than refused. Only at the limit, and
//! only when a further release can free nothing, is the writer turned away.
//!
//! **None of this had a test.** `out of memory buffer` appeared nowhere outside
//! the one line that produces it, so neither the refusal nor -- far more
//! importantly -- the way out of it was pinned.
//!
//! ## What actually fills the buffer
//!
//! Not a fast writer: a proposal past the watermark releases first, and an
//! ordinary workload keeps finding something to release. What stops the
//! releasing is a **state machine that has rejected an apply**. The log is
//! pinned by the lowest index any member still needs, a member needs everything
//! it has not applied, and a rejected apply leaves the applied index where it
//! is while the commit index runs on ahead.
//!
//! So the chain is: an apply is rejected, writes carry on, the buffer reaches
//! the limit with nothing releasable beneath it, and the leader starts refusing
//! writes rather than growing without bound. That is the behaviour worth
//! holding -- unbounded memory would be the alternative.
//!
//! The way out is the state machine succeeding: the apply is retried and
//! confirmed, the log releases, and the write path reopens. Asserted end to end
//! here, because a refusal nobody can recover from is a wedged node, and from
//! the outside the two look identical. A first draft of this test discarded the
//! result of `submit_apply_result` with `let _ =`; the call was failing, the
//! applied index never moved, and correct backpressure read as a livelock.

use matrixraft::{AppendEntriesResponse, Config, Peer, RaftCluster, ReplicaRole, SnapshotState};

/// Small enough that one batch of 64-byte payloads passes it.
const TINY_BUFFER_BYTES: u64 = 1024;
const PAYLOAD_BYTES: usize = 64;
const FIRST_BATCH: usize = 100;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn cluster(max_log_buffer_bytes: u64) -> RaftCluster {
    let config = Config {
        max_log_buffer_bytes,
        // Out of the way: a snapshot releases the log by another route, and
        // this file is about the buffer tiers.
        snapshot_threshold_entries: 1_000_000,
        ..Config::default()
    };
    let mut cluster =
        RaftCluster::new(7, config, vec![peer(1), peer(2), peer(3)]).expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
}

fn replicate_through(cluster: &mut RaftCluster, leader: u64, upto: u64) {
    let term = cluster.status(leader).expect("leader status").term;
    for peer_id in [2u64, 3] {
        cluster.catch_up_peer(peer_id).expect("replicate to peer");
        cluster
            .handle_append_entries_response(
                leader,
                peer_id,
                AppendEntriesResponse {
                    term,
                    success: true,
                    match_index: upto,
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
}

/// Propose until the buffer refuses one. Bounded, so a buffer that never fills
/// fails the test instead of looping.
fn propose_until_refused(cluster: &mut RaftCluster) -> (u64, String) {
    let mut accepted = 0u64;
    for _ in 0..5_000 {
        match cluster.propose(vec![7u8; PAYLOAD_BYTES]) {
            Ok(_) => accepted += 1,
            Err(err) => return (accepted, err.to_string()),
        }
    }
    panic!("the buffer never filled after 5000 proposals of {PAYLOAD_BYTES} bytes");
}

/// Drive a leader to a full, unreleasable buffer: write a batch, replicate it,
/// have every state machine REJECT the apply, then keep writing.
///
/// Returns the leader and the refusal.
fn wedge_on_a_rejected_apply(cluster: &mut RaftCluster) -> (u64, String) {
    let leader = cluster.leader_id().expect("a leader");
    for _ in 0..FIRST_BATCH {
        cluster
            .propose(vec![7u8; PAYLOAD_BYTES])
            .expect("the first batch is absorbed by the release tier");
    }
    let through = cluster
        .status(leader)
        .expect("leader status")
        .last_log_index;
    replicate_through(cluster, leader, through);

    // Every state machine rejects the apply. Not discarded -- a failure here
    // would leave the applied index untouched for the wrong reason and make
    // everything below meaningless.
    for node_id in [1u64, 2, 3] {
        cluster
            .submit_apply_result(node_id, through, /* apply_task_rejected */ true)
            .unwrap_or_else(|err| panic!("node {node_id} rejects the apply at {through}: {err}"));
    }

    let (_, refusal) = propose_until_refused(cluster);
    (leader, refusal)
}

#[test]
fn a_full_log_buffer_refuses_the_write() {
    let mut cluster = cluster(TINY_BUFFER_BYTES);
    let (_leader, refusal) = wedge_on_a_rejected_apply(&mut cluster);

    assert!(
        refusal.contains("out of memory buffer"),
        "the refusal has to name the buffer: {refusal}"
    );
    assert!(
        cluster.is_busy(),
        "and the leader reports itself busy, which is the clause that refused"
    );
    assert_eq!(
        cluster.release_memory(),
        Ok(false),
        "and nothing can be released -- which is WHY it refused instead of \
         absorbing the write, since a buffer with anything left to free would \
         have been absorbed by the tier above"
    );
}

#[test]
fn the_write_path_reopens_once_the_apply_succeeds() {
    // The way out, and the reason the test above is not reporting a wedged
    // node. Replication is not what frees the buffer; applying is.
    let mut cluster = cluster(TINY_BUFFER_BYTES);
    let (leader, _) = wedge_on_a_rejected_apply(&mut cluster);

    let through = cluster
        .status(leader)
        .expect("leader status")
        .last_log_index;
    replicate_through(&mut cluster, leader, through);
    assert_eq!(
        cluster.release_memory(),
        Ok(false),
        "acknowledgement alone frees nothing: these entries are on a quorum and \
         the buffer is still pinned, because no state machine has applied them"
    );

    // The state machine retries and this time succeeds.
    for node_id in [1u64, 2, 3] {
        cluster
            .mark_apply_task_inflight(node_id, through)
            .unwrap_or_else(|err| panic!("node {node_id} marks {through} in flight: {err}"));
        cluster
            .submit_apply_result(node_id, through, /* apply_task_rejected */ false)
            .unwrap_or_else(|err| panic!("node {node_id} confirms {through} applied: {err}"));
    }
    assert_eq!(cluster.release_memory(), Ok(true), "applying does free it");

    let resumed = cluster
        .propose(vec![7u8; PAYLOAD_BYTES])
        .expect("and the write path reopens");
    assert!(
        resumed.index > through,
        "the resumed write extends the log it was blocked on: {} is not past {through}",
        resumed.index
    );
}

#[test]
fn a_buffer_with_room_never_refuses_the_same_workload() {
    // The control. Without it every assertion above would also hold against a
    // leader that refused every write once it had written anything at all.
    let mut cluster = cluster(64 * 1024 * 1024);
    for index in 1..=500u64 {
        cluster
            .propose(vec![7u8; PAYLOAD_BYTES])
            .unwrap_or_else(|err| panic!("proposal {index} must be accepted: {err}"));
    }
    assert!(
        !cluster.is_busy(),
        "a leader with room is never busy on this workload"
    );
}

#[test]
fn an_ordinary_writer_is_absorbed_by_the_release_tier() {
    // The other control, and the one that makes the file a statement about
    // WHEN the buffer fills rather than merely that it can. The same tiny
    // buffer, the same payloads, five times the first batch -- and no refusal,
    // because nothing has rejected an apply and so each proposal past the
    // watermark finds something to release.
    let mut cluster = cluster(TINY_BUFFER_BYTES);
    for index in 1..=(FIRST_BATCH as u64 * 5) {
        cluster
            .propose(vec![7u8; PAYLOAD_BYTES])
            .unwrap_or_else(|err| panic!("proposal {index} must be absorbed, not refused: {err}"));
    }
    assert!(
        !cluster.is_busy(),
        "the release tier kept the leader under its limit throughout"
    );
}

#[test]
fn a_payload_larger_than_the_whole_buffer_closes_the_write_path() {
    // This is what the 90% tier is FOR, and removing it is not a no-op: a
    // single payload four times the whole buffer puts the leader over its
    // limit in one step, and no release can ever bring it back under, because
    // the entry itself does not fit.
    //
    // MEASURED with that tier deleted, the next write is ACCEPTED -- the hard
    // check's own `release_memory()` still had something to give, so it gave it
    // and let the write through while the leader was over its limit. With the
    // tier in place that release has already happened, the hard check finds
    // nothing left, and the write is refused. The tier is load-bearing.
    let mut cluster = cluster(TINY_BUFFER_BYTES);
    for _ in 0..8 {
        cluster
            .propose(vec![7u8; PAYLOAD_BYTES])
            .expect("eight small writes stay under the watermark");
    }
    assert!(
        !cluster.is_busy() && !cluster.should_release_memory(),
        "the premise: still below the watermark, so the tier has not fired"
    );

    cluster
        .propose(vec![7u8; TINY_BUFFER_BYTES as usize * 4])
        .expect("an oversized payload is accepted once");
    assert!(
        cluster.is_busy(),
        "and it puts the leader over its limit in a single step"
    );

    let refused = cluster
        .propose(vec![7u8; PAYLOAD_BYTES])
        .expect_err("the next write must be refused while over the limit");
    assert!(
        refused.to_string().contains("out of memory buffer"),
        "{refused:?}"
    );
}

// ## One case this file does NOT discriminate, said rather than left to be found
//
// The refusal is two clauses -- `is_busy() && !release_memory()?` -- and
// dropping the **second** one, so that a busy leader is refused without a
// release being attempted, is **not caught** by anything here.
//
// It may not be reachable. `should_release_memory()` is nine tenths of the
// limit and `is_busy()` is the whole of it, so `is_busy()` implies
// `should_release_memory()`: the tier above always runs first when the hard
// check is going to matter. Discriminating the clause needs a state where that
// tier releases, the leader is *still* over its limit, and a second release can
// free more again -- which the 90%-of-the-distance release policy makes
// conceivable on a long log, and which I could not construct.
//
// Recorded as unknown rather than asserted either way.
