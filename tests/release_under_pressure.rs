// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A lagging follower does not stall the leader's writes.
//!
//! `release_memory` only ever compacted to `min_replicated_index - 1`, which a
//! follower that was away and came back behind pins -- so nothing could be
//! released while `is_busy` refused proposals over the same limit. Measured on
//! main, with the host applying as it went:
//!
//! ```text
//! node 3 back at match_index=0, leader last=7
//! min_replicated now: Ok(1)
//! leader applied frontier: 10
//! while lagging: accepted=3 refused=27
//! release_memory released anything: false
//! is_busy after release: true
//! ```
//!
//! The state machine had consumed through 10 and the log could safely have been
//! released to there. Twenty-seven of thirty writes were refused for as long as
//! the follower lagged.
//!
//! The policy now has two tiers, as the comparable engine's does: while memory
//! is comfortable, release only up to the slowest replica so a behind follower
//! can still be caught up from the log; once the buffer is at its high
//! watermark, release up to what the host has confirmed applied and let the
//! follower recover by snapshot instead.

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

const LIMIT_BYTES: u64 = 4 * 1024;
const PAYLOAD_BYTES: usize = 900;

fn cluster() -> RaftCluster {
    let mut cluster = RaftCluster::new(
        7,
        Config {
            max_log_buffer_bytes: LIMIT_BYTES,
            max_payload_bytes: 1024,
            ..Config::default()
        },
        vec![peer(1), peer(2), peer(3)],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
}

/// Take node 3 away, grow the log, bring it back behind.
///
/// Replication inside `propose` is synchronous, so this is the only way to
/// reach lagging-and-healthy -- which is what a follower that was away really
/// is, and the state `min_replicated_index` counts.
fn leader_with_a_follower_left_behind(cluster: &mut RaftCluster) -> u64 {
    let payload = vec![b'x'; PAYLOAD_BYTES];
    cluster.set_node_healthy(3, false).expect("node 3 away");
    for _ in 0..6 {
        let _ = cluster.propose(payload.clone());
    }
    cluster
        .set_node_healthy(3, true)
        .expect("node 3 back, behind");
    let behind_at = cluster
        .peer_pipeline_status(3)
        .expect("peer 3 pipeline")
        .match_index;
    let leader = cluster.leader_id().expect("leader");
    assert!(
        behind_at < cluster.status(leader).expect("status").last_log_index,
        "node 3 has to actually be behind for this to be about anything"
    );
    behind_at
}

#[test]
fn a_lagging_follower_does_not_stall_the_leaders_writes() {
    let mut cluster = cluster();
    let leader = cluster.leader_id().expect("leader");
    leader_with_a_follower_left_behind(&mut cluster);

    let payload = vec![b'x'; PAYLOAD_BYTES];
    let mut accepted = 0;
    let mut refused = 0;
    for _ in 0..30 {
        match cluster.propose(payload.clone()) {
            Ok(_) => accepted += 1,
            Err(_) => refused += 1,
        }
        // The host applies as it goes, which is the ordinary case: the state
        // machine consumes committed entries even while a follower lags. Without
        // this the log genuinely cannot be released, and refusing writes is
        // correct -- so the test would pass for the wrong reason.
        let eligible = cluster.status(leader).expect("status").applied_index;
        let _ = cluster.submit_apply_result(leader, eligible, false);
    }

    assert_eq!(
        refused, 0,
        "a lagging follower must not refuse the leader's writes: {accepted} \
         accepted, {refused} refused"
    );
    assert!(
        !cluster.is_busy(),
        "and the buffer must not be left over its limit"
    );
}

#[test]
fn without_pressure_the_log_is_kept_for_the_lagging_follower() {
    // The first tier, and the reason the second one is conditional. With no
    // memory pressure the leader keeps what the follower still needs, so it can
    // be caught up from the log rather than from a snapshot.
    let mut cluster = cluster();
    let leader = cluster.leader_id().expect("leader");
    let behind_at = leader_with_a_follower_left_behind(&mut cluster);

    // Apply everything, so the applied frontier is far ahead of the follower --
    // the second tier would release to there if it fired.
    let eligible = cluster.status(leader).expect("status").applied_index;
    let _ = cluster.submit_apply_result(leader, eligible, false);
    assert!(
        !cluster.should_release_memory(),
        "this test is about the no-pressure tier, so there must be no pressure"
    );

    cluster.release_memory().expect("release memory");

    // Catching up is not the question -- the follower gets there either way,
    // because when the entries are gone it is served a snapshot instead. The
    // question is HOW, and a snapshot attempt is the tell.
    let before = cluster
        .peer_pipeline_status(3)
        .expect("peer 3")
        .snapshot_send_attempts;
    let caught_up = cluster.catch_up_peer(3).expect("the follower catches up");
    assert!(caught_up.caught_up, "either route has to get it there");
    let after = cluster
        .peer_pipeline_status(3)
        .expect("peer 3")
        .snapshot_send_attempts;
    assert_eq!(
        after,
        before,
        "with no memory pressure the follower must be served from the retained \
         log, not a snapshot: it needed index {}",
        behind_at.saturating_add(1)
    );
}
