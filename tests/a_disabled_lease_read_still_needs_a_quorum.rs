// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! With lease reads disabled, a read index is refused for want of a quorum.
//!
//! `read_index` excuses a missing live quorum when the read can be served off
//! the leader lease instead:
//!
//! ```text
//! let lease_read = request.allow_lease_read
//!     && self.config.enable_lease_read
//!     && self.leader_lease_valid;
//! if !self.has_live_quorum() && !lease_read { ...refuse... }
//! ```
//!
//! Planting each clause of that conjunction away showed the `leader_lease_valid`
//! clause held by 18 tests and `enable_lease_read` held by **none** across
//! 1,019. That clause is not redundant: `set_leader_lease_valid` is public and
//! does not consult the feature switch, so a host can leave the flag true while
//! lease reads are configured off.
//!
//! What the clause decides is **which guard refuses**, and that is observable.
//! MEASURED, flag set and quorum gone either way:
//!
//! | `enable_lease_read` | reason |
//! |---|---|
//! | `false` | `no_live_quorum` -- refused by the quorum guard |
//! | `true` | the quorum guard is skipped; the refusal comes from further down |
//!
//! Both answered `safe: false` in that state, because the applied-index fence
//! also had something to say. That is why these tests assert the *reason*
//! rather than the verdict: the reason is the thing this clause controls, and a
//! state where the fence passes but the quorum is gone would turn the skipped
//! guard into a read served without one.

use matrixraft::{Config, Peer, RaftCluster, ReadIndexRequest, ReplicaRole};

const GROUP: u64 = 87;
const LEADER: u64 = 1;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 38_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 39_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// A leader whose lease flag is set by hand and whose followers are gone, so
/// the only question left is whether the quorum guard applies.
fn leader_without_a_quorum(enable_lease_read: bool) -> RaftCluster {
    let config = Config {
        enable_lease_read,
        ..Default::default()
    };
    let mut cluster =
        RaftCluster::new(GROUP, config, vec![peer(1), peer(2), peer(3)]).expect("build");
    cluster.start().expect("start");

    // Public, and it does not consult `enable_lease_read` -- which is the whole
    // reason the conjunction has to.
    cluster.set_leader_lease_valid(true);
    cluster.set_node_healthy(2, false).expect("isolate 2");
    cluster.set_node_healthy(3, false).expect("isolate 3");
    cluster
}

fn read(cluster: &mut RaftCluster) -> (bool, String) {
    let response = cluster
        .read_index(ReadIndexRequest {
            group_id: GROUP,
            requester_id: LEADER,
            min_commit_index: 0,
            allow_lease_read: true,
        })
        .expect("the read index is answered");
    (response.safe, response.reason)
}

#[test]
fn with_lease_reads_off_a_missing_quorum_is_what_refuses_the_read() {
    let mut cluster = leader_without_a_quorum(false);
    let (safe, reason) = read(&mut cluster);

    assert!(!safe, "a read without a live quorum is not safe");
    assert_eq!(
        reason, "no_live_quorum",
        "with lease reads disabled the quorum guard is what must refuse, \
         whatever the lease flag says"
    );
}

#[test]
fn with_lease_reads_on_the_quorum_guard_is_skipped() {
    // The discriminating control. It shows the reason above is caused by the
    // feature switch and not by the request shape: the same request, the same
    // lease flag and the same dead followers reach a different guard when the
    // feature is on. Without this, the test above would still pass if the
    // quorum guard refused unconditionally.
    let mut cluster = leader_without_a_quorum(true);
    let (safe, reason) = read(&mut cluster);

    assert!(!safe, "the applied-index fence still refuses this one");
    assert_ne!(
        reason, "no_live_quorum",
        "with lease reads enabled and the lease flag set, the quorum guard is \
         excused and the refusal has to come from somewhere else"
    );
}
