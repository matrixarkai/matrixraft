// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Who can arm the membership-change fence, and who cannot.
//!
//! `RaftCluster` tracks a membership change through four fields -- pending,
//! saving, stabled, and the set of live change indexes -- and
//! `membership_change_fence_active` is what defers a learner's auto-promotion
//! while one is in flight. The mechanism works; the first assertion here is the
//! control that proves it.
//!
//! What this pins is the **reach** of it. The fence is armed from exactly two
//! places: `propose_with_options` when `ProposeOptions::is_membership_change` is
//! set, and the `begin_*` pair called directly. Neither is reachable through the
//! public `MatrixRaftProposeOptions`, which has no such field -- its `From`
//! conversion hardcodes `false` -- and no shipped layer calls the `begin_*` pair.
//! So on every path a server actually takes, the fence is never armed and the
//! learner-promotion deferral never fires. It is a mechanism for an embedder
//! driving `RaftCluster` itself.
//!
//! That is worth a test rather than a comment because the day somebody adds the
//! field to the public options type, the deferral starts firing -- and whoever
//! does it should find out here rather than from a learner that stopped
//! promoting.

use matrixraft::{
    Config, MatrixRaftProposeOptions, Peer, ProposeOptions, RaftCluster, ReplicaRole,
};

fn voter(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 58_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 59_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started_cluster() -> RaftCluster {
    let mut cluster =
        RaftCluster::new(31, Config::default(), vec![voter(1)]).expect("a single-voter cluster");
    cluster.start().expect("start");
    cluster
}

#[test]
fn the_fence_is_armed_by_the_internal_option_and_not_by_the_public_one() {
    // The control. Without this, everything below would also pass on a build
    // where the fence had simply stopped working, and the test would be claiming
    // "unreachable" about something that was merely broken.
    let mut cluster = started_cluster();
    assert_eq!(
        cluster.pending_membership_change_index(),
        None,
        "nothing is pending before anything is proposed"
    );
    let armed = cluster
        .propose_with_options(
            b"add-a-voter".to_vec(),
            ProposeOptions {
                is_membership_change: true,
                ..Default::default()
            },
        )
        .expect("a membership change proposed through the internal option");
    assert_eq!(
        cluster.pending_membership_change_index(),
        Some(armed.index),
        "the internal option arms the fence, so the mechanism itself works"
    );

    // And the boundary. The public options type cannot express a membership
    // change at all: the conversion sets the flag false whatever the caller asks
    // for, because there is nothing on the public type to ask with.
    let converted: ProposeOptions = MatrixRaftProposeOptions {
        with_term: None,
        is_command: true,
    }
    .into();
    assert!(
        !converted.is_membership_change,
        "the public propose options converted to a membership change, so the \
         reachability this test describes has changed -- if that is deliberate, \
         check that the learner-promotion deferral at the two \
         `membership_change_fence_active` call sites is still what you want, \
         because it starts firing now"
    );

    // Proposing with it leaves the fence exactly where it was.
    let mut fresh = started_cluster();
    let ordinary = fresh
        .propose_with_options(b"ordinary".to_vec(), converted)
        .expect("an ordinary proposal");
    assert!(ordinary.index > 0, "the proposal itself succeeded");
    assert_eq!(
        fresh.pending_membership_change_index(),
        None,
        "a proposal through the public options must not arm the fence"
    );
}
