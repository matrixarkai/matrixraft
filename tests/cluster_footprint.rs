// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! How wide a `RaftCluster` is, and that boxing two of its fields did not change
//! what it serialises to.
//!
//! The width is the thing a shard walks past on its way to the next group it
//! ticks, so it is worth a number that fails when it moves rather than a comment
//! that goes stale. `examples/tick_working_set.rs` is where the consequence is
//! measured; this is the part that can be asserted.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

/// Checked at compile time, so it cannot be skipped by a filter and costs
/// nothing to run. Only on 64-bit targets: the figure is pointer-width.
#[cfg(target_pointer_width = "64")]
const _: () = {
    assert!(
        std::mem::size_of::<RaftCluster>() == 544,
        "RaftCluster changed width. It was 536 bytes until the accumulated vote \
         grants gained the term they were given for, which is what stops a \
         candidate reaching a quorum in one term on grants given for an \
         earlier one -- eight paths cleared the accumulator and two did not, \
         and the suite did not notice when all eight were deleted. 8 bytes, \
         and the alternative was a term beside every grant on the heap, which \
         changes what the struct serialises to. Before that it was 528 bytes \
         until `Config` gained the \
         leader-transfer and peer-offline tick counts, which is what made those \
         two timeouts configurable at all -- a host set them on the options \
         struct and the engine timed both out on the election interval \
         instead. 8 bytes for the pair, not 16: they are `u32`, and `Config` \
         had four bytes of padding after its four bools. Measured A/B with \
         only the width changed, `u64` puts this at 544, so the narrower type \
         is worth 64 KiB at 8,192 groups. Before that it was 520 bytes until \
         `Config` gained the pipeline limits, which is what made the reorder \
         window, the reorder timeout and the in-flight bounds configurable at \
         all -- 8 bytes, because they are boxed; plain they were 56 and took \
         this to 576, +10.8%. `examples/pipeline_limit_width.rs` prints all of \
         it. Before that it was 680 bytes until `leader_transfer` and \
         `snapshot_trigger` were boxed -- both are None in the steady state and \
         were 64 and 112 bytes inline. Every group pays this, and a shard walks \
         all of it between one visit to a group and the next, so a change here is \
         worth noticing. Move the number and say what needs the space."
    );
};

fn voter(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 57_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 56_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn leader() -> RaftCluster {
    let mut cluster = RaftCluster::new(7, Config::default(), vec![voter(1)]).expect("a cluster");
    cluster.start().expect("start");
    cluster
        .campaign(1, false)
        .expect("a lone voter is its own quorum");
    cluster
}

#[test]
fn boxing_a_field_did_not_change_what_it_serialises_to() {
    let mut cluster = leader();
    let meta = cluster.trigger_snapshot().expect("trigger a snapshot");
    assert!(
        cluster.snapshot_trigger_in_progress().is_some(),
        "the trigger must be populated, or this test serialises a None and proves \
         nothing about the boxed value"
    );

    let json = serde_json::to_string(&cluster).expect("serialize");

    // `Box<T>` is transparent to serde, so the field is still the struct itself
    // and not a wrapper around it. A newtype would have shown up here.
    let needle = format!(
        "\"snapshot_trigger\":{{\"meta\":{{\"snapshot_id\":\"{}\"",
        meta.snapshot_id
    );
    assert!(
        json.contains(&needle),
        "the boxed field serialises differently; looked for {needle} in {}",
        &json[..json.len().min(400)]
    );

    let back: RaftCluster = serde_json::from_str(&json).expect("round trip");
    assert_eq!(
        serde_json::to_string(&back).expect("reserialize"),
        json,
        "a cluster that went through JSON and back does not serialise the same"
    );
    assert!(
        back.snapshot_trigger_in_progress().is_some(),
        "the trigger did not survive the round trip"
    );
}

#[test]
fn an_absent_transfer_comes_back_absent() {
    // The other boxed field. Absent, which is the state every group is in
    // almost always, and the one the width argument rests on.
    let cluster = leader();
    assert!(
        cluster.leader_transfer_state().is_none(),
        "nothing has asked for a transfer yet"
    );
    let json = serde_json::to_string(&cluster).expect("serialize");
    let back: RaftCluster = serde_json::from_str(&json).expect("round trip");
    assert!(
        back.leader_transfer_state().is_none(),
        "an absent transfer must come back absent"
    );
    assert_eq!(
        serde_json::to_string(&back).expect("reserialize"),
        json,
        "a cluster with no transfer does not survive JSON unchanged"
    );
}
