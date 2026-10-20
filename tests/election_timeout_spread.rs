// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Election timeouts vary by node and by term.
//!
//! Raft requires an election timeout to be chosen from a random interval and
//! re-chosen each round. Nothing here varied it at all: every node sharing a
//! config reached `election_timeout_ms` on the same tick -- they are all reset
//! by the same heartbeat -- so they campaign together, each votes for itself,
//! no one wins a majority, the term moves, and the same split repeats.
//!
//! The in-process `RaftCluster` cannot show this, which is why the whole suite
//! passed both before and after the fix: one object drives every node's
//! election path. It bites a real deployment, where each `NodeRuntime` is its
//! own process with its own clock and the same config file.
//!
//! **What is asserted here, and what deliberately is not.** The value comes
//! from hashing `(node_id, term)`, so it is reproducible -- and that is the
//! point, since a split is broken by nodes differing from *each other*, not by
//! a sequence nothing can replay. But `DefaultHasher`'s exact output is
//! unspecified across Rust versions, so no test here pins a specific number.
//! What is pinned is the interval, the determinism, and that the spread is wide
//! enough to break a tie -- all three stable under a toolchain change.
//! (`DefaultHasher::new()` has fixed keys, unlike `RandomState`; `driver.rs`
//! and `heartbeat_merge.rs` already depend on that to assign shards.)
//!
//! **One arm is deliberately not pinned, and it is the comparison itself.**
//! Planting the tick back to the un-randomised base leaves every test here
//! green: the assignment and the status still carry the randomised number, so
//! nothing observable changes. The only true observable of "the comparison used
//! this value" is when the node campaigns -- and `election_elapsed_ms` advances
//! by a heartbeat interval per timeout-arm wake rather than per wall
//! millisecond, so a wall-clock assertion is either flaky or vacuous. A first
//! attempt at one WAS vacuous: it asserted no election tick by 28ms, which
//! holds whichever value the tick compares against, and the plant stayed green
//! under it. It was removed rather than kept as false confidence.

use matrixraft::{
    matrixraft_randomised_election_timeout_ms as timeout, Config, NodeOptions, NodeRuntime, Peer,
    ReplicaRole,
};
use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const BASE: u64 = 1_000;

#[test]
fn the_timeout_stays_inside_the_interval_the_paper_asks_for() {
    // [base, 2 * base). Below base would campaign early and risk unseating a
    // healthy leader; at or above 2 * base the worst case doubles for nothing.
    for node_id in 1..200u64 {
        for term in 0..20u64 {
            let got = timeout(BASE, node_id, term);
            assert!(
                (BASE..2 * BASE).contains(&got),
                "node {node_id} term {term} got {got}, outside [{BASE}, {})",
                2 * BASE
            );
        }
    }
}

#[test]
fn nodes_sharing_a_config_do_not_all_wait_the_same_time() {
    // The property that breaks a split vote. Asserted as a spread rather than
    // as any pair differing, because two hashes may legitimately collide --
    // what must not happen is everyone colliding.
    let spread: BTreeSet<u64> = (1..=64u64)
        .map(|node_id| timeout(BASE, node_id, 7))
        .collect();
    assert!(
        spread.len() >= 16,
        "64 nodes produced only {} distinct timeouts, which is not a spread",
        spread.len()
    );
}

#[test]
fn a_node_waits_a_different_time_in_a_different_term() {
    // What makes a repeated split terminate: the term moves on every failed
    // round, so the next round deals a different hand.
    let spread: BTreeSet<u64> = (0..64u64).map(|term| timeout(BASE, 3, term)).collect();
    assert!(
        spread.len() >= 16,
        "node 3 produced only {} distinct timeouts over 64 terms",
        spread.len()
    );
}

#[test]
fn the_same_node_in_the_same_term_waits_the_same_time() {
    // Reproducibility is a feature, not an accident: a node must not re-roll
    // its timeout on every tick, or it would campaign at an unpredictable
    // moment and no test could pin any of this.
    for node_id in [1u64, 2, 17, 4_000] {
        for term in [0u64, 1, 99] {
            let first = timeout(BASE, node_id, term);
            for _ in 0..8 {
                assert_eq!(
                    timeout(BASE, node_id, term),
                    first,
                    "node {node_id} term {term} changed its mind"
                );
            }
        }
    }
}

#[test]
fn a_base_of_zero_still_yields_a_positive_timeout() {
    // `Config::validate` refuses a zero election timeout, so this is defence
    // rather than a reachable case -- but a zero here would make the hash
    // modulo divide by zero.
    let got = timeout(0, 5, 5);
    assert!(
        got >= 1,
        "a zero base must not divide by zero or wait forever"
    );
}

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 33_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 33_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn runtime_for(node_id: u64) -> NodeRuntime {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    let base = std::env::temp_dir().join(format!(
        "matrixraft-election-spread-{node_id}-{}-{nonce}",
        std::process::id()
    ));
    let mut runtime = NodeRuntime::create(NodeOptions {
        group_id: 107,
        node_id,
        raft_addr: format!("127.0.0.1:{}", 33_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 33_500 + node_id),
        wal_dir: base.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config: Config {
            heartbeat_interval_ms: 5,
            election_timeout_ms: 20,
            leader_lease_ms: 5,
            ..Config::default()
        },
        peers: vec![peer(1), peer(2), peer(3)],
    })
    .expect("create the runtime");
    runtime.start().expect("start it");
    runtime
}

/// The effective timeout a runtime reports once its election tick has run.
fn reported_effective(runtime: &NodeRuntime) -> u64 {
    for _ in 0..400 {
        let status = runtime.status().expect("status");
        if status.timer_status.effective_election_timeout_ms > 0 {
            return status.timer_status.effective_election_timeout_ms;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("the election tick never ran, so there is nothing to report");
}

#[test]
fn the_tick_waits_out_the_randomised_timeout_and_reports_which() {
    // The test that was missing when this was first written: every property
    // above is about the FUNCTION, and reverting the tick to the un-randomised
    // base left all of them green. The status reports the same stored value the
    // tick compares against -- computed once, in runtime state -- so asserting
    // on it is asserting on the comparison.
    let mut one = runtime_for(1);
    let mut two = runtime_for(2);

    let first = reported_effective(&one);
    let second = reported_effective(&two);

    for (node_id, got) in [(1u64, first), (2, second)] {
        assert!(
            (20..40).contains(&got),
            "node {node_id} reported {got}, outside [20, 40) for a base of 20"
        );
        assert_ne!(
            got, 20,
            "node {node_id} reported exactly the base, which is what the              un-randomised tick did"
        );
    }

    one.shutdown().expect("shutdown");
    two.shutdown().expect("shutdown");
}
