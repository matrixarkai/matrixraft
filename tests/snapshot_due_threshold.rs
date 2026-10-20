// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The entry threshold decides that a snapshot is due.
//!
//! `snapshot_threshold_entries` was declared, defaulted to 10,000, rejected
//! when zero, and **read nowhere**: there was no log-size snapshot trigger at
//! all, so a snapshot started only when a host asked for one and
//! `tick_snapshot_trigger` only aged out a trigger already in flight.
//!
//! `snapshot_due` is the decision it was missing. It is reported rather than
//! taken, because the cluster has no thread to take it on and a host wanting a
//! different policy needs the numbers rather than the verdict. The node
//! runtime's heartbeat tick is the default policy.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 85_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 86_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// A started three-voter cluster whose threshold is `threshold` entries.
fn cluster_with_threshold(threshold: u64) -> RaftCluster {
    let config = Config {
        snapshot_threshold_entries: threshold,
        ..Config::default()
    };
    let mut cluster =
        RaftCluster::new(99, config, (1..=3u64).map(peer).collect()).expect("cluster");
    cluster.start().expect("start");
    cluster
}

#[test]
fn a_log_below_the_threshold_is_not_due() {
    let mut cluster = cluster_with_threshold(50);
    for _ in 0..5 {
        cluster.propose(b"write".to_vec()).expect("propose");
    }
    let due = cluster.snapshot_due();
    assert!(!due.due, "{due:?}");
    assert!(!due.entries_exceeded);
    assert_eq!(due.threshold_entries, 50);
    assert_eq!(due.reason, "retained_entries_below_threshold");
    assert!(
        due.retained_entries > 0,
        "the leader is holding something, or this test is measuring an empty log: {due:?}"
    );
}

#[test]
fn a_log_at_the_threshold_is_due() {
    // The threshold is small enough that a handful of proposals reaches it,
    // and the assertion is against the reported count rather than the number
    // of proposals, because `start` appends a no-op of its own.
    let mut cluster = cluster_with_threshold(4);
    for _ in 0..6 {
        cluster.propose(b"write".to_vec()).expect("propose");
    }
    let due = cluster.snapshot_due();
    assert!(due.retained_entries >= due.threshold_entries, "{due:?}");
    assert!(due.entries_exceeded, "{due:?}");
    assert!(due.due, "{due:?}");
    assert_eq!(due.reason, "retained_entries_reached_threshold");
    assert_eq!(due.leader_id, cluster.leader_id());
}

#[test]
fn the_threshold_magnitude_decides_it() {
    // What made this knob inert: a grep found a read, but the only read was
    // against zero, so 1 and 10,000 behaved alike. They do not now.
    let mut low = cluster_with_threshold(4);
    let mut high = cluster_with_threshold(10_000);
    for _ in 0..6 {
        low.propose(b"write".to_vec()).expect("propose");
        high.propose(b"write".to_vec()).expect("propose");
    }
    assert!(low.snapshot_due().due);
    assert!(!high.snapshot_due().due);
    assert_eq!(
        low.snapshot_due().retained_entries,
        high.snapshot_due().retained_entries,
        "same log, different threshold -- the threshold is the whole difference"
    );
}

#[test]
fn a_disabled_trigger_is_never_due() {
    let config = Config {
        snapshot_threshold_entries: 1,
        can_trigger_snapshot: false,
        ..Config::default()
    };
    let mut cluster =
        RaftCluster::new(99, config, (1..=3u64).map(peer).collect()).expect("cluster");
    cluster.start().expect("start");
    for _ in 0..6 {
        cluster.propose(b"write".to_vec()).expect("propose");
    }
    let due = cluster.snapshot_due();
    assert!(
        due.entries_exceeded,
        "the log is past the threshold: {due:?}"
    );
    assert!(!due.can_trigger);
    assert!(!due.due, "but the gate is shut: {due:?}");
    assert_eq!(due.reason, "snapshot_trigger_disabled");
}

#[test]
fn a_trigger_already_in_flight_is_not_due_again() {
    // `trigger_snapshot` is idempotent while one is in flight, so a decision
    // that stayed due would have the runtime counting duplicate requests
    // rather than starting anything.
    let mut cluster = cluster_with_threshold(4);
    for _ in 0..6 {
        cluster.propose(b"write".to_vec()).expect("propose");
    }
    assert!(cluster.snapshot_due().due);

    cluster.trigger_snapshot().expect("trigger");
    let due = cluster.snapshot_due();
    assert!(due.trigger_in_flight, "{due:?}");
    assert!(!due.due, "{due:?}");
    assert_eq!(due.reason, "snapshot_trigger_in_flight");
    assert!(
        due.entries_exceeded,
        "the log is still long -- it is the in-flight trigger that makes it not due"
    );
}

#[test]
fn reaching_the_threshold_exactly_is_enough() {
    // The boundary, which the other tests step over: six proposals against a
    // threshold of four is comfortably past it, so `>=` and `>` behave alike
    // and a plant swapping them survived.
    //
    // The exact count is measured rather than assumed, because `start`
    // appends a no-op of its own and hardcoding "six proposals means seven
    // entries" would pin an unrelated implementation detail.
    let mut probe = cluster_with_threshold(u64::MAX);
    for _ in 0..6 {
        probe.propose(b"write".to_vec()).expect("propose");
    }
    let exactly = probe.snapshot_due().retained_entries;
    assert!(
        exactly > 1,
        "the leader has to be holding something: {exactly}"
    );

    let mut at_it = cluster_with_threshold(exactly);
    for _ in 0..6 {
        at_it.propose(b"write".to_vec()).expect("propose");
    }
    let due = at_it.snapshot_due();
    assert_eq!(
        due.retained_entries, exactly,
        "the two clusters have to match"
    );
    assert_eq!(due.threshold_entries, exactly);
    assert!(
        due.due,
        "having reached the threshold is having reached it: {due:?}"
    );

    // And one entry short of it is not.
    let mut short = cluster_with_threshold(exactly + 1);
    for _ in 0..6 {
        short.propose(b"write".to_vec()).expect("propose");
    }
    assert!(!short.snapshot_due().due, "{:?}", short.snapshot_due());
}

#[test]
fn it_is_the_leaders_log_that_counts_not_the_shortest() {
    // A follower that is handed nothing has a shorter log than the leader, so
    // "how many entries are retained" has more than one answer. The decision
    // is about the leader's, because the leader is the one whose log a
    // snapshot would let it release -- a plant reading the minimum across the
    // group survived while every node's log was the same length.
    let mut cluster = cluster_with_threshold(u64::MAX);
    cluster.propose(b"before".to_vec()).expect("propose");
    cluster
        .set_node_healthy(3, false)
        .expect("one follower stops receiving");
    for _ in 0..6 {
        cluster.propose(b"while-behind".to_vec()).expect("propose");
    }

    let leader_entries = cluster.snapshot_due().retained_entries;
    let lagging = cluster
        .catchup_report(3)
        .map(|report| report.learner_match_index)
        .unwrap_or(0);
    assert!(
        lagging < leader_entries,
        "the follower has to be behind for this to be about anything: {lagging} \
         against {leader_entries}"
    );

    // A threshold above the follower's count and at or below the leader's:
    // only reading the leader's log makes this due.
    let threshold = leader_entries;
    let mut measured = cluster_with_threshold(threshold);
    measured.propose(b"before".to_vec()).expect("propose");
    measured
        .set_node_healthy(3, false)
        .expect("one follower stops receiving");
    for _ in 0..6 {
        measured.propose(b"while-behind".to_vec()).expect("propose");
    }
    let due = measured.snapshot_due();
    assert_eq!(due.retained_entries, leader_entries, "{due:?}");
    assert!(
        due.due,
        "the leader is at the threshold even though a follower is not: {due:?}"
    );
}

#[test]
fn the_serde_default_and_the_struct_default_agree() {
    // Two defaults for one field: `Config::default()` sets it literally and
    // serde falls back to `default_can_trigger_snapshot` for a document that
    // omits it. They can drift, and a plant flipping the serde one survived
    // because nothing compared them.
    let without_the_key = r#"{
        "election_timeout_ms": 1000,
        "heartbeat_interval_ms": 100,
        "leader_lease_ms": 500,
        "max_payload_bytes": 8388608,
        "snapshot_threshold_entries": 10000,
        "max_segment_bytes": 67108864,
        "min_keep_segment_num": 2,
        "enable_pre_vote": true,
        "enable_lease_read": true
    }"#;
    let loaded: Config =
        serde_json::from_str(without_the_key).expect("a document without the key loads");
    assert_eq!(
        loaded.can_trigger_snapshot,
        Config::default().can_trigger_snapshot,
        "a document that omits the gate has to get the same answer as Config::default()"
    );
    assert!(
        loaded.can_trigger_snapshot,
        "and that answer is on, so an existing deployment keeps taking snapshots"
    );
}

#[test]
fn the_cheap_verdict_agrees_with_the_report_in_every_state() {
    // Two entry points for one decision, because the report allocates and a
    // tick must not: its `reason` is a `String`, so asking it on every
    // heartbeat cost one allocation per tick per group -- 81,920 a second at
    // 8,192 groups on a 100ms interval. `tests/tick_window.rs` caught that at
    // 1.000 allocations a tick against a ceiling of 0.05, which is what the
    // allocation-free `snapshot_is_due` is for.
    //
    // Two entry points can drift, so this walks the states the other tests
    // build and asserts they do not.
    let mut below = cluster_with_threshold(50);
    let mut at = cluster_with_threshold(4);
    let mut disabled = {
        let config = Config {
            snapshot_threshold_entries: 1,
            can_trigger_snapshot: false,
            ..Config::default()
        };
        let mut cluster =
            RaftCluster::new(99, config, (1..=3u64).map(peer).collect()).expect("cluster");
        cluster.start().expect("start");
        cluster
    };
    let no_leader = {
        let config = Config {
            snapshot_threshold_entries: 1,
            ..Config::default()
        };
        RaftCluster::new(99, config, (1..=3u64).map(peer).collect()).expect("cluster")
    };

    for cluster in [&mut below, &mut at, &mut disabled] {
        for _ in 0..6 {
            cluster.propose(b"write".to_vec()).expect("propose");
        }
    }
    // And one with a trigger already in flight.
    at.trigger_snapshot().expect("trigger");
    let mut in_flight = cluster_with_threshold(4);
    for _ in 0..6 {
        in_flight.propose(b"write".to_vec()).expect("propose");
    }

    let states: [(&str, &RaftCluster); 5] = [
        ("below the threshold", &below),
        ("a trigger in flight", &at),
        ("the gate shut", &disabled),
        ("no leader", &no_leader),
        ("at the threshold", &in_flight),
    ];
    let mut verdicts = Vec::new();
    for (label, cluster) in states {
        let report = cluster.snapshot_due();
        assert_eq!(
            report.due,
            cluster.snapshot_is_due(),
            "the report and the cheap verdict disagree with {label}: {report:?}"
        );
        verdicts.push(report.due);
    }
    assert!(
        verdicts.contains(&true) && verdicts.contains(&false),
        "the states have to cover both answers, or this agrees about nothing: {verdicts:?}"
    );
}

#[test]
fn a_cluster_with_no_leader_is_not_due() {
    let config = Config {
        snapshot_threshold_entries: 1,
        ..Config::default()
    };
    // Not started, so there is no leader and no log.
    let cluster = RaftCluster::new(99, config, (1..=3u64).map(peer).collect()).expect("cluster");
    let due = cluster.snapshot_due();
    assert_eq!(due.leader_id, None);
    assert_eq!(due.retained_entries, 0);
    assert!(!due.due, "{due:?}");
    assert_eq!(due.reason, "no_leader");
}
