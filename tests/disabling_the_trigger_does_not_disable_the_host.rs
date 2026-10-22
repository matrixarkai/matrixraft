// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Turning off automatic snapshot triggering stops the **engine** asking for a
//! snapshot. It must not stop the **host** taking one.
//!
//! `can_trigger_snapshot` exists so a host can own the snapshot schedule
//! itself, and `Config`'s own documentation says as much: with it off,
//! `snapshot_threshold_entries` decides nothing and a snapshot happens only
//! when a host asks for one. The flag therefore has a reach, and the reach is
//! the whole point of it:
//!
//! - **in scope** -- `snapshot_due`, which is what the node runtime consults
//!   before asking the state machine for a snapshot;
//! - **out of scope** -- `trigger_snapshot`, which is the host asking.
//!
//! Nothing pinned the second half. Making the flag refuse the host's own
//! `trigger_snapshot` as well left all 177 test targets green -- and a host
//! that cannot snapshot cannot reclaim its log either, so the log would grow
//! without bound on exactly the configuration chosen to control it. That is
//! the failure this file exists to catch, and it is why the plant here is a
//! *widening* of the rule rather than a removal: the danger is not the flag
//! failing to fire, it is the flag firing somewhere it was never meant to.

use matrixraft::{AppendEntriesResponse, Config, Peer, RaftCluster, ReplicaRole, SnapshotState};

/// What a host gets out of taking its own snapshot. Compared between the two
/// settings of the flag, because "the flag changes nothing here" is the rule.
#[derive(Debug, PartialEq, Eq)]
struct HostSnapshotOutcome {
    snapshot_index: u64,
    entries_released: u64,
    first_retained_index: u64,
    installed_snapshot_index: u64,
}

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

/// A started three-voter group whose threshold is low enough that six
/// proposals pass it, so `entries_exceeded` is true in both arms and the flag
/// is the only difference between them.
fn cluster_with_triggering(can_trigger_snapshot: bool) -> RaftCluster {
    let config = Config {
        can_trigger_snapshot,
        snapshot_threshold_entries: 2,
        ..Config::default()
    };
    let mut cluster = RaftCluster::new(
        7,
        config,
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
}

/// Six proposals, acknowledged by the whole group, so the retained log is
/// comfortably past the threshold and every node has the entries.
fn fill_past_the_threshold(cluster: &mut RaftCluster) -> u64 {
    let leader = cluster.leader_id().expect("a leader");
    let term = cluster.status(leader).expect("leader status").term;
    for _ in 0..6 {
        cluster.propose(b"payload".to_vec()).expect("propose");
    }
    let last = cluster
        .status(leader)
        .expect("leader status")
        .last_log_index;
    for peer_id in [2u64, 3] {
        cluster.catch_up_peer(peer_id).expect("replicate to peer");
        cluster
            .handle_append_entries_response(
                leader,
                peer_id,
                AppendEntriesResponse {
                    term,
                    success: true,
                    match_index: last,
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
    assert!(
        cluster.snapshot_due().entries_exceeded,
        "the premise: the retained log is past the threshold in both arms"
    );
    leader
}

/// The host takes a snapshot and reclaims the log with it, reporting what it
/// got. This is the sequence a host owning its own schedule performs.
fn host_takes_a_snapshot(can_trigger_snapshot: bool) -> HostSnapshotOutcome {
    let mut cluster = cluster_with_triggering(can_trigger_snapshot);
    let leader = fill_past_the_threshold(&mut cluster);

    let meta = cluster
        .trigger_snapshot()
        .expect("a host may always take a snapshot");
    cluster
        .handle_snapshot_ready(&meta.snapshot_id, true)
        .expect("the snapshot finishes");
    let entries_released = cluster.compact_logs_through(meta.last_log_id.index);
    let retained = cluster
        .retained_log_range(leader)
        .expect("the leader's retained range");

    HostSnapshotOutcome {
        snapshot_index: meta.last_log_id.index,
        entries_released,
        first_retained_index: retained.first_log_index,
        installed_snapshot_index: retained.installed_snapshot_index,
    }
}

#[test]
fn a_disabled_trigger_stops_the_engine_asking() {
    let mut cluster = cluster_with_triggering(false);
    fill_past_the_threshold(&mut cluster);

    let report = cluster.snapshot_due();
    assert!(
        report.entries_exceeded,
        "the threshold is passed, so the only reason not to snapshot is the flag"
    );
    assert!(!report.can_trigger, "the flag is off");
    assert!(!report.due, "so the engine must not ask for a snapshot");
    assert_eq!(
        report.reason, "snapshot_trigger_disabled",
        "and the reason must name the flag, not the threshold"
    );
    assert!(
        !cluster.snapshot_is_due(),
        "the short answer must agree with the report"
    );
}

#[test]
fn an_enabled_trigger_does_ask_in_the_same_state() {
    // The control for the test above. Without it, that test would also pass
    // against an engine that never found a snapshot due for any reason.
    let mut cluster = cluster_with_triggering(true);
    fill_past_the_threshold(&mut cluster);

    let report = cluster.snapshot_due();
    assert!(
        report.entries_exceeded,
        "the same state as the disabled arm"
    );
    assert!(report.can_trigger);
    assert!(
        report.due,
        "with triggering enabled the same state is due: {report:?}"
    );
    assert_eq!(report.reason, "retained_entries_reached_threshold");
}

#[test]
fn a_host_can_still_take_a_snapshot_when_the_trigger_is_disabled() {
    let outcome = host_takes_a_snapshot(false);

    assert_eq!(
        outcome.snapshot_index, 7,
        "the host's snapshot reaches the committed log, as it would either way"
    );
    assert!(
        outcome.entries_released > 0,
        "and the log it covers is reclaimable: {outcome:?}"
    );
    assert_eq!(
        outcome.installed_snapshot_index, outcome.snapshot_index,
        "the reclaimed log is replaced by the snapshot that covers it"
    );
    assert_eq!(
        outcome.first_retained_index, 0,
        "nothing is left behind in the log once the snapshot covers all of it"
    );
}

#[test]
fn the_flag_changes_nothing_about_a_host_snapshot() {
    // The rule stated as one assertion: the flag's reach ends at the engine's
    // own decision. Everything a host gets from taking and applying its own
    // snapshot -- the index, the entries freed, where the log then starts and
    // the snapshot floor it leaves -- is identical either way.
    assert_eq!(
        host_takes_a_snapshot(false),
        host_takes_a_snapshot(true),
        "turning off automatic triggering must not change what a host's own \
         snapshot does"
    );
}
