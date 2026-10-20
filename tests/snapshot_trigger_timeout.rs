// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A triggered snapshot gets a snapshot's timescale, not a leader transfer's.
//!
//! `trigger_snapshot` took its timeout from `leader_transfer_timeout_ticks`,
//! which derives `election_timeout_ms / heartbeat_interval_ms`. Measured on the
//! unfixed cluster, at the defaults:
//!
//! ```text
//! the trigger was called timed out after 10 ticks
//! at a 100ms heartbeat that is 1000ms of wall clock
//! ```
//!
//! A snapshot of real state does not finish in a second, so the trigger was
//! reported timed out while it was still running -- and that raises a
//! `snapshot_trigger_timeout` blocker. The two quantities also had no business
//! moving together: tuning a leader transfer changed how long a snapshot was
//! allowed to take.
//!
//! **The setting for this already existed.** `MatrixRaftOptions` has carried
//! `send_snapshot_timeout_ms` all along, defaulted to 60,000 -- the same figure
//! the comparable engine uses, against three ticks for a transfer -- and it
//! reached `snapshot_recycle_options` and an evidence artifact and nothing
//! else. So the setting named for this governed something else, and something
//! else governed this. It is mapped into `Config` now.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

const GROUP_ID: u64 = 103;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 31_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 31_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started(config: Config) -> RaftCluster {
    let mut cluster =
        RaftCluster::new(GROUP_ID, config, vec![peer(1), peer(2), peer(3)]).expect("build");
    cluster.start().expect("start");
    cluster
}

/// Ticks until the trigger is reported timed out, or `None` within the bound.
fn ticks_until_timeout(cluster: &mut RaftCluster, bound: u32) -> Option<u32> {
    for tick in 1..=bound {
        cluster.tick_snapshot_trigger();
        if cluster.snapshot_trigger_status().timed_out {
            return Some(tick);
        }
    }
    None
}

#[test]
fn a_triggered_snapshot_gets_sixty_seconds_of_ticks_by_default() {
    let config = Config::default();
    let heartbeat = config.heartbeat_interval_ms;
    let mut cluster = started(config);
    cluster.propose(b"one".to_vec()).expect("an entry");
    cluster.trigger_snapshot().expect("trigger");

    let ticks = ticks_until_timeout(&mut cluster, 5_000).expect("it times out eventually");
    assert_eq!(
        ticks as u64 * heartbeat,
        60_000,
        "the derived timeout is a snapshot timescale -- {ticks} ticks at {heartbeat}ms"
    );
}

#[test]
fn the_transfer_timeout_no_longer_decides_how_long_a_snapshot_may_take() {
    // The regression this prevents: the two were the same number, so tuning
    // one moved the other.
    let mut cluster = started(Config {
        leader_transfer_timeout_ticks: 3,
        ..Config::default()
    });
    cluster.propose(b"one".to_vec()).expect("an entry");
    cluster.trigger_snapshot().expect("trigger");

    assert!(
        ticks_until_timeout(&mut cluster, 3).is_none(),
        "a three-tick transfer timeout must not abandon a snapshot after three ticks"
    );
}

#[test]
fn an_explicit_snapshot_timeout_is_honoured() {
    let mut cluster = started(Config {
        snapshot_trigger_timeout_ticks: 7,
        ..Config::default()
    });
    cluster.propose(b"one".to_vec()).expect("an entry");
    cluster.trigger_snapshot().expect("trigger");

    assert_eq!(
        ticks_until_timeout(&mut cluster, 50),
        Some(7),
        "a configured value is used as given rather than derived"
    );
}

#[test]
fn the_derived_timeout_follows_the_heartbeat_interval() {
    // Ticks, not milliseconds, so a shorter heartbeat has to mean more of them
    // for the same wall-clock allowance.
    let mut cluster = started(Config {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 1_000,
        ..Config::default()
    });
    cluster.propose(b"one".to_vec()).expect("an entry");
    cluster.trigger_snapshot().expect("trigger");

    assert_eq!(
        ticks_until_timeout(&mut cluster, 20_000),
        Some(6_000),
        "60,000ms over a 10ms tick is 6,000 ticks"
    );
}

#[test]
fn a_snapshot_that_completes_in_time_is_not_reported_timed_out() {
    // The positive control: a timeout that fired immediately would satisfy
    // nothing above except by accident, and one that never fired would satisfy
    // the transfer test on its own.
    let mut cluster = started(Config {
        snapshot_trigger_timeout_ticks: 50,
        ..Config::default()
    });
    cluster.propose(b"one".to_vec()).expect("an entry");
    let meta = cluster.trigger_snapshot().expect("trigger");

    for _ in 0..10 {
        cluster.tick_snapshot_trigger();
    }
    assert!(
        !cluster.snapshot_trigger_status().timed_out,
        "ten ticks of a fifty-tick allowance is not a timeout"
    );
    cluster
        .complete_snapshot_trigger(&meta.snapshot_id)
        .expect("the trigger completes");
    assert!(
        !cluster.snapshot_trigger_status().timed_out,
        "and a completed trigger is not timed out either"
    );
}

#[test]
fn a_snapshot_timeout_shorter_than_a_tick_still_leaves_one_tick() {
    // The options carry milliseconds and `Config` carries ticks, so unlike the
    // transfer and offline settings there is a conversion here -- and dividing
    // 500ms by a 1,000ms tick is zero. A zero timeout reports every trigger
    // timed out on its first tick, which abandons work that was fine.
    // The conversion itself is asserted next to the options it converts, in
    // `matrixraft_builder_compat`. What this pins is that the floor it lands on
    // is a real allowance rather than a formality: one tick means the trigger
    // survives being created and is timed out on the tick after.
    let mut cluster = started(Config {
        snapshot_trigger_timeout_ticks: 1,
        ..Config::default()
    });
    cluster.propose(b"one".to_vec()).expect("an entry");
    cluster.trigger_snapshot().expect("trigger");
    assert!(
        !cluster.snapshot_trigger_status().timed_out,
        "a trigger is not born timed out"
    );
    assert_eq!(ticks_until_timeout(&mut cluster, 5), Some(1));
}
