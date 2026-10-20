// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A lease confirmation is dated from the probe it answers, not from its own
//! arrival.
//!
//! A follower confirming a leader lease promises not to campaign for
//! `leader_lease_ms` from the moment it **saw the probe**. Dating the
//! confirmation from when the reply arrives gives the leader `leader_lease_ms`
//! from the **reply** -- one round trip longer than was promised -- and in that
//! window the leader serves a lease read against a promise that has lapsed
//! while the follower is free to campaign.
//!
//! Measured before this was fixed: a 120ms round trip against a 500ms lease
//! held the lease for 620ms after the probe. No network is needed to show it,
//! because `tick_leader_lease` lets a test inject the round trip and returns
//! true on the tick that expires the lease.
//!
//! The second test is the one that caught the first attempt at this. Reading
//! the lease clock instead of zeroing it is not enough on its own, because
//! `expire_leader_lease` used to say "expired" by pinning that same clock to
//! the full duration -- so a confirmation dated from it after an explicit
//! expiry was born already elapsed, and a legitimate recovery stopped working.

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

const LEASE_MS: u64 = 500;
const ROUND_TRIP_MS: u64 = 120;

fn leader_with_a_lease() -> RaftCluster {
    let mut cluster = RaftCluster::new(
        7,
        Config {
            election_timeout_ms: 1_000,
            heartbeat_interval_ms: 100,
            leader_lease_ms: LEASE_MS,
            ..Config::default()
        },
        vec![peer(1), peer(2), peer(3)],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster.campaign(1, false).ok();
    assert_eq!(cluster.leader_id(), Some(1), "node 1 has to be the leader");
    cluster
}

#[test]
fn the_round_trip_is_charged_against_the_lease() {
    let mut cluster = leader_with_a_lease();

    // The probe for this epoch went out when the leader took office. The round
    // trip elapses before either confirmation comes back.
    assert!(
        !cluster.tick_leader_lease(ROUND_TRIP_MS),
        "the lease must not expire during the round trip itself"
    );

    assert!(
        cluster.receive_leader_lease_confirmation(2, 1),
        "the confirmation has to be accepted, or the rest measures nothing"
    );
    assert!(cluster.receive_leader_lease_confirmation(3, 1));

    // Age by the remainder of what the follower promised. The promise runs
    // from the probe, so it ends exactly here.
    assert!(
        cluster.tick_leader_lease(LEASE_MS - ROUND_TRIP_MS),
        "the lease has to expire {LEASE_MS}ms after the probe, not after the reply"
    );
}

#[test]
fn an_expiry_and_a_fresh_confirmation_re_establish_the_lease() {
    // `expire_leader_lease` says "over" by bumping the epoch and dropping the
    // confirmations. It used to pin the lease clock to the full duration
    // instead, which is the same field a confirmation is now dated from -- so a
    // confirmation arriving afterwards was born elapsed and this recovery
    // stopped working. That is what the first attempt at the fix broke, and it
    // is why expiry no longer touches the clock.
    //
    // Validity is read through `tick_leader_lease`, which returns true on the
    // tick that ends a lease that WAS held. That makes a true return positive
    // proof of validity, which no getter offers.
    let mut cluster = leader_with_a_lease();
    assert!(cluster.receive_leader_lease_confirmation(2, 5));

    cluster.expire_leader_lease();

    // A confirmation older than one already seen is still refused across the
    // expiry: the per-node monotonic counter survives it, and that is why the
    // expiry does not clear `leader_lease_confirmation_epochs`.
    assert!(
        !cluster.receive_leader_lease_confirmation(2, 4),
        "an older confirmation epoch must still be refused after an expiry"
    );

    // A newer one is a new promise, and it re-establishes the lease.
    assert!(cluster.receive_leader_lease_confirmation(2, 6));
    assert!(
        cluster.tick_leader_lease(LEASE_MS),
        "a fresh confirmation after an expiry has to hold the lease again, so \
         ageing it by the full duration has to end it"
    );
}

#[test]
fn a_confirmation_after_an_expiry_is_dated_from_the_last_probe() {
    // An expiry is not a probe: nothing was sent, so the epoch's clock keeps
    // running. A confirmation arriving afterwards is still answering the last
    // real probe, so it carries the REMAINDER of its promise rather than a
    // fresh full duration.
    //
    // An earlier attempt at this fix zeroed the clock on expiry, which
    // pretended a probe had gone out and handed back the whole duration. This
    // pins the version that does not.
    let mut cluster = leader_with_a_lease();
    cluster.tick_leader_lease(ROUND_TRIP_MS);
    assert!(cluster.receive_leader_lease_confirmation(2, 1));
    cluster.expire_leader_lease();

    assert!(cluster.receive_leader_lease_confirmation(2, 2));
    assert!(
        !cluster.tick_leader_lease(LEASE_MS - ROUND_TRIP_MS - 1),
        "the promise must not lapse before its own duration is up"
    );
    assert!(
        cluster.tick_leader_lease(1),
        "and it ends {LEASE_MS}ms after the probe, not {LEASE_MS}ms after the expiry"
    );
}

#[test]
fn a_single_voter_lease_expires_too() {
    // A lone voter is its own quorum, so dropping the confirmations cannot
    // expire its lease -- it counts itself. Expiry therefore needs a state of
    // its own, and this is the group that proves it: with expiry resting on the
    // confirmations alone, the lease survives its own expiry here while every
    // multi-voter test still passes.
    let mut cluster = RaftCluster::new(
        7,
        Config {
            election_timeout_ms: 1_000,
            heartbeat_interval_ms: 100,
            leader_lease_ms: LEASE_MS,
            ..Config::default()
        },
        vec![peer(1)],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
        .campaign(1, false)
        .expect("a lone voter is its own quorum");

    // `campaign` resets the epoch and leaves the lease invalid until something
    // recomputes it, and `tick_leader_lease` reads the previous validity before
    // recomputing -- so the lease has to be renewed before it can be seen to be
    // held at all.
    cluster.renew_leader_lease_from_live_quorum();

    // Held: ageing it by the full duration ends it, which only a held lease
    // can report.
    assert!(
        cluster.tick_leader_lease(LEASE_MS),
        "a lone leader holds its own lease"
    );

    // Re-establish it, then expire it explicitly.
    cluster.renew_leader_lease_from_live_quorum();
    cluster.expire_leader_lease();

    // The tick matters. The expiry sets `leader_lease_valid` false directly, so
    // reading it straight away says nothing about whether the expiry STICKS --
    // and sticking is the whole point, because the quorum is recomputed from
    // scratch on every tick and a lone leader counts itself. Without a durable
    // expiry the lease is back after this one tick.
    assert!(
        !cluster.tick_leader_lease(1),
        "the tick that recomputes the quorum must not revive an expired lease"
    );
    assert!(
        !cluster.tick_leader_lease(LEASE_MS),
        "and nothing is left to expire -- if this reports an expiry, the lease \
         was being held again after expire_leader_lease"
    );
}

#[test]
fn a_renewal_round_lifts_an_expiry() {
    // An expiry has to be liftable, or a group that expired once could never
    // hold a lease again. A renewal round is a new probe, so it lifts it.
    let mut cluster = RaftCluster::new(
        7,
        Config {
            election_timeout_ms: 1_000,
            heartbeat_interval_ms: 100,
            leader_lease_ms: LEASE_MS,
            ..Config::default()
        },
        vec![peer(1)],
    )
    .expect("valid cluster");
    cluster.start().expect("cluster starts");
    cluster
        .campaign(1, false)
        .expect("a lone voter is its own quorum");
    cluster.expire_leader_lease();
    assert!(
        !cluster.tick_leader_lease(1),
        "expired, and it stays expired across a recompute"
    );

    cluster.renew_leader_lease_from_live_quorum();
    assert!(
        cluster.tick_leader_lease(LEASE_MS),
        "the renewal round is a new probe and has to lift the expiry, so the \
         lease is held again and ageing it by the full duration ends it"
    );
}

#[test]
fn a_shorter_follower_promise_is_also_dated_from_the_probe() {
    // The group clock and the per-confirmation clock advance together, so a
    // confirmation dated from its arrival rather than from the probe is
    // invisible while both run for the same duration -- the group clock expires
    // the lease at the right moment either way. It shows up when the follower
    // advertises a SHORTER promise than the lease: dated from the probe the
    // remaining promise is `short - probe_age`, dated from arrival it is the
    // whole `short`.
    //
    // This is the only test that distinguishes the two, which a plant proved by
    // surviving everything else.
    const SHORT_MS: u64 = 200;
    let mut cluster = leader_with_a_lease();
    cluster.tick_leader_lease(ROUND_TRIP_MS);

    assert!(
        cluster.receive_leader_lease_confirmation_with_duration(2, 1, SHORT_MS),
        "the shorter confirmation has to be accepted"
    );

    // Dated from the probe, 120ms of the 200ms promise is already spent, so it
    // has 80ms left. Dated from arrival it would have the whole 200ms.
    assert!(
        !cluster.tick_leader_lease(SHORT_MS - ROUND_TRIP_MS - 1),
        "the promise must not lapse before its own duration is up"
    );
    assert!(
        cluster.tick_leader_lease(1),
        "a {SHORT_MS}ms promise given {ROUND_TRIP_MS}ms before it arrived has \
         {}ms left, not {SHORT_MS}ms",
        SHORT_MS - ROUND_TRIP_MS
    );
}
