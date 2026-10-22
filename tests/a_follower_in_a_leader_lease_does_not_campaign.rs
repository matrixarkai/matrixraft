// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A follower that believes a leader holds a lease does not campaign.
//!
//! This is the promise the whole lease-read scheme rests on. A leader may
//! answer a read from its own state, without a round trip, only because every
//! follower has promised not to campaign for the lease duration. If a follower
//! campaigns anyway and wins, a new leader exists while the old one is still
//! inside its lease and still answering reads -- two leaders serving, which is
//! the one thing the lease was bought to prevent.
//!
//! `campaign` refuses it:
//!
//! ```text
//! if self.is_follower_lease_valid() && !forced {
//!     return Err(.."follower is still in leader lease"..);
//! }
//! ```
//!
//! Planting the two clauses separately is a study in how uneven coverage can
//! be within one line: dropping `!forced` is caught by **386** tests, because
//! forced campaigns run everywhere. **Dropping the lease check itself was
//! caught by nothing** across 1,035 tests.
//!
//! So the mechanism was audited and correct, and simply not defended. The two
//! are different things, and only the second survives a refactor.
//!
//! `forced` bypassing the lease is deliberate, not a hole: a leadership
//! transfer sends `TimeoutNow` precisely to make a follower campaign at once,
//! and the outgoing leader has stopped serving by then. The second test pins
//! that, so the first cannot be "fixed" by refusing every campaign.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole};

const GROUP: u64 = 90;
const FOLLOWER: u64 = 2;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 44_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 45_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// Lease reads on, because `is_follower_lease_valid` is `enable_lease_read &&
/// follower_lease_valid` -- the feature switch is half the condition.
fn leased_cluster() -> RaftCluster {
    let config = Config {
        enable_lease_read: true,
        ..Default::default()
    };
    let mut cluster =
        RaftCluster::new(GROUP, config, vec![peer(1), peer(2), peer(3)]).expect("build");
    cluster.start().expect("start");
    cluster
}

#[test]
fn a_follower_inside_a_leader_lease_is_refused_a_campaign() {
    let mut cluster = leased_cluster();
    assert!(
        cluster.receive_follower_lease_item(1),
        "the premise: the follower has taken a lease"
    );
    assert!(cluster.is_follower_lease_valid());

    let error = cluster
        .campaign(FOLLOWER, false)
        .expect_err("a follower inside a lease must not campaign");
    assert!(
        error
            .to_string()
            .contains("follower is still in leader lease"),
        "and it says why: {error}"
    );
}

#[test]
fn a_forced_campaign_goes_through_the_lease_on_purpose() {
    // Leadership transfer: `TimeoutNow` exists to make a follower campaign at
    // once, and the outgoing leader has stopped serving by then. This also
    // stops the test above being satisfiable by refusing every campaign.
    let mut cluster = leased_cluster();
    cluster.receive_follower_lease_item(1);
    assert!(cluster.is_follower_lease_valid());

    cluster
        .campaign(FOLLOWER, true)
        .expect("a forced campaign is allowed through the lease");
}

#[test]
fn a_follower_with_no_lease_may_campaign() {
    // The other control: same cluster, same unforced campaign, no lease. So the
    // refusal above is attributable to the lease and nothing else.
    let mut cluster = leased_cluster();
    assert!(
        !cluster.is_follower_lease_valid(),
        "the premise: no lease has been taken"
    );

    cluster
        .campaign(FOLLOWER, false)
        .expect("without a lease there is nothing to wait for");
}
