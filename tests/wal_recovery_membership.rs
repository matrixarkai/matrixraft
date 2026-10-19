// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Recovering a WAL record restores the membership the record carries.
//!
//! Every WAL record has carried a `membership` since the format existed, and
//! until this contract existed **nothing read it**: `restore_wal_record` took
//! the hard state, the entries, the installed snapshot and the apply fence,
//! and dropped the membership on the floor. A node that had added a voter and
//! removed another came back from its own log believing in the set it was
//! configured with -- a departed peer counted toward quorum again, and a peer
//! that had joined was not there to count.
//!
//! A snapshot install already did this, through
//! `reset_membership_from_snapshot`. The log did not.
//!
//! **An empty membership means "nothing recorded", not "nobody is a member".**
//! A record built by hand, or one from before anything filled the field, must
//! leave the cluster's own membership alone -- the same convention the
//! comparable engine uses, where an empty member list in a flush task means
//! there is no membership to write.

use matrixraft::{Config, Peer, RaftCluster, ReplicaRole, WalRecord};

const GROUP_ID: u64 = 31;

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 14_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 15_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn three_voters() -> Vec<Peer> {
    vec![
        peer(1, ReplicaRole::Voter),
        peer(2, ReplicaRole::Voter),
        peer(3, ReplicaRole::Voter),
    ]
}

fn started(peers: Vec<Peer>) -> RaftCluster {
    let mut cluster =
        RaftCluster::new(GROUP_ID, Config::default(), peers).expect("build the cluster");
    cluster.start().expect("start the cluster");
    cluster
}

/// A cluster that has added voter 4, taken a witness 5, kept a learner 6 and
/// removed voter 2, and the record its leader would write afterwards.
///
/// **Learner 6 is never promoted on purpose.** Without a surviving learner the
/// record's `learners` list is empty, both sides of every assertion are empty,
/// and dropping the learners from the recovered set is invisible -- that plant
/// survived until this scenario kept one.
fn record_after_membership_changes() -> WalRecord {
    let mut cluster = started(three_voters());
    cluster
        .propose(b"before-the-changes".to_vec())
        .expect("write");

    cluster
        .add_learner(peer(4, ReplicaRole::Learner))
        .expect("add a learner");
    cluster.promote_peer(4).expect("promote it to a voter");
    cluster
        .add_witness(peer(5, ReplicaRole::Witness))
        .expect("add a witness");
    cluster
        .add_learner(peer(6, ReplicaRole::Learner))
        .expect("add a learner that stays one");
    cluster.remove_peer(2).expect("remove voter 2");

    let membership = cluster.membership();
    assert!(membership.voters.contains(&4), "{membership:?}");
    assert!(membership.witnesses.contains(&5), "{membership:?}");
    assert!(membership.learners.contains(&6), "{membership:?}");
    assert!(!membership.voters.contains(&2), "{membership:?}");

    let record = cluster.wal_record_for(1).expect("the leader's WAL record");
    assert_eq!(
        record.membership, membership,
        "the record has to carry the membership for this to be about anything"
    );
    record
}

#[test]
fn recovering_a_record_restores_the_membership_it_carries() {
    let record = record_after_membership_changes();
    let recorded = record.membership.clone();

    // A restart: the process comes back configured with the peers it was
    // originally given, which is not what the log says any more.
    let mut restarted = started(three_voters());
    restarted
        .restore_wal_record(record)
        .expect("restore the record");

    let recovered = restarted.membership();
    assert_eq!(
        recovered.voters, recorded.voters,
        "the voters have to come back from the record, not from the configured peers"
    );
    assert_eq!(recovered.witnesses, recorded.witnesses, "and the witnesses");
    assert_eq!(recovered.learners, recorded.learners, "and the learners");
    assert!(
        recovered.learners.contains(&6),
        "a learner has to come back a learner rather than be dropped or promoted \
         by the recovery: {recovered:?}"
    );
    assert!(
        !recovered.voters.contains(&6),
        "and must not come back a voter: {recovered:?}"
    );
    assert!(
        !recovered.voters.contains(&2),
        "a removed peer must not count toward quorum again after a restart: {recovered:?}"
    );
    assert!(
        recovered.voters.contains(&4),
        "and a peer that joined has to be there to count: {recovered:?}"
    );
}

#[test]
fn a_record_with_no_membership_leaves_the_cluster_s_own_alone() {
    let mut record = record_after_membership_changes();
    record.membership.voters.clear();
    record.membership.learners.clear();
    record.membership.witnesses.clear();

    let mut restarted = started(three_voters());
    let before = restarted.membership();
    restarted
        .restore_wal_record(record)
        .expect("restore the record");

    assert_eq!(
        restarted.membership(),
        before,
        "an empty membership records nothing, so it must not empty the cluster"
    );
}

#[test]
fn recovering_a_record_keeps_the_node_the_record_belongs_to() {
    // Node 3 is a member throughout, so this is not about node 3 surviving a
    // removal -- it is that the restore looks up `record.node_id` and would be
    // restoring into a node it had just removed if the reconcile ran first.
    let record = record_after_membership_changes();
    let mut restarted = started(three_voters());
    restarted
        .restore_wal_record(record)
        .expect("restore the record");
    assert!(
        restarted.status(1).is_ok(),
        "the node the record belongs to has to still be there afterwards"
    );
}
