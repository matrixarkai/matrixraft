// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The two WAL record builders agree about where the group has got to.
//!
//! There are two: `wal_record_for`, which has **no callers in `src/`** and is
//! the accessor a host uses to persist a group itself, and
//! `wal_record_for_coverage`, which the node runtime's four append sites use.
//! Both fill the same `apply_snapshot_fence` and the same hard state, from the
//! same node, with the same code written twice.
//!
//! Nothing checked that they still say the same thing. That matters because a
//! host driving persistence through the first has to recover the progress the
//! runtime would have recovered through the second -- and because editing one
//! of two identical copies is the mistake this crate has already made twice,
//! once across four arithmetic sites and once across five.
//!
//! **What agrees and what does not.** The fence and the hard state must match:
//! they describe where the group is. The entries need not, and with a coverage
//! argument they deliberately do not -- that is the whole point of the second
//! builder, which emits a delta against what a segment already holds. The last
//! test here is that distinction, so a future reader does not "fix" the two
//! into producing identical records.

use matrixraft::{
    ApplySnapshotFence, Config, LogId, Peer, RaftCluster, RaftSnapshot, ReplicaRole,
    SnapshotMetadata,
};

const GROUP_ID: u64 = 115;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 39_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 39_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started_with_entries(count: u64) -> (RaftCluster, u64) {
    let mut cluster =
        RaftCluster::new(GROUP_ID, Config::default(), vec![peer(1), peer(2), peer(3)])
            .expect("build the cluster");
    cluster.start().expect("start the cluster");
    for i in 0..count {
        cluster
            .propose(format!("entry-{i}").into_bytes())
            .expect("a write");
    }
    let leader = cluster.leader_id().expect("a leader");
    (cluster, leader)
}

#[test]
fn both_builders_report_the_same_fence() {
    let (cluster, leader) = started_with_entries(3);
    let whole = cluster
        .wal_record_for(leader)
        .expect("the whole-log record");
    let coverage = cluster
        .wal_record_for_coverage(leader, None)
        .expect("the coverage record");

    assert_eq!(
        whole.apply_snapshot_fence, coverage.apply_snapshot_fence,
        "a host persisting through `wal_record_for` has to recover the progress \
         the runtime recovers through `wal_record_for_coverage`"
    );
    assert!(
        whole.apply_snapshot_fence.applied_index > 0,
        "the fence has to say something for the comparison to mean anything: {:?}",
        whole.apply_snapshot_fence
    );
}

#[test]
fn both_builders_report_the_same_hard_state_and_membership() {
    let (cluster, leader) = started_with_entries(3);
    let whole = cluster.wal_record_for(leader).expect("whole");
    let coverage = cluster
        .wal_record_for_coverage(leader, None)
        .expect("coverage");

    assert_eq!(
        whole.hard_state, coverage.hard_state,
        "the term and the vote are what a restart reads to decide elections"
    );
    assert_eq!(
        whole.membership, coverage.membership,
        "and recovery reconciles the group's members from this field"
    );
    assert!(
        whole.hard_state.committed.is_some(),
        "there has to be a committed index here: {:?}",
        whole.hard_state
    );
}

#[test]
fn the_coverage_builder_emits_a_delta_and_that_is_the_difference() {
    // The distinction worth keeping: with nothing covered the two records carry
    // the same entries, and with a segment already covering a prefix the second
    // carries only what follows. Anyone "fixing" the two to agree completely
    // would undo the reason the second one exists.
    let (cluster, leader) = started_with_entries(4);
    let whole = cluster.wal_record_for(leader).expect("whole");
    assert!(
        !whole.entries_are_delta,
        "the whole-log record is never a delta"
    );

    let first = whole.entries.first().expect("entries").log_id.index;
    let second = whole.entries.get(1).expect("a second entry").log_id.clone();
    let delta = cluster
        .wal_record_for_coverage(leader, Some((first, second.index, second.term)))
        .expect("a delta record");

    assert!(
        delta.entries.len() < whole.entries.len(),
        "covering a prefix has to shorten what the record carries: {} against {}",
        delta.entries.len(),
        whole.entries.len()
    );
    assert_eq!(
        delta.apply_snapshot_fence, whole.apply_snapshot_fence,
        "and shortening the entries must not change where the group has got to"
    );
}

#[test]
fn they_agree_about_the_retained_floor_once_a_snapshot_is_installed() {
    // Without a snapshot, `snapshot_index` is zero and both arms of the floor
    // compute the same thing -- so planting a divergence in one builder's floor
    // went unnoticed until this case existed. With one installed, the
    // `snapshot_index > 0` arm runs and the floor is the snapshot's own index
    // plus one.
    let (mut cluster, leader) = started_with_entries(4);
    // Above the committed index, because `install_snapshot_to` refuses a
    // snapshot that is not newer than what the group has already committed --
    // the first version of this asked for index 2 against a committed 5 and
    // panicked, which made every planted arm look red for the wrong reason.
    let committed = cluster
        .wal_record_for(leader)
        .expect("a record to read the committed index from")
        .hard_state
        .committed
        .expect("a committed index")
        .index;
    let snapshot_index = committed + 1;
    cluster
        .install_snapshot_to(
            leader,
            RaftSnapshot {
                group_id: GROUP_ID,
                meta: SnapshotMetadata {
                    snapshot_id: "installed".to_string(),
                    last_log_id: LogId {
                        term: 1,
                        index: snapshot_index,
                    },
                    membership: vec![1, 2, 3],
                    members: Vec::new(),
                    payload_bytes: 0,
                },
                payload: b"payload".to_vec(),
            },
            ApplySnapshotFence {
                applied_index: snapshot_index,
                commit_index: snapshot_index,
                installed_snapshot_index: snapshot_index,
                first_retained_log_index: snapshot_index + 1,
            },
        )
        .expect("install a snapshot on the leader");

    let whole = cluster.wal_record_for(leader).expect("whole");
    let coverage = cluster
        .wal_record_for_coverage(leader, None)
        .expect("coverage");

    assert_eq!(
        whole.apply_snapshot_fence.installed_snapshot_index, snapshot_index,
        "the snapshot has to be on the record, or the arm this test is about          does not run: {:?}",
        whole.apply_snapshot_fence
    );
    assert_eq!(
        whole.apply_snapshot_fence.first_retained_log_index,
        snapshot_index + 1,
        "the floor continues the snapshot"
    );
    assert_eq!(
        whole.apply_snapshot_fence, coverage.apply_snapshot_fence,
        "and both builders compute it the same way"
    );
}
