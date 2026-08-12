// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    rustraft_validate_snapshot_floor_log_matching, rustraft_validate_snapshot_install,
    rustraft_wal_checksum, rustraft_wal_checksum_valid, JointConsensusMembership, LocalRaftWal,
    RaftCluster, RaftHardState, RaftMembership, RaftSnapshot, RaftSnapshotInstallState,
    RaftWalRecord, RustRaftApplySnapshotFence, RustRaftLogEntry, RustRaftLogId, RustRaftPeer,
    RustRaftReplicaRole, RustRaftSnapshotChunk, RustRaftSnapshotMeta,
};

fn peer(node_id: u64, role: RustRaftReplicaRole) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 8_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn wal_record(index: u64) -> RaftWalRecord {
    let mut record = RaftWalRecord {
        group_id: 9,
        node_id: 1,
        hard_state: RaftHardState {
            current_term: 2,
            voted_for: Some(1),
            committed: Some(RustRaftLogId { term: 2, index }),
        },
        membership: RaftMembership {
            group_id: 9,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries: vec![RustRaftLogEntry {
            log_id: RustRaftLogId { term: 2, index },
            payload: vec![index as u8],
            is_command: true,
        }],
        installed_snapshot: None,
        apply_snapshot_fence: RustRaftApplySnapshotFence {
            applied_index: index,
            commit_index: index,
            installed_snapshot_index: 0,
            first_retained_log_index: 0,
        },
        checksum: String::new(),
    };
    record.checksum = rustraft_wal_checksum(&record);
    record
}

#[test]
fn membership_helpers_cover_learners_witnesses_joint_quorum_and_catchup() {
    let mut membership = RaftMembership {
        group_id: 9,
        voters: vec![1, 2, 3],
        learners: Vec::new(),
        witnesses: Vec::new(),
        epoch: 1,
    };

    assert_eq!(membership.quorum_size(), 2);
    assert!(membership.quorum_reached([1, 2]));

    membership.add_learner(4).expect("add learner");
    let lagging = membership.catchup_report(4, 8, 10);
    assert!(!lagging.promotable);
    assert_eq!(lagging.reason, "learner_lagging");

    let caught_up = membership.catchup_report(4, 10, 10);
    assert!(caught_up.promotable);
    membership.promote_learner(4).expect("promote learner");
    membership.add_witness(5).expect("add witness");
    assert!(membership.quorum_reached([1, 4, 5]));
    assert_eq!(membership.quorum_size_with_witness_policy(true), 3);
    assert!(!membership.quorum_reached_with_witness_policy([1, 5], true));
    assert!(membership.quorum_reached_with_witness_policy([1, 2, 4, 5], true));
    membership.remove_peer(2).expect("remove peer");
    assert!(!membership.voters.contains(&2));

    let joint = JointConsensusMembership {
        old_voters: vec![1, 2, 3],
        new_voters: vec![1, 3, 4],
    };
    assert!(joint.quorum_reached([1, 3]));
    assert!(!joint.quorum_reached([1, 4]));
}

#[test]
fn cluster_membership_methods_add_promote_remove_and_report_catchup() {
    let mut cluster = RaftCluster::new(
        9,
        Default::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"a".to_vec()).expect("write");

    cluster
        .add_learner(peer(4, RustRaftReplicaRole::Voter))
        .expect("add learner");
    let report = cluster.catchup_report(4).expect("catchup report");
    assert!(report.promotable);
    cluster.promote_peer(4).expect("promote");
    cluster
        .add_witness(peer(5, RustRaftReplicaRole::Voter))
        .expect("witness");
    cluster.remove_peer(2).expect("remove peer");

    let membership = cluster.membership();
    assert!(membership.voters.contains(&4));
    assert!(membership.witnesses.contains(&5));
    assert!(!membership.voters.contains(&2));
}

#[test]
fn cluster_catches_up_late_learner_with_snapshot_before_membership_config_entry() {
    let mut cluster = RaftCluster::new(
        17,
        Default::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");
    cluster
        .propose(b"temporalstore-write-1".to_vec())
        .expect("write 1");
    cluster
        .propose(b"temporalstore-write-2".to_vec())
        .expect("write 2");
    assert_eq!(cluster.status(1).expect("leader status").commit_index, 3);

    cluster
        .add_learner(peer(3, RustRaftReplicaRole::Learner))
        .expect("late learner");
    assert!(
        cluster
            .catchup_report(3)
            .expect("immediate add-member catch-up")
            .promotable
    );
    cluster
        .set_node_healthy(3, false)
        .expect("isolate learner after add");
    cluster
        .propose(b"temporalstore-write-3".to_vec())
        .expect("write 3");
    cluster.compact_logs_through(4);
    let lagging = cluster.catchup_report(3).expect("lagging report");
    assert!(!lagging.promotable);
    cluster
        .set_node_healthy(3, true)
        .expect("restore learner for snapshot");

    let snapshot = RaftSnapshot {
        group_id: 17,
        meta: RustRaftSnapshotMeta {
            snapshot_id: "temporalstore-catchup-through-four".to_string(),
            last_log_id: RustRaftLogId { term: 1, index: 4 },
            membership: vec![1, 2, 3],
            members: Vec::new(),
        },
        payload: b"opaque-temporalstore-state".to_vec(),
    };
    cluster
        .install_snapshot_with_tail_to(
            3,
            snapshot,
            RustRaftApplySnapshotFence {
                applied_index: 4,
                commit_index: 4,
                installed_snapshot_index: 4,
                first_retained_log_index: 5,
            },
            Vec::new(),
        )
        .expect("snapshot catch-up");
    assert!(cluster.catchup_report(3).expect("caught up").promotable);

    cluster.promote_peer(3).expect("promote");
    let membership_entry = cluster
        .propose(b"temporalstore-membership:promote:3".to_vec())
        .expect("membership config entry");
    assert_eq!(membership_entry.index, 5);

    let read = cluster
        .read_index(matrixraft::RustRaftReadIndexRequest {
            group_id: 17,
            requester_id: 1,
            min_commit_index: 5,
            allow_lease_read: true,
        })
        .expect("read-index");
    assert!(read.safe, "{read:?}");
    assert_eq!(read.read_index, 5);
    assert_eq!(cluster.membership().voters, vec![1, 2, 3]);
}

#[test]
fn local_raft_wal_segments_recovers_and_truncates_corrupt_tail() {
    let mut wal = LocalRaftWal::new(2).expect("wal");
    wal.append(wal_record(1)).expect("append");
    wal.append(wal_record(2)).expect("append");
    wal.append(wal_record(3)).expect("append");
    assert_eq!(wal.segments().len(), 2);
    assert!(rustraft_wal_checksum_valid(&wal.records()[2]));

    wal.corrupt_tail_for_test().expect("corrupt tail");
    let report = wal.recover().expect("recover");
    assert!(report.truncated_corrupt_tail);
    assert_eq!(report.removed_records, 1);
    assert_eq!(
        report
            .recovered
            .expect("latest valid")
            .hard_state
            .committed
            .expect("commit")
            .index,
        2
    );
    assert_eq!(wal.records().len(), 2);
}

#[test]
fn chunked_snapshot_install_validates_offsets_and_snapshot_fence() {
    let meta = RustRaftSnapshotMeta {
        snapshot_id: "snapshot-10".to_string(),
        last_log_id: RustRaftLogId { term: 4, index: 10 },
        membership: vec![1, 2, 3],
        members: Vec::new(),
    };
    let mut install = RaftSnapshotInstallState::new(meta.clone());
    install
        .install_chunk(RustRaftSnapshotChunk {
            meta: meta.clone(),
            offset: 0,
            data: b"hello ".to_vec(),
            done: false,
        })
        .expect("first chunk");
    install
        .install_chunk(RustRaftSnapshotChunk {
            meta: meta.clone(),
            offset: 6,
            data: b"snapshot".to_vec(),
            done: true,
        })
        .expect("second chunk");

    let snapshot = install.finish(9).expect("finish snapshot");
    assert_eq!(snapshot.payload, b"hello snapshot");
    let fence = RustRaftApplySnapshotFence {
        applied_index: 10,
        commit_index: 10,
        installed_snapshot_index: 10,
        first_retained_log_index: 11,
    };
    rustraft_validate_snapshot_install(&snapshot, &fence).expect("valid install");
    rustraft_validate_snapshot_floor_log_matching(
        &meta,
        11,
        Some(&RustRaftLogId { term: 4, index: 10 }),
    )
    .expect("floor matches");
    assert!(rustraft_validate_snapshot_floor_log_matching(
        &meta,
        11,
        Some(&RustRaftLogId { term: 3, index: 10 }),
    )
    .is_err());
}
