// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    MatrixRaftConfState, MatrixRaftEntry, MatrixRaftEntryType, MatrixRaftGroupStorage,
    MatrixRaftHardState, MatrixRaftInitialState, MatrixRaftLogRange, MatrixRaftLogSegmentEventKind,
    MatrixRaftLogStorage, MatrixRaftLogStorageOptions, MatrixRaftLogStoragePrepareOptions,
    MatrixRaftLogStorageWriteTask, MatrixRaftMemberId, MatrixRaftMemoryGroupStorage,
    MatrixRaftPropose, RustRaftPeer, RustRaftReplicaRole,
};

fn member(node_id: u64) -> MatrixRaftMemberId {
    MatrixRaftMemberId {
        id: node_id,
        raft_addr: format!("127.0.0.1:{}", 50_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 51_000 + node_id),
        is_from_options: true,
        conf_state: MatrixRaftConfState::Voter,
        auto_promote: false,
    }
}

fn peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 50_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 51_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn entry(index: u64, term: u64, data: &[u8]) -> MatrixRaftEntry {
    MatrixRaftEntry {
        entry_type: MatrixRaftEntryType::Normal,
        term,
        index,
        propose: Some(MatrixRaftPropose {
            request_id: Some(index),
            data: data.to_vec(),
            context: Vec::new(),
            is_command: true,
        }),
        config_change: None,
        memberships: Vec::new(),
        request_id: index,
        bytes_size: data.len() as u64,
    }
}

#[test]
fn matrixraft_storage_contract_covers_group_prepare_open_log_write_and_truncate() {
    let mut group = MatrixRaftMemoryGroupStorage::new(42).with_overflow_limit(1);
    let local_id = MatrixRaftMemberId::from(&peer(1));
    let members = vec![member(1), member(2), member(3)];

    group.begin();
    group
        .prepare(
            "/raft/group-42",
            MatrixRaftLogStoragePrepareOptions {
                peer_id: 1,
                max_segment_bytes: 64 * 1024 * 1024,
                initial_state: MatrixRaftInitialState { index: 0, term: 0 },
                role: RustRaftReplicaRole::Voter,
                local_id: local_id.clone(),
                members: members.clone(),
            },
        )
        .expect("prepare log");
    group
        .prepare(
            "/raft/group-42",
            MatrixRaftLogStoragePrepareOptions {
                peer_id: 2,
                max_segment_bytes: 64 * 1024 * 1024,
                initial_state: MatrixRaftInitialState { index: 0, term: 0 },
                role: RustRaftReplicaRole::Voter,
                local_id: member(2),
                members: members.clone(),
            },
        )
        .expect("prepare second log");
    assert!(group.overflow());
    group.commit().expect("commit group batch");
    assert!(group.exists(1));
    assert!(group.exists(2));
    assert_eq!(group.group_id(), 42);

    let mut log = group
        .open(
            "/raft/group-42",
            MatrixRaftLogStorageOptions {
                peer_id: 1,
                max_segment_bytes: 64 * 1024 * 1024,
                applied_index: 0,
                local_id,
                sync: true,
            },
        )
        .expect("open log");
    assert!(log.is_segment_based());
    assert_eq!(log.range(), MatrixRaftLogRange::new(1, 1));
    assert_eq!(log.members(), members);

    log.write(MatrixRaftLogStorageWriteTask {
        sync_meta: true,
        committed_index: 2,
        size_hint: 0,
        hard_state: Some(MatrixRaftHardState {
            current_term: 7,
            voted_for: Some(1),
        }),
        members: vec![member(1), member(2)],
        entries: vec![entry(1, 7, b"a"), entry(2, 7, b"bb"), entry(3, 8, b"ccc")],
    })
    .expect("write task");

    assert_eq!(log.first_index(), 1);
    assert_eq!(log.last_index(), 3);
    assert_eq!(log.range(), MatrixRaftLogRange::new(1, 4));
    assert_eq!(log.term(2).expect("term"), 7);
    assert_eq!(log.term(3).expect("term"), 8);
    assert_eq!(log.current_term(), 7);
    assert_eq!(log.voted_for(), Some(1));
    assert_eq!(log.committed_index(), 2);
    assert_eq!(log.stabled_committed_index(), 2);
    assert_eq!(log.size_until(2), 3);
    assert_eq!(log.file_indexes(), vec![1]);

    let loaded = log
        .load_entries(MatrixRaftLogRange::new(2, 4))
        .expect("load entries");
    assert_eq!(
        loaded.iter().map(|entry| entry.index).collect::<Vec<_>>(),
        vec![2, 3]
    );

    log.truncate_until(MatrixRaftInitialState { index: 2, term: 7 })
        .expect("truncate until");
    assert_eq!(
        log.initial_state(),
        MatrixRaftInitialState { index: 2, term: 7 }
    );
    assert_eq!(log.first_index(), 3);
    assert_eq!(log.file_indexes(), vec![3]);
    assert_eq!(log.term(2).expect("initial term"), 7);

    log.write(MatrixRaftLogStorageWriteTask {
        committed_index: 4,
        entries: vec![entry(3, 9, b"replacement"), entry(4, 9, b"d")],
        ..MatrixRaftLogStorageWriteTask::default()
    })
    .expect("rewrite tail");
    assert_eq!(log.term(3).expect("rewritten term"), 9);
    assert_eq!(log.last_index(), 4);

    log.truncate_from_index(4).expect("truncate from");
    assert_eq!(log.last_index(), 3);
    log.release_hint(3);
    assert_eq!(log.last_release_hint(), 3);

    group.delete(1).expect("delete prepared log");
    assert!(!group.exists(1));
    assert!(group.exists(2));
}

#[test]
fn matrixraft_storage_compaction_report_covers_released_and_trimmed_segments() {
    let mut group = MatrixRaftMemoryGroupStorage::new(44);
    let local_id = MatrixRaftMemberId::from(&peer(1));
    group
        .prepare(
            "/raft/group-44",
            MatrixRaftLogStoragePrepareOptions {
                peer_id: 1,
                max_segment_bytes: 4,
                initial_state: MatrixRaftInitialState { index: 0, term: 0 },
                role: RustRaftReplicaRole::Voter,
                local_id: local_id.clone(),
                members: vec![member(1), member(2)],
            },
        )
        .expect("prepare log");

    let mut log = group
        .open(
            "/raft/group-44",
            MatrixRaftLogStorageOptions {
                peer_id: 1,
                max_segment_bytes: 4,
                applied_index: 0,
                local_id,
                sync: true,
            },
        )
        .expect("open log");

    log.write(MatrixRaftLogStorageWriteTask {
        entries: vec![
            entry(1, 1, b"aa"),
            entry(2, 1, b"bb"),
            entry(3, 1, b"c"),
            entry(4, 1, b"d"),
            entry(5, 1, b"e"),
        ],
        ..MatrixRaftLogStorageWriteTask::default()
    })
    .expect("write compactable segments");
    assert_eq!(log.file_indexes(), vec![1, 3]);

    let report = log
        .compact_until(MatrixRaftInitialState { index: 3, term: 1 })
        .expect("compact through first entry in second segment");
    assert_eq!(
        report.initial_state,
        MatrixRaftInitialState { index: 3, term: 1 }
    );
    assert_eq!(report.first_retained_index, 4);
    assert_eq!(report.last_index, 5);
    assert_eq!(report.released_segments.len(), 1);
    assert_eq!(report.released_segments[0].segment_id, 1);
    assert_eq!(report.truncated_segments.len(), 1);
    assert_eq!(report.truncated_segments[0].segment_id, 4);
    assert_eq!(report.retained_segments.len(), 1);
    assert_eq!(report.retained_segments[0].first_index, 4);
    assert_eq!(log.file_indexes(), vec![4]);
    assert_eq!(
        log.load_entries(MatrixRaftLogRange::new(4, 6))
            .expect("load retained entries")
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>(),
        vec![4, 5]
    );
}

#[test]
fn matrixraft_storage_contract_exposes_segment_switch_release_and_truncate_events() {
    let mut group = MatrixRaftMemoryGroupStorage::new(43);
    let local_id = MatrixRaftMemberId::from(&peer(1));
    let members = vec![member(1), member(2)];
    group
        .prepare(
            "/raft/group-43",
            MatrixRaftLogStoragePrepareOptions {
                peer_id: 1,
                max_segment_bytes: 4,
                initial_state: MatrixRaftInitialState { index: 0, term: 0 },
                role: RustRaftReplicaRole::Voter,
                local_id: local_id.clone(),
                members,
            },
        )
        .expect("prepare log");

    let mut log = group
        .open(
            "/raft/group-43",
            MatrixRaftLogStorageOptions {
                peer_id: 1,
                max_segment_bytes: 4,
                applied_index: 0,
                local_id,
                sync: true,
            },
        )
        .expect("open log");

    log.write(MatrixRaftLogStorageWriteTask {
        entries: vec![entry(1, 1, b"aa"), entry(2, 1, b"bb"), entry(3, 1, b"ccc")],
        ..MatrixRaftLogStorageWriteTask::default()
    })
    .expect("write entries across segment boundary");

    assert_eq!(log.file_indexes(), vec![1, 3]);
    assert_eq!(log.segments().len(), 2);
    assert!(log.segments()[0].sealed);
    assert_eq!(log.segments()[0].first_index, 1);
    assert_eq!(log.segments()[0].last_index, 2);
    assert_eq!(log.segments()[0].bytes, 4);
    assert_eq!(log.segments()[1].first_index, 3);
    assert_eq!(log.segments()[1].last_index, 3);
    assert_eq!(
        log.segment_events()
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![
            MatrixRaftLogSegmentEventKind::Open,
            MatrixRaftLogSegmentEventKind::Switch
        ]
    );

    let released = log.release_segments_until(2);
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].kind, MatrixRaftLogSegmentEventKind::Release);
    assert_eq!(released[0].segment_id, 1);
    assert_eq!(log.file_indexes(), vec![3]);

    let switched = log.switch_segment(4).expect("manual switch segment");
    assert_eq!(switched.kind, MatrixRaftLogSegmentEventKind::Switch);
    assert_eq!(switched.previous_segment_id, Some(3));
    assert_eq!(log.file_indexes(), vec![3, 4]);

    log.truncate_from_index(4)
        .expect("truncate empty tail segment");
    assert_eq!(log.file_indexes(), vec![3]);
    assert!(log.drain_segment_events().iter().any(|event| event.kind
        == MatrixRaftLogSegmentEventKind::Truncate
        && event.segment_id == 4));
}
