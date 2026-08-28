// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    snapshot::{matrixraft_validate_snapshot_tail_catchup, RaftSnapshot, RustRaftSnapshotMeta},
    storage::{matrixraft_validate_storage_apply_fence, RustRaftStorageApplyFence},
    wal::{
        matrixraft_durability_parity_report, matrixraft_validate_hard_state_persistence,
        matrixraft_wal_checksum, LocalRaftWal, RaftWalRecord, RustRaftHardState,
    },
    RaftMembership, RustRaftApplySnapshotFence, RustRaftLogEntry, RustRaftLogId,
};

fn snapshot_meta() -> RustRaftSnapshotMeta {
    RustRaftSnapshotMeta {
        snapshot_id: "durability-parity-10".to_string(),
        last_log_id: RustRaftLogId { term: 4, index: 10 },
        membership: vec![1, 2, 3],
        members: Vec::new(),
    }
}

fn tail_entry(index: u64) -> RustRaftLogEntry {
    RustRaftLogEntry {
        log_id: RustRaftLogId { term: 4, index },
        payload: format!("tail-{index}").into_bytes(),
        is_command: true,
    }
}

fn wal_record(committed_index: u64) -> RaftWalRecord {
    let meta = snapshot_meta();
    let mut record = RaftWalRecord {
        entries_are_delta: false,
        group_id: 404,
        node_id: 1,
        hard_state: RustRaftHardState {
            current_term: 4,
            voted_for: Some(1),
            committed: Some(RustRaftLogId {
                term: 4,
                index: committed_index,
            }),
        },
        membership: RaftMembership {
            group_id: 404,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 4,
        },
        entries: vec![tail_entry(11)],
        installed_snapshot: Some(meta),
        apply_snapshot_fence: RustRaftApplySnapshotFence {
            applied_index: committed_index,
            commit_index: committed_index,
            installed_snapshot_index: 10,
            first_retained_log_index: 11,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

fn storage_fence() -> RustRaftStorageApplyFence {
    RustRaftStorageApplyFence {
        group_id: 404,
        node_id: 1,
        committed_index: 11,
        applied_index: 11,
        durable_applied_index: 11,
        storage_flushed_index: 11,
        installed_snapshot_index: 10,
        first_retained_log_index: 11,
    }
}

#[test]
fn durability_report_accepts_generic_wal_snapshot_and_storage_fences() {
    let good = wal_record(11);
    let mut wal = LocalRaftWal::new(1).expect("segmented wal");
    wal.append(good.clone()).expect("append valid record");
    wal.append(wal_record(12)).expect("append tail record");
    wal.corrupt_tail_for_test().expect("corrupt tail");

    let recovery = wal.recover().expect("recover and truncate corrupt tail");
    let recovered = recovery.recovered.clone().expect("recovered record");
    assert_eq!(recovered.hard_state.committed.as_ref().unwrap().index, 11);
    matrixraft_validate_hard_state_persistence(&recovered).expect("hard-state persisted");

    let snapshot = RaftSnapshot {
        group_id: 404,
        meta: snapshot_meta(),
        payload: b"opaque snapshot bytes".to_vec(),
    };
    let tail = vec![tail_entry(11)];
    matrixraft_validate_snapshot_tail_catchup(&snapshot.meta, &tail).expect("tail catch-up");
    matrixraft_validate_storage_apply_fence(&storage_fence()).expect("storage apply fence");

    let report = matrixraft_durability_parity_report(
        &recovered,
        &recovery,
        Some(&snapshot),
        &tail,
        &storage_fence(),
    );
    assert!(report.ready, "{report:#?}");
    assert!(report.segmented_wal_recovered);
    assert!(report.corrupt_tail_truncated);
    assert!(report.snapshot_floor_preserved);
    assert!(report.snapshot_tail_catchup_valid);
    assert!(report.apply_snapshot_fence_valid);
    assert!(report.storage_apply_fence_valid);
}

#[test]
fn storage_apply_fence_rejects_durable_apply_ahead_of_memory_apply() {
    let mut fence = storage_fence();
    fence.durable_applied_index = 12;

    let err = matrixraft_validate_storage_apply_fence(&fence).expect_err("invalid fence");
    assert!(err.to_string().contains("durable applied index"));
}

#[test]
fn snapshot_tail_catchup_rejects_overlap_and_gaps_after_snapshot_floor() {
    let meta = snapshot_meta();
    let overlap = vec![tail_entry(10)];
    assert!(matrixraft_validate_snapshot_tail_catchup(&meta, &overlap)
        .unwrap_err()
        .to_string()
        .contains("overlaps installed snapshot"));

    let gap = vec![tail_entry(12)];
    assert!(matrixraft_validate_snapshot_tail_catchup(&meta, &gap)
        .unwrap_err()
        .to_string()
        .contains("not contiguous"));
}
