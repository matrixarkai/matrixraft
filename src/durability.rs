// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Durable apply, WAL recovery, and snapshot/storage fence parity reports.

use crate::{
    matrixraft_validate_apply_snapshot_fence, matrixraft_validate_hard_state_persistence,
    matrixraft_validate_snapshot_floor_log_matching, matrixraft_validate_snapshot_install,
    matrixraft_validate_snapshot_tail_catchup, matrixraft_validate_storage_apply_fence,
    matrixraft_wal_checksum_valid, DurabilityParityReport, LogEntry, RaftSnapshot,
    StorageApplyFence, WalRecord, WalRecoveryReport,
};

pub fn matrixraft_durability_parity_report(
    wal_record: &WalRecord,
    recovery_report: &WalRecoveryReport,
    snapshot: Option<&RaftSnapshot>,
    tail_entries: &[LogEntry],
    storage_fence: &StorageApplyFence,
) -> DurabilityParityReport {
    let hard_state_persisted = matrixraft_validate_hard_state_persistence(wal_record).is_ok();
    let wal_record_valid = matrixraft_wal_checksum_valid(wal_record);
    let segmented_wal_recovered = recovery_report.recovered.is_some();
    let corrupt_tail_truncated =
        recovery_report.truncated_corrupt_tail || recovery_report.removed_records > 0;
    let apply_snapshot_fence_valid = matrixraft_validate_apply_snapshot_fence(wal_record).is_ok();
    let storage_apply_fence_valid = matrixraft_validate_storage_apply_fence(storage_fence).is_ok();
    let (snapshot_install_valid, snapshot_floor_preserved, snapshot_tail_catchup_valid) =
        if let Some(snapshot) = snapshot {
            (
                matrixraft_validate_snapshot_install(snapshot, &wal_record.apply_snapshot_fence)
                    .is_ok(),
                matrixraft_validate_snapshot_floor_log_matching(
                    &snapshot.meta,
                    wal_record.apply_snapshot_fence.first_retained_log_index,
                    Some(&snapshot.meta.last_log_id),
                )
                .is_ok(),
                matrixraft_validate_snapshot_tail_catchup(&snapshot.meta, tail_entries).is_ok(),
            )
        } else {
            (true, true, tail_entries.is_empty())
        };

    let checks = [
        ("hard_state_persisted", hard_state_persisted),
        ("wal_record_valid", wal_record_valid),
        ("segmented_wal_recovered", segmented_wal_recovered),
        ("corrupt_tail_truncated", corrupt_tail_truncated),
        ("snapshot_install_valid", snapshot_install_valid),
        ("snapshot_floor_preserved", snapshot_floor_preserved),
        ("snapshot_tail_catchup_valid", snapshot_tail_catchup_valid),
        ("apply_snapshot_fence_valid", apply_snapshot_fence_valid),
        ("storage_apply_fence_valid", storage_apply_fence_valid),
    ];
    let blockers = checks
        .into_iter()
        .filter(|&(_name, passed)| !passed)
        .map(|(name, _passed)| name.to_string())
        .collect::<Vec<_>>();
    DurabilityParityReport {
        ready: blockers.is_empty(),
        hard_state_persisted,
        wal_record_valid,
        segmented_wal_recovered,
        corrupt_tail_truncated,
        snapshot_install_valid,
        snapshot_floor_preserved,
        snapshot_tail_catchup_valid,
        apply_snapshot_fence_valid,
        storage_apply_fence_valid,
        blockers,
    }
}
