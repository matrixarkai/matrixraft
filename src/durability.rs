// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Durable apply, WAL recovery, and snapshot/storage fence parity reports.

use crate::{
    rustraft_validate_apply_snapshot_fence, rustraft_validate_hard_state_persistence,
    rustraft_validate_snapshot_floor_log_matching, rustraft_validate_snapshot_install,
    rustraft_validate_snapshot_tail_catchup, rustraft_validate_storage_apply_fence,
    rustraft_wal_checksum_valid, RaftSnapshot, RaftWalRecoveryReport,
    RustRaftDurabilityParityReport, RustRaftLogEntry, RustRaftStorageApplyFence, RustRaftWalRecord,
};

pub fn rustraft_durability_parity_report(
    wal_record: &RustRaftWalRecord,
    recovery_report: &RaftWalRecoveryReport,
    snapshot: Option<&RaftSnapshot>,
    tail_entries: &[RustRaftLogEntry],
    storage_fence: &RustRaftStorageApplyFence,
) -> RustRaftDurabilityParityReport {
    let hard_state_persisted = rustraft_validate_hard_state_persistence(wal_record).is_ok();
    let wal_record_valid = rustraft_wal_checksum_valid(wal_record);
    let segmented_wal_recovered = recovery_report.recovered.is_some();
    let corrupt_tail_truncated =
        recovery_report.truncated_corrupt_tail || recovery_report.removed_records > 0;
    let apply_snapshot_fence_valid = rustraft_validate_apply_snapshot_fence(wal_record).is_ok();
    let storage_apply_fence_valid = rustraft_validate_storage_apply_fence(storage_fence).is_ok();
    let (snapshot_install_valid, snapshot_floor_preserved, snapshot_tail_catchup_valid) =
        if let Some(snapshot) = snapshot {
            (
                rustraft_validate_snapshot_install(snapshot, &wal_record.apply_snapshot_fence)
                    .is_ok(),
                rustraft_validate_snapshot_floor_log_matching(
                    &snapshot.meta,
                    wal_record.apply_snapshot_fence.first_retained_log_index,
                    Some(&snapshot.meta.last_log_id),
                )
                .is_ok(),
                rustraft_validate_snapshot_tail_catchup(&snapshot.meta, tail_entries).is_ok(),
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
    RustRaftDurabilityParityReport {
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
