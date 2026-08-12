// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    rustraft_wal_checksum_format, rustraft_wal_lifecycle_evidence, PersistentRaftWal,
    PersistentRaftWalOptions, RaftWalRecord, RustRaftApplySnapshotFence, RustRaftHardState,
    RustRaftLogEntry, RustRaftLogId, RustRaftMembership, RustRaftStorageApplyFence,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_wal_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("rustraft-{name}-{}-{nonce}", std::process::id()))
}

fn wal_options(dir: PathBuf) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir,
        max_records_per_segment: 2,
        max_segment_bytes: 4096,
        min_keep_segments: 1,
        fsync_on_append: true,
    }
}

fn wal_record(index: u64) -> RaftWalRecord {
    RaftWalRecord {
        group_id: 9,
        node_id: 1,
        hard_state: RustRaftHardState {
            current_term: 3,
            voted_for: Some(1),
            committed: Some(RustRaftLogId { term: 3, index }),
        },
        membership: RustRaftMembership {
            group_id: 9,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 3,
        },
        entries: vec![RustRaftLogEntry {
            log_id: RustRaftLogId { term: 3, index },
            payload: format!("entry-{index}").into_bytes(),
            is_command: true,
        }],
        installed_snapshot: None,
        apply_snapshot_fence: RustRaftApplySnapshotFence {
            applied_index: index,
            commit_index: index,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    }
}

#[test]
fn persistent_wal_rolls_segments_and_recovers_after_restart() {
    let dir = temp_wal_dir("restart");
    let options = wal_options(dir.clone());
    {
        let mut wal = PersistentRaftWal::open(options.clone()).expect("open wal");
        wal.append(wal_record(1)).expect("append 1");
        wal.append(wal_record(2)).expect("append 2");
        wal.append(wal_record(3)).expect("append 3");
        assert_eq!(wal.status().segment_count, 2);
        assert_eq!(wal.status().last_log_index, 3);
    }

    let mut reopened = PersistentRaftWal::open(options).expect("reopen wal");
    let report = reopened.recover().expect("recover wal");
    assert!(!report.truncated_corrupt_tail);
    assert_eq!(
        report
            .recovered
            .expect("latest")
            .hard_state
            .committed
            .expect("commit")
            .index,
        3
    );
    assert_eq!(reopened.records().len(), 3);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_wal_writer_reports_checksum_segment_index_and_retained_range() {
    let dir = temp_wal_dir("writer-report");
    let options = wal_options(dir.clone());
    let mut wal = PersistentRaftWal::open(options).expect("open wal");

    let first = wal.append_with_report(wal_record(1)).expect("append first");
    assert_eq!(first.segment_id, 0);
    assert_eq!(first.log_index, 1);
    assert_eq!(first.checksum_format, rustraft_wal_checksum_format());
    assert!(first.hard_state_persisted);
    assert!(first.fsync_on_append);
    assert_eq!(first.retained_range.first_log_index, 1);
    assert_eq!(first.retained_range.last_log_index, 1);

    wal.append_with_report(wal_record(2))
        .expect("append second");
    let third = wal
        .append_with_report(wal_record(3))
        .expect("append third rolls segment");
    assert!(third.segment_rolled);
    assert_eq!(third.segment_id, 1);

    let index = wal.segment_index();
    assert_eq!(index.len(), 2);
    assert_eq!(index[0].record_count, 2);
    assert!(index[0].sealed);
    assert!(index[0].bytes > 0);
    assert_eq!(wal.retained_log_range().first_log_index, 1);
    assert_eq!(wal.retained_log_range().last_log_index, 3);
    assert_eq!(wal.checksum_format().algorithm, "fnv1a64-rustraft-v1");

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_wal_truncates_corrupt_tail_on_recovery() {
    let dir = temp_wal_dir("corrupt-tail");
    let options = wal_options(dir.clone());
    {
        let mut wal = PersistentRaftWal::open(options.clone()).expect("open wal");
        wal.append(wal_record(1)).expect("append 1");
        wal.append(wal_record(2)).expect("append 2");
        wal.corrupt_tail_for_test().expect("corrupt tail");
    }

    let mut reopened = PersistentRaftWal::open(options).expect("reopen wal");
    let report = reopened.recover().expect("recover");
    assert!(report.truncated_corrupt_tail);
    assert_eq!(report.surviving_records, 2);
    assert_eq!(report.segments_scanned, 1);
    assert_eq!(
        report.checksum_format.expect("checksum format"),
        rustraft_wal_checksum_format()
    );
    assert_eq!(
        report
            .retained_range
            .expect("retained range")
            .last_log_index,
        2
    );
    assert_eq!(reopened.records().len(), 2);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_wal_compacts_released_segments_and_reports_lifecycle_evidence() {
    let dir = temp_wal_dir("compact");
    let options = wal_options(dir.clone());
    let mut wal = PersistentRaftWal::open(options).expect("open wal");
    for index in 1..=5 {
        wal.append(wal_record(index)).expect("append");
    }
    assert_eq!(wal.status().segment_count, 3);

    let released = wal.compact_through(4).expect("compact");
    assert_eq!(released, 2);
    let status = wal.status();
    assert_eq!(status.segment_count, 1);
    assert_eq!(status.first_log_index, 5);
    assert_eq!(status.released_segment_count, 2);
    let evidence = rustraft_wal_lifecycle_evidence(&status);
    assert!(evidence.segment_lifecycle_present);
    assert!(evidence.compaction_observed);

    let mut reopened = PersistentRaftWal::open(PersistentRaftWalOptions {
        dir: dir.clone(),
        max_records_per_segment: 2,
        max_segment_bytes: 4096,
        min_keep_segments: 1,
        fsync_on_append: true,
    })
    .expect("reopen compacted");
    let report = reopened.recover().expect("recover compacted");
    assert_eq!(
        report
            .recovered
            .expect("latest")
            .hard_state
            .committed
            .expect("commit")
            .index,
        5
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_wal_reports_slow_fsync_backpressure_through_lifecycle_status() {
    let dir = temp_wal_dir("slow-fsync");
    let options = wal_options(dir.clone());
    let mut wal = PersistentRaftWal::open(options).expect("open wal");
    wal.set_slow_fsync_threshold_ms(50);

    wal.inject_next_fsync_delay_for_test(75);
    let slow = wal
        .append_with_report(wal_record(1))
        .expect("append slow fsync");
    assert!(slow.fsync_on_append);
    assert!(slow.slow_fsync_observed);
    assert_eq!(slow.slow_fsync_threshold_ms, 50);
    assert!(slow.fsync_elapsed_ms >= 50);

    let status = wal.status();
    assert!(status.slow_fsync_backpressure_observed);
    assert_eq!(status.slow_fsync_threshold_ms, 50);
    assert_eq!(status.slow_fsync_count, 1);
    assert_eq!(status.consecutive_slow_fsync_count, 1);
    assert!(status.max_fsync_elapsed_ms >= slow.fsync_elapsed_ms);
    assert_eq!(status.compacted_after_slow_fsync_count, 0);

    wal.append_with_report(wal_record(2))
        .expect("append fast fsync");
    let status = wal.status();
    assert_eq!(status.slow_fsync_count, 1);
    assert_eq!(status.consecutive_slow_fsync_count, 0);

    for index in 3..=6 {
        wal.append(wal_record(index))
            .expect("append for compaction");
    }
    let released = wal
        .compact_through_with_fence(
            4,
            &RustRaftStorageApplyFence {
                group_id: 9,
                node_id: 1,
                committed_index: 6,
                applied_index: 6,
                durable_applied_index: 4,
                storage_flushed_index: 4,
                installed_snapshot_index: 0,
                first_retained_log_index: 1,
            },
        )
        .expect("compact after slow fsync");
    assert!(released.fence_valid);
    assert!(released.released_segments > 0);

    let evidence = rustraft_wal_lifecycle_evidence(&wal.status());
    assert!(evidence.compaction_observed);
    assert!(evidence.slow_fsync_backpressure_observed);
    assert!(evidence.compaction_after_slow_fsync_observed);
    assert_eq!(
        wal.status().compacted_after_slow_fsync_count,
        released.released_segments
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_wal_compaction_fence_blocks_unsafe_release_and_reports_range() {
    let dir = temp_wal_dir("compaction-fence");
    let options = wal_options(dir.clone());
    let mut wal = PersistentRaftWal::open(options).expect("open wal");
    for index in 1..=5 {
        wal.append(wal_record(index)).expect("append");
    }

    let blocked = wal
        .compact_through_with_fence(
            4,
            &RustRaftStorageApplyFence {
                group_id: 9,
                node_id: 1,
                committed_index: 5,
                applied_index: 5,
                durable_applied_index: 3,
                storage_flushed_index: 5,
                installed_snapshot_index: 0,
                first_retained_log_index: 1,
            },
        )
        .expect("blocked report");
    assert!(!blocked.fence_valid);
    assert_eq!(blocked.released_segments, 0);
    assert!(blocked.blocker.expect("blocker").contains("behind"));

    let released = wal
        .compact_through_with_fence(
            4,
            &RustRaftStorageApplyFence {
                group_id: 9,
                node_id: 1,
                committed_index: 5,
                applied_index: 5,
                durable_applied_index: 4,
                storage_flushed_index: 4,
                installed_snapshot_index: 0,
                first_retained_log_index: 1,
            },
        )
        .expect("safe compaction");
    assert!(released.fence_valid);
    assert_eq!(released.released_segments, 2);
    assert_eq!(released.retained_range.first_log_index, 5);
    assert_eq!(released.retained_range.last_log_index, 5);

    let _ = fs::remove_dir_all(dir);
}
