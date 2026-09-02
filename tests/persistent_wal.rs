// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_fold_wal_records, matrixraft_recover_from_wal_records,
    matrixraft_recover_latest_wal_record, matrixraft_wal_checksum, matrixraft_wal_checksum_format,
    matrixraft_wal_lifecycle_evidence, ApplySnapshotFence, HardState, LogEntry, LogId, Membership,
    PersistentRaftWal, PersistentRaftWalOptions, StorageApplyFence, WalRecord,
};
use std::fs;
use std::path::Path;
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

fn wal_record(index: u64) -> WalRecord {
    WalRecord {
        entries_are_delta: false,
        group_id: 9,
        node_id: 1,
        hard_state: HardState {
            current_term: 3,
            voted_for: Some(1),
            committed: Some(LogId { term: 3, index }),
        },
        membership: Membership {
            group_id: 9,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 3,
        },
        entries: vec![LogEntry {
            log_id: LogId { term: 3, index },
            payload: format!("entry-{index}").into_bytes(),
            is_command: true,
        }],
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
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
    assert_eq!(first.checksum_format, matrixraft_wal_checksum_format());
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
        matrixraft_wal_checksum_format()
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
    let evidence = matrixraft_wal_lifecycle_evidence(&status);
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
            &StorageApplyFence {
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

    let evidence = matrixraft_wal_lifecycle_evidence(&wal.status());
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
            &StorageApplyFence {
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
            &StorageApplyFence {
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

// ---------------------------------------------------------------------------
// Records are stored as deltas against the segment they land in. These pin the
// two things that has to be true: what comes back out is the whole log, and a
// log that stops being an extension of the segment is stored whole instead.
// ---------------------------------------------------------------------------

/// A record whose log is every index from 1 to `last`, at `term`.
fn growing_wal_record(last: u64, term: u64) -> WalRecord {
    let mut record = wal_record(last);
    record.hard_state.current_term = term;
    record.hard_state.committed = Some(LogId { term, index: last });
    record.entries = (1..=last)
        .map(|index| LogEntry {
            log_id: LogId {
                // The tail carries the newer term; the prefix keeps term 3.
                term: if index == last { term } else { 3 },
                index,
            },
            payload: format!("entry-{index}").into_bytes(),
            is_command: true,
        })
        .collect();
    record.apply_snapshot_fence.applied_index = last;
    record.apply_snapshot_fence.commit_index = last;
    record
}

fn wide_segment_options(dir: PathBuf) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir,
        max_records_per_segment: 1_000,
        max_segment_bytes: u64::MAX,
        min_keep_segments: 1,
        fsync_on_append: false,
    }
}

fn stored_bytes(dir: &PathBuf) -> u64 {
    fs::read_dir(dir)
        .expect("read wal dir")
        .flatten()
        .filter_map(|entry| entry.metadata().ok())
        .map(|meta| meta.len())
        .sum()
}

fn stored_delta_count(dir: &PathBuf) -> usize {
    fs::read_dir(dir)
        .expect("read wal dir")
        .flatten()
        .filter_map(|entry| fs::read_to_string(entry.path()).ok())
        .map(|text| text.matches("\"entries_are_delta\":true").count())
        .sum()
}

#[test]
fn a_growing_log_is_stored_as_deltas_and_recovers_whole() {
    let dir = temp_wal_dir("delta-growing");
    let last = 200_u64;
    {
        let mut wal = PersistentRaftWal::open(wide_segment_options(dir.clone())).expect("open");
        for index in 1..=last {
            wal.append(growing_wal_record(index, 3)).expect("append");
        }
        // The optimisation has to have engaged, or the rest of this test is
        // just re-testing whole-log records.
        assert_eq!(
            stored_delta_count(&dir),
            (last - 1) as usize,
            "every record after the first in the segment should be a delta"
        );
    }

    let mut reopened = PersistentRaftWal::open(wide_segment_options(dir.clone())).expect("reopen");
    let report = reopened.recover().expect("recover");
    let recovered = report.recovered.expect("a record survives recovery");
    assert_eq!(
        recovered.entries.len(),
        last as usize,
        "recovery must fold the deltas back into the whole log"
    );
    assert!(!recovered.entries_are_delta);
    for (offset, entry) in recovered.entries.iter().enumerate() {
        assert_eq!(entry.log_id.index, offset as u64 + 1);
        assert_eq!(entry.payload, format!("entry-{}", offset + 1).into_bytes());
    }

    // The point of the change is that a record's cost stops growing with the
    // log. Stored whole, the average record here would carry ~100 entries; as
    // deltas it carries one.
    let bytes = stored_bytes(&dir);
    let bytes_per_record = bytes / last;
    assert!(
        bytes_per_record < 1_000,
        "expected a roughly constant per-record cost, got {bytes_per_record} bytes          across {bytes} total"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_log_truncated_by_a_conflict_is_stored_whole() {
    let dir = temp_wal_dir("delta-conflict");
    {
        let mut wal = PersistentRaftWal::open(wide_segment_options(dir.clone())).expect("open");
        for index in 1..=5 {
            wal.append(growing_wal_record(index, 3)).expect("append");
        }
        assert_eq!(stored_delta_count(&dir), 4);

        // A conflict truncates the log back to 3 and rewrites index 3 under a
        // newer term. That is not an extension of what the segment describes,
        // so it must be stored whole -- a delta here would leave recovery
        // rebuilding the entries that were thrown away.
        wal.append(growing_wal_record(3, 7))
            .expect("append conflict");
        assert_eq!(
            stored_delta_count(&dir),
            4,
            "the diverging record must not be stored as a delta"
        );
    }

    let reopened = PersistentRaftWal::open(wide_segment_options(dir.clone())).expect("reopen");
    // Read the folded records rather than going through recovery, which picks
    // by highest committed index and would hand back the pre-conflict record.
    let folded = reopened.records();
    let last = folded.last().expect("a record was stored");
    assert_eq!(
        last.entries.len(),
        3,
        "folding must not resurrect the truncated tail"
    );
    assert_eq!(last.entries[2].log_id.term, 7);
    assert!(!last.entries_are_delta);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn each_segment_can_be_read_without_the_ones_compaction_removed() {
    let dir = temp_wal_dir("delta-compaction");
    let options = PersistentRaftWalOptions {
        dir: dir.clone(),
        max_records_per_segment: 10,
        max_segment_bytes: u64::MAX,
        min_keep_segments: 1,
        fsync_on_append: false,
    };
    {
        let mut wal = PersistentRaftWal::open(options.clone()).expect("open");
        for index in 1..=50 {
            wal.append(growing_wal_record(index, 3)).expect("append");
        }
        assert!(wal.segments().len() >= 4);
        // Drop the early segments. Segments no longer open with a whole-log
        // record, so compaction has to materialise the first surviving one
        // before it deletes the records that one is a delta against.
        wal.compact_through(20).expect("compact");
    }

    let mut reopened = PersistentRaftWal::open(options).expect("reopen");
    let recovered = reopened
        .recover()
        .expect("recover")
        .recovered
        .expect("a record survives recovery");
    assert_eq!(recovered.entries.len(), 50);
    assert_eq!(recovered.entries.last().expect("tail").log_id.index, 50);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_tail_rewritten_at_the_same_index_is_stored_whole() {
    // The nastiest divergence: the log keeps the same last index but that entry
    // now carries a newer term. Nothing about the shape of the log changed, so
    // only comparing the term catches it. Storing a delta here would write an
    // empty delta and leave folding handing back the entry that was replaced.
    let dir = temp_wal_dir("delta-rewritten-tail");
    {
        let mut wal = PersistentRaftWal::open(wide_segment_options(dir.clone())).expect("open");
        for index in 1..=5 {
            wal.append(growing_wal_record(index, 3)).expect("append");
        }
        assert_eq!(stored_delta_count(&dir), 4);

        wal.append(growing_wal_record(5, 7))
            .expect("append rewritten tail");
        assert_eq!(
            stored_delta_count(&dir),
            4,
            "a tail rewritten under a newer term must not be stored as a delta"
        );
    }

    let reopened = PersistentRaftWal::open(wide_segment_options(dir.clone())).expect("reopen");
    let folded = reopened.records();
    let last = folded.last().expect("a record was stored");
    assert_eq!(last.entries.len(), 5);
    assert_eq!(
        last.entries[4].log_id.term, 7,
        "folding handed back the replaced entry instead of the rewritten one"
    );

    let _ = fs::remove_dir_all(&dir);
}

/// Build a real WAL record carrying `payload`, then rewrite its entry payloads
/// back into the JSON number array that earlier versions wrote.
///
/// Derived from a live record rather than hand-written so it cannot drift from
/// the struct shape -- only the payload encoding is turned back to the old form.
fn legacy_wal_record_json(payload: &[u8]) -> (matrixraft::WalRecord, String) {
    fn peer(node_id: u64) -> matrixraft::Peer {
        matrixraft::Peer {
            node_id,
            raft_addr: format!("127.0.0.1:{}", 9_400 + node_id),
            snapshot_addr: format!("127.0.0.1:{}", 9_500 + node_id),
            role: matrixraft::ReplicaRole::Voter,
            auto_promote: false,
        }
    }
    let mut cluster = matrixraft::RaftCluster::new(
        7,
        matrixraft::Config::default(),
        vec![peer(1), peer(2), peer(3)],
    )
    .expect("valid cluster");
    cluster.start().expect("start");
    cluster.campaign(1, true).expect("campaign");
    cluster.propose(payload.to_vec()).expect("propose");
    let record = cluster.wal_record_for(1).expect("wal record");

    let mut value = serde_json::to_value(&record).expect("record to value");
    for entry in value["entries"]
        .as_array_mut()
        .expect("entries is an array")
        .iter_mut()
    {
        // Whatever this entry's payload is, write it the old way.
        let bytes = record
            .entries
            .iter()
            .find(|candidate| candidate.log_id.index == entry["log_id"]["index"].as_u64().unwrap())
            .map(|candidate| candidate.payload.clone())
            .expect("entry present in the typed record");
        entry["payload"] =
            serde_json::Value::Array(bytes.into_iter().map(serde_json::Value::from).collect());
    }
    (record, serde_json::to_string(&value).expect("legacy json"))
}

#[test]
fn wal_records_written_before_base64_payloads_still_recover() {
    // Segments in the old encoding exist on disk in deployments, so the reader
    // has to keep taking them even though nothing writes them any more.
    let (original, legacy_json) = legacy_wal_record_json(b"foobar");
    assert!(
        legacy_json.contains(r#""payload":[102,111,111,98,97,114]"#),
        "fixture should be in the old number-array form: {legacy_json}"
    );

    let parsed: matrixraft::WalRecord =
        serde_json::from_str(&legacy_json).expect("legacy WAL record still parses");
    assert_eq!(
        parsed, original,
        "legacy decoding must reproduce the record"
    );

    // Re-encoding uses the compact form and round-trips, so a segment rewritten
    // by compaction keeps its contents.
    let reencoded = serde_json::to_string(&original).expect("record encodes");
    assert!(
        reencoded.contains(r#""payload":"Zm9vYmFy""#),
        "expected a base64 payload, got: {reencoded}"
    );
    let reparsed: matrixraft::WalRecord =
        serde_json::from_str(&reencoded).expect("re-encoded record parses");
    assert_eq!(reparsed, original);
}

#[test]
fn wal_payload_encoding_shrinks_a_realistic_record() {
    // Guards the win, not the mechanism. A uniform 4 KiB payload is the case
    // where `serde_json`'s number array costs four characters a byte, and it is
    // ~98% of the record, so it sets the amplification on its own.
    let (record, legacy_json) = legacy_wal_record_json(&vec![200u8; 4096]);
    let encoded = serde_json::to_string(&record).expect("record encodes");

    assert!(
        legacy_json.len() > 16_000,
        "expected the old form to cost over 16000 bytes, got {}",
        legacy_json.len()
    );
    assert!(
        encoded.len() < 6_500,
        "expected a 4 KiB payload to cost under 6500 WAL bytes, got {}",
        encoded.len()
    );

    // The shared entry type is untouched: RPC and the JSON debug surfaces still
    // see a number array, which is the readable form there and is not paid for
    // by the megabyte.
    let bare_entries = serde_json::to_string(&record.entries).expect("bare entries encode");
    assert!(
        bare_entries.len() > 16_000,
        "expected the shared entry encoding to be unchanged, got {}",
        bare_entries.len()
    );
}

/// Builds a stored record: whole when `from` is zero, otherwise a delta that
/// carries entries from `from` onward. Checksummed as the writer would.
fn stored_record(last: u64, term: u64, from: u64) -> WalRecord {
    let mut record = growing_wal_record(last, term);
    if from > 0 {
        record.entries.retain(|entry| entry.log_id.index >= from);
        record.entries_are_delta = true;
    }
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

/// The streaming recovery has to pick exactly what the fold-everything path
/// picked. It exists to avoid materialising a whole-log record per stored
/// record, not to change which record recovery restores from.
fn assert_recovery_agrees(case: &str, stored: &[WalRecord]) {
    let folded = matrixraft_fold_wal_records(stored);
    let expected_record = matrixraft_recover_latest_wal_record(&folded).ok();
    let (surviving, recovered) = matrixraft_recover_from_wal_records(stored);

    assert_eq!(
        surviving,
        folded.len(),
        "{case}: surviving record count must match the fold"
    );
    assert_eq!(
        recovered.as_ref().map(|record| record.entries.len()),
        expected_record.as_ref().map(|record| record.entries.len()),
        "{case}: recovered entry count must match"
    );
    assert_eq!(
        recovered, expected_record,
        "{case}: recovered record must match"
    );
}

#[test]
fn streaming_recovery_picks_what_folding_everything_picked() {
    // A plain growing log stored as one whole record then deltas.
    let mut deltas = vec![stored_record(1, 3, 0)];
    for index in 2..=12 {
        deltas.push(stored_record(index, 3, index));
    }
    assert_recovery_agrees("growing log", &deltas);

    // A tail rewritten at the same index in a newer term, which the fold has to
    // resolve in favour of the rewrite rather than appending after it.
    let mut rewritten = deltas.clone();
    rewritten.push(stored_record(12, 4, 10));
    assert_recovery_agrees("rewritten tail", &rewritten);

    // A corrupt record: everything after it is discarded, by both paths.
    let mut corrupt = deltas.clone();
    let mut bad = stored_record(13, 3, 13);
    bad.checksum = "not-a-checksum".to_string();
    corrupt.push(bad);
    corrupt.push(stored_record(14, 3, 14));
    assert_recovery_agrees("corrupt tail", &corrupt);

    // Nothing valid at all.
    let mut only_bad = stored_record(1, 3, 0);
    only_bad.checksum = "not-a-checksum".to_string();
    assert_recovery_agrees("no valid records", &[only_bad]);

    // Empty.
    assert_recovery_agrees("empty", &[]);
}

fn group_commit_options(dir: PathBuf) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir,
        max_records_per_segment: 1_000,
        max_segment_bytes: u64::MAX,
        min_keep_segments: 1,
        fsync_on_append: true,
    }
}

/// A durable append is its fsync, so what a batch has to prove is that it paid
/// for one and not for each record. Asserted by counting: the count is exact,
/// where the seconds move with the disk and whatever else is running.
#[test]
fn a_batch_pays_for_one_fsync_not_one_per_record() {
    let dir = temp_wal_dir("group-commit-count");
    let mut wal = PersistentRaftWal::open(group_commit_options(dir.clone())).expect("open");

    assert_eq!(wal.fsync_count(), 0, "opening does not fsync a record");
    wal.append_batch(Vec::new()).expect("an empty batch");
    assert_eq!(wal.fsync_count(), 0, "an empty batch has nothing to sync");

    let batch: Vec<_> = (1..=25).map(|index| growing_wal_record(index, 3)).collect();
    let reports = wal.append_batch(batch).expect("batch");
    assert_eq!(reports.len(), 25, "a report per record");
    assert_eq!(
        wal.fsync_count(),
        1,
        "twenty-five records should cost one fsync"
    );

    // One at a time still pays per record, which is what makes the batch worth
    // having rather than a rewrite of the same cost.
    for index in 26..=30 {
        wal.append(growing_wal_record(index, 3)).expect("append");
    }
    assert_eq!(
        wal.fsync_count(),
        6,
        "five single appends add five fsyncs to the batch's one"
    );
    fs::remove_dir_all(&dir).ok();
}

fn roll_options(dir: PathBuf) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir,
        max_records_per_segment: 10,
        max_segment_bytes: u64::MAX,
        min_keep_segments: 1,
        fsync_on_append: false,
    }
}

/// Records held in memory, which is now only the active segment's.
fn in_memory_record_count(wal: &PersistentRaftWal) -> usize {
    wal.segments()
        .iter()
        .map(|segment| segment.records.len())
        .sum()
}

/// A segment's first record, read from the file rather than from memory.
///
/// Sealed segments release their records once they are on disk, so a test that
/// wants to see what was *stored* has to read the file, which is the point.
fn first_stored_record(dir: &Path, segment_id: u64) -> WalRecord {
    let path = dir.join(format!("{segment_id:020}.wal"));
    let text =
        fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    let line = text.lines().next().expect("the segment has a record");
    serde_json::from_str(line).expect("a stored record parses")
}

/// A segment roll used to rewrite the whole retained log, so N appends cost
/// about N^2/2S entries rather than N.
///
/// This asserts the shape by counting rather than by timing: entries held has
/// an exact expected value, and does not move with machine load.
#[test]
fn a_segment_roll_no_longer_rewrites_the_whole_log() {
    let dir = temp_wal_dir("roll-cost");
    let mut wal = PersistentRaftWal::open(roll_options(dir.clone())).expect("open");
    for index in 1..=50 {
        wal.append(growing_wal_record(index, 3)).expect("append");
    }

    assert!(
        wal.segments().len() >= 5,
        "the log has to cross several segments for this to mean anything"
    );
    // Counted from the files, because sealed segments no longer keep their
    // records in memory. 50 records with one whole one means 49 deltas; a
    // whole record per segment roll would leave 45.
    assert_eq!(
        stored_delta_count(&dir),
        49,
        "only the very first record of the WAL should be stored whole; a roll          that rewrote the log would leave 5 whole records here"
    );
    assert_eq!(
        in_memory_record_count(&wal),
        wal.segments()
            .last()
            .expect("an active segment")
            .records
            .len(),
        "only the active segment's records belong in memory"
    );
    assert!(
        in_memory_record_count(&wal) <= 10,
        "memory should be bounded by the segment being written, not the log"
    );
    fs::remove_dir_all(&dir).ok();
}

/// Batching changes when the fsync happens, and nothing else. What is stored
/// and what recovers has to be identical to appending one at a time.
#[test]
fn a_batched_wal_recovers_exactly_as_a_one_at_a_time_wal_does() {
    let batched_dir = temp_wal_dir("group-commit-batched");
    let single_dir = temp_wal_dir("group-commit-single");
    {
        let mut wal =
            PersistentRaftWal::open(group_commit_options(batched_dir.clone())).expect("open");
        let batch: Vec<_> = (1..=40).map(|index| growing_wal_record(index, 3)).collect();
        wal.append_batch(batch).expect("batch");
    }
    {
        let mut wal =
            PersistentRaftWal::open(group_commit_options(single_dir.clone())).expect("open");
        for index in 1..=40 {
            wal.append(growing_wal_record(index, 3)).expect("append");
        }
    }

    let mut batched =
        PersistentRaftWal::open(group_commit_options(batched_dir.clone())).expect("reopen");
    let mut single =
        PersistentRaftWal::open(group_commit_options(single_dir.clone())).expect("reopen");
    let batched_report = batched.recover().expect("recover batched");
    let single_report = single.recover().expect("recover single");

    assert_eq!(
        batched_report.surviving_records, single_report.surviving_records,
        "the same records survive either way"
    );
    assert_eq!(
        batched_report.recovered, single_report.recovered,
        "the recovered record must not depend on when the fsync happened"
    );
    assert_eq!(
        batched_report
            .recovered
            .as_ref()
            .map(|record| record.entries.len()),
        Some(40)
    );
    fs::remove_dir_all(&batched_dir).ok();
    fs::remove_dir_all(&single_dir).ok();
}

/// Compaction deletes whole segment files. A survivor that opens with a delta
/// has just lost the records it was a delta against, so compaction has to turn
/// it back into a whole record first -- and must do it without losing an entry.
#[test]
fn compaction_materialises_the_record_its_survivors_depend_on() {
    let dir = temp_wal_dir("materialise");
    let options = roll_options(dir.clone());
    {
        let mut wal = PersistentRaftWal::open(options.clone()).expect("open");
        for index in 1..=50 {
            wal.append(growing_wal_record(index, 3)).expect("append");
        }
        let first_survivor_was_a_delta =
            first_stored_record(&dir, wal.segments()[1].segment_id).entries_are_delta;
        assert!(
            first_survivor_was_a_delta,
            "the segment that survives has to start as a delta, or this proves nothing"
        );
        wal.compact_through(20).expect("compact");
    }

    let mut reopened = PersistentRaftWal::open(options).expect("reopen");
    assert!(
        !first_stored_record(&dir, reopened.segments()[0].segment_id).entries_are_delta,
        "the first surviving record must have been materialised on disk"
    );
    let recovered = reopened
        .recover()
        .expect("recover")
        .recovered
        .expect("a record survives recovery");
    assert_eq!(
        recovered.entries.len(),
        50,
        "compaction must not drop an entry it was still the base for"
    );
    fs::remove_dir_all(&dir).ok();
}

/// Compaction happening more than once is the case where a materialised record
/// becomes the base for the next materialisation.
#[test]
fn a_log_recovers_exactly_after_repeated_compaction() {
    let dir = temp_wal_dir("repeat-compaction");
    let options = roll_options(dir.clone());
    {
        let mut wal = PersistentRaftWal::open(options.clone()).expect("open");
        for index in 1..=30 {
            wal.append(growing_wal_record(index, 3)).expect("append");
        }
        wal.compact_through(10).expect("first compaction");
        for index in 31..=60 {
            wal.append(growing_wal_record(index, 3)).expect("append");
        }
        wal.compact_through(40).expect("second compaction");
    }

    let mut reopened = PersistentRaftWal::open(options).expect("reopen");
    let recovered = reopened
        .recover()
        .expect("recover")
        .recovered
        .expect("a record survives recovery");
    assert_eq!(recovered.entries.len(), 60);
    assert_eq!(
        recovered.entries.last().expect("tail").log_id.index,
        60,
        "the tail has to survive two compactions intact"
    );
    fs::remove_dir_all(&dir).ok();
}
