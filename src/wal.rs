// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WAL records, segmented recovery, checksums, and persistent local WAL API.

use serde::{Deserialize, Serialize};

pub use crate::{
    matrixraft_durability_parity_report, DurabilityParityReport, FileRaftWal, HardState,
    LocalRaftWal, PersistentRaftWal, PersistentRaftWalOptions, Term,
};

use crate::{
    ApplySnapshotFence, GroupId, LogEntry, LogIndex, Membership, NodeId, RaftError,
    SnapshotMetadata,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalChecksumFormat {
    pub algorithm: String,
    pub encoding: String,
    pub covered_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogRetainedRange {
    pub first_log_index: LogIndex,
    pub last_log_index: LogIndex,
    pub first_segment_id: u64,
    pub last_segment_id: u64,
    pub record_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalSegmentIndex {
    pub segment_id: u64,
    pub first_log_index: LogIndex,
    pub last_log_index: LogIndex,
    pub record_count: u64,
    pub sealed: bool,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalWriteReport {
    pub segment_id: u64,
    pub log_index: LogIndex,
    pub checksum: String,
    pub checksum_format: WalChecksumFormat,
    pub bytes_written: u64,
    pub fsync_on_append: bool,
    #[serde(default)]
    pub fsync_elapsed_ms: u64,
    #[serde(default)]
    pub slow_fsync_threshold_ms: u64,
    #[serde(default)]
    pub slow_fsync_observed: bool,
    pub segment_rolled: bool,
    pub hard_state_persisted: bool,
    pub retained_range: LogRetainedRange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalCompactionReport {
    pub requested_log_index: LogIndex,
    pub released_segments: u64,
    pub retained_range: LogRetainedRange,
    pub fence_valid: bool,
    pub blocker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalRecord {
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub hard_state: HardState,
    pub membership: Membership,
    #[serde(default)]
    pub entries: Vec<LogEntry>,
    /// When true, `entries` carries only what was appended since the previous
    /// record in the same segment rather than the whole retained log. The first
    /// record of every segment is always a full one, so a segment can be read
    /// without reading any other -- which is what lets whole-segment compaction
    /// stay as simple as it was.
    #[serde(default)]
    pub entries_are_delta: bool,
    #[serde(default)]
    pub installed_snapshot: Option<SnapshotMetadata>,
    pub apply_snapshot_fence: ApplySnapshotFence,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalSegment {
    pub segment_id: u64,
    pub first_index: LogIndex,
    pub last_index: LogIndex,
    pub records: Vec<WalRecord>,
    pub sealed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalRecoveryReport {
    pub recovered: Option<WalRecord>,
    pub truncated_corrupt_tail: bool,
    pub surviving_records: usize,
    pub removed_records: usize,
    #[serde(default)]
    pub segments_scanned: u64,
    #[serde(default)]
    pub checksum_format: Option<WalChecksumFormat>,
    #[serde(default)]
    pub retained_range: Option<LogRetainedRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalLifecycleStatus {
    pub segment_count: u64,
    pub active_segment_id: u64,
    pub first_retained_segment_id: u64,
    pub last_retained_segment_id: u64,
    pub total_bytes: u64,
    pub active_segment_bytes: u64,
    pub total_records: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub first_log_index: u64,
    pub last_log_index: u64,
    pub released_segment_count: u64,
    pub slow_fsync_backpressure_observed: bool,
    #[serde(default)]
    pub slow_fsync_threshold_ms: u64,
    #[serde(default)]
    pub slow_fsync_count: u64,
    #[serde(default)]
    pub consecutive_slow_fsync_count: u64,
    #[serde(default)]
    pub max_fsync_elapsed_ms: u64,
    #[serde(default)]
    pub compacted_after_slow_fsync_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalLifecycleEvidence {
    pub segment_lifecycle_present: bool,
    pub retained_range_present: bool,
    pub sequence_range_present: bool,
    pub log_index_range_present: bool,
    pub compaction_observed: bool,
    pub slow_fsync_backpressure_observed: bool,
    #[serde(default)]
    pub compaction_after_slow_fsync_observed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalLifecycleEvidenceArtifact {
    pub schema: String,
    pub status: WalLifecycleStatus,
    pub evidence: WalLifecycleEvidence,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalLifecycleEvidenceValidationReport {
    pub valid: bool,
    pub schema_valid: bool,
    pub segment_lifecycle_present: bool,
    pub retained_range_present: bool,
    pub sequence_range_present: bool,
    pub log_index_range_present: bool,
    pub compaction_observed: bool,
    pub slow_fsync_backpressure_observed: bool,
    #[serde(default)]
    pub compaction_after_slow_fsync_observed: bool,
    #[serde(default)]
    pub missing: Vec<String>,
}

pub fn matrixraft_wal_checksum(record: &WalRecord) -> String {
    let mut hash = 14_695_981_039_346_656_037_u64;
    let mut mix = |value: u64| {
        for byte in value.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(1_099_511_628_211);
        }
    };
    mix(record.group_id);
    mix(record.node_id);
    mix(record.hard_state.current_term);
    mix(record.hard_state.voted_for.unwrap_or_default());
    if let Some(committed) = &record.hard_state.committed {
        mix(committed.term);
        mix(committed.index);
    }
    for entry in &record.entries {
        mix(entry.log_id.term);
        mix(entry.log_id.index);
        mix(entry.payload.len() as u64);
    }
    if record.entries_are_delta {
        // Mixed only when set: a full record hashes exactly as it did before
        // this field existed, so WAL files written earlier still validate.
        mix(1);
    }
    if let Some(snapshot) = &record.installed_snapshot {
        mix(snapshot.last_log_id.term);
        mix(snapshot.last_log_id.index);
    }
    mix(record.apply_snapshot_fence.applied_index);
    mix(record.apply_snapshot_fence.commit_index);
    mix(record.apply_snapshot_fence.installed_snapshot_index);
    mix(record.apply_snapshot_fence.first_retained_log_index);
    format!("{hash:016x}")
}

pub fn matrixraft_wal_checksum_format() -> WalChecksumFormat {
    WalChecksumFormat {
        algorithm: "fnv1a64-rustraft-v1".to_string(),
        encoding: "lower_hex_16".to_string(),
        covered_fields: vec![
            "group_id".to_string(),
            "node_id".to_string(),
            "hard_state.current_term".to_string(),
            "hard_state.voted_for".to_string(),
            "hard_state.committed".to_string(),
            "entries.log_id".to_string(),
            "entries.payload_len".to_string(),
            "entries_are_delta".to_string(),
            "installed_snapshot.last_log_id".to_string(),
            "apply_snapshot_fence".to_string(),
        ],
    }
}

pub fn matrixraft_wal_checksum_valid(record: &WalRecord) -> bool {
    record.checksum == matrixraft_wal_checksum(record)
}

pub fn matrixraft_validate_hard_state_persistence(record: &WalRecord) -> Result<(), RaftError> {
    if let Some(committed) = &record.hard_state.committed {
        if committed.term > record.hard_state.current_term {
            return Err(RaftError::Storage(
                "committed term is ahead of persisted current term".to_string(),
            ));
        }
        let last_entry_index = record
            .entries
            .last()
            .map(|entry| entry.log_id.index)
            .unwrap_or_default();
        let snapshot_index = record
            .installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_log_id.index)
            .unwrap_or_default();
        if committed.index > last_entry_index.max(snapshot_index) {
            return Err(RaftError::Storage(
                "committed index is ahead of persisted log and snapshot".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn matrixraft_validate_apply_snapshot_fence(record: &WalRecord) -> Result<(), RaftError> {
    let fence = &record.apply_snapshot_fence;
    let committed_index = record
        .hard_state
        .committed
        .as_ref()
        .map(|log_id| log_id.index)
        .unwrap_or_default();
    if fence.applied_index > committed_index {
        return Err(RaftError::Storage(
            "apply snapshot fence is ahead of committed index".to_string(),
        ));
    }
    if fence.commit_index != committed_index {
        return Err(RaftError::Storage(
            "apply snapshot fence commit index does not match hard state".to_string(),
        ));
    }
    if let Some(snapshot) = &record.installed_snapshot {
        if fence.installed_snapshot_index != snapshot.last_log_id.index {
            return Err(RaftError::Storage(
                "apply snapshot fence does not match installed snapshot".to_string(),
            ));
        }
        if fence.first_retained_log_index > 0
            && fence.first_retained_log_index <= snapshot.last_log_id.index
        {
            return Err(RaftError::Storage(
                "first retained log index overlaps installed snapshot".to_string(),
            ));
        }
    }
    Ok(())
}

/// Folds stored records back into whole-log records.
///
/// Records are stored so that the first one in a segment carries the whole
/// retained log and the rest carry only what was appended after it. Every
/// reader upstream of this function expects whole-log records, so folding
/// happens here, once, on the way out.
///
/// Checksums are verified against the bytes as stored, before folding, and the
/// fold stops at the first record that fails -- the same place a reader
/// scanning for a valid prefix would have stopped. The folded records are then
/// re-checksummed so they validate as the whole-log records they now are.
pub fn matrixraft_fold_wal_records(stored: &[WalRecord]) -> Vec<WalRecord> {
    let mut folded_entries: Vec<LogEntry> = Vec::new();
    let mut out = Vec::with_capacity(stored.len());
    for record in stored {
        if !matrixraft_wal_checksum_valid(record) {
            break;
        }
        if record.entries_are_delta {
            if let Some(first) = record.entries.first() {
                let cut =
                    folded_entries.partition_point(|entry| entry.log_id.index < first.log_id.index);
                folded_entries.truncate(cut);
            }
            folded_entries.extend(record.entries.iter().cloned());
        } else {
            folded_entries.clone_from(&record.entries);
        }
        let mut whole = record.clone();
        whole.entries.clone_from(&folded_entries);
        whole.entries_are_delta = false;
        whole.checksum = matrixraft_wal_checksum(&whole);
        out.push(whole);
    }
    out
}

/// Whether `entries` extends `covered` rather than diverging from it.
///
/// A delta is only sound when the record continues the log the segment already
/// describes. If the log was truncated by a conflict and rewritten, or compacted
/// so it now starts later than the segment does, the overlap no longer matches
/// and the record has to be stored whole.
pub fn matrixraft_wal_delta_base(
    entries: &[LogEntry],
    covered_first_index: LogIndex,
    covered_last_index: LogIndex,
    covered_last_term: Term,
) -> Option<usize> {
    let first = entries.first()?;
    if first.log_id.index > covered_first_index {
        // The log was compacted past where this segment starts; folding would
        // resurrect entries the node no longer holds.
        return None;
    }
    let position = entries
        .binary_search_by(|entry| entry.log_id.index.cmp(&covered_last_index))
        .ok()?;
    if entries[position].log_id.term != covered_last_term {
        // Same index, different term: the log diverged here.
        return None;
    }
    Some(position + 1)
}

pub fn matrixraft_recover_latest_wal_record(records: &[WalRecord]) -> Result<WalRecord, RaftError> {
    let valid_records = records
        .iter()
        .take_while(|record| matrixraft_wal_checksum_valid(record))
        .collect::<Vec<_>>();
    let Some(record) = valid_records
        .into_iter()
        .filter(|record| {
            matrixraft_validate_hard_state_persistence(record).is_ok()
                && matrixraft_validate_apply_snapshot_fence(record).is_ok()
        })
        .max_by_key(|record| {
            record
                .hard_state
                .committed
                .as_ref()
                .map(|log_id| log_id.index)
                .unwrap_or_default()
        })
    else {
        return Err(RaftError::Storage(
            "no valid WAL record survived recovery".to_string(),
        ));
    };
    Ok(record.clone())
}

pub fn matrixraft_wal_lifecycle_evidence(status: &WalLifecycleStatus) -> WalLifecycleEvidence {
    WalLifecycleEvidence {
        segment_lifecycle_present: status.segment_count > 0
            && status.active_segment_id >= status.first_retained_segment_id
            && status.last_retained_segment_id >= status.first_retained_segment_id,
        retained_range_present: status.first_retained_segment_id <= status.last_retained_segment_id,
        sequence_range_present: status.first_sequence <= status.last_sequence
            && status.total_records > 0,
        log_index_range_present: status.first_log_index <= status.last_log_index
            && status.last_log_index > 0,
        compaction_observed: status.released_segment_count > 0,
        slow_fsync_backpressure_observed: status.slow_fsync_backpressure_observed
            && status.slow_fsync_count > 0
            && status.max_fsync_elapsed_ms >= status.slow_fsync_threshold_ms,
        compaction_after_slow_fsync_observed: status.compacted_after_slow_fsync_count > 0
            && status.released_segment_count >= status.compacted_after_slow_fsync_count
            && status.slow_fsync_count > 0,
    }
}

pub fn matrixraft_wal_lifecycle_evidence_artifact(
    status: WalLifecycleStatus,
) -> WalLifecycleEvidenceArtifact {
    let evidence = matrixraft_wal_lifecycle_evidence(&status);
    WalLifecycleEvidenceArtifact {
        schema: "rustraft.wal_lifecycle_evidence.v1".to_string(),
        status,
        evidence,
    }
}

pub fn matrixraft_validate_wal_lifecycle_evidence_artifact(
    artifact: &WalLifecycleEvidenceArtifact,
) -> WalLifecycleEvidenceValidationReport {
    let schema_valid = artifact.schema == "rustraft.wal_lifecycle_evidence.v1";
    let recomputed = matrixraft_wal_lifecycle_evidence(&artifact.status);
    let segment_lifecycle_present =
        recomputed.segment_lifecycle_present && artifact.evidence.segment_lifecycle_present;
    let retained_range_present =
        recomputed.retained_range_present && artifact.evidence.retained_range_present;
    let sequence_range_present =
        recomputed.sequence_range_present && artifact.evidence.sequence_range_present;
    let log_index_range_present =
        recomputed.log_index_range_present && artifact.evidence.log_index_range_present;
    let compaction_observed =
        recomputed.compaction_observed && artifact.evidence.compaction_observed;
    let slow_fsync_backpressure_observed = recomputed.slow_fsync_backpressure_observed
        && artifact.evidence.slow_fsync_backpressure_observed;
    let compaction_after_slow_fsync_observed = recomputed.compaction_after_slow_fsync_observed
        && artifact.evidence.compaction_after_slow_fsync_observed;

    let mut missing = Vec::new();
    for (present, requirement) in [
        (schema_valid, "schema_valid"),
        (segment_lifecycle_present, "segment_lifecycle_present"),
        (retained_range_present, "retained_range_present"),
        (sequence_range_present, "sequence_range_present"),
        (log_index_range_present, "log_index_range_present"),
        (compaction_observed, "compaction_observed"),
        (
            slow_fsync_backpressure_observed,
            "slow_fsync_backpressure_observed",
        ),
        (
            compaction_after_slow_fsync_observed,
            "compaction_after_slow_fsync_observed",
        ),
    ] {
        if !present {
            missing.push(requirement.to_string());
        }
    }

    WalLifecycleEvidenceValidationReport {
        valid: missing.is_empty(),
        schema_valid,
        segment_lifecycle_present,
        retained_range_present,
        sequence_range_present,
        log_index_range_present,
        compaction_observed,
        slow_fsync_backpressure_observed,
        compaction_after_slow_fsync_observed,
        missing,
    }
}
