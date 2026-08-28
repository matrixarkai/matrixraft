// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WAL records, segmented recovery, checksums, and persistent local WAL API.

use serde::{Deserialize, Serialize};

pub use crate::{
    matrixraft_durability_parity_report, FileRaftWal, LocalRaftWal, PersistentRaftWal,
    PersistentRaftWalOptions, RaftHardState, RustRaftDurabilityParityReport, RustRaftHardState,
};

use crate::{
    RustRaftApplySnapshotFence, RustRaftError, RustRaftGroupId, RustRaftLogEntry, RustRaftLogIndex,
    RustRaftMembership, RustRaftNodeId, RustRaftSnapshotMeta,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalChecksumFormat {
    pub algorithm: String,
    pub encoding: String,
    pub covered_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftLogRetainedRange {
    pub first_log_index: RustRaftLogIndex,
    pub last_log_index: RustRaftLogIndex,
    pub first_segment_id: u64,
    pub last_segment_id: u64,
    pub record_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalSegmentIndex {
    pub segment_id: u64,
    pub first_log_index: RustRaftLogIndex,
    pub last_log_index: RustRaftLogIndex,
    pub record_count: u64,
    pub sealed: bool,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalWriteReport {
    pub segment_id: u64,
    pub log_index: RustRaftLogIndex,
    pub checksum: String,
    pub checksum_format: RaftWalChecksumFormat,
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
    pub retained_range: RaftLogRetainedRange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalCompactionReport {
    pub requested_log_index: RustRaftLogIndex,
    pub released_segments: u64,
    pub retained_range: RaftLogRetainedRange,
    pub fence_valid: bool,
    pub blocker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftWalRecord {
    pub group_id: RustRaftGroupId,
    pub node_id: RustRaftNodeId,
    pub hard_state: RustRaftHardState,
    pub membership: RustRaftMembership,
    #[serde(default)]
    pub entries: Vec<RustRaftLogEntry>,
    #[serde(default)]
    pub installed_snapshot: Option<RustRaftSnapshotMeta>,
    pub apply_snapshot_fence: RustRaftApplySnapshotFence,
    pub checksum: String,
}

pub type RaftWalRecord = RustRaftWalRecord;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalSegment {
    pub segment_id: u64,
    pub first_index: RustRaftLogIndex,
    pub last_index: RustRaftLogIndex,
    pub records: Vec<RaftWalRecord>,
    pub sealed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalRecoveryReport {
    pub recovered: Option<RaftWalRecord>,
    pub truncated_corrupt_tail: bool,
    pub surviving_records: usize,
    pub removed_records: usize,
    #[serde(default)]
    pub segments_scanned: u64,
    #[serde(default)]
    pub checksum_format: Option<RaftWalChecksumFormat>,
    #[serde(default)]
    pub retained_range: Option<RaftLogRetainedRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftWalLifecycleStatus {
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
pub struct RustRaftWalLifecycleEvidence {
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
pub struct RustRaftWalLifecycleEvidenceArtifact {
    pub schema: String,
    pub status: RustRaftWalLifecycleStatus,
    pub evidence: RustRaftWalLifecycleEvidence,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftWalLifecycleEvidenceValidationReport {
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

pub fn matrixraft_wal_checksum(record: &RaftWalRecord) -> String {
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

pub fn matrixraft_wal_checksum_format() -> RaftWalChecksumFormat {
    RaftWalChecksumFormat {
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
            "installed_snapshot.last_log_id".to_string(),
            "apply_snapshot_fence".to_string(),
        ],
    }
}

pub fn matrixraft_wal_checksum_valid(record: &RaftWalRecord) -> bool {
    record.checksum == matrixraft_wal_checksum(record)
}

pub fn matrixraft_validate_hard_state_persistence(
    record: &RustRaftWalRecord,
) -> Result<(), RustRaftError> {
    if let Some(committed) = &record.hard_state.committed {
        if committed.term > record.hard_state.current_term {
            return Err(RustRaftError::Storage(
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
            return Err(RustRaftError::Storage(
                "committed index is ahead of persisted log and snapshot".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn matrixraft_validate_apply_snapshot_fence(
    record: &RustRaftWalRecord,
) -> Result<(), RustRaftError> {
    let fence = &record.apply_snapshot_fence;
    let committed_index = record
        .hard_state
        .committed
        .as_ref()
        .map(|log_id| log_id.index)
        .unwrap_or_default();
    if fence.applied_index > committed_index {
        return Err(RustRaftError::Storage(
            "apply snapshot fence is ahead of committed index".to_string(),
        ));
    }
    if fence.commit_index != committed_index {
        return Err(RustRaftError::Storage(
            "apply snapshot fence commit index does not match hard state".to_string(),
        ));
    }
    if let Some(snapshot) = &record.installed_snapshot {
        if fence.installed_snapshot_index != snapshot.last_log_id.index {
            return Err(RustRaftError::Storage(
                "apply snapshot fence does not match installed snapshot".to_string(),
            ));
        }
        if fence.first_retained_log_index > 0
            && fence.first_retained_log_index <= snapshot.last_log_id.index
        {
            return Err(RustRaftError::Storage(
                "first retained log index overlaps installed snapshot".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn matrixraft_recover_latest_wal_record(
    records: &[RustRaftWalRecord],
) -> Result<RustRaftWalRecord, RustRaftError> {
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
        return Err(RustRaftError::Storage(
            "no valid WAL record survived recovery".to_string(),
        ));
    };
    Ok(record.clone())
}

pub fn matrixraft_wal_lifecycle_evidence(
    status: &RustRaftWalLifecycleStatus,
) -> RustRaftWalLifecycleEvidence {
    RustRaftWalLifecycleEvidence {
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
    status: RustRaftWalLifecycleStatus,
) -> RustRaftWalLifecycleEvidenceArtifact {
    let evidence = matrixraft_wal_lifecycle_evidence(&status);
    RustRaftWalLifecycleEvidenceArtifact {
        schema: "rustraft.wal_lifecycle_evidence.v1".to_string(),
        status,
        evidence,
    }
}

pub fn matrixraft_validate_wal_lifecycle_evidence_artifact(
    artifact: &RustRaftWalLifecycleEvidenceArtifact,
) -> RustRaftWalLifecycleEvidenceValidationReport {
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

    RustRaftWalLifecycleEvidenceValidationReport {
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
