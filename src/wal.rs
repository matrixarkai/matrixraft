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

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalRecord {
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub hard_state: HardState,
    pub membership: Membership,
    #[serde(
        default,
        with = "wal_entry_payloads",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub entries: Vec<LogEntry>,
    /// When true, `entries` carries only what was appended since the previous
    /// record in the same segment rather than the whole retained log. The first
    /// record of every segment is always a full one, so a segment can be read
    /// without reading any other -- which is what lets whole-segment compaction
    /// stay as simple as it was.
    // Both of these already carry `#[serde(default)]`, so omitting them when
    // they hold that default is invisible to any reader, old or new -- no
    // format version and no delta bookkeeping. Measured over 199 consecutive
    // records, neither ever changed from its default, so this is 52 bytes off
    // every record for nothing.
    #[serde(default, skip_serializing_if = "is_false")]
    pub entries_are_delta: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    /// Every fsync this WAL has done, not only the slow ones. This is what says
    /// whether batching amortised anything.
    #[serde(default)]
    pub fsync_count: u64,
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
    matrixraft_fold_wal_record_iter(stored)
}

/// Fold from any iterator of records.
///
/// Recovery holds its records in per-segment vectors, so the slice form above
/// forced it to concatenate a clone of the whole WAL first -- payloads
/// included -- purely to get one contiguous slice. This takes the segments as
/// they are.
pub fn matrixraft_fold_wal_record_iter<'a, I>(stored: I) -> Vec<WalRecord>
where
    I: IntoIterator<Item = &'a WalRecord>,
{
    let mut folded_entries: Vec<LogEntry> = Vec::new();
    let mut out = Vec::new();
    for record in stored {
        if !matrixraft_wal_checksum_valid(record) {
            break;
        }
        fold_wal_entries_step(&mut folded_entries, record);
        out.push(whole_from_folded(record, &folded_entries));
    }
    out
}

/// Applies one stored record to the entries folded so far.
///
/// A whole record replaces them. A delta truncates back to where it starts and
/// then extends, so a record that rewrites a diverged tail wins over what it
/// replaces rather than being appended after it.
fn fold_wal_entries_step(folded: &mut Vec<LogEntry>, record: &WalRecord) {
    if record.entries_are_delta {
        if let Some(first) = record.entries.first() {
            let cut = folded.partition_point(|entry| entry.log_id.index < first.log_id.index);
            folded.truncate(cut);
        }
        folded.extend(record.entries.iter().cloned());
    } else {
        folded.clone_from(&record.entries);
    }
}

/// The whole-log record a stored record folds to, given the entries folded up
/// to and including it.
fn whole_from_folded(record: &WalRecord, folded: &[LogEntry]) -> WalRecord {
    let mut whole = record.clone();
    whole.entries.clear();
    whole.entries.extend_from_slice(folded);
    whole.entries_are_delta = false;
    whole.checksum = matrixraft_wal_checksum(&whole);
    whole
}

/// Folds `records` into the log they describe, without building a record per
/// step.
///
/// [`matrixraft_fold_wal_records`] answers "what did the log look like at each
/// record". This answers only "what does it look like at the end", which is what
/// compaction needs when it has to turn a delta back into a whole record, and
/// what a reopen needs to know what the retained records already cover. Both
/// share one `fold_wal_entries_step`, so the two cannot drift apart.
///
/// Like the record fold, this stops at the first record whose checksum does not
/// validate, which is where a reader scanning for a valid prefix would stop.
pub fn matrixraft_fold_wal_entries<'a, I>(records: I) -> Vec<LogEntry>
where
    I: IntoIterator<Item = &'a WalRecord>,
{
    let mut folded: Vec<LogEntry> = Vec::new();
    for record in records {
        if !matrixraft_wal_checksum_valid(record) {
            break;
        }
        fold_wal_entries_step(&mut folded, record);
    }
    folded
}

/// What recovery needs from a WAL: how many records survived, and the one
/// record it would restore from.
///
/// [`matrixraft_fold_wal_records`] builds a whole-log record for *every* stored
/// record, and each of those carries the log as it stood at that point. For an
/// N-record WAL that is about N^2/2 entries held at once -- a gigabyte at four
/// thousand records, and quadratic from there, which is enough to stop a node
/// restarting at all.
///
/// Recovery never needed all of them. It needs the surviving count and the
/// single latest valid record, so this folds in one streaming pass, keeping only
/// the log itself, and then replays to rebuild just the record it picked.
///
/// The record chosen is the same one [`matrixraft_recover_latest_wal_record`]
/// would choose: among records that pass hard-state and fence validation, the
/// one with the highest committed index, and the last of those on a tie.
pub fn matrixraft_recover_from_wal_records(stored: &[WalRecord]) -> (usize, Option<WalRecord>) {
    // One pass for the surviving prefix and each record's committed index. The
    // validity checks look at the folded entries, so they cannot be decided
    // here -- candidates are ranked now and verified by replay below.
    let mut surviving = 0usize;
    let mut candidates: Vec<(u64, usize)> = Vec::new();
    for (position, record) in stored.iter().enumerate() {
        if !matrixraft_wal_checksum_valid(record) {
            break;
        }
        surviving += 1;
        let committed = record
            .hard_state
            .committed
            .as_ref()
            .map(|log_id| log_id.index)
            .unwrap_or_default();
        candidates.push((committed, position));
    }
    // Highest committed index first, and the later position on a tie, which is
    // what `max_by_key` settles on.
    candidates.sort_by(|left, right| right.cmp(left));

    for (_, position) in candidates {
        let mut folded: Vec<LogEntry> = Vec::new();
        for record in stored.iter().take(position + 1) {
            fold_wal_entries_step(&mut folded, record);
        }
        let whole = whole_from_folded(&stored[position], &folded);
        if matrixraft_validate_hard_state_persistence(&whole).is_ok()
            && matrixraft_validate_apply_snapshot_fence(&whole).is_ok()
        {
            return (surviving, Some(whole));
        }
    }
    (surviving, None)
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

/// On-disk encoding for WAL entry payloads.
///
/// `serde_json` writes a `Vec<u8>` as an array of decimal numbers, so a byte
/// costs up to four characters on disk (`200,`). Payload dominates a WAL record
/// once it is more than a few dozen bytes -- 97.7% of a record carrying a 4 KiB
/// payload -- so that encoding sets the write amplification almost by itself.
/// Base64 costs 1.33 characters per byte instead of 4.
///
/// Reading accepts both forms. Records written before this change carry numeric
/// arrays, and they must still recover, so the deserializer takes either a
/// base64 string or the legacy array. Only writing changed.
///
/// This is deliberately local to the WAL record rather than applied to
/// `GenericLogEntry` itself: the same type is serialized for RPC and for the
/// JSON debug surfaces, where an array of numbers is the readable form and
/// nothing is paying for it by the megabyte.
pub(crate) mod wal_entry_payloads {
    use serde::de::{Deserializer, Error as DeError};
    use serde::ser::{SerializeSeq, Serializer};
    use serde::Deserialize;

    use crate::{LogEntry, LogId};

    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub(crate) fn encode(input: &[u8]) -> String {
        let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
        for chunk in input.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = *chunk.get(1).unwrap_or(&0) as u32;
            let b2 = *chunk.get(2).unwrap_or(&0) as u32;
            let triple = (b0 << 16) | (b1 << 8) | b2;
            out.push(ALPHABET[((triple >> 18) & 63) as usize] as char);
            out.push(ALPHABET[((triple >> 12) & 63) as usize] as char);
            out.push(if chunk.len() > 1 {
                ALPHABET[((triple >> 6) & 63) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                ALPHABET[(triple & 63) as usize] as char
            } else {
                '='
            });
        }
        out
    }

    fn sextet(byte: u8) -> Option<u32> {
        match byte {
            b'A'..=b'Z' => Some((byte - b'A') as u32),
            b'a'..=b'z' => Some((byte - b'a') as u32 + 26),
            b'0'..=b'9' => Some((byte - b'0') as u32 + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    pub(crate) fn decode(input: &str) -> Result<Vec<u8>, String> {
        let bytes = input.as_bytes();
        if bytes.len() % 4 != 0 {
            return Err(format!(
                "base64 length {} is not a multiple of four",
                bytes.len()
            ));
        }
        let chunk_count = bytes.len() / 4;
        let mut out = Vec::with_capacity(chunk_count * 3);
        for (chunk_index, chunk) in bytes.chunks(4).enumerate() {
            let is_last = chunk_index + 1 == chunk_count;
            let mut triple = 0u32;
            let mut padding = 0usize;
            for &byte in chunk {
                if byte == b'=' {
                    if !is_last {
                        return Err("base64 padding before the final quad".to_string());
                    }
                    padding += 1;
                    triple <<= 6;
                    continue;
                }
                if padding > 0 {
                    return Err("base64 padding inside a quad".to_string());
                }
                let value =
                    sextet(byte).ok_or_else(|| format!("invalid base64 byte {byte:#04x}"))?;
                triple = (triple << 6) | value;
            }
            if padding > 2 {
                return Err("base64 quad is entirely padding".to_string());
            }
            out.push((triple >> 16) as u8);
            if padding < 2 {
                out.push((triple >> 8) as u8);
            }
            if padding < 1 {
                out.push(triple as u8);
            }
        }
        Ok(out)
    }

    /// One entry as written: `payload` is base64 rather than a number array.
    #[derive(serde::Serialize)]
    struct EntryOut<'a> {
        log_id: &'a LogId,
        payload: String,
        is_command: bool,
    }

    /// One entry as read. `payload` takes either encoding so that WAL segments
    /// written before this change still recover.
    #[derive(Deserialize)]
    struct EntryIn {
        log_id: LogId,
        #[serde(default)]
        payload: PayloadIn,
        #[serde(default)]
        is_command: bool,
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum PayloadIn {
        /// Current form.
        Base64(String),
        /// The form `serde_json` produces for a `Vec<u8>`, written by earlier
        /// versions.
        Legacy(Vec<u8>),
    }

    impl Default for PayloadIn {
        fn default() -> Self {
            Self::Legacy(Vec::new())
        }
    }

    pub(crate) fn serialize<S>(entries: &[LogEntry], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(entries.len()))?;
        for entry in entries {
            seq.serialize_element(&EntryOut {
                log_id: &entry.log_id,
                payload: encode(&entry.payload),
                is_command: entry.is_command,
            })?;
        }
        seq.end()
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<LogEntry>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Vec::<EntryIn>::deserialize(deserializer)?;
        raw.into_iter()
            .map(|entry| {
                let payload = match entry.payload {
                    PayloadIn::Base64(text) => decode(&text).map_err(DeError::custom)?,
                    PayloadIn::Legacy(bytes) => bytes,
                };
                Ok(LogEntry {
                    log_id: entry.log_id,
                    payload,
                    is_command: entry.is_command,
                })
            })
            .collect()
    }
}

/// Base64 for a bare byte-buffer field, with the same legacy tolerance as
/// [`wal_entry_payloads`]: decode accepts the old number-array form, so data
/// written before this codec still reads.
///
/// Generic over the payload type so it can sit on a generic struct field. The
/// bounds are the codec's real requirements -- the bytes must be readable to
/// encode and constructible to decode -- and a payload type that is not
/// byte-like was never serializable as base64 anyway.
pub(crate) mod bytes_as_base64 {
    use serde::de::{Deserializer, Error as DeError};
    use serde::{Deserialize, Serializer};

    pub(crate) fn serialize<S, P>(data: &P, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        P: AsRef<[u8]>,
    {
        serializer.serialize_str(&super::wal_entry_payloads::encode(data.as_ref()))
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BytesIn {
        Text(String),
        Numbers(Vec<u8>),
    }

    pub(crate) fn deserialize<'de, D, P>(deserializer: D) -> Result<P, D::Error>
    where
        D: Deserializer<'de>,
        P: From<Vec<u8>>,
    {
        match BytesIn::deserialize(deserializer)? {
            BytesIn::Text(text) => super::wal_entry_payloads::decode(&text)
                .map(P::from)
                .map_err(DeError::custom),
            BytesIn::Numbers(bytes) => Ok(P::from(bytes)),
        }
    }
}

#[cfg(test)]
mod wal_entry_payload_codec_tests {
    use super::wal_entry_payloads::{decode, encode};

    /// RFC 4648 section 10 test vectors. Hand-rolled base64 is easy to get
    /// subtly wrong in the padded cases, which are exactly the short payloads a
    /// Raft log is full of.
    #[test]
    fn matches_the_rfc4648_vectors() {
        for (plain, encoded) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(encode(plain.as_bytes()), encoded, "encoding {plain:?}");
            assert_eq!(
                decode(encoded).expect("decodes"),
                plain.as_bytes(),
                "decoding {encoded:?}"
            );
        }
    }

    #[test]
    fn round_trips_every_length_and_every_byte_value() {
        // Lengths through several multiples of three cover all three padding
        // cases repeatedly; the byte pattern walks the whole 0..=255 range so
        // the sextet table is exercised end to end.
        for len in 0..=64usize {
            let payload: Vec<u8> = (0..len).map(|i| (i * 7 % 256) as u8).collect();
            let encoded = encode(&payload);
            assert_eq!(encoded.len() % 4, 0, "length {len} is not padded to a quad");
            assert_eq!(decode(&encoded).expect("decodes"), payload, "length {len}");
        }
        let all_bytes: Vec<u8> = (0..=255u8).collect();
        assert_eq!(decode(&encode(&all_bytes)).expect("decodes"), all_bytes);
    }

    #[test]
    fn rejects_malformed_input_rather_than_guessing() {
        // A truncated quad, a character outside the alphabet, and padding in
        // the wrong place all mean the segment is damaged. Recovery treats a
        // decode failure as a corrupt tail, so these must be errors and not
        // silently short reads.
        assert!(decode("Zm9").is_err(), "length not a multiple of four");
        assert!(decode("Zm9*").is_err(), "character outside the alphabet");
        assert!(decode("Zg==Zg==").is_err(), "padding before the final quad");
        assert!(decode("Z=g=").is_err(), "padding inside a quad");
        assert!(decode("====").is_err(), "quad that is entirely padding");
    }

    #[test]
    fn costs_a_third_of_what_a_number_array_costs() {
        // The reason this module exists. `serde_json` writes a byte >= 100 as
        // four characters ("200,"); base64 writes three bytes as four.
        let payload = vec![200u8; 4096];
        let as_numbers = serde_json::to_string(&payload).expect("array encodes");
        let as_base64 = serde_json::to_string(&encode(&payload)).expect("string encodes");
        // The ratio is 4 chars/byte against 4 chars per 3 bytes, so very close
        // to 3x -- close enough that asserting exactly 3x fails on the quotes
        // and brackets. Assert the conservative half, and report the real
        // figure so a regression shows the number rather than just a boolean.
        assert!(
            as_base64.len() * 2 < as_numbers.len(),
            "expected base64 ({}) to be well under half the array form ({}), ratio {:.2}x",
            as_base64.len(),
            as_numbers.len(),
            as_numbers.len() as f64 / as_base64.len() as f64
        );
    }
}
