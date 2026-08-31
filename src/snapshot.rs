// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Snapshot model, snapshot lifecycle, install validation, and persistent snapshot store API.

use serde::{Deserialize, Serialize};

pub use crate::{
    ApplySnapshotFence, GenericSnapshot, GenericSnapshotChunk, InstallSnapshotRequest,
    InstallSnapshotResponse, LogEntry, LogId, LogIndex, PeerProgress, PersistentRaftSnapshotStore,
    PersistentRaftSnapshotStoreOptions, RaftSnapshot, SnapshotChunk, SnapshotInstallState,
    SnapshotLifecycle, SnapshotLifecycleConfig, SnapshotLifecycleStatus, SnapshotMetadata,
    SnapshotSendState,
};

use crate::RaftError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotLifecycleEvidence {
    pub sender_lifecycle_present: bool,
    pub downloader_lifecycle_present: bool,
    pub retry_backpressure_present: bool,
    pub chunk_retry_present: bool,
    pub send_timeout_present: bool,
    pub rate_limit_present: bool,
    pub sustained_sender_load_present: bool,
    pub sustained_downloader_load_present: bool,
    #[serde(default)]
    pub sustained_sender_completion_present: bool,
    #[serde(default)]
    pub sustained_downloader_completion_present: bool,
    pub install_progress_present: bool,
    pub install_rollback_present: bool,
    pub membership_change_present: bool,
    pub rejoin_after_compacted_log_present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotLifecycleEvidenceArtifact {
    pub schema: String,
    pub send_snapshot_timeout_ms: u64,
    pub max_inflights_replicate: u64,
    pub peers: Vec<PeerProgress>,
    pub evidence: SnapshotLifecycleEvidence,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotLifecycleEvidenceValidationReport {
    pub valid: bool,
    pub schema_valid: bool,
    pub sender_lifecycle_present: bool,
    pub downloader_lifecycle_present: bool,
    pub retry_backpressure_present: bool,
    pub chunk_retry_present: bool,
    pub send_timeout_present: bool,
    pub rate_limit_present: bool,
    pub sustained_sender_load_present: bool,
    pub sustained_downloader_load_present: bool,
    pub sustained_sender_completion_present: bool,
    pub sustained_downloader_completion_present: bool,
    pub install_progress_present: bool,
    pub install_rollback_present: bool,
    pub membership_change_present: bool,
    pub rejoin_after_compacted_log_present: bool,
    #[serde(default)]
    pub missing: Vec<String>,
}

pub fn matrixraft_validate_snapshot_floor_log_matching(
    snapshot: &SnapshotMetadata,
    first_retained_log_index: LogIndex,
    prev_log_id: Option<&LogId>,
) -> Result<(), RaftError> {
    if first_retained_log_index > 0 && first_retained_log_index <= snapshot.last_log_id.index {
        return Err(RaftError::Storage(
            "first retained log index overlaps snapshot floor".to_string(),
        ));
    }
    if let Some(prev_log_id) = prev_log_id {
        if prev_log_id.index < snapshot.last_log_id.index {
            return Err(RaftError::Storage(
                "previous log id is below snapshot floor".to_string(),
            ));
        }
        if prev_log_id.index == snapshot.last_log_id.index
            && prev_log_id.term != snapshot.last_log_id.term
        {
            return Err(RaftError::Storage(
                "snapshot floor term does not match previous log id".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn matrixraft_validate_snapshot_install(
    snapshot: &RaftSnapshot,
    fence: &ApplySnapshotFence,
) -> Result<(), RaftError> {
    if fence.installed_snapshot_index != snapshot.meta.last_log_id.index {
        return Err(RaftError::Storage(
            "snapshot install fence does not match snapshot last log index".to_string(),
        ));
    }
    matrixraft_validate_snapshot_floor_log_matching(
        &snapshot.meta,
        fence.first_retained_log_index,
        Some(&snapshot.meta.last_log_id),
    )
}

pub fn matrixraft_validate_snapshot_tail_catchup(
    snapshot: &SnapshotMetadata,
    tail_entries: &[LogEntry],
) -> Result<(), RaftError> {
    for (offset, entry) in tail_entries.iter().enumerate() {
        let expected_index = snapshot.last_log_id.index + 1 + offset as u64;
        if entry.log_id.index <= snapshot.last_log_id.index {
            return Err(RaftError::Storage(
                "tail catch-up entry overlaps installed snapshot".to_string(),
            ));
        }
        if entry.log_id.index != expected_index {
            return Err(RaftError::Storage(
                "tail catch-up entries are not contiguous after snapshot".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn matrixraft_snapshot_lifecycle_evidence(
    peers: &[PeerProgress],
    send_snapshot_timeout_ms: u64,
    max_inflights_replicate: u64,
) -> SnapshotLifecycleEvidence {
    SnapshotLifecycleEvidence {
        sender_lifecycle_present: send_snapshot_timeout_ms > 0
            && peers
                .iter()
                .any(|peer| peer.snapshot_sending || peer.snapshot_send_attempts > 0),
        downloader_lifecycle_present: peers
            .iter()
            .any(|peer| peer.snapshot_installing || peer.snapshot_install_total_chunks > 0),
        retry_backpressure_present: peers.iter().any(|peer| {
            peer.snapshot_backpressure_rejections > 0
                || (max_inflights_replicate > 0
                    && peer.snapshot_send_attempts > max_inflights_replicate)
        }),
        chunk_retry_present: peers.iter().any(|peer| peer.snapshot_chunk_retry_count > 0),
        send_timeout_present: peers.iter().any(|peer| peer.snapshot_send_timeouts > 0),
        rate_limit_present: peers
            .iter()
            .any(|peer| peer.snapshot_rate_limit_rejections > 0),
        sustained_sender_load_present: peers.iter().any(|peer| {
            peer.snapshot_send_attempts > 0
                && peer.snapshot_install_total_chunks >= max_inflights_replicate.max(2)
                && (peer.snapshot_backpressure_rejections > 0
                    || peer.snapshot_rate_limit_rejections > 0
                    || peer.snapshot_chunk_retry_count > 0
                    || peer.snapshot_send_timeouts > 0
                    || peer.snapshot_install_progress_per_mille > 0)
        }),
        sustained_downloader_load_present: peers.iter().any(|peer| {
            peer.snapshot_install_total_chunks >= 4
                && (peer.snapshot_installing
                    || peer.snapshot_install_progress_per_mille > 0
                    || peer.snapshot_installed_index > 0)
                && (peer.snapshot_install_progress_per_mille > 0
                    || peer.snapshot_installed_index > 0)
        }),
        sustained_sender_completion_present: peers.iter().any(|peer| {
            peer.snapshot_send_attempts >= max_inflights_replicate.max(2)
                && peer.snapshot_install_total_chunks >= max_inflights_replicate.max(2)
                && peer.required_snapshot_index > 0
                && peer.acked_snapshot_index >= peer.required_snapshot_index
        }),
        sustained_downloader_completion_present: peers.iter().any(|peer| {
            peer.snapshot_install_total_chunks >= 4
                && peer.snapshot_installed_index > 0
                && peer.snapshot_install_progress_per_mille >= 1000
                && !peer.snapshot_installing
        }),
        install_progress_present: peers.iter().any(|peer| {
            peer.snapshot_installed_index > 0 || peer.snapshot_install_progress_per_mille > 0
        }),
        install_rollback_present: peers
            .iter()
            .any(|peer| peer.snapshot_install_rolled_back > 0),
        membership_change_present: peers
            .iter()
            .any(|peer| peer.snapshot_during_membership_change),
        rejoin_after_compacted_log_present: peers
            .iter()
            .any(|peer| peer.snapshot_rejoin_after_compacted_log),
    }
}

pub fn matrixraft_snapshot_lifecycle_evidence_artifact(
    peers: Vec<PeerProgress>,
    send_snapshot_timeout_ms: u64,
    max_inflights_replicate: u64,
) -> SnapshotLifecycleEvidenceArtifact {
    let evidence = matrixraft_snapshot_lifecycle_evidence(
        &peers,
        send_snapshot_timeout_ms,
        max_inflights_replicate,
    );
    SnapshotLifecycleEvidenceArtifact {
        schema: "rustraft.snapshot_lifecycle_evidence.v1".to_string(),
        send_snapshot_timeout_ms,
        max_inflights_replicate,
        peers,
        evidence,
    }
}

pub fn matrixraft_validate_snapshot_lifecycle_evidence_artifact(
    artifact: &SnapshotLifecycleEvidenceArtifact,
) -> SnapshotLifecycleEvidenceValidationReport {
    let schema_valid = artifact.schema == "rustraft.snapshot_lifecycle_evidence.v1";
    let recomputed = matrixraft_snapshot_lifecycle_evidence(
        &artifact.peers,
        artifact.send_snapshot_timeout_ms,
        artifact.max_inflights_replicate,
    );
    let sender_lifecycle_present =
        recomputed.sender_lifecycle_present && artifact.evidence.sender_lifecycle_present;
    let downloader_lifecycle_present =
        recomputed.downloader_lifecycle_present && artifact.evidence.downloader_lifecycle_present;
    let retry_backpressure_present =
        recomputed.retry_backpressure_present && artifact.evidence.retry_backpressure_present;
    let chunk_retry_present =
        recomputed.chunk_retry_present && artifact.evidence.chunk_retry_present;
    let send_timeout_present =
        recomputed.send_timeout_present && artifact.evidence.send_timeout_present;
    let rate_limit_present = recomputed.rate_limit_present && artifact.evidence.rate_limit_present;
    let sustained_sender_load_present =
        recomputed.sustained_sender_load_present && artifact.evidence.sustained_sender_load_present;
    let sustained_downloader_load_present = recomputed.sustained_downloader_load_present
        && artifact.evidence.sustained_downloader_load_present;
    let sustained_sender_completion_present = recomputed.sustained_sender_completion_present
        && artifact.evidence.sustained_sender_completion_present;
    let sustained_downloader_completion_present = recomputed
        .sustained_downloader_completion_present
        && artifact.evidence.sustained_downloader_completion_present;
    let install_progress_present =
        recomputed.install_progress_present && artifact.evidence.install_progress_present;
    let install_rollback_present =
        recomputed.install_rollback_present && artifact.evidence.install_rollback_present;
    let membership_change_present =
        recomputed.membership_change_present && artifact.evidence.membership_change_present;
    let rejoin_after_compacted_log_present = recomputed.rejoin_after_compacted_log_present
        && artifact.evidence.rejoin_after_compacted_log_present;

    let mut missing = Vec::new();
    for (present, requirement) in [
        (schema_valid, "schema_valid"),
        (sender_lifecycle_present, "sender_lifecycle_present"),
        (downloader_lifecycle_present, "downloader_lifecycle_present"),
        (retry_backpressure_present, "retry_backpressure_present"),
        (chunk_retry_present, "chunk_retry_present"),
        (send_timeout_present, "send_timeout_present"),
        (rate_limit_present, "rate_limit_present"),
        (
            sustained_sender_load_present,
            "sustained_sender_load_present",
        ),
        (
            sustained_downloader_load_present,
            "sustained_downloader_load_present",
        ),
        (
            sustained_sender_completion_present,
            "sustained_sender_completion_present",
        ),
        (
            sustained_downloader_completion_present,
            "sustained_downloader_completion_present",
        ),
        (install_progress_present, "install_progress_present"),
        (install_rollback_present, "install_rollback_present"),
        (membership_change_present, "membership_change_present"),
        (
            rejoin_after_compacted_log_present,
            "rejoin_after_compacted_log_present",
        ),
    ] {
        if !present {
            missing.push(requirement.to_string());
        }
    }

    SnapshotLifecycleEvidenceValidationReport {
        valid: missing.is_empty(),
        schema_valid,
        sender_lifecycle_present,
        downloader_lifecycle_present,
        retry_backpressure_present,
        chunk_retry_present,
        send_timeout_present,
        rate_limit_present,
        sustained_sender_load_present,
        sustained_downloader_load_present,
        sustained_sender_completion_present,
        sustained_downloader_completion_present,
        install_progress_present,
        install_rollback_present,
        membership_change_present,
        rejoin_after_compacted_log_present,
        missing,
    }
}
