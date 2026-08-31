// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! BaselineRaft-derived operational evidence bundle helpers.

use serde::{Deserialize, Serialize};

use crate::{
    matrixraft_membership_semantics_evidence_artifact, matrixraft_read_safety_evidence_artifact,
    matrixraft_replication_pipeline_evidence_artifact,
    matrixraft_snapshot_lifecycle_evidence_artifact,
    matrixraft_validate_membership_semantics_evidence_artifact,
    matrixraft_validate_read_safety_evidence_artifact,
    matrixraft_validate_replication_pipeline_evidence_artifact,
    matrixraft_validate_snapshot_lifecycle_evidence_artifact,
    matrixraft_validate_wal_lifecycle_evidence_artifact,
    matrixraft_wal_lifecycle_evidence_artifact, MembershipSemanticsEvidenceArtifact, PeerProgress,
    PipelineLimits, ReadSafetyEvidenceArtifact, ReplicationPipelineEvidenceArtifact,
    SnapshotLifecycleEvidenceArtifact, WalLifecycleEvidenceArtifact, WalLifecycleStatus,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaselineRaftOperationalEvidenceBundle {
    pub schema: String,
    pub read_safety: ReadSafetyEvidenceArtifact,
    pub membership: MembershipSemanticsEvidenceArtifact,
    pub replication_pipeline: ReplicationPipelineEvidenceArtifact,
    pub snapshot_lifecycle: SnapshotLifecycleEvidenceArtifact,
    pub wal_lifecycle: WalLifecycleEvidenceArtifact,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaselineRaftOperationalEvidenceBundleValidationReport {
    pub valid: bool,
    pub schema_valid: bool,
    pub read_safety_valid: bool,
    pub membership_valid: bool,
    pub replication_pipeline_valid: bool,
    pub snapshot_lifecycle_valid: bool,
    pub wal_lifecycle_valid: bool,
    #[serde(default)]
    pub missing: Vec<String>,
}

pub fn matrixraft_baseline_raft_operational_evidence_bundle(
    pipeline_peers: Vec<PeerProgress>,
    pipeline_limits: PipelineLimits,
    snapshot_peers: Vec<PeerProgress>,
    send_snapshot_timeout_ms: u64,
    snapshot_max_inflights_replicate: u64,
    wal_status: WalLifecycleStatus,
) -> BaselineRaftOperationalEvidenceBundle {
    BaselineRaftOperationalEvidenceBundle {
        schema: "rustraft.baseline_raft_operational_evidence_bundle.v1".to_string(),
        read_safety: matrixraft_read_safety_evidence_artifact(),
        membership: matrixraft_membership_semantics_evidence_artifact(),
        replication_pipeline: matrixraft_replication_pipeline_evidence_artifact(
            pipeline_peers,
            pipeline_limits,
        ),
        snapshot_lifecycle: matrixraft_snapshot_lifecycle_evidence_artifact(
            snapshot_peers,
            send_snapshot_timeout_ms,
            snapshot_max_inflights_replicate,
        ),
        wal_lifecycle: matrixraft_wal_lifecycle_evidence_artifact(wal_status),
    }
}

pub fn matrixraft_validate_baseline_raft_operational_evidence_bundle(
    bundle: &BaselineRaftOperationalEvidenceBundle,
) -> BaselineRaftOperationalEvidenceBundleValidationReport {
    let schema_valid = bundle.schema == "rustraft.baseline_raft_operational_evidence_bundle.v1";
    let read_safety = matrixraft_validate_read_safety_evidence_artifact(&bundle.read_safety);
    let membership = matrixraft_validate_membership_semantics_evidence_artifact(&bundle.membership);
    let replication_pipeline =
        matrixraft_validate_replication_pipeline_evidence_artifact(&bundle.replication_pipeline);
    let snapshot_lifecycle =
        matrixraft_validate_snapshot_lifecycle_evidence_artifact(&bundle.snapshot_lifecycle);
    let wal_lifecycle = matrixraft_validate_wal_lifecycle_evidence_artifact(&bundle.wal_lifecycle);

    let mut missing = Vec::new();
    if !schema_valid {
        missing.push("schema_valid".to_string());
    }
    for (prefix, valid, fields) in [
        ("read_safety", read_safety.valid, read_safety.missing),
        ("membership", membership.valid, membership.missing),
        (
            "replication_pipeline",
            replication_pipeline.valid,
            replication_pipeline.missing,
        ),
        (
            "snapshot_lifecycle",
            snapshot_lifecycle.valid,
            snapshot_lifecycle.missing,
        ),
        ("wal_lifecycle", wal_lifecycle.valid, wal_lifecycle.missing),
    ] {
        if !valid && fields.is_empty() {
            missing.push(prefix.to_string());
        }
        missing.extend(fields.into_iter().map(|field| format!("{prefix}.{field}")));
    }

    BaselineRaftOperationalEvidenceBundleValidationReport {
        valid: missing.is_empty(),
        schema_valid,
        read_safety_valid: read_safety.valid,
        membership_valid: membership.valid,
        replication_pipeline_valid: replication_pipeline.valid,
        snapshot_lifecycle_valid: snapshot_lifecycle.valid,
        wal_lifecycle_valid: wal_lifecycle.valid,
        missing,
    }
}
