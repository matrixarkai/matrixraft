// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ReferenceRaft-derived operational evidence bundle helpers.

use serde::{Deserialize, Serialize};

use crate::{
    rustraft_membership_semantics_evidence_artifact, rustraft_read_safety_evidence_artifact,
    rustraft_replication_pipeline_evidence_artifact, rustraft_snapshot_lifecycle_evidence_artifact,
    rustraft_validate_membership_semantics_evidence_artifact,
    rustraft_validate_read_safety_evidence_artifact,
    rustraft_validate_replication_pipeline_evidence_artifact,
    rustraft_validate_snapshot_lifecycle_evidence_artifact,
    rustraft_validate_wal_lifecycle_evidence_artifact, rustraft_wal_lifecycle_evidence_artifact,
    RustRaftMembershipSemanticsEvidenceArtifact, RustRaftPeerPipelineStatus,
    RustRaftPipelineLimits, RustRaftReadSafetyEvidenceArtifact,
    RustRaftReplicationPipelineEvidenceArtifact, RustRaftSnapshotLifecycleEvidenceArtifact,
    RustRaftWalLifecycleEvidenceArtifact, RustRaftWalLifecycleStatus,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReferenceRaftOperationalEvidenceBundle {
    pub schema: String,
    pub read_safety: RustRaftReadSafetyEvidenceArtifact,
    pub membership: RustRaftMembershipSemanticsEvidenceArtifact,
    pub replication_pipeline: RustRaftReplicationPipelineEvidenceArtifact,
    pub snapshot_lifecycle: RustRaftSnapshotLifecycleEvidenceArtifact,
    pub wal_lifecycle: RustRaftWalLifecycleEvidenceArtifact,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReferenceRaftOperationalEvidenceBundleValidationReport {
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

pub fn rustraft_reference_raft_operational_evidence_bundle(
    pipeline_peers: Vec<RustRaftPeerPipelineStatus>,
    pipeline_limits: RustRaftPipelineLimits,
    snapshot_peers: Vec<RustRaftPeerPipelineStatus>,
    send_snapshot_timeout_ms: u64,
    snapshot_max_inflights_replicate: u64,
    wal_status: RustRaftWalLifecycleStatus,
) -> RustRaftReferenceRaftOperationalEvidenceBundle {
    RustRaftReferenceRaftOperationalEvidenceBundle {
        schema: "rustraft.reference_raft_operational_evidence_bundle.v1".to_string(),
        read_safety: rustraft_read_safety_evidence_artifact(),
        membership: rustraft_membership_semantics_evidence_artifact(),
        replication_pipeline: rustraft_replication_pipeline_evidence_artifact(
            pipeline_peers,
            pipeline_limits,
        ),
        snapshot_lifecycle: rustraft_snapshot_lifecycle_evidence_artifact(
            snapshot_peers,
            send_snapshot_timeout_ms,
            snapshot_max_inflights_replicate,
        ),
        wal_lifecycle: rustraft_wal_lifecycle_evidence_artifact(wal_status),
    }
}

pub fn rustraft_validate_reference_raft_operational_evidence_bundle(
    bundle: &RustRaftReferenceRaftOperationalEvidenceBundle,
) -> RustRaftReferenceRaftOperationalEvidenceBundleValidationReport {
    let schema_valid = bundle.schema == "rustraft.reference_raft_operational_evidence_bundle.v1";
    let read_safety = rustraft_validate_read_safety_evidence_artifact(&bundle.read_safety);
    let membership = rustraft_validate_membership_semantics_evidence_artifact(&bundle.membership);
    let replication_pipeline =
        rustraft_validate_replication_pipeline_evidence_artifact(&bundle.replication_pipeline);
    let snapshot_lifecycle =
        rustraft_validate_snapshot_lifecycle_evidence_artifact(&bundle.snapshot_lifecycle);
    let wal_lifecycle = rustraft_validate_wal_lifecycle_evidence_artifact(&bundle.wal_lifecycle);

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

    RustRaftReferenceRaftOperationalEvidenceBundleValidationReport {
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
