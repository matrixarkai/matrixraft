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
    /// True when `read_safety` is byte-for-byte the built-in conformance
    /// vector, i.e. nobody attached evidence from a running cluster.
    ///
    /// `read_safety_valid` says the artifact is well formed; it does not say
    /// where it came from. The vector runs real decision logic over fixed
    /// inputs, so it does test something -- but it tests this crate, not the
    /// caller's deployment.
    #[serde(default)]
    pub read_safety_is_reference: bool,
    /// True when `membership` is byte-for-byte the built-in reference artifact.
    ///
    /// This one matters more than the read-safety flag: the reference artifact
    /// hardcodes its conclusions rather than deriving them, so
    /// `membership_valid` is true for every caller on every cluster whenever
    /// this flag is set. Treat `membership_valid: true` as evidence about a
    /// deployment only when this is false.
    #[serde(default)]
    pub membership_is_reference: bool,
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
    // Every parameter above feeds the last three artifacts. The first two have
    // no inputs and so cannot describe the caller's cluster: they are the
    // built-in reference artifacts. Callers holding real read-safety and
    // membership evidence should use
    // [`matrixraft_baseline_raft_operational_evidence_bundle_from_artifacts`],
    // which takes them, rather than shipping a bundle whose membership half is
    // fixed. The validation report flags which halves were the reference.
    matrixraft_baseline_raft_operational_evidence_bundle_from_artifacts(
        matrixraft_read_safety_evidence_artifact(),
        matrixraft_membership_semantics_evidence_artifact(),
        pipeline_peers,
        pipeline_limits,
        snapshot_peers,
        send_snapshot_timeout_ms,
        snapshot_max_inflights_replicate,
        wal_status,
    )
}

/// Build the bundle from read-safety and membership evidence the caller has
/// actually observed, rather than the built-in reference artifacts.
///
/// The other three artifacts are derived from the runtime state passed in, as
/// they already were.
#[allow(clippy::too_many_arguments)]
pub fn matrixraft_baseline_raft_operational_evidence_bundle_from_artifacts(
    read_safety: ReadSafetyEvidenceArtifact,
    membership: MembershipSemanticsEvidenceArtifact,
    pipeline_peers: Vec<PeerProgress>,
    pipeline_limits: PipelineLimits,
    snapshot_peers: Vec<PeerProgress>,
    send_snapshot_timeout_ms: u64,
    snapshot_max_inflights_replicate: u64,
    wal_status: WalLifecycleStatus,
) -> BaselineRaftOperationalEvidenceBundle {
    BaselineRaftOperationalEvidenceBundle {
        schema: "rustraft.baseline_raft_operational_evidence_bundle.v1".to_string(),
        read_safety,
        membership,
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

    // Whether each of the two input-free artifacts is still the built-in one.
    // `*_valid` only says an artifact is well formed; for the membership
    // reference, well formed is unconditional, because the producer hardcodes
    // exactly the fields the validator checks.
    let read_safety_is_reference = bundle.read_safety == matrixraft_read_safety_evidence_artifact();
    let membership_is_reference =
        bundle.membership == matrixraft_membership_semantics_evidence_artifact();

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
        read_safety_is_reference,
        membership_is_reference,
        missing,
    }
}
