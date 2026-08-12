// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// read-safety evidence and runtime decision types.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftTransportValidationReport {
    pub rpc: String,
    pub valid: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadSafetyDecision {
    pub safe: bool,
    pub read_index: RustRaftLogIndex,
    pub lease_read: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadQuorumReport {
    pub required: u64,
    pub live_voters: Vec<RustRaftNodeId>,
    pub live_witnesses: Vec<RustRaftNodeId>,
    pub acknowledgements: Vec<RustRaftNodeId>,
    pub reached: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppliedIndexFenceReport {
    pub min_commit_index: RustRaftLogIndex,
    pub commit_index: RustRaftLogIndex,
    pub applied_index: RustRaftLogIndex,
    pub passed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLeaseReadEligibilityReport {
    pub node_id: RustRaftNodeId,
    pub leader_id: Option<RustRaftNodeId>,
    pub config_enabled: bool,
    pub requester_is_leader: bool,
    pub leader_lease_valid: bool,
    pub applied_index_fence_passed: bool,
    pub eligible: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftBoundedStaleReadReport {
    pub node_id: RustRaftNodeId,
    pub leader_id: RustRaftNodeId,
    pub node_commit_index: RustRaftLogIndex,
    pub leader_commit_index: RustRaftLogIndex,
    pub lag: RustRaftLogIndex,
    pub max_stale_index_lag: RustRaftLogIndex,
    pub allowed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadPathReport {
    pub safe: bool,
    pub read_index: RustRaftLogIndex,
    pub lease_read: bool,
    pub stale_leader_rejected: bool,
    pub reason: String,
    pub quorum: RustRaftReadQuorumReport,
    pub applied_index_fence: RustRaftAppliedIndexFenceReport,
    pub lease_read_eligibility: RustRaftLeaseReadEligibilityReport,
    pub bounded_stale: Option<RustRaftBoundedStaleReadReport>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftReadSafetyOperation {
    ReadIndex,
    LeaseRead,
    BoundedStaleRead,
    Write,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadSafetyRuntimeInput {
    pub operation: RustRaftReadSafetyOperation,
    pub node_id: u64,
    pub leader_id: u64,
    pub node_alive: bool,
    pub role_can_serve_data: bool,
    pub leader_lease_valid: bool,
    pub has_majority: bool,
    pub node_commit_index: u64,
    pub leader_commit_index: u64,
    pub max_stale_index_lag: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadSafetyRuntimeDecision {
    pub allowed: bool,
    pub read_index: u64,
    pub reason: String,
    pub stale_leader_lease_rejected: bool,
    pub lagging_follower_read_rejected: bool,
    pub stale_follower_write_rejected: bool,
    pub minority_partition_read_rejected: bool,
    pub minority_partition_write_rejected: bool,
    pub healed_follower_catchup_observed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadSafetyEvidenceArtifact {
    pub schema: String,
    pub stale_leader_lease: RustRaftReadSafetyRuntimeDecision,
    pub lagging_follower_read: RustRaftReadSafetyRuntimeDecision,
    pub stale_follower_write: RustRaftReadSafetyRuntimeDecision,
    pub bounded_stale_read_accept: RustRaftReadSafetyRuntimeDecision,
    pub bounded_stale_read_reject: RustRaftReadSafetyRuntimeDecision,
    pub minority_partition_read: RustRaftReadSafetyRuntimeDecision,
    pub minority_partition_write: RustRaftReadSafetyRuntimeDecision,
    pub healed_follower_catchup: RustRaftReadSafetyRuntimeDecision,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadSafetyEvidenceValidationReport {
    pub valid: bool,
    pub schema_valid: bool,
    pub stale_leader_lease_rejected: bool,
    pub lagging_follower_read_rejected: bool,
    pub stale_follower_write_rejected: bool,
    pub bounded_stale_read_accepted: bool,
    pub bounded_stale_read_rejected: bool,
    pub minority_partition_read_rejected: bool,
    pub minority_partition_write_rejected: bool,
    pub healed_follower_catchup_observed: bool,
    #[serde(default)]
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLearnerPromotionDecision {
    pub promotable: bool,
    pub learner_id: u64,
    pub learner_match_index: u64,
    pub required_match_index: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipSemanticsEvidenceArtifact {
    pub schema: String,
    pub learner_add: RustRaftMembershipTransitionEvidence,
    pub learner_catchup: RustRaftLearnerPromotionDecision,
    pub learner_promote: RustRaftMembershipTransitionEvidence,
    pub leader_transfer: RustRaftMembershipTransitionEvidence,
    pub voter_remove: RustRaftMembershipTransitionEvidence,
    pub auto_promote_learner_observed: bool,
    pub auto_promote_blocked_by_pending_joint_observed: bool,
    pub pending_joint_consensus_restart_observed: bool,
    pub pending_joint_consensus_restart_recovered: bool,
    pub witness_role_supported: bool,
    pub witness_promotion_rejected_observed: bool,
    pub witness_role_blocker: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipSemanticsEvidenceValidationReport {
    pub valid: bool,
    pub schema_valid: bool,
    pub learner_added: bool,
    pub learner_caught_up: bool,
    pub learner_promoted: bool,
    pub leader_transferred: bool,
    pub voter_removed: bool,
    pub auto_promote_learner_observed: bool,
    pub auto_promote_blocked_by_pending_joint_observed: bool,
    pub pending_joint_consensus_restart_observed: bool,
    pub pending_joint_consensus_restart_recovered: bool,
    pub witness_promotion_rejected_observed: bool,
    pub witness_role_accounted_for: bool,
    #[serde(default)]
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendSafetyDecision {
    pub accepted: bool,
    pub rejected_compacted_entry: bool,
    pub reason: String,
}
