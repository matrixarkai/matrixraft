// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// read-safety evidence and runtime decision types.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportValidationReport {
    pub rpc: String,
    pub valid: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadSafetyDecision {
    pub safe: bool,
    pub read_index: LogIndex,
    pub lease_read: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadQuorumReport {
    pub required: u64,
    pub live_voters: Vec<NodeId>,
    pub live_witnesses: Vec<NodeId>,
    pub acknowledgements: Vec<NodeId>,
    pub reached: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppliedIndexFenceReport {
    pub min_commit_index: LogIndex,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub passed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaseReadEligibilityReport {
    pub node_id: NodeId,
    pub leader_id: Option<NodeId>,
    pub config_enabled: bool,
    pub requester_is_leader: bool,
    pub leader_lease_valid: bool,
    pub applied_index_fence_passed: bool,
    pub eligible: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoundedStaleReadReport {
    pub node_id: NodeId,
    pub leader_id: NodeId,
    pub node_commit_index: LogIndex,
    pub leader_commit_index: LogIndex,
    pub lag: LogIndex,
    pub max_stale_index_lag: LogIndex,
    pub allowed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadPathReport {
    pub safe: bool,
    pub read_index: LogIndex,
    pub lease_read: bool,
    pub stale_leader_rejected: bool,
    pub reason: String,
    pub quorum: ReadQuorumReport,
    pub applied_index_fence: AppliedIndexFenceReport,
    pub lease_read_eligibility: LeaseReadEligibilityReport,
    pub bounded_stale: Option<BoundedStaleReadReport>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadSafetyOperation {
    ReadIndex,
    LeaseRead,
    BoundedStaleRead,
    Write,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadSafetyRuntimeInput {
    pub operation: ReadSafetyOperation,
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
pub struct ReadSafetyRuntimeDecision {
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
pub struct ReadSafetyEvidenceArtifact {
    pub schema: String,
    pub stale_leader_lease: ReadSafetyRuntimeDecision,
    pub lagging_follower_read: ReadSafetyRuntimeDecision,
    pub stale_follower_write: ReadSafetyRuntimeDecision,
    pub bounded_stale_read_accept: ReadSafetyRuntimeDecision,
    pub bounded_stale_read_reject: ReadSafetyRuntimeDecision,
    pub minority_partition_read: ReadSafetyRuntimeDecision,
    pub minority_partition_write: ReadSafetyRuntimeDecision,
    pub healed_follower_catchup: ReadSafetyRuntimeDecision,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadSafetyEvidenceValidationReport {
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
pub struct LearnerPromotionDecision {
    pub promotable: bool,
    pub learner_id: u64,
    pub learner_match_index: u64,
    pub required_match_index: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipSemanticsEvidenceArtifact {
    pub schema: String,
    pub learner_add: MembershipTransitionEvidence,
    pub learner_catchup: LearnerPromotionDecision,
    pub learner_promote: MembershipTransitionEvidence,
    pub leader_transfer: MembershipTransitionEvidence,
    pub voter_remove: MembershipTransitionEvidence,
    pub auto_promote_learner_observed: bool,
    pub auto_promote_blocked_by_pending_joint_observed: bool,
    pub pending_joint_consensus_restart_observed: bool,
    pub pending_joint_consensus_restart_recovered: bool,
    pub witness_role_supported: bool,
    pub witness_promotion_rejected_observed: bool,
    pub witness_role_blocker: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipSemanticsEvidenceValidationReport {
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
pub struct AppendSafetyDecision {
    pub accepted: bool,
    pub rejected_compacted_entry: bool,
    pub reason: String,
}
