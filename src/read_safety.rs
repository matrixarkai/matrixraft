// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Read-index, lease-read, bounded-stale, and partition safety helpers.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::{
    AppendEntriesRequest, AppendSafetyDecision, AppliedIndexFenceReport, BoundedStaleReadReport,
    LeaseReadEligibilityReport, LogIndex, NodeId, ReadIndexRequest, ReadSafetyDecision,
    ReadSafetyEvidenceArtifact, ReadSafetyEvidenceValidationReport, ReadSafetyOperation,
    ReadSafetyRuntimeDecision, ReadSafetyRuntimeInput, StateRole, StatusSnapshot,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingReadIndex {
    pub request: ReadIndexRequest,
    pub read_index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingReadIndexResult {
    pub request: ReadIndexRequest,
    pub read_index: LogIndex,
    pub applied_index: LogIndex,
    pub ready: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingReadIndexQueue {
    pending: VecDeque<PendingReadIndex>,
}

impl PendingReadIndexQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, request: ReadIndexRequest, read_index: LogIndex) -> usize {
        self.pending.push_back(PendingReadIndex {
            request,
            read_index,
        });
        self.pending.len()
    }

    pub fn notify_applied(&mut self, applied_index: LogIndex) -> Vec<PendingReadIndexResult> {
        let mut ready = Vec::new();
        while self
            .pending
            .front()
            .map(|pending| applied_index >= pending.fence_index())
            .unwrap_or(false)
        {
            let pending = self.pending.pop_front().expect("front is ready");
            ready.push(PendingReadIndexResult {
                request: pending.request,
                read_index: pending.read_index,
                applied_index,
                ready: true,
                reason: "read_index_applied".to_string(),
            });
        }
        ready
    }

    pub fn release_all(
        &mut self,
        applied_index: LogIndex,
        reason: impl Into<String>,
    ) -> Vec<PendingReadIndexResult> {
        let reason = reason.into();
        self.pending
            .drain(..)
            .map(|pending| PendingReadIndexResult {
                request: pending.request,
                read_index: pending.read_index,
                applied_index,
                ready: false,
                reason: reason.clone(),
            })
            .collect()
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

impl PendingReadIndex {
    pub fn fence_index(&self) -> LogIndex {
        self.read_index.max(self.request.min_commit_index)
    }
}

pub fn matrixraft_append_safety_decision(
    first_retained_log_index: u64,
    snapshot_index: u64,
    request: &AppendEntriesRequest,
) -> AppendSafetyDecision {
    let prev_index = request.prev_log_id.as_ref().map(|id| id.index).unwrap_or(0);
    if prev_index > 0 && prev_index < first_retained_log_index && prev_index <= snapshot_index {
        return AppendSafetyDecision {
            accepted: false,
            rejected_compacted_entry: true,
            reason: "prev_log_compacted".to_string(),
        };
    }
    if request
        .entries
        .iter()
        .any(|entry| entry.log_id.index < first_retained_log_index)
    {
        return AppendSafetyDecision {
            accepted: false,
            rejected_compacted_entry: true,
            reason: "entry_compacted".to_string(),
        };
    }
    AppendSafetyDecision {
        accepted: true,
        rejected_compacted_entry: false,
        reason: "accepted".to_string(),
    }
}

pub fn matrixraft_read_safety_decision(
    status: &StatusSnapshot,
    request: &ReadIndexRequest,
) -> ReadSafetyDecision {
    if status.group_id != request.group_id {
        return ReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "group_mismatch".to_string(),
        };
    }
    if !matches!(status.role, StateRole::Leader) {
        return ReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "not_leader".to_string(),
        };
    }
    if status.applied_index < request.min_commit_index {
        return ReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "apply_lag".to_string(),
        };
    }
    ReadSafetyDecision {
        safe: true,
        read_index: status.commit_index,
        lease_read: request.allow_lease_read,
        reason: "safe".to_string(),
    }
}

pub fn matrixraft_applied_index_fence_report(
    min_commit_index: LogIndex,
    commit_index: LogIndex,
    applied_index: LogIndex,
) -> AppliedIndexFenceReport {
    let passed = applied_index >= min_commit_index && applied_index <= commit_index;
    AppliedIndexFenceReport {
        min_commit_index,
        commit_index,
        applied_index,
        passed,
        reason: if applied_index < min_commit_index {
            "applied_index_behind_min_commit".to_string()
        } else if applied_index > commit_index {
            "applied_index_ahead_of_commit".to_string()
        } else {
            "applied_index_fence_passed".to_string()
        },
    }
}

pub fn matrixraft_lease_read_eligibility_report(
    node_id: NodeId,
    leader_id: Option<NodeId>,
    config_enabled: bool,
    leader_lease_valid: bool,
    applied_index_fence_passed: bool,
) -> LeaseReadEligibilityReport {
    let requester_is_leader = leader_id == Some(node_id);
    let eligible =
        config_enabled && requester_is_leader && leader_lease_valid && applied_index_fence_passed;
    LeaseReadEligibilityReport {
        node_id,
        leader_id,
        config_enabled,
        requester_is_leader,
        leader_lease_valid,
        applied_index_fence_passed,
        eligible,
        reason: if !config_enabled {
            "lease_read_disabled".to_string()
        } else if !requester_is_leader {
            "requester_not_leader".to_string()
        } else if !leader_lease_valid {
            "stale_leader_lease".to_string()
        } else if !applied_index_fence_passed {
            "applied_index_fence_failed".to_string()
        } else {
            "lease_read_eligible".to_string()
        },
    }
}

pub fn matrixraft_bounded_stale_read_report(
    node_id: NodeId,
    leader_id: NodeId,
    node_commit_index: LogIndex,
    leader_commit_index: LogIndex,
    max_stale_index_lag: LogIndex,
) -> BoundedStaleReadReport {
    let lag = leader_commit_index.saturating_sub(node_commit_index);
    let allowed = lag <= max_stale_index_lag;
    BoundedStaleReadReport {
        node_id,
        leader_id,
        node_commit_index,
        leader_commit_index,
        lag,
        max_stale_index_lag,
        allowed,
        reason: if allowed {
            "bounded_stale_read_allowed".to_string()
        } else {
            "replica_lagging".to_string()
        },
    }
}

pub fn matrixraft_read_safety_runtime_decision(
    input: ReadSafetyRuntimeInput,
) -> ReadSafetyRuntimeDecision {
    let is_follower = input.node_id != input.leader_id;
    if matches!(input.operation, ReadSafetyOperation::Write) {
        let stale_follower_write_rejected = is_follower;
        let minority_partition_write_rejected = !input.has_majority;
        let allowed = !stale_follower_write_rejected && !minority_partition_write_rejected;
        return ReadSafetyRuntimeDecision {
            allowed,
            read_index: input.leader_commit_index,
            reason: if allowed {
                "write_authority".to_string()
            } else if stale_follower_write_rejected {
                "not_leader".to_string()
            } else {
                "minority_partition".to_string()
            },
            stale_leader_lease_rejected: false,
            lagging_follower_read_rejected: false,
            stale_follower_write_rejected,
            minority_partition_read_rejected: false,
            minority_partition_write_rejected,
            healed_follower_catchup_observed: false,
        };
    }

    if !input.node_alive || !input.role_can_serve_data {
        return ReadSafetyRuntimeDecision {
            allowed: false,
            read_index: input.node_commit_index,
            reason: "node_unavailable".to_string(),
            stale_leader_lease_rejected: false,
            lagging_follower_read_rejected: false,
            stale_follower_write_rejected: false,
            minority_partition_read_rejected: false,
            minority_partition_write_rejected: false,
            healed_follower_catchup_observed: false,
        };
    }

    if matches!(input.operation, ReadSafetyOperation::LeaseRead) && !input.leader_lease_valid {
        return ReadSafetyRuntimeDecision {
            allowed: false,
            read_index: input.node_commit_index,
            reason: "stale_leader_lease".to_string(),
            stale_leader_lease_rejected: true,
            lagging_follower_read_rejected: false,
            stale_follower_write_rejected: false,
            minority_partition_read_rejected: false,
            minority_partition_write_rejected: false,
            healed_follower_catchup_observed: false,
        };
    }

    if matches!(
        input.operation,
        ReadSafetyOperation::ReadIndex | ReadSafetyOperation::LeaseRead
    ) && !input.has_majority
    {
        return ReadSafetyRuntimeDecision {
            allowed: false,
            read_index: input.node_commit_index,
            reason: "minority_partition".to_string(),
            stale_leader_lease_rejected: false,
            lagging_follower_read_rejected: false,
            stale_follower_write_rejected: false,
            minority_partition_read_rejected: true,
            minority_partition_write_rejected: false,
            healed_follower_catchup_observed: false,
        };
    }

    let lag = input
        .leader_commit_index
        .saturating_sub(input.node_commit_index);
    let max_lag = if matches!(input.operation, ReadSafetyOperation::BoundedStaleRead) {
        input.max_stale_index_lag
    } else {
        0
    };
    if lag > max_lag {
        return ReadSafetyRuntimeDecision {
            allowed: false,
            read_index: input.node_commit_index,
            reason: "replica_lagging".to_string(),
            stale_leader_lease_rejected: false,
            lagging_follower_read_rejected: is_follower,
            stale_follower_write_rejected: false,
            minority_partition_read_rejected: false,
            minority_partition_write_rejected: false,
            healed_follower_catchup_observed: false,
        };
    }

    ReadSafetyRuntimeDecision {
        allowed: true,
        read_index: input.node_commit_index,
        reason: "safe".to_string(),
        stale_leader_lease_rejected: false,
        lagging_follower_read_rejected: false,
        stale_follower_write_rejected: false,
        minority_partition_read_rejected: false,
        minority_partition_write_rejected: false,
        healed_follower_catchup_observed: is_follower
            && input.node_commit_index == input.leader_commit_index,
    }
}

pub fn matrixraft_read_safety_evidence_artifact() -> ReadSafetyEvidenceArtifact {
    ReadSafetyEvidenceArtifact {
        schema: "rustraft.read_safety_evidence.v1".to_string(),
        stale_leader_lease: matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
            operation: ReadSafetyOperation::LeaseRead,
            node_id: 1,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: false,
            has_majority: true,
            node_commit_index: 10,
            leader_commit_index: 10,
            max_stale_index_lag: 0,
        }),
        lagging_follower_read: matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
            operation: ReadSafetyOperation::ReadIndex,
            node_id: 2,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: true,
            has_majority: true,
            node_commit_index: 7,
            leader_commit_index: 10,
            max_stale_index_lag: 0,
        }),
        stale_follower_write: matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
            operation: ReadSafetyOperation::Write,
            node_id: 2,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: true,
            has_majority: true,
            node_commit_index: 10,
            leader_commit_index: 10,
            max_stale_index_lag: 0,
        }),
        bounded_stale_read_accept: matrixraft_read_safety_runtime_decision(
            ReadSafetyRuntimeInput {
                operation: ReadSafetyOperation::BoundedStaleRead,
                node_id: 2,
                leader_id: 1,
                node_alive: true,
                role_can_serve_data: true,
                leader_lease_valid: true,
                has_majority: true,
                node_commit_index: 9,
                leader_commit_index: 10,
                max_stale_index_lag: 1,
            },
        ),
        bounded_stale_read_reject: matrixraft_read_safety_runtime_decision(
            ReadSafetyRuntimeInput {
                operation: ReadSafetyOperation::BoundedStaleRead,
                node_id: 2,
                leader_id: 1,
                node_alive: true,
                role_can_serve_data: true,
                leader_lease_valid: true,
                has_majority: true,
                node_commit_index: 8,
                leader_commit_index: 10,
                max_stale_index_lag: 1,
            },
        ),
        minority_partition_read: matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
            operation: ReadSafetyOperation::ReadIndex,
            node_id: 1,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: true,
            has_majority: false,
            node_commit_index: 10,
            leader_commit_index: 10,
            max_stale_index_lag: 0,
        }),
        minority_partition_write: matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
            operation: ReadSafetyOperation::Write,
            node_id: 1,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: true,
            has_majority: false,
            node_commit_index: 10,
            leader_commit_index: 10,
            max_stale_index_lag: 0,
        }),
        healed_follower_catchup: matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
            operation: ReadSafetyOperation::ReadIndex,
            node_id: 2,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: true,
            has_majority: true,
            node_commit_index: 10,
            leader_commit_index: 10,
            max_stale_index_lag: 0,
        }),
    }
}

pub fn matrixraft_validate_read_safety_evidence_artifact(
    artifact: &ReadSafetyEvidenceArtifact,
) -> ReadSafetyEvidenceValidationReport {
    let schema_valid = artifact.schema == "rustraft.read_safety_evidence.v1";
    let stale_leader_lease_rejected = !artifact.stale_leader_lease.allowed
        && artifact.stale_leader_lease.stale_leader_lease_rejected
        && artifact.stale_leader_lease.reason == "stale_leader_lease";
    let lagging_follower_read_rejected = !artifact.lagging_follower_read.allowed
        && artifact
            .lagging_follower_read
            .lagging_follower_read_rejected
        && artifact.lagging_follower_read.reason == "replica_lagging";
    let stale_follower_write_rejected = !artifact.stale_follower_write.allowed
        && artifact.stale_follower_write.stale_follower_write_rejected
        && artifact.stale_follower_write.reason == "not_leader";
    let bounded_stale_read_accepted = artifact.bounded_stale_read_accept.allowed
        && artifact.bounded_stale_read_accept.reason == "safe";
    let bounded_stale_read_rejected = !artifact.bounded_stale_read_reject.allowed
        && artifact
            .bounded_stale_read_reject
            .lagging_follower_read_rejected
        && artifact.bounded_stale_read_reject.reason == "replica_lagging";
    let minority_partition_read_rejected = !artifact.minority_partition_read.allowed
        && artifact
            .minority_partition_read
            .minority_partition_read_rejected
        && artifact.minority_partition_read.reason == "minority_partition";
    let minority_partition_write_rejected = !artifact.minority_partition_write.allowed
        && artifact
            .minority_partition_write
            .minority_partition_write_rejected
        && artifact.minority_partition_write.reason == "minority_partition";
    let healed_follower_catchup_observed = artifact.healed_follower_catchup.allowed
        && artifact
            .healed_follower_catchup
            .healed_follower_catchup_observed;
    let mut missing = Vec::new();
    for (present, requirement) in [
        (schema_valid, "schema_valid"),
        (stale_leader_lease_rejected, "stale_leader_lease_rejected"),
        (
            lagging_follower_read_rejected,
            "lagging_follower_read_rejected",
        ),
        (
            stale_follower_write_rejected,
            "stale_follower_write_rejected",
        ),
        (bounded_stale_read_accepted, "bounded_stale_read_accepted"),
        (bounded_stale_read_rejected, "bounded_stale_read_rejected"),
        (
            minority_partition_read_rejected,
            "minority_partition_read_rejected",
        ),
        (
            minority_partition_write_rejected,
            "minority_partition_write_rejected",
        ),
        (
            healed_follower_catchup_observed,
            "healed_follower_catchup_observed",
        ),
    ] {
        if !present {
            missing.push(requirement.to_string());
        }
    }
    ReadSafetyEvidenceValidationReport {
        valid: missing.is_empty(),
        schema_valid,
        stale_leader_lease_rejected,
        lagging_follower_read_rejected,
        stale_follower_write_rejected,
        bounded_stale_read_accepted,
        bounded_stale_read_rejected,
        minority_partition_read_rejected,
        minority_partition_write_rejected,
        healed_follower_catchup_observed,
        missing,
    }
}
