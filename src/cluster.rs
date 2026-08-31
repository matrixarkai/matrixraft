// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Cluster consensus API used by TemporalStore and other RustRaft consumers.

pub use crate::{
    matrixraft_append_safety_decision, matrixraft_applied_index_fence_report,
    matrixraft_bounded_stale_read_report, matrixraft_learner_promotion_decision,
    matrixraft_lease_read_eligibility_report, matrixraft_read_safety_decision,
    matrixraft_read_safety_runtime_decision, AppendSafetyDecision, AppliedIndexFenceReport,
    BoundedStaleReadReport, Consensus, LearnerPromotionDecision, LeaseReadEligibilityReport,
    LogEntry, LogId, ProposeOptions, RaftCluster, RaftError, ReadIndexRequest, ReadIndexResponse,
    ReadPathReport, ReadQuorumReport, ReadSafetyDecision, ReadSafetyOperation,
    ReadSafetyRuntimeDecision, ReadSafetyRuntimeInput,
};
