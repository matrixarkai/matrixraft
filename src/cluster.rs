// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Cluster consensus API used by TemporalStore and other RustRaft consumers.

pub use crate::{
    rustraft_append_safety_decision, rustraft_applied_index_fence_report,
    rustraft_bounded_stale_read_report, rustraft_learner_promotion_decision,
    rustraft_lease_read_eligibility_report, rustraft_read_safety_decision,
    rustraft_read_safety_runtime_decision, RaftCluster, ReadIndexRequest, ReadIndexResponse,
    RustRaftAppendSafetyDecision, RustRaftAppliedIndexFenceReport, RustRaftBoundedStaleReadReport,
    RustRaftConsensus, RustRaftError, RustRaftLearnerPromotionDecision,
    RustRaftLeaseReadEligibilityReport, RustRaftLogEntry, RustRaftLogId, RustRaftProposeOptions,
    RustRaftReadIndexRequest, RustRaftReadIndexResponse, RustRaftReadPathReport,
    RustRaftReadQuorumReport, RustRaftReadSafetyDecision, RustRaftReadSafetyOperation,
    RustRaftReadSafetyRuntimeDecision, RustRaftReadSafetyRuntimeInput,
};
