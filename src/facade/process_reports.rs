// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// process rollout and cross-plane evidence report structs.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftDataNodeProcessRolloutReport {
    pub shard_id: u64,
    #[serde(default)]
    pub voters: Vec<u64>,
    #[serde(default)]
    pub learners: Vec<u64>,
    pub nodes: Vec<RustRaftProcessNodeEvidence>,
    #[serde(default)]
    pub spawned_process_count: u64,
    #[serde(default)]
    pub independent_wal_dirs: bool,
    #[serde(default)]
    pub independent_snapshot_dirs: bool,
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub restarted_node_count: u64,
    #[serde(default)]
    pub per_node_log_store_inspection_count: u64,
    pub write_proposed_through_process_api: bool,
    #[serde(default)]
    pub leader_transfer_validated: bool,
    #[serde(default)]
    pub failover_validated: bool,
    #[serde(default)]
    pub secondary_lag_observed: bool,
    #[serde(default)]
    pub lagging_follower_read_rejection_observed: bool,
    #[serde(default)]
    pub stale_follower_write_rejection_observed: bool,
    #[serde(default)]
    pub catchup_read_eligibility_observed: bool,
    #[serde(default)]
    pub minority_partition_rejection_observed: bool,
    #[serde(default)]
    pub bounded_stale_read_eligibility_observed: bool,
    #[serde(default)]
    pub healed_follower_catchup_observed: bool,
    #[serde(default)]
    pub lagging_follower_observed_lag: u64,
    #[serde(default)]
    pub membership_change_validated: bool,
    #[serde(default)]
    pub follower_lag_validated: bool,
    #[serde(default)]
    pub secondary_read_validated: bool,
    pub recovered_after_restart: bool,
    #[serde(default)]
    pub restart_recovery_validated: bool,
    pub snapshot_install_validated: bool,
    pub applied_fence_validated: bool,
    #[serde(default)]
    pub crash_after_storage_mutation_recovered: bool,
    #[serde(default)]
    pub crash_after_wal_persist_recovered: bool,
    #[serde(default)]
    pub crash_during_snapshot_install_recovered: bool,
    #[serde(default)]
    pub apply_fence_recovered_after_restart: bool,
    pub multi_process_log_store_validated: bool,
    #[serde(default)]
    pub operational_semantics: RustRaftProcessOperationalSemanticsEvidence,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMetaProcessRolloutReport {
    #[serde(default)]
    pub voters: Vec<u64>,
    #[serde(default)]
    pub learners: Vec<u64>,
    pub nodes: Vec<RustRaftProcessNodeEvidence>,
    #[serde(default)]
    pub spawned_process_count: u64,
    #[serde(default)]
    pub independent_wal_dirs: bool,
    #[serde(default)]
    pub independent_snapshot_dirs: bool,
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub restarted_node_count: u64,
    #[serde(default)]
    pub per_node_log_store_inspection_count: u64,
    pub mutation_proposed_through_process_api: bool,
    #[serde(default)]
    pub applied_raft_mutations: u64,
    #[serde(default)]
    pub generated_scheduler_tasks: u64,
    #[serde(default)]
    pub scheduler_retries: u64,
    #[serde(default)]
    pub stale_scheduler_token_rejected: bool,
    #[serde(default)]
    pub data_node_membership_results_ready: bool,
    #[serde(default)]
    pub scheduler_mutations_proposed_through_process_api: bool,
    #[serde(default)]
    pub scheduler_task_replay_from_raft_log_observed: bool,
    #[serde(default)]
    pub membership_mutations_proposed_through_process_api: bool,
    #[serde(default)]
    pub data_node_membership_workflow_report_attached: bool,
    #[serde(default)]
    pub data_node_raft_group_results_observed: bool,
    #[serde(default)]
    pub failover_validated: bool,
    #[serde(default)]
    pub membership_change_validated: bool,
    #[serde(default)]
    pub follower_lag_validated: bool,
    #[serde(default)]
    pub secondary_read_validated: bool,
    pub read_index_validated: bool,
    pub snapshot_install_validated: bool,
    pub recovered_after_restart: bool,
    pub scheduler_task_replay_validated: bool,
    #[serde(default)]
    pub crash_after_meta_mutation_recovered: bool,
    #[serde(default)]
    pub crash_after_meta_wal_persist_recovered: bool,
    #[serde(default)]
    pub crash_during_meta_snapshot_install_recovered: bool,
    #[serde(default)]
    pub meta_apply_fence_recovered_after_restart: bool,
    pub multi_process_log_store_validated: bool,
    #[serde(default)]
    pub operational_semantics: RustRaftProcessOperationalSemanticsEvidence,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftCrossPlaneProcessReadinessReport {
    pub ready: bool,
    pub multi_process_data_node_and_metaserver_raft: bool,
    pub failover_on_both_planes: bool,
    pub membership_add_remove_under_load: bool,
    pub secondary_lag_and_catchup: bool,
    pub snapshot_restart_after_compaction: bool,
    #[serde(default)]
    pub remaining_blockers: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftCrossPlaneProcessReadinessBlockerReport {
    pub ready: bool,
    pub multi_process_data_node_and_metaserver_raft: bool,
    pub failover_on_both_planes: bool,
    pub membership_add_remove_under_load: bool,
    pub secondary_lag_and_catchup: bool,
    pub snapshot_restart_after_compaction: bool,
    #[serde(default)]
    pub remaining_blockers: Vec<RustRaftProcessReadinessBlocker>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftCrossPlaneProcessEvidenceSummary {
    pub data_node_spawned_process_count: u64,
    pub metaserver_spawned_process_count: u64,
    pub total_spawned_process_count: u64,
    pub data_node_observed_process_requests: u64,
    pub metaserver_observed_process_requests: u64,
    pub total_observed_process_requests: u64,
    pub data_node_read_index_responses_observed: u64,
    pub metaserver_read_index_responses_observed: u64,
    pub total_read_index_responses_observed: u64,
    pub data_node_restarted_node_count: u64,
    pub metaserver_restarted_node_count: u64,
    pub total_restarted_node_count: u64,
    pub data_node_per_node_log_store_inspection_count: u64,
    pub metaserver_per_node_log_store_inspection_count: u64,
    pub total_per_node_log_store_inspection_count: u64,
    pub independent_wal_dirs_on_both_planes: bool,
    pub independent_snapshot_dirs_on_both_planes: bool,
    pub write_or_mutation_proposed_through_process_api_on_both_planes: bool,
    pub multi_process_log_store_validated_on_both_planes: bool,
    pub restart_recovery_validated_on_both_planes: bool,
    pub read_index_observed_on_both_planes: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftCrossPlaneProcessEvidenceArtifact {
    pub schema: String,
    pub readiness: RustRaftCrossPlaneProcessReadinessBlockerReport,
    pub summary: RustRaftCrossPlaneProcessEvidenceSummary,
    pub prometheus: RustRaftPrometheusMetricSet,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftCrossPlaneProcessEvidenceArtifactValidationReport {
    pub valid: bool,
    pub schema_valid: bool,
    pub readiness_ready: bool,
    pub summary_ready: bool,
    pub prometheus_complete: bool,
    #[serde(default)]
    pub missing: Vec<String>,
}

