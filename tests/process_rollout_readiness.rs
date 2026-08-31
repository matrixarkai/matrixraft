// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    readiness::{
        matrixraft_data_node_process_rollout_readiness_report,
        matrixraft_meta_process_rollout_readiness_report,
    },
    DataNodeProcessRolloutReport, MetaProcessRolloutReport, ProcessNodeEvidence,
    ProcessOperationalSemanticsEvidence, ProductionStatus,
};

fn ready_process_nodes() -> Vec<ProcessNodeEvidence> {
    vec![
        ProcessNodeEvidence {
            node_id: 1,
            addr: "127.0.0.1:7001".to_string(),
            wal_dir: "/tmp/rustraft-node-1/wal".to_string(),
            snapshot_dir: "/tmp/rustraft-node-1/snapshot".to_string(),
            commit_index: 12,
            applied_index: 12,
            snapshot_id: Some("snap-12".to_string()),
            restarted: true,
            log_store_validated: true,
        },
        ProcessNodeEvidence {
            node_id: 2,
            addr: "127.0.0.1:7002".to_string(),
            wal_dir: "/tmp/rustraft-node-2/wal".to_string(),
            snapshot_dir: "/tmp/rustraft-node-2/snapshot".to_string(),
            commit_index: 12,
            applied_index: 12,
            snapshot_id: Some("snap-12".to_string()),
            restarted: true,
            log_store_validated: true,
        },
        ProcessNodeEvidence {
            node_id: 3,
            addr: "127.0.0.1:7003".to_string(),
            wal_dir: "/tmp/rustraft-node-3/wal".to_string(),
            snapshot_dir: "/tmp/rustraft-node-3/snapshot".to_string(),
            commit_index: 12,
            applied_index: 12,
            snapshot_id: Some("snap-12".to_string()),
            restarted: true,
            log_store_validated: true,
        },
    ]
}

fn ready_semantics() -> ProcessOperationalSemanticsEvidence {
    ProcessOperationalSemanticsEvidence {
        api_presence_only_rejected: true,
        process_path_validated: true,
        read_index_validated: true,
        leader_lease_validated: true,
        stale_leader_lease_rejection_observed: true,
        follower_lease_expiration_observed: true,
        lagging_follower_read_rejected: true,
        bounded_stale_read_acceptance_observed: true,
        bounded_stale_read_rejection_observed: true,
        minority_partition_read_rejection_observed: true,
        healed_follower_catchup_observed: true,
        stale_follower_write_rejected: true,
        leader_transfer_exact_once_validated: true,
        leader_transfer_under_load_validated: true,
        snapshot_bootstrap_validated: true,
        snapshot_install_restart_validated: true,
        membership_rescale_validated: true,
        membership_add_promote_remove_validated: true,
        follower_rejoin_after_compaction_validated: true,
        secondary_read_eligibility_validated: true,
        apply_pipeline_converged: true,
        wal_persistence_observed: true,
        fsm_apply_idempotent_replay_observed: true,
        storage_mutation_wal_fence_atomicity_observed: true,
        snapshot_install_apply_fence_atomicity_observed: true,
        process_restart_after_apply_crash_recovered: true,
        ready: true,
        blockers: Vec::new(),
    }
}

fn ready_data_rollout() -> DataNodeProcessRolloutReport {
    DataNodeProcessRolloutReport {
        shard_id: 9,
        voters: vec![1, 2, 3],
        learners: vec![4],
        nodes: ready_process_nodes(),
        spawned_process_count: 3,
        independent_wal_dirs: true,
        independent_snapshot_dirs: true,
        observed_process_requests: 18,
        read_index_responses_observed: 3,
        restarted_node_count: 3,
        per_node_log_store_inspection_count: 3,
        write_proposed_through_process_api: true,
        leader_transfer_validated: true,
        failover_validated: true,
        secondary_lag_observed: true,
        lagging_follower_read_rejection_observed: true,
        stale_follower_write_rejection_observed: true,
        catchup_read_eligibility_observed: true,
        minority_partition_rejection_observed: true,
        bounded_stale_read_eligibility_observed: true,
        healed_follower_catchup_observed: true,
        lagging_follower_observed_lag: 2,
        membership_change_validated: true,
        follower_lag_validated: true,
        secondary_read_validated: true,
        recovered_after_restart: true,
        restart_recovery_validated: true,
        snapshot_install_validated: true,
        applied_fence_validated: true,
        crash_after_storage_mutation_recovered: true,
        crash_after_wal_persist_recovered: true,
        crash_during_snapshot_install_recovered: true,
        apply_fence_recovered_after_restart: true,
        multi_process_log_store_validated: true,
        operational_semantics: ready_semantics(),
        ready: true,
        blockers: Vec::new(),
    }
}

fn ready_meta_rollout() -> MetaProcessRolloutReport {
    MetaProcessRolloutReport {
        voters: vec![1, 2, 3],
        learners: vec![4],
        nodes: ready_process_nodes(),
        spawned_process_count: 3,
        independent_wal_dirs: true,
        independent_snapshot_dirs: true,
        observed_process_requests: 24,
        read_index_responses_observed: 4,
        restarted_node_count: 3,
        per_node_log_store_inspection_count: 3,
        mutation_proposed_through_process_api: true,
        applied_raft_mutations: 5,
        generated_scheduler_tasks: 2,
        scheduler_retries: 1,
        stale_scheduler_token_rejected: true,
        data_node_membership_results_ready: true,
        scheduler_mutations_proposed_through_process_api: true,
        scheduler_task_replay_from_raft_log_observed: true,
        membership_mutations_proposed_through_process_api: true,
        data_node_membership_workflow_report_attached: true,
        data_node_raft_group_results_observed: true,
        failover_validated: true,
        membership_change_validated: true,
        follower_lag_validated: true,
        secondary_read_validated: true,
        read_index_validated: true,
        snapshot_install_validated: true,
        recovered_after_restart: true,
        scheduler_task_replay_validated: true,
        crash_after_meta_mutation_recovered: true,
        crash_after_meta_wal_persist_recovered: true,
        crash_during_meta_snapshot_install_recovered: true,
        meta_apply_fence_recovered_after_restart: true,
        multi_process_log_store_validated: true,
        operational_semantics: ready_semantics(),
        ready: true,
        blockers: Vec::new(),
    }
}

#[test]
fn data_node_process_rollout_report_fails_closed_on_missing_process_proof() {
    let mut rollout = ready_data_rollout();
    rollout.independent_wal_dirs = false;
    rollout.operational_semantics.stale_follower_write_rejected = false;

    let report = matrixraft_data_node_process_rollout_readiness_report(&rollout);
    assert!(!report.ready);
    assert_eq!(report.production_status, ProductionStatus::Blocked);
    assert!(report
        .missing
        .contains(&"data_node:independent_wal_dirs".to_string()));
    assert!(report
        .missing
        .contains(&"data_node:semantics:stale_follower_write_rejected".to_string()));
}

#[test]
fn data_node_process_rollout_report_accepts_complete_process_evidence() {
    let report = matrixraft_data_node_process_rollout_readiness_report(&ready_data_rollout());
    assert!(report.ready, "{report:#?}");
    assert_eq!(report.production_status, ProductionStatus::ProductionReady);
    assert!(report.missing.is_empty());
    assert!(report
        .satisfied
        .contains(&"data_node:multi_process_log_store".to_string()));
}

#[test]
fn metaserver_process_rollout_report_tracks_scheduler_and_membership_proof() {
    let mut rollout = ready_meta_rollout();
    rollout.scheduler_task_replay_validated = false;
    rollout.data_node_raft_group_results_observed = false;

    let report = matrixraft_meta_process_rollout_readiness_report(&rollout);
    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"metaserver:scheduler_replay".to_string()));
    assert!(report
        .missing
        .contains(&"metaserver:data_node_membership_workflow".to_string()));
}

#[test]
fn metaserver_process_rollout_report_accepts_complete_process_evidence() {
    let report = matrixraft_meta_process_rollout_readiness_report(&ready_meta_rollout());
    assert!(report.ready, "{report:#?}");
    assert_eq!(report.scope, "metaserver");
    assert!(report
        .satisfied
        .contains(&"metaserver:multi_process_log_store".to_string()));
}
