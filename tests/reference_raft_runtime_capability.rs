// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    fault, rustraft_admin_status_surface_evidence, rustraft_capability_evidence_from_fields,
    rustraft_cross_plane_process_evidence_artifact,
    rustraft_cross_plane_process_evidence_prometheus,
    rustraft_cross_plane_process_evidence_summary,
    rustraft_cross_plane_process_readiness_blocker_report,
    rustraft_cross_plane_process_readiness_report, rustraft_data_node_process_rollout_blockers,
    rustraft_data_node_strict_process_rollout_validated,
    rustraft_membership_semantics_evidence_artifact, rustraft_meta_process_rollout_blockers,
    rustraft_meta_strict_process_rollout_validated, rustraft_named_readiness_blockers,
    rustraft_process_readiness_blocker, rustraft_production_readiness_report,
    rustraft_read_safety_evidence_artifact, rustraft_reference_raft_operational_evidence_bundle,
    rustraft_reference_raft_runtime_capability_prometheus,
    rustraft_reference_raft_runtime_capability_report,
    rustraft_replication_pipeline_evidence_artifact, rustraft_require_production_ready,
    rustraft_runtime_capability_report_from_evidence,
    rustraft_snapshot_lifecycle_evidence_artifact,
    rustraft_validate_cross_plane_process_evidence_artifact, rustraft_validate_deployment_mode,
    rustraft_validate_deployment_readiness,
    rustraft_validate_membership_semantics_evidence_artifact,
    rustraft_validate_read_safety_evidence_artifact,
    rustraft_validate_reference_raft_operational_evidence_bundle,
    rustraft_validate_replication_pipeline_evidence_artifact,
    rustraft_validate_snapshot_lifecycle_evidence_artifact,
    rustraft_validate_wal_lifecycle_evidence_artifact, rustraft_wal_lifecycle_evidence_artifact,
    RaftCapabilityEvidence, RustRaftAdminStatusSurfaceEvidence, RustRaftAdminStatusSurfaceInput,
    RustRaftDataNodeProcessRolloutReport, RustRaftDeploymentMode, RustRaftLearnerPromotionDecision,
    RustRaftMembershipScope, RustRaftMembershipTransitionEvidence,
    RustRaftMembershipTransitionKind, RustRaftMetaProcessRolloutReport, RustRaftPeerPipelineStatus,
    RustRaftPipelineEvidence, RustRaftPipelineLimits, RustRaftProcessNodeEvidence,
    RustRaftProcessOperationalSemanticsEvidence, RustRaftProductionReadinessInput,
    RustRaftReadinessSnapshot, RustRaftSnapshotLifecycleEvidence, RustRaftWalLifecycleEvidence,
    RustRaftWalLifecycleStatus,
};

fn ready_snapshot() -> RustRaftReadinessSnapshot {
    RustRaftReadinessSnapshot {
        rustraft_leader_write_authority_present: true,
        rustraft_operator_observability_present: true,
        rustraft_rpc_transport_contract_present: true,
        rustraft_log_retention_snapshot_trigger_present: true,
        rustraft_apply_snapshot_fence_present: true,
        raft_storage_apply_fence_present: true,
        rustraft_snapshot_floor_log_matching_present: true,
        rustraft_snapshot_tail_catchup_present: true,
        rustraft_compacted_entry_rejection_present: true,
        rustraft_metaserver_snapshot_floor_election_present: true,
        learner_catchup_promotion_present: true,
        metaserver_membership_workflow_present: true,
    }
}

fn ready_semantics() -> RustRaftProcessOperationalSemanticsEvidence {
    RustRaftProcessOperationalSemanticsEvidence {
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

fn process_nodes() -> Vec<RustRaftProcessNodeEvidence> {
    vec![
        RustRaftProcessNodeEvidence {
            node_id: 1,
            addr: "127.0.0.1:21001".to_string(),
            wal_dir: "/tmp/rustraft/capability/node-1/wal".to_string(),
            snapshot_dir: "/tmp/rustraft/capability/node-1/snapshots".to_string(),
            commit_index: 64,
            applied_index: 64,
            snapshot_id: Some("snap-60".to_string()),
            restarted: true,
            log_store_validated: true,
        },
        RustRaftProcessNodeEvidence {
            node_id: 2,
            addr: "127.0.0.1:21002".to_string(),
            wal_dir: "/tmp/rustraft/capability/node-2/wal".to_string(),
            snapshot_dir: "/tmp/rustraft/capability/node-2/snapshots".to_string(),
            commit_index: 64,
            applied_index: 64,
            snapshot_id: Some("snap-60".to_string()),
            restarted: true,
            log_store_validated: true,
        },
    ]
}

fn process_nodes_three() -> Vec<RustRaftProcessNodeEvidence> {
    let mut nodes = process_nodes();
    nodes.push(RustRaftProcessNodeEvidence {
        node_id: 3,
        addr: "127.0.0.1:21003".to_string(),
        wal_dir: "/tmp/rustraft/capability/node-3/wal".to_string(),
        snapshot_dir: "/tmp/rustraft/capability/node-3/snapshots".to_string(),
        commit_index: 64,
        applied_index: 64,
        snapshot_id: Some("snap-60".to_string()),
        restarted: true,
        log_store_validated: true,
    });
    nodes
}

fn ready_data_rollout() -> RustRaftDataNodeProcessRolloutReport {
    RustRaftDataNodeProcessRolloutReport {
        shard_id: 11,
        voters: vec![1, 2, 3],
        learners: vec![4],
        nodes: process_nodes(),
        spawned_process_count: 2,
        independent_wal_dirs: true,
        independent_snapshot_dirs: true,
        observed_process_requests: 20,
        read_index_responses_observed: 8,
        restarted_node_count: 2,
        per_node_log_store_inspection_count: 2,
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
        lagging_follower_observed_lag: 3,
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

fn ready_data_rollout_three_processes() -> RustRaftDataNodeProcessRolloutReport {
    let mut rollout = ready_data_rollout();
    rollout.nodes = process_nodes_three();
    rollout.spawned_process_count = 3;
    rollout.observed_process_requests = 30;
    rollout.restarted_node_count = 3;
    rollout.per_node_log_store_inspection_count = 3;
    rollout
}

fn ready_meta_rollout() -> RustRaftMetaProcessRolloutReport {
    RustRaftMetaProcessRolloutReport {
        voters: vec![1, 2, 3],
        learners: vec![4],
        nodes: process_nodes(),
        spawned_process_count: 2,
        independent_wal_dirs: true,
        independent_snapshot_dirs: true,
        observed_process_requests: 24,
        read_index_responses_observed: 9,
        restarted_node_count: 2,
        per_node_log_store_inspection_count: 2,
        mutation_proposed_through_process_api: true,
        applied_raft_mutations: 12,
        generated_scheduler_tasks: 4,
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

fn ready_meta_rollout_three_processes() -> RustRaftMetaProcessRolloutReport {
    let mut rollout = ready_meta_rollout();
    rollout.nodes = process_nodes_three();
    rollout.spawned_process_count = 3;
    rollout.observed_process_requests = 32;
    rollout.restarted_node_count = 3;
    rollout.per_node_log_store_inspection_count = 3;
    rollout
}

fn transition(
    scope: RustRaftMembershipScope,
    transition: RustRaftMembershipTransitionKind,
) -> RustRaftMembershipTransitionEvidence {
    let (before_voters, after_voters, before_learners, after_learners, added, removed) =
        match transition {
            RustRaftMembershipTransitionKind::Failover => (
                vec![1, 2, 3],
                vec![1, 2, 3],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                vec![1],
            ),
            RustRaftMembershipTransitionKind::ScaleUp => (
                vec![1, 2, 3],
                vec![1, 2, 3, 4],
                vec![4],
                Vec::new(),
                vec![4],
                Vec::new(),
            ),
            RustRaftMembershipTransitionKind::ScaleDown => (
                vec![1, 2, 3, 4],
                vec![1, 2, 3],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                vec![4],
            ),
        };
    RustRaftMembershipTransitionEvidence {
        scope,
        transition,
        before_voters: before_voters.clone(),
        after_voters: after_voters.clone(),
        before_learners,
        after_learners,
        leader_before: Some(1),
        leader_after: Some(2),
        failed_or_removed_nodes: removed,
        added_nodes: added,
        caught_up_nodes: vec![1, 2, 3],
        commit_index_before: 90,
        commit_index_after: 96,
        applied_index_after: 96,
        joint_consensus_used: true,
        old_majority_preserved: true,
        new_majority_reached: true,
        joint_old_quorum_size: if matches!(transition, RustRaftMembershipTransitionKind::Failover) {
            0
        } else {
            before_voters.len() / 2 + 1
        },
        joint_new_quorum_size: if matches!(transition, RustRaftMembershipTransitionKind::Failover) {
            0
        } else {
            after_voters.len() / 2 + 1
        },
        joint_acknowledged_voters: if matches!(
            transition,
            RustRaftMembershipTransitionKind::ScaleUp | RustRaftMembershipTransitionKind::ScaleDown
        ) {
            vec![1, 2, 3, 4]
        } else {
            Vec::new()
        },
        joint_old_majority_acked: !matches!(transition, RustRaftMembershipTransitionKind::Failover),
        joint_new_majority_acked: !matches!(transition, RustRaftMembershipTransitionKind::Failover),
        stale_leader_rejected: true,
        read_index_validated_after: true,
        write_validated_after: true,
        snapshot_floor_preserved: true,
        secondary_replication_visible: true,
        scheduler_generation_advanced: matches!(scope, RustRaftMembershipScope::Metaserver),
        blockers: Vec::new(),
    }
}

fn transitions() -> Vec<RustRaftMembershipTransitionEvidence> {
    [
        RustRaftMembershipScope::Metaserver,
        RustRaftMembershipScope::DataNode,
    ]
    .into_iter()
    .flat_map(|scope| {
        [
            RustRaftMembershipTransitionKind::Failover,
            RustRaftMembershipTransitionKind::ScaleUp,
            RustRaftMembershipTransitionKind::ScaleDown,
        ]
        .into_iter()
        .map(move |kind| transition(scope, kind))
    })
    .collect()
}

fn ready_admin_status_surface() -> RustRaftAdminStatusSurfaceEvidence {
    let limits = RustRaftPipelineLimits::production_default();
    let mut peer_2 = RustRaftPeerPipelineStatus::new(2, 105, limits);
    peer_2.match_index = 104;
    peer_2.append_requests = 8;
    peer_2.append_accepted = 8;
    peer_2.append_queue_max_depth = 4;

    let mut peer_3 = RustRaftPeerPipelineStatus::new(3, 105, limits);
    peer_3.match_index = 104;
    peer_3.append_requests = 7;
    peer_3.append_accepted = 7;
    peer_3.inflight_entries = 1;
    peer_3.inflight_bytes = 128;

    rustraft_admin_status_surface_evidence(&RustRaftAdminStatusSurfaceInput {
        commit_index: 104,
        max_observed_node_commit_index: 104,
        quorum_size: 2,
        quorum_peer_ids: vec![2, 3],
        peer_pipeline: vec![peer_2, peer_3],
        wal_last_log_index: 110,
        wal_segment_lifecycle_present: true,
    })
}

fn complete_replication_pipeline_peers() -> Vec<RustRaftPeerPipelineStatus> {
    let limits = RustRaftPipelineLimits::production_default();
    let mut peer_2 = RustRaftPeerPipelineStatus::new(2, 105, limits);
    peer_2.append_queue_depth = limits.max_inflights_replicate;
    peer_2.append_queue_max_depth = limits.max_inflights_replicate;
    peer_2.apply_queue_max_depth = limits.max_inflights_apply_task;
    peer_2.memory_backpressure_rejections = 1;
    peer_2.oversized_log_rejections = 1;
    peer_2.stale_term_rejections = 1;
    peer_2.reorder_queue_depth = 1;

    let mut peer_3 = RustRaftPeerPipelineStatus::new(3, 105, limits);
    peer_3.reorder_queue_depth = 1;
    peer_3.reorder_entry_timeouts = 1;
    peer_3.reorder_dropped_packages = 1;
    peer_3.out_of_order_append_rejections = 1;
    peer_3.packet_loss_events = 2;
    peer_3.network_error_probe_transitions = 1;
    peer_3.append_accepted = 2;
    peer_3.match_index = 104;
    peer_3.next_index = 105;
    peer_3.reorder_queue_depth = 0;

    vec![peer_2, peer_3]
}

fn complete_snapshot_lifecycle_peers() -> Vec<RustRaftPeerPipelineStatus> {
    let limits = RustRaftPipelineLimits::production_default();
    let mut sender = RustRaftPeerPipelineStatus::new(2, 105, limits);
    sender.snapshot_sending = true;
    sender.snapshot_send_attempts = 2;
    sender.snapshot_install_total_chunks = 8;
    sender.snapshot_install_progress_per_mille = 250;
    sender.snapshot_backpressure_rejections = 1;
    sender.snapshot_chunk_retry_count = 1;
    sender.snapshot_send_timeouts = 1;
    sender.snapshot_rate_limit_rejections = 1;
    sender.snapshot_during_membership_change = true;
    sender.required_snapshot_index = 128;
    sender.acked_snapshot_index = 128;

    let mut installer = RustRaftPeerPipelineStatus::new(3, 105, limits);
    installer.snapshot_install_total_chunks = 4;
    installer.snapshot_install_progress_per_mille = 1000;
    installer.snapshot_installed_index = 128;
    installer.snapshot_install_rolled_back = 1;
    installer.snapshot_rejoin_after_compacted_log = true;

    vec![sender, installer]
}

fn complete_wal_lifecycle_status() -> RustRaftWalLifecycleStatus {
    RustRaftWalLifecycleStatus {
        segment_count: 3,
        active_segment_id: 7,
        first_retained_segment_id: 5,
        last_retained_segment_id: 7,
        total_bytes: 64 * 1024,
        active_segment_bytes: 8 * 1024,
        total_records: 128,
        first_sequence: 42,
        last_sequence: 169,
        first_log_index: 101,
        last_log_index: 228,
        released_segment_count: 4,
        slow_fsync_backpressure_observed: true,
        slow_fsync_threshold_ms: 10,
        slow_fsync_count: 2,
        consecutive_slow_fsync_count: 1,
        max_fsync_elapsed_ms: 42,
        compacted_after_slow_fsync_count: 2,
    }
}

fn ready_fault_harness() -> fault::RustRaftFaultHarnessReadinessReport {
    let evidence = fault::rustraft_reference_raft_fault_scenarios()
        .into_iter()
        .map(|requirement| fault::RustRaftFaultScenarioEvidence {
            scenario: requirement.scenario,
            process_path_observed: true,
            spawned_process_count: 3,
            observed_process_ids: vec![10_001, 10_002, 10_003],
            scenario_runtime_ms: 1_500,
            client_operation_count: 256,
            injected_fault_count: 3,
            independent_wal_dirs_observed: true,
            independent_snapshot_dirs_observed: true,
            safety_observed: true,
            recovery_observed: true,
            metrics_observed: true,
            observed_acceptance: requirement.acceptance,
            report_path: Some(format!("reports/{}.json", requirement.scenario.id())),
        })
        .collect::<Vec<_>>();
    fault::rustraft_fault_harness_readiness_report(&evidence)
}

fn ready_input() -> RustRaftProductionReadinessInput {
    RustRaftProductionReadinessInput {
        readiness: ready_snapshot(),
        peer_pipeline: Some(RustRaftPipelineEvidence {
            per_peer_pipeline_state_present: true,
            append_backpressure_enforced: true,
            apply_backpressure_enforced: true,
            memory_replicate_bytes_enforced: true,
            oversized_log_rejection_present: true,
            out_of_order_append_handling_present: true,
            reorder_timeout_drop_present: true,
            packet_loss_probe_present: true,
            packet_loss_recovery_present: true,
            reorder_convergence_present: true,
            packet_loss_reorder_same_peer_recovered: true,
            stale_term_rejection_present: true,
            reorder_queue_enabled: true,
        }),
        snapshot_lifecycle: Some(RustRaftSnapshotLifecycleEvidence {
            sender_lifecycle_present: true,
            downloader_lifecycle_present: true,
            retry_backpressure_present: true,
            chunk_retry_present: true,
            send_timeout_present: true,
            rate_limit_present: true,
            sustained_sender_load_present: true,
            sustained_downloader_load_present: true,
            sustained_sender_completion_present: true,
            sustained_downloader_completion_present: true,
            install_progress_present: true,
            install_rollback_present: true,
            membership_change_present: true,
            rejoin_after_compacted_log_present: true,
        }),
        wal_lifecycle: Some(RustRaftWalLifecycleEvidence {
            segment_lifecycle_present: true,
            retained_range_present: true,
            sequence_range_present: true,
            log_index_range_present: true,
            compaction_observed: true,
            slow_fsync_backpressure_observed: true,
            compaction_after_slow_fsync_observed: true,
        }),
        admin_status_surface: Some(ready_admin_status_surface()),
        fault_harness: Some(ready_fault_harness()),
        data_node_rollout: Some(ready_data_rollout()),
        metaserver_rollout: Some(ready_meta_rollout()),
        membership_transitions: transitions(),
        reference_raft_benchmark: Some(matrixraft::RustRaftReferenceRaftBenchmarkEvidence {
            real_reference_raft: true,
            rustraft_runtime: true,
            reference_raft_cpp_reference: true,
            rustraft_rust_candidate: true,
            correctness_passed: true,
            performance_within_threshold: true,
            workloads: matrixraft::benchmark::rustraft_reference_raft_benchmark_workloads()
                .into_iter()
                .map(|workload| workload.id().to_string())
                .collect(),
            blockers: Vec::new(),
            missing_reference_raft_binaries: Vec::new(),
            unsupported_workloads: Vec::new(),
            correctness_blockers: Vec::new(),
            performance_blockers: Vec::new(),
        }),
    }
}

#[test]
fn reference_raft_runtime_capability_report_accepts_complete_evidence() {
    let report = rustraft_reference_raft_runtime_capability_report(&ready_input());
    assert!(report.ready, "{report:#?}");
    assert!(report.missing.is_empty());
    assert!(report.blockers.is_empty());
    assert!(report
        .satisfied
        .contains(&"wal_segment_lifecycle".to_string()));
    assert!(report
        .satisfied
        .contains(&"read_index_and_lease_safety".to_string()));
}

#[test]
fn capability_evidence_from_fields_formats_present_and_missing_rows() {
    let evidence = rustraft_capability_evidence_from_fields(
        "wal_segment_lifecycle",
        "product_admin_report",
        [
            (true, "wal.first_index_status"),
            (false, "wal.segment_release_rules"),
        ],
    );

    assert_eq!(evidence.capability, "wal_segment_lifecycle");
    assert!(!evidence.present);
    assert_eq!(evidence.source_reference, "product_admin_report");
    assert_eq!(
        evidence.evidence,
        vec![
            "present:wal.first_index_status".to_string(),
            "missing:wal.segment_release_rules".to_string(),
        ]
    );
}

#[test]
fn runtime_capability_report_builder_accepts_product_evidence_rows() {
    let report = rustraft_runtime_capability_report_from_evidence(
        vec![
            RaftCapabilityEvidence {
                capability: "process_path_rollout_evidence".to_string(),
                present: true,
                evidence: vec!["present:data_node.ready".to_string()],
                source_reference: "product_admin_report".to_string(),
            },
            RaftCapabilityEvidence {
                capability: "wal_segment_lifecycle".to_string(),
                present: false,
                evidence: vec![
                    "missing:wal.first_index_status".to_string(),
                    "wal.segment_release_rules".to_string(),
                ],
                source_reference: "product_admin_report".to_string(),
            },
        ],
        ["product:blocker:external_process_evidence_missing"],
    );

    assert!(!report.ready);
    assert_eq!(
        report.satisfied,
        vec!["process_path_rollout_evidence".to_string()]
    );
    assert_eq!(report.missing, vec!["wal_segment_lifecycle".to_string()]);
    assert!(report
        .blockers
        .contains(&"wal_segment_lifecycle:missing:wal.first_index_status".to_string()));
    assert!(report
        .blockers
        .contains(&"wal_segment_lifecycle:missing:wal.segment_release_rules".to_string()));
    assert!(report
        .blockers
        .contains(&"product:blocker:external_process_evidence_missing".to_string()));
}

#[test]
fn reference_raft_runtime_capability_report_fails_closed_on_missing_wal_lifecycle() {
    let mut input = ready_input();
    input.wal_lifecycle = Some(RustRaftWalLifecycleEvidence {
        segment_lifecycle_present: true,
        retained_range_present: true,
        sequence_range_present: true,
        log_index_range_present: true,
        compaction_observed: true,
        slow_fsync_backpressure_observed: false,
        compaction_after_slow_fsync_observed: false,
    });

    let report = rustraft_reference_raft_runtime_capability_report(&input);
    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"wal_segment_lifecycle".to_string()));
    assert!(report.blockers.iter().any(|blocker| {
        blocker == "wal_segment_lifecycle:missing:wal.slow_fsync_backpressure_observed"
    }));
}

#[test]
fn reference_raft_runtime_capability_report_names_process_path_missing_fields() {
    let mut input = ready_input();
    input
        .data_node_rollout
        .as_mut()
        .unwrap()
        .observed_process_requests = 0;
    input
        .metaserver_rollout
        .as_mut()
        .unwrap()
        .per_node_log_store_inspection_count = 0;

    let report = rustraft_reference_raft_runtime_capability_report(&input);
    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"process_path_rollout_evidence".to_string()));
    assert!(report.blockers.iter().any(|blocker| {
        blocker == "process_path_rollout_evidence:missing:data_node.observed_process_requests"
    }));
    assert!(report.blockers.iter().any(|blocker| {
        blocker == "process_path_rollout_evidence:missing:metaserver.per_node_log_store_inspection"
    }));
}

#[test]
fn cross_plane_process_readiness_requires_real_three_process_evidence() {
    let data = ready_data_rollout_three_processes();
    let meta = ready_meta_rollout_three_processes();

    let report = rustraft_cross_plane_process_readiness_report(&data, &meta);

    assert!(report.ready);
    assert!(report.multi_process_data_node_and_metaserver_raft);
    assert!(report.failover_on_both_planes);
    assert!(report.membership_add_remove_under_load);
    assert!(report.secondary_lag_and_catchup);
    assert!(report.snapshot_restart_after_compaction);
    assert!(report.remaining_blockers.is_empty());
}

#[test]
fn cross_plane_process_readiness_names_missing_evidence_fields() {
    let mut data = ready_data_rollout_three_processes();
    let mut meta = ready_meta_rollout_three_processes();
    data.independent_wal_dirs = false;
    meta.operational_semantics.read_index_validated = false;

    let report = rustraft_cross_plane_process_readiness_report(&data, &meta);

    assert!(!report.ready);
    assert!(!report.multi_process_data_node_and_metaserver_raft);
    assert!(report
        .remaining_blockers
        .contains(&"data_node_report.independent_wal_dirs".to_string()));
    assert!(report
        .remaining_blockers
        .contains(&"metaserver_report.operational_semantics.read_index_validated".to_string()));
}

#[test]
fn process_readiness_blocker_classifier_is_library_owned() {
    let blocker = rustraft_process_readiness_blocker("data_node_report.independent_wal_dirs");

    assert_eq!(
        blocker.blocker,
        "data_node_report_independent_wal_dirs_missing"
    );
    assert_eq!(
        blocker.evidence_field,
        "data_node_report.independent_wal_dirs"
    );
    assert_eq!(
        blocker.detail,
        "each process must use an independent WAL directory"
    );

    let operational = rustraft_process_readiness_blocker(
        "metaserver_report.operational_semantics.read_index_validated",
    );
    assert_eq!(
        operational.detail,
        "RustRaft/ReferenceRaft-derived operational semantics evidence is incomplete"
    );
}

#[test]
fn process_rollout_blocker_expansion_is_library_owned() {
    let missing = rustraft_data_node_process_rollout_blockers("data_node_report", None);
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].blocker, "data_node_report_missing");
    assert_eq!(missing[0].evidence_field, "data_node_report");
    assert!(missing[0].detail.contains("No process-harness report"));

    let mut data = ready_data_rollout_three_processes();
    data.independent_wal_dirs = false;
    data.read_index_responses_observed = 0;
    let data_blockers =
        rustraft_data_node_process_rollout_blockers("data_node_report", Some(&data));
    assert!(data_blockers.iter().any(|blocker| {
        blocker.evidence_field == "data_node_report.independent_wal_dirs"
            && blocker.detail == "each process must use an independent WAL directory"
    }));
    assert!(data_blockers.iter().any(|blocker| {
        blocker.evidence_field == "data_node_report.read_index_responses_observed"
    }));

    let mut meta = ready_meta_rollout_three_processes();
    meta.operational_semantics.read_index_validated = false;
    let meta_blockers = rustraft_meta_process_rollout_blockers("metaserver_report", Some(&meta));
    assert!(meta_blockers.iter().any(|blocker| {
        blocker.evidence_field == "metaserver_report.operational_semantics.read_index_validated"
            && blocker.detail
                == "RustRaft/ReferenceRaft-derived operational semantics evidence is incomplete"
    }));
}

#[test]
fn cross_plane_blocker_report_is_library_owned() {
    let mut data = ready_data_rollout_three_processes();
    let mut meta = ready_meta_rollout_three_processes();
    data.secondary_read_validated = false;
    meta.operational_semantics
        .follower_rejoin_after_compaction_validated = false;

    let string_report = rustraft_cross_plane_process_readiness_report(&data, &meta);
    let blocker_report = rustraft_cross_plane_process_readiness_blocker_report(&data, &meta);

    assert_eq!(blocker_report.ready, string_report.ready);
    assert_eq!(
        blocker_report.secondary_lag_and_catchup,
        string_report.secondary_lag_and_catchup
    );
    assert_eq!(
        blocker_report.snapshot_restart_after_compaction,
        string_report.snapshot_restart_after_compaction
    );
    assert_eq!(
        blocker_report.remaining_blockers.len(),
        string_report.remaining_blockers.len()
    );
    assert!(blocker_report.remaining_blockers.iter().any(|blocker| {
        blocker.evidence_field == "data_node_report.secondary_read_validated"
            && blocker.detail == "secondary read eligibility after catch-up must be validated"
    }));
    assert!(blocker_report.remaining_blockers.iter().any(|blocker| {
        blocker.evidence_field
            == "metaserver_report.operational_semantics.follower_rejoin_after_compaction_validated"
            && blocker.detail
                == "RustRaft/ReferenceRaft-derived operational semantics evidence is incomplete"
    }));
}

#[test]
fn cross_plane_process_evidence_summary_is_library_owned() {
    let data = ready_data_rollout_three_processes();
    let meta = ready_meta_rollout_three_processes();

    let summary = rustraft_cross_plane_process_evidence_summary(&data, &meta);

    assert_eq!(summary.data_node_spawned_process_count, 3);
    assert_eq!(summary.metaserver_spawned_process_count, 3);
    assert_eq!(summary.total_spawned_process_count, 6);
    assert_eq!(summary.data_node_observed_process_requests, 30);
    assert_eq!(summary.metaserver_observed_process_requests, 32);
    assert_eq!(summary.total_observed_process_requests, 62);
    assert_eq!(summary.total_read_index_responses_observed, 17);
    assert_eq!(summary.total_restarted_node_count, 6);
    assert_eq!(summary.total_per_node_log_store_inspection_count, 6);
    assert!(summary.independent_wal_dirs_on_both_planes);
    assert!(summary.independent_snapshot_dirs_on_both_planes);
    assert!(summary.write_or_mutation_proposed_through_process_api_on_both_planes);
    assert!(summary.multi_process_log_store_validated_on_both_planes);
    assert!(summary.restart_recovery_validated_on_both_planes);
    assert!(summary.read_index_observed_on_both_planes);
}

#[test]
fn cross_plane_process_evidence_prometheus_is_library_owned() {
    let data = ready_data_rollout_three_processes();
    let meta = ready_meta_rollout_three_processes();
    let summary = rustraft_cross_plane_process_evidence_summary(&data, &meta);

    let metrics = rustraft_cross_plane_process_evidence_prometheus(
        &summary,
        &[("cluster", "a\"b\\c\n"), ("source", "process")],
    );

    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert_eq!(metrics.metric_count, 21);
    assert!(metrics.text.contains("# HELP rustraft_process_evidence_count Cross-plane process-path evidence counters by plane and evidence kind."));
    assert!(metrics.text.contains(
        "rustraft_process_evidence_count{cluster=\"a\\\"b\\\\c\\n\",source=\"process\",plane=\"both\",evidence=\"spawned_process_count\"} 6"
    ));
    assert!(metrics.text.contains(
        "rustraft_process_evidence_count{cluster=\"a\\\"b\\\\c\\n\",source=\"process\",plane=\"both\",evidence=\"observed_process_requests\"} 62"
    ));
    assert!(metrics.text.contains(
        "rustraft_process_evidence_count{cluster=\"a\\\"b\\\\c\\n\",source=\"process\",plane=\"both\",evidence=\"read_index_responses_observed\"} 17"
    ));
    assert!(metrics.text.contains(
        "rustraft_process_evidence_ready{cluster=\"a\\\"b\\\\c\\n\",source=\"process\",evidence=\"multi_process_log_store_validated_on_both_planes\"} 1"
    ));
    assert!(metrics.text.contains(
        "rustraft_process_evidence_ready{cluster=\"a\\\"b\\\\c\\n\",source=\"process\",evidence=\"read_index_observed_on_both_planes\"} 1"
    ));
}

#[test]
fn cross_plane_process_evidence_artifact_is_library_owned() {
    let data = ready_data_rollout_three_processes();
    let meta = ready_meta_rollout_three_processes();

    let artifact =
        rustraft_cross_plane_process_evidence_artifact(&data, &meta, &[("cluster", "raft-a")]);
    let json = serde_json::to_string(&artifact).expect("serialize process evidence artifact");

    assert_eq!(artifact.schema, "rustraft.cross_plane_process_evidence.v1");
    assert!(artifact.readiness.ready);
    assert_eq!(artifact.summary.total_spawned_process_count, 6);
    assert_eq!(artifact.summary.total_observed_process_requests, 62);
    assert_eq!(artifact.prometheus.metric_count, 21);
    assert!(artifact
        .prometheus
        .text
        .contains("rustraft_process_evidence_count"));
    assert!(json.contains("\"schema\":\"rustraft.cross_plane_process_evidence.v1\""));
    assert!(json.contains("\"total_spawned_process_count\":6"));
    assert!(json.contains("\"read_index_observed_on_both_planes\":true"));
}

#[test]
fn cross_plane_process_evidence_artifact_validator_is_library_owned() {
    let data = ready_data_rollout_three_processes();
    let meta = ready_meta_rollout_three_processes();
    let artifact =
        rustraft_cross_plane_process_evidence_artifact(&data, &meta, &[("cluster", "raft-a")]);

    let validation = rustraft_validate_cross_plane_process_evidence_artifact(&artifact);

    assert!(validation.valid);
    assert!(validation.schema_valid);
    assert!(validation.readiness_ready);
    assert!(validation.summary_ready);
    assert!(validation.prometheus_complete);
    assert!(validation.missing.is_empty());
}

#[test]
fn cross_plane_process_evidence_artifact_validator_reports_missing_fields() {
    let data = ready_data_rollout_three_processes();
    let meta = ready_meta_rollout_three_processes();
    let mut artifact =
        rustraft_cross_plane_process_evidence_artifact(&data, &meta, &[("cluster", "raft-a")]);
    artifact.schema = "old.schema".to_string();
    artifact.readiness.ready = false;
    artifact.summary.total_spawned_process_count = 2;
    artifact.summary.data_node_spawned_process_count = 1;
    artifact.summary.independent_wal_dirs_on_both_planes = false;
    artifact.prometheus.metric_count = 0;
    artifact.prometheus.text.clear();

    let validation = rustraft_validate_cross_plane_process_evidence_artifact(&artifact);

    assert!(!validation.valid);
    assert!(!validation.schema_valid);
    assert!(!validation.readiness_ready);
    assert!(!validation.summary_ready);
    assert!(!validation.prometheus_complete);
    assert!(validation
        .missing
        .contains(&"schema must be rustraft.cross_plane_process_evidence.v1".to_string()));
    assert!(validation
        .missing
        .contains(&"readiness.ready must be true with zero remaining blockers".to_string()));
    assert!(validation.missing.contains(
        &"summary.total_spawned_process_count must cover both 3-node planes".to_string()
    ));
    assert!(validation
        .missing
        .contains(&"summary.independent_wal_dirs_on_both_planes must be true".to_string()));
    assert!(validation.missing.contains(
        &"prometheus must include process evidence count and readiness metrics".to_string()
    ));
}

#[test]
fn read_safety_evidence_artifact_is_library_owned() {
    let artifact = rustraft_read_safety_evidence_artifact();

    assert_eq!(artifact.schema, "rustraft.read_safety_evidence.v1");
    assert!(!artifact.stale_leader_lease.allowed);
    assert!(artifact.stale_leader_lease.stale_leader_lease_rejected);
    assert!(!artifact.lagging_follower_read.allowed);
    assert!(
        artifact
            .lagging_follower_read
            .lagging_follower_read_rejected
    );
    assert!(!artifact.stale_follower_write.allowed);
    assert!(artifact.stale_follower_write.stale_follower_write_rejected);
    assert!(artifact.bounded_stale_read_accept.allowed);
    assert!(!artifact.bounded_stale_read_reject.allowed);
    assert!(!artifact.minority_partition_read.allowed);
    assert!(
        artifact
            .minority_partition_read
            .minority_partition_read_rejected
    );
    assert!(!artifact.minority_partition_write.allowed);
    assert!(
        artifact
            .minority_partition_write
            .minority_partition_write_rejected
    );
    assert!(artifact.healed_follower_catchup.allowed);
    assert!(
        artifact
            .healed_follower_catchup
            .healed_follower_catchup_observed
    );
}

#[test]
fn read_safety_evidence_artifact_validator_reports_missing_fields() {
    let artifact = rustraft_read_safety_evidence_artifact();
    let valid = rustraft_validate_read_safety_evidence_artifact(&artifact);
    assert!(valid.valid);
    assert!(valid.missing.is_empty());

    let mut broken = artifact;
    broken.schema = "old.schema".to_string();
    broken.stale_leader_lease.allowed = true;
    broken.bounded_stale_read_accept.allowed = false;
    broken.minority_partition_write.reason = "write_authority".to_string();
    let invalid = rustraft_validate_read_safety_evidence_artifact(&broken);

    assert!(!invalid.valid);
    assert!(!invalid.schema_valid);
    assert!(!invalid.stale_leader_lease_rejected);
    assert!(!invalid.bounded_stale_read_accepted);
    assert!(!invalid.minority_partition_write_rejected);
    assert!(invalid.missing.contains(&"schema_valid".to_string()));
    assert!(invalid
        .missing
        .contains(&"stale_leader_lease_rejected".to_string()));
    assert!(invalid
        .missing
        .contains(&"bounded_stale_read_accepted".to_string()));
    assert!(invalid
        .missing
        .contains(&"minority_partition_write_rejected".to_string()));
}

#[test]
fn membership_semantics_evidence_artifact_is_library_owned() {
    let artifact = rustraft_membership_semantics_evidence_artifact();

    assert_eq!(artifact.schema, "rustraft.membership_semantics_evidence.v1");
    assert!(artifact
        .learner_add
        .added_nodes
        .contains(&artifact.learner_catchup.learner_id));
    assert!(artifact.learner_catchup.promotable);
    assert!(artifact
        .learner_promote
        .after_voters
        .contains(&artifact.learner_catchup.learner_id));
    assert_ne!(
        artifact.leader_transfer.leader_before,
        artifact.leader_transfer.leader_after
    );
    assert!(!artifact.voter_remove.failed_or_removed_nodes.is_empty());
    assert!(artifact.auto_promote_learner_observed);
    assert!(artifact.auto_promote_blocked_by_pending_joint_observed);
    assert!(artifact.pending_joint_consensus_restart_observed);
    assert!(artifact.pending_joint_consensus_restart_recovered);
    assert!(artifact.witness_role_supported);
    assert!(artifact.witness_promotion_rejected_observed);
}

#[test]
fn membership_semantics_evidence_artifact_validator_reports_missing_fields() {
    let artifact = rustraft_membership_semantics_evidence_artifact();
    let valid = rustraft_validate_membership_semantics_evidence_artifact(&artifact);
    assert!(valid.valid);
    assert!(valid.missing.is_empty());

    let mut broken = artifact;
    broken.schema = "old.schema".to_string();
    broken.learner_catchup = RustRaftLearnerPromotionDecision {
        promotable: false,
        learner_id: 4,
        learner_match_index: 120,
        required_match_index: 144,
        reason: "not_caught_up".to_string(),
    };
    broken
        .learner_promote
        .after_voters
        .retain(|node| *node != 4);
    broken.leader_transfer.leader_after = broken.leader_transfer.leader_before;
    broken.voter_remove.failed_or_removed_nodes.clear();
    broken.auto_promote_learner_observed = false;
    broken.auto_promote_blocked_by_pending_joint_observed = false;
    broken.pending_joint_consensus_restart_observed = false;
    broken.pending_joint_consensus_restart_recovered = false;
    broken.witness_role_supported = false;
    broken.witness_promotion_rejected_observed = false;
    broken.witness_role_blocker = None;

    let invalid = rustraft_validate_membership_semantics_evidence_artifact(&broken);

    assert!(!invalid.valid);
    assert!(!invalid.schema_valid);
    assert!(!invalid.learner_caught_up);
    assert!(!invalid.learner_promoted);
    assert!(!invalid.leader_transferred);
    assert!(!invalid.voter_removed);
    assert!(!invalid.auto_promote_learner_observed);
    assert!(!invalid.auto_promote_blocked_by_pending_joint_observed);
    assert!(!invalid.pending_joint_consensus_restart_observed);
    assert!(!invalid.pending_joint_consensus_restart_recovered);
    assert!(!invalid.witness_promotion_rejected_observed);
    assert!(!invalid.witness_role_accounted_for);
    for expected in [
        "schema_valid",
        "learner_caught_up",
        "learner_promoted",
        "leader_transferred",
        "voter_removed",
        "auto_promote_learner_observed",
        "auto_promote_blocked_by_pending_joint_observed",
        "pending_joint_consensus_restart_observed",
        "pending_joint_consensus_restart_recovered",
        "witness_promotion_rejected_observed",
        "witness_role_accounted_for",
    ] {
        assert!(invalid.missing.contains(&expected.to_string()));
    }
}

#[test]
fn membership_semantics_evidence_requires_joint_quorum_commit_proof() {
    let mut artifact = rustraft_membership_semantics_evidence_artifact();
    artifact.learner_add.joint_acknowledged_voters = vec![1, 2];
    artifact.learner_add.joint_new_majority_acked = false;
    artifact.voter_remove.joint_acknowledged_voters = vec![1, 2];
    artifact.voter_remove.joint_old_majority_acked = false;

    let invalid = rustraft_validate_membership_semantics_evidence_artifact(&artifact);

    assert!(!invalid.valid);
    assert!(!invalid.learner_added);
    assert!(!invalid.voter_removed);
    assert!(invalid.missing.contains(&"learner_added".to_string()));
    assert!(invalid.missing.contains(&"voter_removed".to_string()));
}

#[test]
fn replication_pipeline_evidence_artifact_is_library_owned() {
    let limits = RustRaftPipelineLimits::production_default();
    let artifact = rustraft_replication_pipeline_evidence_artifact(
        complete_replication_pipeline_peers(),
        limits,
    );

    assert_eq!(artifact.schema, "rustraft.replication_pipeline_evidence.v1");
    assert_eq!(artifact.limits, limits);
    assert_eq!(artifact.peers.len(), 2);
    assert!(artifact.evidence.per_peer_pipeline_state_present);
    assert!(artifact.evidence.append_backpressure_enforced);
    assert!(artifact.evidence.apply_backpressure_enforced);
    assert!(artifact.evidence.memory_replicate_bytes_enforced);
    assert!(artifact.evidence.oversized_log_rejection_present);
    assert!(artifact.evidence.out_of_order_append_handling_present);
    assert!(artifact.evidence.reorder_timeout_drop_present);
    assert!(artifact.evidence.packet_loss_probe_present);
    assert!(artifact.evidence.packet_loss_recovery_present);
    assert!(artifact.evidence.reorder_convergence_present);
    assert!(artifact.evidence.packet_loss_reorder_same_peer_recovered);
    assert!(artifact.evidence.stale_term_rejection_present);
    assert!(artifact.evidence.reorder_queue_enabled);
}

#[test]
fn replication_pipeline_evidence_artifact_validator_reports_missing_fields() {
    let limits = RustRaftPipelineLimits::production_default();
    let artifact = rustraft_replication_pipeline_evidence_artifact(
        complete_replication_pipeline_peers(),
        limits,
    );
    let valid = rustraft_validate_replication_pipeline_evidence_artifact(&artifact);
    assert!(valid.valid);
    assert!(valid.missing.is_empty());

    let mut broken = artifact;
    broken.schema = "old.schema".to_string();
    broken.peers.clear();
    broken.evidence.append_backpressure_enforced = false;
    broken.evidence.packet_loss_probe_present = false;
    broken.evidence.packet_loss_recovery_present = false;
    broken.evidence.reorder_convergence_present = false;
    broken.evidence.packet_loss_reorder_same_peer_recovered = false;
    broken.evidence.reorder_queue_enabled = false;

    let invalid = rustraft_validate_replication_pipeline_evidence_artifact(&broken);

    assert!(!invalid.valid);
    assert!(!invalid.schema_valid);
    assert!(!invalid.peer_state_present);
    assert!(!invalid.append_backpressure_enforced);
    assert!(!invalid.packet_loss_probe_present);
    assert!(!invalid.packet_loss_recovery_present);
    assert!(!invalid.reorder_convergence_present);
    assert!(!invalid.packet_loss_reorder_same_peer_recovered);
    assert!(!invalid.reorder_queue_enabled);
    for expected in [
        "schema_valid",
        "peer_state_present",
        "append_backpressure_enforced",
        "packet_loss_probe_present",
        "packet_loss_recovery_present",
        "reorder_convergence_present",
        "packet_loss_reorder_same_peer_recovered",
        "reorder_queue_enabled",
    ] {
        assert!(invalid.missing.contains(&expected.to_string()));
    }
}

#[test]
fn replication_pipeline_evidence_requires_recovery_after_packet_loss_and_reorder() {
    let limits = RustRaftPipelineLimits::production_default();
    let mut peers = complete_replication_pipeline_peers();
    let recovered_peer = peers
        .iter_mut()
        .find(|peer| peer.packet_loss_events > 0)
        .expect("packet-loss peer");
    recovered_peer.append_accepted = 0;
    recovered_peer.match_index = recovered_peer.next_index.saturating_sub(2);
    recovered_peer.reorder_queue_depth = 1;

    let artifact = rustraft_replication_pipeline_evidence_artifact(peers, limits);
    let invalid = rustraft_validate_replication_pipeline_evidence_artifact(&artifact);

    assert!(!invalid.valid);
    assert!(invalid.packet_loss_probe_present);
    assert!(invalid.out_of_order_append_handling_present);
    assert!(!invalid.packet_loss_recovery_present);
    assert!(!invalid.reorder_convergence_present);
    assert!(invalid
        .missing
        .contains(&"packet_loss_recovery_present".to_string()));
    assert!(invalid
        .missing
        .contains(&"reorder_convergence_present".to_string()));
}

#[test]
fn replication_pipeline_evidence_rejects_split_peer_packet_loss_and_reorder_recovery() {
    let limits = RustRaftPipelineLimits::production_default();
    let mut peers = complete_replication_pipeline_peers();
    let packet_loss_peer = peers
        .iter_mut()
        .find(|peer| peer.packet_loss_events > 0)
        .expect("packet-loss peer");
    packet_loss_peer.out_of_order_append_rejections = 0;
    packet_loss_peer.reorder_entry_timeouts = 0;
    packet_loss_peer.reorder_dropped_packages = 0;

    let reorder_peer = peers
        .iter_mut()
        .find(|peer| peer.packet_loss_events == 0)
        .expect("reorder peer");
    reorder_peer.out_of_order_append_rejections = 1;
    reorder_peer.append_accepted = 1;
    reorder_peer.match_index = reorder_peer.next_index.saturating_sub(1);
    reorder_peer.reorder_queue_depth = 0;

    let artifact = rustraft_replication_pipeline_evidence_artifact(peers, limits);
    let invalid = rustraft_validate_replication_pipeline_evidence_artifact(&artifact);

    assert!(!invalid.valid);
    assert!(invalid.packet_loss_recovery_present);
    assert!(invalid.reorder_convergence_present);
    assert!(!invalid.packet_loss_reorder_same_peer_recovered);
    assert!(invalid
        .missing
        .contains(&"packet_loss_reorder_same_peer_recovered".to_string()));
}

#[test]
fn snapshot_lifecycle_evidence_artifact_is_library_owned() {
    let artifact = rustraft_snapshot_lifecycle_evidence_artifact(
        complete_snapshot_lifecycle_peers(),
        1_000,
        1,
    );

    assert_eq!(artifact.schema, "rustraft.snapshot_lifecycle_evidence.v1");
    assert_eq!(artifact.send_snapshot_timeout_ms, 1_000);
    assert_eq!(artifact.max_inflights_replicate, 1);
    assert_eq!(artifact.peers.len(), 2);
    assert!(artifact.evidence.sender_lifecycle_present);
    assert!(artifact.evidence.downloader_lifecycle_present);
    assert!(artifact.evidence.retry_backpressure_present);
    assert!(artifact.evidence.chunk_retry_present);
    assert!(artifact.evidence.send_timeout_present);
    assert!(artifact.evidence.rate_limit_present);
    assert!(artifact.evidence.sustained_sender_load_present);
    assert!(artifact.evidence.sustained_downloader_load_present);
    assert!(artifact.evidence.sustained_sender_completion_present);
    assert!(artifact.evidence.sustained_downloader_completion_present);
    assert!(artifact.evidence.install_progress_present);
    assert!(artifact.evidence.install_rollback_present);
    assert!(artifact.evidence.membership_change_present);
    assert!(artifact.evidence.rejoin_after_compacted_log_present);
}

#[test]
fn snapshot_lifecycle_evidence_artifact_validator_reports_missing_fields() {
    let artifact = rustraft_snapshot_lifecycle_evidence_artifact(
        complete_snapshot_lifecycle_peers(),
        1_000,
        1,
    );
    let valid = rustraft_validate_snapshot_lifecycle_evidence_artifact(&artifact);
    assert!(valid.valid);
    assert!(valid.missing.is_empty());

    let mut broken = artifact;
    broken.schema = "old.schema".to_string();
    broken.send_snapshot_timeout_ms = 0;
    broken.peers.clear();
    broken.evidence.chunk_retry_present = false;
    broken.evidence.sustained_sender_load_present = false;
    broken.evidence.sustained_sender_completion_present = false;
    broken.evidence.sustained_downloader_completion_present = false;
    broken.evidence.install_rollback_present = false;

    let invalid = rustraft_validate_snapshot_lifecycle_evidence_artifact(&broken);

    assert!(!invalid.valid);
    assert!(!invalid.schema_valid);
    assert!(!invalid.sender_lifecycle_present);
    assert!(!invalid.downloader_lifecycle_present);
    assert!(!invalid.chunk_retry_present);
    assert!(!invalid.sustained_sender_load_present);
    assert!(!invalid.sustained_downloader_load_present);
    assert!(!invalid.sustained_sender_completion_present);
    assert!(!invalid.sustained_downloader_completion_present);
    assert!(!invalid.install_rollback_present);
    assert!(!invalid.rejoin_after_compacted_log_present);
    for expected in [
        "schema_valid",
        "sender_lifecycle_present",
        "downloader_lifecycle_present",
        "chunk_retry_present",
        "sustained_sender_load_present",
        "sustained_downloader_load_present",
        "sustained_sender_completion_present",
        "sustained_downloader_completion_present",
        "install_rollback_present",
        "rejoin_after_compacted_log_present",
    ] {
        assert!(invalid.missing.contains(&expected.to_string()));
    }
}

#[test]
fn snapshot_lifecycle_evidence_requires_sustained_completion() {
    let mut peers = complete_snapshot_lifecycle_peers();
    let sender = peers
        .iter_mut()
        .find(|peer| peer.snapshot_send_attempts > 0)
        .expect("sender peer");
    sender.acked_snapshot_index = sender.required_snapshot_index.saturating_sub(1);
    let downloader = peers
        .iter_mut()
        .find(|peer| peer.snapshot_installed_index > 0)
        .expect("downloader peer");
    downloader.snapshot_installing = true;
    downloader.snapshot_install_progress_per_mille = 750;

    let artifact = rustraft_snapshot_lifecycle_evidence_artifact(peers, 1_000, 1);
    let invalid = rustraft_validate_snapshot_lifecycle_evidence_artifact(&artifact);

    assert!(!invalid.valid);
    assert!(invalid.sustained_sender_load_present);
    assert!(invalid.sustained_downloader_load_present);
    assert!(!invalid.sustained_sender_completion_present);
    assert!(!invalid.sustained_downloader_completion_present);
    assert!(invalid
        .missing
        .contains(&"sustained_sender_completion_present".to_string()));
    assert!(invalid
        .missing
        .contains(&"sustained_downloader_completion_present".to_string()));
}

#[test]
fn wal_lifecycle_evidence_artifact_is_library_owned() {
    let artifact = rustraft_wal_lifecycle_evidence_artifact(complete_wal_lifecycle_status());

    assert_eq!(artifact.schema, "rustraft.wal_lifecycle_evidence.v1");
    assert_eq!(artifact.status.segment_count, 3);
    assert_eq!(artifact.status.first_log_index, 101);
    assert_eq!(artifact.status.last_log_index, 228);
    assert!(artifact.evidence.segment_lifecycle_present);
    assert!(artifact.evidence.retained_range_present);
    assert!(artifact.evidence.sequence_range_present);
    assert!(artifact.evidence.log_index_range_present);
    assert!(artifact.evidence.compaction_observed);
    assert!(artifact.evidence.slow_fsync_backpressure_observed);
    assert!(artifact.evidence.compaction_after_slow_fsync_observed);
}

#[test]
fn wal_lifecycle_evidence_artifact_validator_reports_missing_fields() {
    let artifact = rustraft_wal_lifecycle_evidence_artifact(complete_wal_lifecycle_status());
    let valid = rustraft_validate_wal_lifecycle_evidence_artifact(&artifact);
    assert!(valid.valid);
    assert!(valid.missing.is_empty());

    let mut broken = artifact;
    broken.schema = "old.schema".to_string();
    broken.status.segment_count = 0;
    broken.status.total_records = 0;
    broken.status.last_log_index = 0;
    broken.status.released_segment_count = 0;
    broken.status.slow_fsync_backpressure_observed = false;
    broken.status.compacted_after_slow_fsync_count = 0;
    broken.evidence.retained_range_present = false;

    let invalid = rustraft_validate_wal_lifecycle_evidence_artifact(&broken);

    assert!(!invalid.valid);
    assert!(!invalid.schema_valid);
    assert!(!invalid.segment_lifecycle_present);
    assert!(!invalid.retained_range_present);
    assert!(!invalid.sequence_range_present);
    assert!(!invalid.log_index_range_present);
    assert!(!invalid.compaction_observed);
    assert!(!invalid.slow_fsync_backpressure_observed);
    assert!(!invalid.compaction_after_slow_fsync_observed);
    for expected in [
        "schema_valid",
        "segment_lifecycle_present",
        "retained_range_present",
        "sequence_range_present",
        "log_index_range_present",
        "compaction_observed",
        "slow_fsync_backpressure_observed",
        "compaction_after_slow_fsync_observed",
    ] {
        assert!(invalid.missing.contains(&expected.to_string()));
    }
}

#[test]
fn reference_raft_operational_evidence_bundle_is_library_owned() {
    let bundle = rustraft_reference_raft_operational_evidence_bundle(
        complete_replication_pipeline_peers(),
        RustRaftPipelineLimits::production_default(),
        complete_snapshot_lifecycle_peers(),
        1_000,
        1,
        complete_wal_lifecycle_status(),
    );

    assert_eq!(
        bundle.schema,
        "rustraft.reference_raft_operational_evidence_bundle.v1"
    );
    let validation = rustraft_validate_reference_raft_operational_evidence_bundle(&bundle);
    assert!(validation.valid, "{validation:#?}");
    assert!(validation.read_safety_valid);
    assert!(validation.membership_valid);
    assert!(validation.replication_pipeline_valid);
    assert!(validation.snapshot_lifecycle_valid);
    assert!(validation.wal_lifecycle_valid);
    assert!(validation.missing.is_empty());
}

#[test]
fn reference_raft_operational_evidence_bundle_validator_prefixes_missing_fields() {
    let mut bundle = rustraft_reference_raft_operational_evidence_bundle(
        complete_replication_pipeline_peers(),
        RustRaftPipelineLimits::production_default(),
        complete_snapshot_lifecycle_peers(),
        1_000,
        1,
        complete_wal_lifecycle_status(),
    );
    bundle.schema = "old.schema".to_string();
    bundle.read_safety.stale_leader_lease.allowed = true;
    bundle.replication_pipeline.peers.clear();
    bundle.snapshot_lifecycle.peers.clear();
    bundle.wal_lifecycle.status.released_segment_count = 0;

    let validation = rustraft_validate_reference_raft_operational_evidence_bundle(&bundle);

    assert!(!validation.valid);
    assert!(!validation.schema_valid);
    assert!(!validation.read_safety_valid);
    assert!(!validation.replication_pipeline_valid);
    assert!(!validation.snapshot_lifecycle_valid);
    assert!(!validation.wal_lifecycle_valid);
    for expected in [
        "schema_valid",
        "read_safety.stale_leader_lease_rejected",
        "replication_pipeline.peer_state_present",
        "snapshot_lifecycle.sender_lifecycle_present",
        "wal_lifecycle.compaction_observed",
    ] {
        assert!(validation.missing.contains(&expected.to_string()));
    }
}

#[test]
fn strict_process_rollout_helpers_require_crash_window_evidence() {
    let mut data = ready_data_rollout_three_processes();
    let mut meta = ready_meta_rollout_three_processes();
    assert!(rustraft_data_node_strict_process_rollout_validated(&data));
    assert!(rustraft_meta_strict_process_rollout_validated(&meta));

    data.crash_after_storage_mutation_recovered = false;
    meta.crash_during_meta_snapshot_install_recovered = false;

    assert!(!rustraft_data_node_strict_process_rollout_validated(&data));
    assert!(!rustraft_meta_strict_process_rollout_validated(&meta));
}

#[test]
fn reference_raft_runtime_capability_report_requires_read_safety_on_both_planes() {
    let mut input = ready_input();
    input
        .metaserver_rollout
        .as_mut()
        .unwrap()
        .operational_semantics
        .minority_partition_read_rejection_observed = false;

    let report = rustraft_reference_raft_runtime_capability_report(&input);
    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"read_index_and_lease_safety".to_string()));
    assert!(report.blockers.iter().any(|blocker| {
        blocker
            == "read_index_and_lease_safety:missing:semantics.minority_partition_read_rejection_observed"
    }));
}

#[test]
fn reference_raft_runtime_capability_prometheus_exports_generic_metrics() {
    let mut input = ready_input();
    input.wal_lifecycle.as_mut().unwrap().compaction_observed = false;

    let report = rustraft_reference_raft_runtime_capability_report(&input);
    let metrics = rustraft_reference_raft_runtime_capability_prometheus(
        &report,
        &[("plane", "data_node"), ("cluster", "a\"b\\c\n")],
    );

    assert_eq!(metrics.format, "prometheus_text_v0.0.4");
    assert!(metrics.metric_count > report.capability_evidence.len() as u64);
    assert!(metrics
        .text
        .contains("# HELP rustraft_reference_raft_ready"));
    assert!(metrics.text.contains(
        "rustraft_reference_raft_ready{plane=\"data_node\",cluster=\"a\\\"b\\\\c\\n\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_reference_raft_capability_ready{plane=\"data_node\",cluster=\"a\\\"b\\\\c\\n\",capability=\"wal_segment_lifecycle\""
    ));
    assert!(metrics.text.contains(
        "rustraft_reference_raft_capability_field_present{plane=\"data_node\",cluster=\"a\\\"b\\\\c\\n\",capability=\"wal_segment_lifecycle\",field=\"wal.compaction_observed\"} 0"
    ));
    assert!(metrics.text.contains(
        "rustraft_reference_raft_blocker_present{plane=\"data_node\",cluster=\"a\\\"b\\\\c\\n\",blocker=\"wal_segment_lifecycle:missing:wal.compaction_observed\"} 1"
    ));
    assert!(metrics
        .text
        .contains("rustraft_reference_raft_missing_capability_count"));
}

#[test]
fn deployment_mode_validation_is_library_owned_and_fails_closed() {
    let ready = rustraft_production_readiness_report(&ready_input());
    assert!(ready.ready);
    assert!(rustraft_validate_deployment_mode(
        RustRaftDeploymentMode::ProductionDistributed,
        &ready
    )
    .is_ok());
    assert!(rustraft_require_production_ready(&ready).is_ok());

    let local_err =
        rustraft_validate_deployment_mode(RustRaftDeploymentMode::LocalModel, &ready).unwrap_err();
    assert_eq!(local_err.mode, RustRaftDeploymentMode::LocalModel);
    assert!(local_err
        .message
        .contains("local Raft deployment mode is disabled"));

    let mut blocked_input = ready_input();
    blocked_input.peer_pipeline = None;
    let blocked = rustraft_production_readiness_report(&blocked_input);
    let production_err =
        rustraft_validate_deployment_mode(RustRaftDeploymentMode::ProductionDistributed, &blocked)
            .unwrap_err();
    assert_eq!(
        production_err.mode,
        RustRaftDeploymentMode::ProductionDistributed
    );
    assert!(production_err
        .message
        .contains("distributed Raft is not production-ready"));
    assert!(production_err
        .missing
        .contains(&"pipeline:evidence_present".to_string()));

    let direct_err = rustraft_validate_deployment_readiness(
        RustRaftDeploymentMode::ProductionDistributed,
        false,
        vec!["process_path:missing".to_string()],
    )
    .unwrap_err();
    assert_eq!(direct_err.missing, vec!["process_path:missing"]);
}

#[test]
fn named_readiness_blockers_expand_component_missing_details() {
    let blockers = rustraft_named_readiness_blockers(
        "transport_security_missing",
        "transport_security.{mtls,auth}",
        ["mtls disabled", "auth token missing"],
    );

    assert_eq!(blockers.len(), 2);
    assert!(blockers.iter().all(|blocker| {
        blocker.blocker == "transport_security_missing"
            && blocker.evidence_field == "transport_security.{mtls,auth}"
    }));
    assert_eq!(blockers[0].detail, "mtls disabled");
    assert_eq!(blockers[1].detail, "auth token missing");
}
