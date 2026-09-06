// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// crate-level regression tests.
// Split from src/lib.rs to keep the crate facade small and focused.

#[cfg(test)]
mod log_addressing_tests {
    use super::*;

    fn entry(term: Term, index: LogIndex, bytes: usize) -> LogEntry {
        LogEntry {
            log_id: LogId { term, index },
            payload: vec![b'x'; bytes],
            is_command: false,
        }
    }

    fn summed_payload_bytes(node: &Node) -> u64 {
        node.log.iter().map(|entry| entry.payload.len() as u64).sum()
    }

    fn assert_byte_count_matches_log(node: &Node, stage: &str) {
        assert_eq!(
            node.retained_log_bytes(),
            summed_payload_bytes(node),
            "retained byte count drifted from the log after {stage}"
        );
    }

    fn voter() -> Node {
        Node::new(1, ReplicaRole::Voter, false)
    }

    #[test]
    fn retained_byte_count_follows_every_log_mutation() {
        let mut node = voter();
        assert_byte_count_matches_log(&node, "construction");

        for index in 1..=10 {
            node.append_entry(entry(1, index, index as usize * 4));
        }
        assert_byte_count_matches_log(&node, "appends");
        assert_eq!(node.retained_log_bytes(), (1..=10_u64).map(|i| i * 4).sum::<u64>());

        // Re-appending an occupied index drops the conflicting tail with it.
        node.append_entry(entry(2, 6, 1));
        assert_byte_count_matches_log(&node, "conflicting append");
        assert_eq!(node.log.len(), 6);
        assert_eq!(node.log_term_at(6), Some(2));

        node.truncate_log_from(4);
        assert_byte_count_matches_log(&node, "truncate");
        assert_eq!(node.log.len(), 3);

        // Truncating past the tail is a no-op, not a rewind.
        node.truncate_log_from(99);
        assert_byte_count_matches_log(&node, "truncate past the tail");
        assert_eq!(node.log.len(), 3);

        let discarded = node.discard_log_through(2);
        assert_eq!(discarded, 2);
        assert_byte_count_matches_log(&node, "discard");
        assert_eq!(node.log.first().map(|entry| entry.log_id.index), Some(3));

        node.set_log(vec![entry(3, 20, 7), entry(3, 21, 9)]);
        assert_byte_count_matches_log(&node, "restore");
        assert_eq!(node.retained_log_bytes(), 16);
    }

    #[test]
    fn log_term_lookup_survives_a_compacted_prefix() {
        let mut node = voter();
        for index in 1..=8 {
            let term = if index <= 4 { 1 } else { 2 };
            node.append_entry(entry(term, index, 3));
        }
        node.discard_log_through(5);

        assert_eq!(node.log_term_at(0), Some(0));
        // Indices below the retained prefix are gone, not mislabelled.
        assert_eq!(node.log_term_at(3), None);
        assert_eq!(node.log_term_at(5), None);
        assert_eq!(node.log_term_at(6), Some(2));
        assert_eq!(node.log_term_at(8), Some(2));
        assert_eq!(node.log_term_at(9), None);
    }

    #[test]
    fn log_positions_agree_with_a_scan() {
        let mut node = voter();
        for index in 1..=32 {
            node.append_entry(entry(1, index, 2));
        }
        node.discard_log_through(7);

        for log_index in 0..40 {
            let scanned = node
                .log
                .iter()
                .position(|entry| entry.log_id.index == log_index);
            assert_eq!(
                node.log_position(log_index),
                scanned,
                "log_position disagreed with a scan at index {log_index}"
            );

            let scanned_after = node
                .log
                .iter()
                .position(|entry| entry.log_id.index >= log_index);
            assert_eq!(
                node.log_position_at_or_after(log_index),
                scanned_after,
                "log_position_at_or_after disagreed with a scan at index {log_index}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_snapshot() -> ReadinessSnapshot {
        ReadinessSnapshot {
            matrixraft_leader_write_authority_present: true,
            matrixraft_operator_observability_present: true,
            matrixraft_rpc_transport_contract_present: true,
            matrixraft_log_retention_snapshot_trigger_present: true,
            matrixraft_apply_snapshot_fence_present: true,
            raft_storage_apply_fence_present: true,
            matrixraft_snapshot_floor_log_matching_present: true,
            matrixraft_snapshot_tail_catchup_present: true,
            matrixraft_compacted_entry_rejection_present: true,
            matrixraft_metaserver_snapshot_floor_election_present: true,
            learner_catchup_promotion_present: true,
            metaserver_membership_workflow_present: true,
        }
    }

    fn ready_operational_semantics() -> ProcessOperationalSemanticsEvidence {
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

    fn ready_process_nodes() -> Vec<ProcessNodeEvidence> {
        vec![
            ProcessNodeEvidence {
                node_id: 1,
                addr: "127.0.0.1:19001".to_string(),
                wal_dir: "/tmp/rustraft/node1/wal".to_string(),
                snapshot_dir: "/tmp/rustraft/node1/snapshots".to_string(),
                commit_index: 42,
                applied_index: 42,
                snapshot_id: Some("snap-40".to_string()),
                restarted: true,
                log_store_validated: true,
            },
            ProcessNodeEvidence {
                node_id: 2,
                addr: "127.0.0.1:19002".to_string(),
                wal_dir: "/tmp/rustraft/node2/wal".to_string(),
                snapshot_dir: "/tmp/rustraft/node2/snapshots".to_string(),
                commit_index: 42,
                applied_index: 42,
                snapshot_id: Some("snap-40".to_string()),
                restarted: true,
                log_store_validated: true,
            },
        ]
    }

    fn ready_data_node_rollout() -> DataNodeProcessRolloutReport {
        DataNodeProcessRolloutReport {
            shard_id: 7,
            voters: vec![1, 2, 3],
            learners: vec![4],
            nodes: ready_process_nodes(),
            spawned_process_count: 2,
            independent_wal_dirs: true,
            independent_snapshot_dirs: true,
            observed_process_requests: 16,
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
            operational_semantics: ready_operational_semantics(),
            ready: true,
            blockers: Vec::new(),
        }
    }

    fn ready_meta_rollout() -> MetaProcessRolloutReport {
        MetaProcessRolloutReport {
            voters: vec![1, 2, 3],
            learners: vec![4],
            nodes: ready_process_nodes(),
            spawned_process_count: 2,
            independent_wal_dirs: true,
            independent_snapshot_dirs: true,
            observed_process_requests: 20,
            read_index_responses_observed: 10,
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
            operational_semantics: ready_operational_semantics(),
            ready: true,
            blockers: Vec::new(),
        }
    }

    fn membership_transition(
        scope: MembershipScope,
        transition: MembershipTransitionKind,
    ) -> MembershipTransitionEvidence {
        match transition {
            MembershipTransitionKind::Failover => MembershipTransitionEvidence {
                scope,
                transition,
                before_voters: vec![1, 2, 3],
                after_voters: vec![1, 2, 3],
                before_learners: Vec::new(),
                after_learners: Vec::new(),
                leader_before: Some(1),
                leader_after: Some(2),
                failed_or_removed_nodes: vec![1],
                added_nodes: Vec::new(),
                caught_up_nodes: vec![2, 3],
                commit_index_before: 100,
                commit_index_after: 104,
                applied_index_after: 104,
                joint_consensus_used: false,
                old_majority_preserved: true,
                new_majority_reached: true,
                joint_old_quorum_size: 0,
                joint_new_quorum_size: 0,
                joint_acknowledged_voters: Vec::new(),
                joint_old_majority_acked: false,
                joint_new_majority_acked: false,
                stale_leader_rejected: true,
                read_index_validated_after: true,
                write_validated_after: true,
                snapshot_floor_preserved: true,
                secondary_replication_visible: true,
                scheduler_generation_advanced: matches!(scope, MembershipScope::Metaserver),
                blockers: Vec::new(),
            },
            MembershipTransitionKind::ScaleUp => MembershipTransitionEvidence {
                scope,
                transition,
                before_voters: vec![1, 2, 3],
                after_voters: vec![1, 2, 3, 4],
                before_learners: vec![4],
                after_learners: Vec::new(),
                leader_before: Some(1),
                leader_after: Some(1),
                failed_or_removed_nodes: Vec::new(),
                added_nodes: vec![4],
                caught_up_nodes: vec![4],
                commit_index_before: 100,
                commit_index_after: 108,
                applied_index_after: 108,
                joint_consensus_used: true,
                old_majority_preserved: true,
                new_majority_reached: true,
                joint_old_quorum_size: 2,
                joint_new_quorum_size: 3,
                joint_acknowledged_voters: vec![1, 2, 3, 4],
                joint_old_majority_acked: true,
                joint_new_majority_acked: true,
                stale_leader_rejected: true,
                read_index_validated_after: true,
                write_validated_after: true,
                snapshot_floor_preserved: true,
                secondary_replication_visible: true,
                scheduler_generation_advanced: matches!(scope, MembershipScope::Metaserver),
                blockers: Vec::new(),
            },
            MembershipTransitionKind::ScaleDown => MembershipTransitionEvidence {
                scope,
                transition,
                before_voters: vec![1, 2, 3, 4],
                after_voters: vec![1, 2, 3],
                before_learners: Vec::new(),
                after_learners: Vec::new(),
                leader_before: Some(1),
                leader_after: Some(1),
                failed_or_removed_nodes: vec![4],
                added_nodes: Vec::new(),
                caught_up_nodes: vec![1, 2, 3],
                commit_index_before: 108,
                commit_index_after: 112,
                applied_index_after: 112,
                joint_consensus_used: true,
                old_majority_preserved: true,
                new_majority_reached: true,
                joint_old_quorum_size: 3,
                joint_new_quorum_size: 2,
                joint_acknowledged_voters: vec![1, 2, 3, 4],
                joint_old_majority_acked: true,
                joint_new_majority_acked: true,
                stale_leader_rejected: true,
                read_index_validated_after: true,
                write_validated_after: true,
                snapshot_floor_preserved: true,
                secondary_replication_visible: true,
                scheduler_generation_advanced: matches!(scope, MembershipScope::Metaserver),
                blockers: Vec::new(),
            },
        }
    }

    fn ready_membership_transitions() -> Vec<MembershipTransitionEvidence> {
        [
            MembershipScope::Metaserver,
            MembershipScope::DataNode,
        ]
        .into_iter()
        .flat_map(|scope| {
            [
                MembershipTransitionKind::Failover,
                MembershipTransitionKind::ScaleUp,
                MembershipTransitionKind::ScaleDown,
            ]
            .into_iter()
            .map(move |transition| membership_transition(scope, transition))
        })
        .collect()
    }

    fn ready_admin_status_surface() -> AdminStatusSurfaceEvidence {
        let limits = PipelineLimits::production_default();
        let mut peer_2 = PeerProgress::new(2, 105, limits);
        peer_2.match_index = 104;
        peer_2.append_requests = 8;
        peer_2.append_accepted = 8;
        peer_2.append_queue_max_depth = 4;
        peer_2.apply_queue_max_depth = 3;

        let mut peer_3 = PeerProgress::new(3, 105, limits);
        peer_3.match_index = 104;
        peer_3.append_requests = 7;
        peer_3.append_accepted = 7;
        peer_3.inflight_entries = 1;
        peer_3.inflight_bytes = 128;

        matrixraft_admin_status_surface_evidence(&AdminStatusSurfaceInput {
            commit_index: 104,
            max_observed_node_commit_index: 104,
            quorum_size: 2,
            quorum_peer_ids: vec![2, 3],
            peer_pipeline: vec![peer_2, peer_3],
            wal_last_log_index: 110,
            wal_segment_lifecycle_present: true,
        })
    }

    fn ready_fault_harness() -> fault::FaultHarnessReadinessReport {
        let evidence = fault::matrixraft_baseline_raft_fault_scenarios()
            .into_iter()
            .map(|requirement| fault::FaultScenarioEvidence {
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
                observed_acceptance: requirement.acceptance.clone(),
                report_path: Some(format!("reports/{}.json", requirement.scenario.id())),
            })
            .collect::<Vec<_>>();
        fault::matrixraft_fault_harness_readiness_report(&evidence)
    }

    fn ready_production_input() -> ProductionReadinessInput {
        ProductionReadinessInput {
            readiness: ready_snapshot(),
            peer_pipeline: Some(PipelineEvidence {
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
                peer_pipeline_observed_peer_count: 2,
                packet_loss_reorder_faulted_peer_count: 1,
                packet_loss_reorder_recovered_peer_count: 1,
                packet_loss_reorder_all_faulted_peers_recovered: true,
                stale_term_rejection_present: true,
                reorder_queue_enabled: true,
            }),
            runtime_pressure_admission: Some(RuntimePressureAdmission {
                accepted: true,
                memory_pressure: false,
                memory_pressure_details: Vec::new(),
                latency_pressure: false,
                latency_pressure_details: Vec::new(),
                scale_pressure: false,
                scale_pressure_details: Vec::new(),
                pipeline_pressure: false,
                pipeline_pressure_details: Vec::new(),
                read_backlog_pressure: false,
                read_backlog_pressure_details: Vec::new(),
                node_runtime_timer_pressure: false,
                node_runtime_timer_pressure_details: Vec::new(),
                reason: "accepted_no_pressure".to_string(),
                rejected_component: None,
                actions: Vec::new(),
            }),
            snapshot_lifecycle: Some(SnapshotLifecycleEvidence {
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
                sustained_transfer_completion_present: true,
                snapshot_peer_count: 2,
                sustained_transfer_completed_peer_count: 1,
                install_progress_present: true,
                install_rollback_present: true,
                membership_change_present: true,
                rejoin_after_compacted_log_present: true,
            }),
            wal_lifecycle: Some(WalLifecycleEvidence {
                segment_lifecycle_present: true,
                retained_range_present: true,
                sequence_range_present: true,
                log_index_range_present: true,
                compaction_observed: true,
                slow_fsync_backpressure_observed: true,
                compaction_after_slow_fsync_observed: true,
                released_segment_count: 1,
                compacted_after_slow_fsync_count: 1,
                slow_fsync_segment_count: 1,
                compacted_slow_fsync_segment_count: 1,
            }),
            admin_status_surface: Some(ready_admin_status_surface()),
            fault_harness: Some(ready_fault_harness()),
            data_node_rollout: Some(ready_data_node_rollout()),
            metaserver_rollout: Some(ready_meta_rollout()),
            membership_transitions: ready_membership_transitions(),
            baseline_raft_benchmark: Some(BaselineRaftBenchmarkEvidence {
                real_baseline_raft: true,
                matrixraft_runtime: true,
                baseline_raft_reference: true,
                matrixraft_rust_candidate: true,
                correctness_passed: true,
                performance_within_threshold: true,
                resource_within_threshold: true,
                workloads: crate::benchmark::matrixraft_baseline_raft_benchmark_workloads()
                    .into_iter()
                    .map(|workload| workload.id().to_string())
                    .collect(),
                blockers: Vec::new(),
                missing_baseline_raft_binaries: Vec::new(),
                unsupported_workloads: Vec::new(),
                correctness_blockers: Vec::new(),
                performance_blockers: Vec::new(),
            }),
        }
    }

    fn ready_benchmark_sample(
        workload: crate::benchmark::BenchmarkWorkload,
        engine: crate::benchmark::BenchmarkEngine,
        engine_source: crate::benchmark::BenchmarkEngineSource,
        p50_latency_micros: u64,
        p99_latency_micros: u64,
        throughput_ops_per_sec: f64,
    ) -> crate::benchmark::BenchmarkSample {
        let operation_count = match workload {
            crate::benchmark::BenchmarkWorkload::BatchedWrites
            | crate::benchmark::BenchmarkWorkload::ReplicationBatching => 128 * 16,
            _ => 128,
        };
        let operations_per_timed_iteration = match workload {
            crate::benchmark::BenchmarkWorkload::BatchedWrites
            | crate::benchmark::BenchmarkWorkload::ReplicationBatching => 16,
            _ => 1,
        };
        let total_duration_micros =
            ((operation_count as f64 / throughput_ops_per_sec) * 1_000_000.0).round() as u64;
        crate::benchmark::BenchmarkSample {
            workload,
            engine,
            engine_source,
            benchmark_run_id: "ready-benchmark-run".to_string(),
            implementation: match engine {
                crate::benchmark::BenchmarkEngine::BaselineRaft => {
                    crate::benchmark::BenchmarkImplementation::BaselineRaft
                }
                crate::benchmark::BenchmarkEngine::RustRaft => {
                    crate::benchmark::BenchmarkImplementation::RustRaftRust
                }
            },
            binary_path: Some(ready_benchmark_binary_path(engine)),
            git_revision: Some(
                match engine {
                    crate::benchmark::BenchmarkEngine::BaselineRaft => {
                        "1111111111111111111111111111111111111111"
                    }
                    crate::benchmark::BenchmarkEngine::RustRaft => {
                        "2222222222222222222222222222222222222222"
                    }
                }
                .to_string(),
            ),
            build_profile: "release".to_string(),
            harness_kind: match engine {
                crate::benchmark::BenchmarkEngine::BaselineRaft => {
                    crate::benchmark::BenchmarkHarnessKind::FullBaselineRaftHarness
                }
                crate::benchmark::BenchmarkEngine::RustRaft => {
                    crate::benchmark::BenchmarkHarnessKind::RustRaftRuntime
                }
            },
            node_count: crate::benchmark::MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_NODE_COUNT,
            iterations_per_workload: 128,
            batch_size: 16,
            payload_size_bytes: crate::benchmark::MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_PAYLOAD_SIZE_BYTES,
            timed_iteration_count: 128,
            operations_per_timed_iteration,
            total_duration_micros,
            operation_count,
            p50_latency_micros,
            p99_latency_micros,
            throughput_ops_per_sec,
            cpu_utilization_percent: match engine {
                crate::benchmark::BenchmarkEngine::BaselineRaft => 50.0,
                crate::benchmark::BenchmarkEngine::RustRaft => 52.0,
            },
            peak_resident_memory_bytes: match engine {
                crate::benchmark::BenchmarkEngine::BaselineRaft => 512 * 1024 * 1024,
                crate::benchmark::BenchmarkEngine::RustRaft => 544 * 1024 * 1024,
            },
            correctness_passed: true,
            blockers: Vec::new(),
        }
    }

    fn ready_benchmark_binary_path(
        engine: crate::benchmark::BenchmarkEngine,
    ) -> String {
        let name = match engine {
            crate::benchmark::BenchmarkEngine::BaselineRaft => {
                "baseline_raft_kvbench"
            }
            crate::benchmark::BenchmarkEngine::RustRaft => "rustraft-kvbench",
        };
        let dir = std::env::temp_dir().join(format!(
            "rustraft-ready-benchmark-bins-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create ready benchmark binary dir");
        let path = dir.join(name);
        if !path.is_file() {
            std::fs::write(&path, b"ready benchmark binary fixture")
                .expect("write ready benchmark binary fixture");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = std::fs::metadata(&path)
                .expect("ready benchmark binary metadata")
                .permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&path, permissions)
                .expect("mark ready benchmark binary executable");
        }
        path.display().to_string()
    }

    fn ready_benchmark_report() -> crate::benchmark::BenchmarkReport {
        let comparisons = crate::benchmark::matrixraft_baseline_raft_benchmark_workloads()
            .into_iter()
            .map(|workload| {
                let baseline_raft = ready_benchmark_sample(
                    workload,
                    crate::benchmark::BenchmarkEngine::BaselineRaft,
                    crate::benchmark::BenchmarkEngineSource::RealBaselineRaft,
                    100,
                    200,
                    1_000.0,
                );
                let rustraft = ready_benchmark_sample(
                    workload,
                    crate::benchmark::BenchmarkEngine::RustRaft,
                    crate::benchmark::BenchmarkEngineSource::RustRaftRuntime,
                    110,
                    220,
                    900.0,
                );
                crate::benchmark::BenchmarkComparison {
                    workload,
                    baseline_raft,
                    rustraft,
                    p50_ratio: 1.1,
                    p99_ratio: 1.1,
                    throughput_ratio: 0.9,
                    cpu_ratio: 1.04,
                    peak_resident_memory_ratio: 1.0625,
                    passed: true,
                    blockers: Vec::new(),
                }
            })
            .collect();
        crate::benchmark::BenchmarkReport {
            schema: crate::benchmark::MATRIXRAFT_BENCHMARK_REPORT_SCHEMA.to_string(),
            generated_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_millis() as u64,
            benchmark_run_id: "ready-benchmark-run".to_string(),
            environment_fingerprint:
                "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false"
                    .to_string(),
            node_count: crate::benchmark::MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_NODE_COUNT,
            options: crate::benchmark::BenchmarkOptions::default(),
            pass_tolerance_percent: crate::benchmark::MATRIXRAFT_BENCHMARK_MAX_PRODUCTION_PASS_TOLERANCE_PERCENT,
            correctness_required: true,
            required_workloads: crate::benchmark::matrixraft_baseline_raft_benchmark_required_workloads(),
            passed: true,
            comparisons,
        }
    }

    #[test]
    fn contract_is_openraft_free_and_complete() {
        let contract = matrixraft_parity_contract();
        assert!(contract.openraft_dependency_removed);
        assert_eq!(contract.requirements.len(), 12);
    }

    #[test]
    fn crate_readme_documents_open_source_contract_surface() {
        let readme = include_str!("../../README.md");
        assert!(readme.contains("RustRaft"));
        assert!(readme.contains("matrixraft_parity_report"));
        assert!(readme.contains("matrixraft_production_readiness_report"));
        assert!(readme.contains("matrixraft_membership_readiness_metric_names"));
        assert!(readme.contains("matrixraft_membership_readiness_prometheus"));
        assert!(readme.contains("matrixraft_membership_readiness_grafana_panels"));
        assert!(readme.contains("matrixraft_membership_readiness_diagnostic_json_lines"));
        assert!(readme.contains(
            "matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts"
        ));
        assert!(readme.contains(
            "matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts"
        ));
        assert!(readme.contains("matrixraft_benchmark_runtime_pressure_readiness_artifact"));
        assert!(readme.contains("matrixraft_read_release_pressure_snapshot"));
        assert!(readme.contains("matrixraft_write_release_pressure_snapshot_atomic"));
        assert!(readme.contains(
            "matrixraft_validate_benchmark_runtime_pressure_readiness_artifact"
        ));
        assert!(readme.contains("rustraft.runtime_pressure.memory"));
        assert!(readme.contains("rustraft.runtime_pressure.latency"));
        assert!(readme.contains("rustraft.runtime_pressure.scale"));
        assert!(readme.contains("rustraft_membership_*"));
        assert!(readme.contains("OpenRaft-free"));
        assert!(readme.contains("Apache-2.0"));
    }

    #[test]
    fn report_fails_closed() {
        let mut snapshot = ready_snapshot();
        snapshot.raft_storage_apply_fence_present = false;
        let report = matrixraft_parity_report(&snapshot);
        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert_eq!(report.missing, vec!["storage_apply_fence".to_string()]);
    }

    #[test]
    fn production_readiness_gate_accepts_complete_evidence() {
        let report = matrixraft_production_readiness_report(&ready_production_input());
        assert!(report.ready, "{report:#?}");
        assert_eq!(
            report.production_status,
            ProductionStatus::ProductionReady
        );
        assert!(report.missing.is_empty());
        assert!(report.production_blockers.is_empty());
        assert_eq!(report.public_api.storage_trait, "Storage");
        assert!(report
            .satisfied
            .contains(&"public_api:contract_valid".to_string()));
        assert!(report
            .satisfied
            .contains(&"public_api:required_reference_mappings_present".to_string()));
    }

    #[test]
    fn production_readiness_gate_fails_closed_on_public_api_mapping_drift() {
        let mut satisfied = Vec::new();
        let mut missing = Vec::new();
        let mut blockers = Vec::new();
        let mut actions = Vec::new();
        let validation = PublicApiContractValidationReport {
            ready: false,
            mapped_canonical_names: vec!["Storage".to_string()],
            unmapped_advertised_names: vec!["ReadIndexRequest".to_string()],
            unmapped_reference_required_names: vec!["ReadIndexRequest".to_string()],
            api_mapping_coverage_percent: 50,
            mapping_coverage_by_category: Vec::new(),
            reference_required_names: vec!["Storage".to_string(), "ReadIndexRequest".to_string()],
            interface_name_count: 2,
            blockers: vec![
                "api_mapping:missing_required_canonical:ReadIndexRequest".to_string(),
            ],
        };

        require_public_api_contract_validation(
            &validation,
            &mut satisfied,
            &mut missing,
            &mut blockers,
            &mut actions,
        );

        assert!(satisfied.is_empty());
        assert!(missing.contains(&"public_api:contract_valid".to_string()));
        assert!(missing.contains(&"public_api:required_reference_mappings_present".to_string()));
        assert!(blockers.contains(
            &"public_api:api_mapping:missing_required_canonical:ReadIndexRequest".to_string()
        ));
        assert!(actions.iter().any(|action| {
            action.contains("fix RustRaft public API canonical names and TiKV/ByteRaft mappings")
        }));
    }

    #[test]
    fn production_readiness_gate_requires_coherent_sustained_snapshot_transfer() {
        let mut input = ready_production_input();
        input
            .snapshot_lifecycle
            .as_mut()
            .expect("ready production input includes snapshot lifecycle evidence")
            .sustained_transfer_completion_present = false;

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .missing
            .contains(&"snapshot:sustained_transfer_completion".to_string()));
        assert!(report
            .production_blockers
            .contains(&"snapshot:sustained_transfer_completion".to_string()));
        assert!(report.recommended_next_actions.iter().any(|action| {
            action == "prove one sustained snapshot transfer completed sender ack and downloader install"
        }));
    }

    #[test]
    fn production_readiness_report_prometheus_exports_fail_closed_gate() {
        let mut input = ready_production_input();
        input.peer_pipeline = None;
        let report = matrixraft_production_readiness_report(&input);
        assert!(!report.ready);
        assert!(report
            .missing
            .contains(&"pipeline:evidence_present".to_string()));
        assert!(report
            .production_blockers
            .contains(&"pipeline:evidence_present".to_string()));

        let metrics = matrixraft_production_readiness_report_prometheus(
            &report,
            &[("service", "raft\"a"), ("cluster", "prod\n1")],
        );

        assert_eq!(metrics.format, "prometheus_text_v0.0.4");
        assert_eq!(
            metrics.metric_count,
            5 + report.missing.len() as u64
                + report.production_blockers.len() as u64
                + report.recommended_next_actions.len() as u64
        );
        assert!(metrics.text.contains(
            "rustraft_production_readiness_ready{service=\"raft\\\"a\",cluster=\"prod\\n1\",status=\"blocked\"} 0"
        ));
        assert!(metrics.text.contains(
            "rustraft_production_readiness_missing_total{service=\"raft\\\"a\",cluster=\"prod\\n1\"}"
        ));
        assert!(metrics.text.contains(
            "rustraft_production_readiness_blocker_present{service=\"raft\\\"a\",cluster=\"prod\\n1\",blocker=\"pipeline:evidence_present\"} 1"
        ));
        assert!(metrics.text.contains(
            "rustraft_production_readiness_missing_present{service=\"raft\\\"a\",cluster=\"prod\\n1\",missing=\"pipeline:evidence_present\"} 1"
        ));
        assert!(metrics.text.contains("rustraft_production_readiness_next_action_present"));
        assert!(metrics
            .text
            .contains("attach per-peer pipeline evidence from the running RustRaft group"));

        let entries = matrixraft_production_readiness_diagnostic_log_entries(&report);
        assert_eq!(
            entries.len(),
            1 + report.missing.len()
                + report.production_blockers.len()
                + report.recommended_next_actions.len()
        );
        let summary = entries
            .iter()
            .find(|entry| entry.target == "rustraft.production_readiness")
            .expect("production readiness summary entry");
        assert_eq!(summary.severity, DiagnosticSeverity::Error);
        assert_eq!(summary.message, "rustraft production readiness blocked");
        assert!(summary
            .fields
            .contains(&("ready".to_string(), "false".to_string())));
        assert!(summary
            .fields
            .contains(&("production_status".to_string(), "Blocked".to_string())));
        assert!(entries.iter().any(|entry| {
            entry.target == "rustraft.production_readiness.missing"
                && entry.severity == DiagnosticSeverity::Warn
                && entry.message == "pipeline:evidence_present"
        }));
        assert!(entries.iter().any(|entry| {
            entry.target == "rustraft.production_readiness.blocker"
                && entry.severity == DiagnosticSeverity::Error
                && entry.message == "pipeline:evidence_present"
        }));
        assert!(entries.iter().any(|entry| {
            entry.target == "rustraft.production_readiness.next_action"
                && entry.severity == DiagnosticSeverity::Warn
                && entry
                    .message
                    .contains("attach per-peer pipeline evidence")
        }));

        let json_lines = matrixraft_production_readiness_diagnostic_json_lines(&report);
        assert!(json_lines.contains("\"target\":\"rustraft.production_readiness\""));
        assert!(json_lines.contains("\"severity\":\"error\""));
        assert!(json_lines.contains("\"target\":\"rustraft.production_readiness.blocker\""));
        assert!(json_lines.contains("pipeline:evidence_present"));
    }

    #[test]
    fn production_readiness_gate_accepts_validated_benchmark_artifacts() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(report.ready, "{report:#?}");
        assert!(report
            .satisfied
            .contains(&"benchmark:evidence_present".to_string()));
        assert!(report
            .satisfied
            .contains(&"benchmark:real_baseline_raft".to_string()));
        assert!(report
            .satisfied
            .contains(&"benchmark:rustraft_runtime".to_string()));
    }

    #[test]
    fn production_readiness_artifact_gate_rejects_missing_environment_fingerprint() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.environment_fingerprint.clear();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .production_blockers
            .contains(&"benchmark:report_environment_fingerprint_missing".to_string()));
    }

    #[test]
    fn production_readiness_artifact_gate_rejects_debug_benchmark_environment() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.environment_fingerprint =
            "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=true"
                .to_string();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .production_blockers
            .contains(&"benchmark:report_environment_debug_assertions_enabled".to_string()));
    }

    #[test]
    fn production_readiness_artifact_gate_does_not_claim_real_evidence_for_empty_workloads() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.comparisons.clear();
        benchmark.passed = true;
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report_evidence = crate::benchmark::matrixraft_baseline_raft_benchmark_evidence(&benchmark);
        assert!(!report_evidence.real_baseline_raft);
        assert!(!report_evidence.matrixraft_runtime);
        assert!(!report_evidence.baseline_raft_reference);
        assert!(!report_evidence.matrixraft_rust_candidate);
        assert!(!report_evidence.correctness_passed);
        assert!(!report_evidence.performance_within_threshold);
        assert!(!report_evidence.resource_within_threshold);

        let summary_evidence =
            crate::benchmark::matrixraft_baseline_raft_benchmark_evidence_from_summary(&summary);
        assert!(!summary_evidence.real_baseline_raft);
        assert!(!summary_evidence.matrixraft_runtime);
        assert!(!summary_evidence.baseline_raft_reference);
        assert!(!summary_evidence.matrixraft_rust_candidate);
        assert!(!summary_evidence.correctness_passed);
        assert!(!summary_evidence.performance_within_threshold);
        assert!(!summary_evidence.resource_within_threshold);
        assert!(!summary.production_evidence_ready);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "benchmark:report_required_workload_count_mismatch:declared_0_required_9"
        }));
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "benchmark:report_passed_mismatch:declared_true_actual_false"
        }));
        assert!(!report
            .satisfied
            .contains(&"benchmark:real_baseline_raft".to_string()));
        assert!(!report
            .satisfied
            .contains(&"benchmark:rustraft_runtime".to_string()));

        let summary_input =
            matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);
        let summary_report = matrixraft_production_readiness_report(&summary_input);
        assert!(!summary_report.ready);
        assert!(summary_report.production_blockers.iter().any(|blocker| {
            blocker == "benchmark:summary_passed_mismatch:declared_true_actual_false"
        }));
    }

    #[test]
    fn production_readiness_artifact_gate_rejects_stale_report_schema() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.schema = "matrixraft.baseline_raft_benchmark_report.v0".to_string();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker
                == "benchmark:report_schema_mismatch:matrixraft.baseline_raft_benchmark_report.v0:matrixraft.baseline_raft_benchmark_report.v1"
        }));
    }

    #[test]
    fn production_readiness_artifact_gate_rejects_mixed_sample_run_ids() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.comparisons[0].rustraft.benchmark_run_id = "different-run".to_string();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker
                == "benchmark:report_rustraft_run_id_mismatch:single_key_writes:different-run:ready-benchmark-run"
        }));
    }

    #[test]
    fn production_readiness_artifact_gate_rejects_sample_shape_mismatch() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.comparisons[0].baseline_raft.node_count = 1;
        benchmark.comparisons[0].baseline_raft.payload_size_bytes = 512;
        benchmark.comparisons[0].baseline_raft.implementation =
            crate::benchmark::BenchmarkImplementation::Model;
        benchmark.comparisons[0].rustraft.batch_size = 1;
        benchmark.comparisons[0].rustraft.implementation =
            crate::benchmark::BenchmarkImplementation::Unknown;
        benchmark.comparisons[0].rustraft.operation_count = 1;
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.missing.contains(&"benchmark:blockers".to_string()));
        assert!(report
            .missing
            .contains(&"benchmark:correctness_blockers".to_string()));
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "single_key_writes:benchmark:baseline_raft_sample_node_count_mismatch:1:5"
        }));
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "single_key_writes:benchmark:baseline_raft_sample_payload_size_mismatch:512:4096"
        }));
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "single_key_writes:benchmark:baseline_raft_implementation_mismatch:model:baseline_raft"
        }));
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "single_key_writes:benchmark:rustraft_sample_batch_size_mismatch:1:16"
        }));
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "single_key_writes:benchmark:rustraft_implementation_mismatch:unknown:rustraft_rust"
        }));
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "single_key_writes:benchmark:rustraft_sample_operation_count_mismatch:1:128"
        }));
    }

    #[test]
    fn production_readiness_artifact_gate_rejects_ratio_tampering() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.comparisons[0].rustraft.p99_latency_micros = 400;
        benchmark.comparisons[0].p99_ratio = 1.1;
        benchmark.comparisons[0].passed = true;
        benchmark.passed = true;
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker.starts_with(
                "single_key_writes:benchmark:comparison_p99_ratio_mismatch:1.100000:2.000000",
            )
        }));
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "single_key_writes:benchmark:comparison_passed_despite_regression"
        }));
    }

    #[test]
    fn production_readiness_artifact_gate_rejects_invalid_latency_order() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.comparisons[0].baseline_raft.p50_latency_micros = 300;
        benchmark.comparisons[0].baseline_raft.p99_latency_micros = 200;
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker
                == "single_key_writes:benchmark:baseline_raft_sample_latency_order_invalid:300:200"
        }));
    }

    #[test]
    fn production_readiness_artifact_gate_rejects_mixed_build_profiles() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.comparisons[0].rustraft.build_profile = "debug".to_string();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "single_key_writes:benchmark:build_profile_mismatch:release:debug"
        }));
    }

    #[test]
    fn production_readiness_gate_accepts_validated_benchmark_summary() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);
        let report = matrixraft_production_readiness_report(&input);

        assert!(report.ready, "{report:#?}");
        assert!(report
            .satisfied
            .contains(&"benchmark:evidence_present".to_string()));
        assert!(report
            .satisfied
            .contains(&"benchmark:real_baseline_raft".to_string()));
        assert!(report
            .satisfied
            .contains(&"benchmark:rustraft_runtime".to_string()));
    }

    #[test]
    fn production_readiness_gate_rejects_summary_ratio_regression() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.workloads[0].p99_ratio = 2.0;
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "benchmark:summary_p99_regression:single_key_writes:2.000000:1.100000"
        }));
    }

    #[test]
    fn production_readiness_gate_rejects_summary_raw_latency_tampering() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.workloads[0].matrixraft_p99_latency_micros = 100;
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker
                == "benchmark:summary_rustraft_latency_order_invalid:single_key_writes:110:100"
        }));
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker
                == "benchmark:summary_workload_p99_ratio_mismatch:single_key_writes:1.100000:0.500000"
        }));
    }

    #[test]
    fn production_readiness_gate_rejects_summary_worst_ratio_tampering() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.worst_p99_ratio = 1.0;
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "benchmark:summary_worst_p99_ratio_mismatch:1.000000:1.100000"
        }));
    }

    #[test]
    fn production_readiness_gate_rejects_summary_tiny_iteration_count() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.options.iterations_per_workload = 2;
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "benchmark:iterations_per_workload_below_production_min:2:128"
        }));
    }

    #[test]
    fn production_readiness_gate_rejects_summary_missing_environment_fingerprint() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.environment_fingerprint.clear();
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .production_blockers
            .contains(&"benchmark:summary_environment_fingerprint_missing".to_string()));
    }

    #[test]
    fn production_readiness_gate_rejects_summary_debug_benchmark_environment() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.environment_fingerprint =
            "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=true"
                .to_string();
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .production_blockers
            .contains(&"benchmark:summary_environment_debug_assertions_enabled".to_string()));
    }

    #[test]
    fn production_readiness_gate_rejects_stale_summary_schema() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.schema = "matrixraft.baseline_raft_benchmark_summary.v0".to_string();
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker
                == "benchmark:summary_schema_mismatch:matrixraft.baseline_raft_benchmark_summary.v0:matrixraft.baseline_raft_benchmark_summary.v1"
        }));
    }

    #[test]
    fn production_readiness_gate_rejects_summary_mixed_run_ids() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.workloads[0].matrixraft_benchmark_run_id = "different-run".to_string();
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker
                == "benchmark:summary_rustraft_run_id_mismatch:single_key_writes:different-run:ready-benchmark-run"
        }));
    }

    #[test]
    fn production_readiness_gate_rejects_summary_rustraft_node_count_skew() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.workloads[0].matrixraft_node_count = 3;
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "benchmark:summary_rustraft_node_count_mismatch:single_key_writes:3:5"
        }));
    }

    #[test]
    fn production_readiness_gate_rejects_summary_build_profile_mismatch() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.workloads[0].matrixraft_build_profile = "debug".to_string();
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker == "benchmark:summary_build_profile_mismatch:single_key_writes:release:debug"
        }));
    }

    #[test]
    fn production_readiness_gate_rejects_summary_without_full_baseline_raft_harness() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.workloads[0].baseline_raft_harness_kind =
            crate::benchmark::BenchmarkHarnessKind::NativeKvbenchPartial;
        let input = matrixraft_production_readiness_input_with_benchmark_summary(input, &summary);

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.missing.contains(&"benchmark:blockers".to_string()));
        assert!(report.production_blockers.iter().any(|blocker| {
            blocker
                == "benchmark:summary_baseline_raft_full_harness_missing:single_key_writes:native_kvbench_partial"
        }));
    }

    #[test]
    fn production_readiness_gate_requires_every_benchmark_workload() {
        let mut input = ready_production_input();
        input
            .baseline_raft_benchmark
            .as_mut()
            .unwrap()
            .workloads
            .retain(|workload| {
                workload != crate::benchmark::BenchmarkWorkload::WalFsync.id()
            });

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .missing
            .contains(&"benchmark:workload:wal_fsync".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:workload:wal_fsync".to_string()));
        assert!(report
            .recommended_next_actions
            .iter()
            .any(|action| action.contains("every required BaselineRaft-vs-RustRaft")));
    }

    #[test]
    fn production_readiness_gate_rejects_duplicate_benchmark_workloads() {
        let mut input = ready_production_input();
        input
            .baseline_raft_benchmark
            .as_mut()
            .unwrap()
            .workloads
            .push(
                crate::benchmark::BenchmarkWorkload::SingleKeyWrites
                    .id()
                    .to_string(),
            );

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .missing
            .contains(&"benchmark:workload_duplicate:single_key_writes".to_string()));
        assert!(report
            .missing
            .contains(&"benchmark:workload_set_exact".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:workload_duplicate:single_key_writes".to_string()));
        assert!(report
            .recommended_next_actions
            .iter()
            .any(|action| action.contains("deduplicate benchmark workloads")));
    }

    #[test]
    fn production_readiness_gate_rejects_unknown_benchmark_workloads() {
        let mut input = ready_production_input();
        input
            .baseline_raft_benchmark
            .as_mut()
            .unwrap()
            .workloads
            .push("ad_hoc_latency_probe".to_string());

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .missing
            .contains(&"benchmark:workload_unknown:ad_hoc_latency_probe".to_string()));
        assert!(report
            .missing
            .contains(&"benchmark:workload_set_exact".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:workload_unknown:ad_hoc_latency_probe".to_string()));
        assert!(report
            .recommended_next_actions
            .iter()
            .any(|action| action.contains("remove non-canonical benchmark workloads")));
    }

    #[test]
    fn production_readiness_gate_rejects_generic_benchmark_blockers() {
        let mut input = ready_production_input();
        input
            .baseline_raft_benchmark
            .as_mut()
            .unwrap()
            .blockers
            .push("benchmark:full_harness_observation_missing:lease_reads".to_string());

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.missing.contains(&"benchmark:blockers".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:blockers".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:full_harness_observation_missing:lease_reads".to_string()));
        assert!(report
            .recommended_next_actions
            .iter()
            .any(|action| action.contains("clear all BaselineRaft-vs-RustRaft")));
    }

    #[test]
    fn production_readiness_artifact_gate_rejects_stale_benchmark_summary() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let mut summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        summary.workload_count = 0;

        let error = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap_err();

        assert_eq!(error, "benchmark:summary_artifact_mismatch");
    }

    #[test]
    fn production_readiness_artifact_gate_reports_benchmark_failures() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.passed = false;
        let comparison = benchmark.comparisons.first_mut().unwrap();
        comparison.rustraft.correctness_passed = false;
        comparison
            .rustraft
            .blockers
            .push("benchmark:rustraft_correctness_failed:single_key_writes".to_string());
        comparison.blockers.push(
            "single_key_writes:benchmark:rustraft_correctness_failed:single_key_writes".to_string(),
        );
        comparison.passed = false;
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = matrixraft_production_readiness_report_with_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .unwrap();

        assert!(!report.ready);
        assert!(report
            .production_blockers
            .contains(&"benchmark:correctness".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:correctness_blockers".to_string()));
    }

    #[test]
    fn asserted_production_readiness_artifact_gate_accepts_clean_benchmark_artifacts() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let benchmark = ready_benchmark_report();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report = crate::benchmark::matrixraft_production_readiness_report_with_asserted_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .expect("clean asserted benchmark artifacts should satisfy the production gate");

        assert!(report.ready, "{report:#?}");
        assert!(report
            .satisfied
            .contains(&"benchmark:performance_threshold".to_string()));
    }

    #[test]
    fn asserted_production_readiness_artifact_gate_rejects_matched_failing_artifacts() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.passed = false;
        let comparison = benchmark.comparisons.first_mut().unwrap();
        comparison.rustraft.correctness_passed = false;
        comparison
            .rustraft
            .blockers
            .push("benchmark:rustraft_correctness_failed:single_key_writes".to_string());
        comparison.blockers.push(
            "single_key_writes:benchmark:rustraft_correctness_failed:single_key_writes".to_string(),
        );
        comparison.passed = false;
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let error = crate::benchmark::matrixraft_production_readiness_report_with_asserted_benchmark_artifacts(
            &input, &benchmark, &summary,
        )
        .expect_err("asserted benchmark artifacts must fail closed on production blockers");

        assert!(
            error.contains("benchmark:summary_report_failed"),
            "{error}"
        );
        assert!(
            error.contains("benchmark:summary_correctness_blockers"),
            "{error}"
        );
        assert!(
            error.contains("benchmark:rustraft_correctness_failed:single_key_writes"),
            "{error}"
        );
    }

    #[test]
    fn production_readiness_gate_fails_closed_without_runtime_evidence() {
        let report = matrixraft_production_readiness_report(&ProductionReadinessInput {
            readiness: ready_snapshot(),
            peer_pipeline: None,
            runtime_pressure_admission: None,
            snapshot_lifecycle: None,
            wal_lifecycle: None,
            admin_status_surface: None,
            fault_harness: None,
            data_node_rollout: None,
            metaserver_rollout: None,
            membership_transitions: Vec::new(),
            baseline_raft_benchmark: None,
        });
        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .missing
            .contains(&"pipeline:evidence_present".to_string()));
        assert!(report
            .missing
            .contains(&"runtime_pressure:admission_evidence_present".to_string()));
        assert!(report
            .missing
            .contains(&"snapshot:evidence_present".to_string()));
        assert!(report.missing.contains(&"wal:evidence_present".to_string()));
        assert!(report
            .missing
            .contains(&"data_node:evidence_present".to_string()));
        assert!(report
            .missing
            .contains(&"metaserver:evidence_present".to_string()));
        assert!(report
            .missing
            .iter()
            .any(|item| item == "membership:datanode:scaledown:evidence_present"));
        assert!(report
            .missing
            .contains(&"benchmark:evidence_present".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:real_baseline_raft_missing".to_string()));
        assert!(report
            .production_blockers
            .contains(&"status:admin_surface_missing".to_string()));
        assert!(report
            .production_blockers
            .contains(&"fault:partition_heal_missing".to_string()));
    }

    #[test]
    fn production_readiness_gate_requires_runtime_pressure_admission_evidence() {
        let mut input = ready_production_input();
        input.runtime_pressure_admission = None;
        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert!(report
            .missing
            .contains(&"runtime_pressure:admission_evidence_present".to_string()));
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:admission_missing".to_string()));
        assert!(report.recommended_next_actions.iter().any(|action| action
            .contains("attach runtime-pressure admission evidence")));
    }

    #[test]
    fn production_readiness_gate_rejects_malformed_runtime_pressure_admission_evidence() {
        let mut input = ready_production_input();
        let admission = input
            .runtime_pressure_admission
            .as_mut()
            .expect("ready input should include runtime pressure evidence");
        admission.latency_pressure_details.push(LatencyPressureDetail {
            component: "latency.append".to_string(),
            sample_count: 0,
            observed_p95_ms: 160,
            observed_p99_ms: 150,
            threshold_p99_ms: 100,
            excess_ms: 5,
        });

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert!(report
            .missing
            .contains(&"runtime_pressure:evidence_valid".to_string()));
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:evidence_valid".to_string()));
        assert!(report.production_blockers.iter().any(|blocker| blocker
            == "runtime_pressure:evidence_invalid:runtime_pressure:latency_pressure_details_without_signal"));
        assert!(report.production_blockers.iter().any(|blocker| blocker
            == "runtime_pressure:evidence_invalid:runtime_pressure:latency_pressure_detail_sample_count_zero:latency.append"));
        assert!(report.recommended_next_actions.iter().any(|action| action
            .contains("fix malformed runtime-pressure admission evidence")));

        let entries = matrixraft_production_readiness_diagnostic_log_entries(&report);
        assert!(entries.iter().any(|entry| {
            entry.target == "rustraft.production_readiness.runtime_pressure_evidence"
                && entry.severity == DiagnosticSeverity::Error
                && entry.message
                    == "runtime_pressure:latency_pressure_detail_sample_count_zero:latency.append"
                && entry.fields.iter().any(|(key, value)| {
                    key == "blocker"
                        && value == "runtime_pressure:evidence_invalid:runtime_pressure:latency_pressure_detail_sample_count_zero:latency.append"
                })
        }));
        let json_lines = matrixraft_production_readiness_diagnostic_json_lines(&report);
        assert!(json_lines.contains(
            "\"target\":\"rustraft.production_readiness.runtime_pressure_evidence\""
        ));
        assert!(json_lines
            .contains("runtime_pressure:latency_pressure_detail_sample_count_zero:latency.append"));
    }

    #[test]
    fn production_readiness_gate_blocks_node_runtime_timer_pressure() {
        let mut input = ready_production_input();
        let admission = input
            .runtime_pressure_admission
            .as_mut()
            .expect("ready input should include runtime pressure evidence");
        admission.node_runtime_timer_pressure = true;
        admission.node_runtime_timer_pressure_details =
            vec![NodeRuntimeTimerPressureDetail {
                component: "node_runtime.timer_utilization".to_string(),
                observed_percent: 100,
                threshold_percent: 80,
                excess_percent: 20,
            }];
        admission.reason = "accepted_observe_only_pressure".to_string();
        admission.actions = vec!["raise_timer_queue_capacity_or_reduce_tick_burst".to_string()];

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert!(report
            .missing
            .contains(&"runtime_pressure:no_node_runtime_timer_pressure".to_string()));
        assert!(report.production_blockers.contains(
            &"runtime_pressure:no_node_runtime_timer_pressure".to_string()
        ));
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:no_pending_actions".to_string()));
        assert!(report.production_blockers.contains(
            &"runtime_pressure:top_bottleneck:rank1:node_runtime_timer:node_runtime.timer_utilization:score_percent:25".to_string()
        ));
        assert!(report.recommended_next_actions.iter().any(|action| action
            .contains("clear node-runtime timer queue saturation")));
        assert!(report.recommended_next_actions.iter().any(|action| action
            .contains("resolve the top runtime-pressure bottleneck")));

        let entries = matrixraft_production_readiness_diagnostic_log_entries(&report);
        let bottleneck_entry = entries
            .iter()
            .find(|entry| {
                entry.target == "rustraft.production_readiness.blocker"
                    && entry.message == "runtime_pressure:top_bottleneck:rank1:node_runtime_timer:node_runtime.timer_utilization:score_percent:25"
            })
            .expect("ranked runtime-pressure bottleneck blocker diagnostic");
        assert!(bottleneck_entry.fields.contains(&(
            "runtime_pressure_bottleneck_rank".to_string(),
            "rank1".to_string()
        )));
        assert!(bottleneck_entry.fields.contains(&(
            "runtime_pressure_bottleneck_category".to_string(),
            "node_runtime_timer".to_string()
        )));
        assert!(bottleneck_entry.fields.contains(&(
            "runtime_pressure_bottleneck_component".to_string(),
            "node_runtime.timer_utilization".to_string()
        )));
        assert!(bottleneck_entry.fields.contains(&(
            "runtime_pressure_bottleneck_score_percent".to_string(),
            "25".to_string()
        )));
        let json_lines = matrixraft_production_readiness_diagnostic_json_lines(&report);
        assert!(json_lines.contains("\"runtime_pressure_bottleneck_component\""));
        assert!(json_lines.contains("node_runtime.timer_utilization"));

        let metrics = matrixraft_production_readiness_report_prometheus(
            &report,
            &[("service", "raft"), ("cluster", "prod")],
        );
        assert!(metrics.text.contains(
            "rustraft_production_readiness_runtime_pressure_bottleneck_score_percent{service=\"raft\",cluster=\"prod\",rank=\"1\",category=\"node_runtime_timer\",component=\"node_runtime.timer_utilization\"} 25"
        ));
        assert_eq!(
            metrics.metric_count,
            5 + report.missing.len() as u64
                + report.production_blockers.len() as u64
                + 1
                + report.recommended_next_actions.len() as u64
        );
    }

    #[test]
    fn production_readiness_input_can_attach_runtime_pressure_from_release_scale_evidence() {
        let mut input = ready_production_input();
        input.runtime_pressure_admission = None;
        let input = crate::benchmark::matrixraft_production_readiness_input_with_runtime_pressure_evidence(
            input,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &ScaleRateMetrics {
                proposal_qps: 2_000,
                append_entries_qps: 4_000,
                read_index_qps: 3_000,
                apply_entries_qps: 3_500,
                replication_mib_per_sec: 256,
                apply_mib_per_sec: 256,
            },
            &ScaleOptimizationTargets {
                min_proposal_qps: 1_000,
                min_append_entries_qps: 2_000,
                min_read_index_qps: 1_500,
                min_apply_entries_qps: 1_500,
                min_replication_mib_per_sec: 128,
                min_apply_mib_per_sec: 128,
            },
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
        );

        let report = matrixraft_production_readiness_report(&input);

        assert!(report.ready, "clean runtime pressure evidence should satisfy the production gate: {report:#?}");
        let admission = input
            .runtime_pressure_admission
            .expect("helper must attach runtime pressure evidence");
        assert!(admission.accepted);
        assert!(!admission.memory_pressure);
        assert!(!admission.latency_pressure);
        assert!(!admission.scale_pressure);
        assert!(!admission.pipeline_pressure);
        assert!(!admission.read_backlog_pressure);
        assert!(admission.actions.is_empty());
    }

    #[test]
    fn production_readiness_input_can_attach_benchmark_runtime_pressure_artifacts() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let benchmark = ready_benchmark_report();
        let summary = crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let input =
            crate::benchmark::matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts(
                input,
                &benchmark,
                &summary,
                &MemoryMetrics::zero(),
                &MemoryOptimizationThresholds::default(),
                &LatencyMetrics::zero(),
                &LatencyOptimizationThresholds::default(),
                &[],
                &RuntimePressureAdmissionPolicy::fail_closed(),
            )
            .expect("valid benchmark artifacts should attach production readiness evidence");
        let report = matrixraft_production_readiness_report(&input);

        assert!(report.ready, "benchmark-derived runtime pressure evidence should satisfy the production gate: {report:#?}");
        let admission = input
            .runtime_pressure_admission
            .expect("helper should attach runtime pressure admission");
        assert!(admission.accepted);
        assert!(!admission.scale_pressure);
        assert!(input
            .baseline_raft_benchmark
            .as_ref()
            .expect("helper should attach benchmark evidence")
            .performance_within_threshold);
        assert!(input
            .baseline_raft_benchmark
            .expect("helper should attach benchmark evidence")
            .resource_within_threshold);
    }

    #[test]
    fn production_readiness_input_benchmark_runtime_pressure_helper_preserves_scale_pressure() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.pass_tolerance_percent = 5.0;
        let summary = crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let input =
            crate::benchmark::matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts(
                input,
                &benchmark,
                &summary,
                &MemoryMetrics::zero(),
                &MemoryOptimizationThresholds::default(),
                &LatencyMetrics::zero(),
                &LatencyOptimizationThresholds::default(),
                &[],
                &RuntimePressureAdmissionPolicy::fail_closed(),
            )
            .expect("valid but failing benchmark artifacts should still attach pressure evidence");
        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:admission_accepted".to_string()));
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:no_scale_pressure".to_string()));
        assert!(input.baseline_raft_benchmark.is_some());
        let admission = input
            .runtime_pressure_admission
            .expect("helper should attach runtime pressure admission");
        assert!(!admission.accepted);
        assert!(admission.scale_pressure);
    }

    #[test]
    fn asserted_benchmark_runtime_pressure_helper_accepts_clean_artifacts() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let benchmark = ready_benchmark_report();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let input = crate::benchmark::matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts(
            input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
        )
        .expect("clean benchmark/runtime-pressure artifacts should attach production evidence");

        let report = matrixraft_production_readiness_report(&input);
        assert!(report.ready, "{report:#?}");
        assert!(input
            .runtime_pressure_admission
            .expect("helper should attach runtime pressure admission")
            .accepted);
    }

    #[test]
    fn asserted_benchmark_runtime_pressure_helper_rejects_failing_artifacts_before_pressure() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.passed = false;
        let comparison = benchmark.comparisons.first_mut().unwrap();
        comparison.rustraft.correctness_passed = false;
        comparison
            .rustraft
            .blockers
            .push("benchmark:rustraft_correctness_failed:single_key_writes".to_string());
        comparison.blockers.push(
            "single_key_writes:benchmark:rustraft_correctness_failed:single_key_writes".to_string(),
        );
        comparison.passed = false;
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let error = crate::benchmark::matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts(
            input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
        )
        .expect_err("asserted runtime-pressure helper must reject failing benchmark evidence");

        assert!(
            error.contains("benchmark:summary_production_evidence_not_ready"),
            "{error}"
        );
        assert!(
            error.contains("benchmark:summary_correctness_blockers"),
            "{error}"
        );
        assert!(
            error.contains("benchmark:rustraft_correctness_failed:single_key_writes"),
            "{error}"
        );
    }

    #[test]
    fn asserted_benchmark_read_backlog_helper_rejects_failing_artifacts_before_backlog_pressure() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.passed = false;
        let comparison = benchmark.comparisons.first_mut().unwrap();
        comparison.rustraft.correctness_passed = false;
        comparison
            .rustraft
            .blockers
            .push("benchmark:rustraft_correctness_failed:single_key_writes".to_string());
        comparison.blockers.push(
            "single_key_writes:benchmark:rustraft_correctness_failed:single_key_writes".to_string(),
        );
        comparison.passed = false;
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let error = crate::benchmark::matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts(
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &ReadBacklogMetrics::zero(),
            &ReadBacklogThresholds::default(),
            &RuntimePressureAdmissionPolicy::fail_closed(),
        )
        .expect_err("asserted read-backlog helper must reject failing benchmark evidence");

        assert!(
            error.contains("benchmark:summary_production_evidence_not_ready"),
            "{error}"
        );
        assert!(
            error.contains("benchmark:summary_correctness_blockers"),
            "{error}"
        );
    }

    #[test]
    fn asserted_benchmark_timer_helper_rejects_failing_artifacts_before_timer_pressure() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.passed = false;
        let comparison = benchmark.comparisons.first_mut().unwrap();
        comparison.rustraft.correctness_passed = false;
        comparison
            .rustraft
            .blockers
            .push("benchmark:rustraft_correctness_failed:single_key_writes".to_string());
        comparison.blockers.push(
            "single_key_writes:benchmark:rustraft_correctness_failed:single_key_writes".to_string(),
        );
        comparison.passed = false;
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let error = crate::benchmark::matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts(
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &ReadBacklogMetrics::zero(),
            &ReadBacklogThresholds::default(),
            &crate::benchmark::matrixraft_release_benchmark_runtime_timer_status(),
            &NodeRuntimeTimerThresholds::default(),
            &RuntimePressureAdmissionPolicy::fail_closed(),
        )
        .expect_err("asserted timer helper must reject failing benchmark evidence");

        assert!(
            error.contains("benchmark:summary_production_evidence_not_ready"),
            "{error}"
        );
        assert!(
            error.contains("benchmark:rustraft_correctness_failed:single_key_writes"),
            "{error}"
        );
    }

    #[test]
    fn production_readiness_report_can_use_benchmark_runtime_pressure_artifacts() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let benchmark = ready_benchmark_report();
        let summary = crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report =
            crate::benchmark::matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts(
                &input,
                &benchmark,
                &summary,
                &MemoryMetrics::zero(),
                &MemoryOptimizationThresholds::default(),
                &LatencyMetrics::zero(),
                &LatencyOptimizationThresholds::default(),
                &[],
                &RuntimePressureAdmissionPolicy::fail_closed(),
            )
            .expect("valid benchmark artifacts should produce a readiness report");

        assert!(report.ready, "clean benchmark-derived pressure evidence should pass the gate: {report:#?}");
        assert!(report
            .satisfied
            .contains(&"runtime_pressure:admission_accepted".to_string()));
        assert!(report
            .satisfied
            .contains(&"benchmark:performance_threshold".to_string()));
    }

    #[test]
    fn production_readiness_report_benchmark_runtime_pressure_helper_rejects_scale_pressure() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.pass_tolerance_percent = 5.0;
        let summary = crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let report =
            crate::benchmark::matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts(
                &input,
                &benchmark,
                &summary,
                &MemoryMetrics::zero(),
                &MemoryOptimizationThresholds::default(),
                &LatencyMetrics::zero(),
                &LatencyOptimizationThresholds::default(),
                &[],
                &RuntimePressureAdmissionPolicy::fail_closed(),
            )
            .expect("valid but pressured benchmark artifacts should still produce a report");

        assert!(!report.ready);
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:admission_accepted".to_string()));
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:no_scale_pressure".to_string()));
    }

    #[test]
    fn benchmark_runtime_pressure_readiness_artifact_exports_report_metrics_and_logs() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let benchmark = ready_benchmark_report();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let artifact = crate::benchmark::matrixraft_benchmark_runtime_pressure_readiness_artifact(
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect("valid benchmark artifacts should produce a release artifact");

        assert!(artifact.report.ready);
        assert_eq!(
            artifact.labels,
            vec![
                ("service".to_string(), "rustraft-ci".to_string()),
                ("workload".to_string(), "release-scale".to_string())
            ]
        );
        assert!(artifact
            .prometheus
            .text
            .contains("rustraft_production_readiness_ready"));
        assert!(artifact
            .runtime_pressure_prometheus
            .text
            .contains("rustraft_runtime_pressure_admission_accepted"));
        assert!(artifact
            .prometheus
            .text
            .contains("service=\"rustraft-ci\",workload=\"release-scale\",status=\"production_ready\""));
        assert!(artifact
            .diagnostic_json_lines
            .contains("rustraft.production_readiness"));
        crate::benchmark::matrixraft_validate_benchmark_runtime_pressure_readiness_artifact(
            &artifact,
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect("fresh artifact should validate against its source inputs");
        assert!(serde_json::to_string(&artifact)
            .expect("release artifact serializes")
            .contains(crate::benchmark::MATRIXRAFT_BENCHMARK_RUNTIME_PRESSURE_READINESS_ARTIFACT_SCHEMA));
        assert!(serde_json::to_string(&artifact)
            .expect("release artifact serializes")
            .contains("diagnostic_json_lines"));
        assert!(serde_json::to_string(&artifact)
            .expect("release artifact serializes")
            .contains("runtime_pressure_prometheus"));
    }

    #[test]
    fn asserted_benchmark_runtime_pressure_artifact_rejects_failing_benchmark_before_output() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.passed = false;
        benchmark.comparisons[0].passed = false;
        benchmark.comparisons[0]
            .blockers
            .push("benchmark:rustraft_latency_regressed:single_key_writes".to_string());
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let error =
            crate::benchmark::matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact(
                &input,
                &benchmark,
                &summary,
                &MemoryMetrics::zero(),
                &MemoryOptimizationThresholds::default(),
                &LatencyMetrics::zero(),
                &LatencyOptimizationThresholds::default(),
                &[],
                &RuntimePressureAdmissionPolicy::fail_closed(),
                &[("service", "rustraft-ci"), ("workload", "release-scale")],
            )
            .expect_err("asserted artifacts must not emit output for failing benchmark evidence");

        assert!(error.contains("benchmark:summary_report_failed"));
        assert!(error.contains("benchmark:rustraft_latency_regressed:single_key_writes"));
    }

    #[test]
    fn asserted_benchmark_complete_pressure_artifact_exports_and_validates_clean_release_gate() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let benchmark = ready_benchmark_report();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        let timer_status = crate::benchmark::matrixraft_release_benchmark_runtime_timer_status();

        let artifact = crate::benchmark::matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &ReadBacklogMetrics::zero(),
            &ReadBacklogThresholds::default(),
            &timer_status,
            &NodeRuntimeTimerThresholds::default(),
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect("asserted complete-pressure artifact should build from clean release evidence");

        assert!(artifact.report.ready);
        assert!(artifact
            .runtime_pressure_prometheus
            .text
            .contains("rustraft_runtime_pressure_node_runtime_timer"));
        crate::benchmark::matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &artifact,
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &ReadBacklogMetrics::zero(),
            &ReadBacklogThresholds::default(),
            &timer_status,
            &NodeRuntimeTimerThresholds::default(),
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect("asserted complete-pressure artifact should validate through the same clean gate");
    }

    #[test]
    fn benchmark_runtime_pressure_artifact_can_fail_closed_on_node_timer_pressure() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let benchmark = ready_benchmark_report();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);
        let timer_status = RuntimeTimerStatus {
            heartbeat_interval_ms: 10,
            election_timeout_ms: 50,
            leader_lease_timeout_ms: 20,
            leader_lease_elapsed_ms: 0,
            leader_lease_valid: true,
            heartbeat_ticks: 8,
            election_ticks: 2,
            pending_ticks: 4,
            max_pending_ticks: 5,
            accepted_ticks: 12,
            rejected_ticks: 0,
            completed_ticks: 8,
            pre_vote_executions: 0,
            campaign_executions: 0,
            leader_transfer_executions: 0,
            last_tick_reason: "heartbeat".to_string(),
            last_tick_admission_reason: "tick_admitted".to_string(),
        };

        let artifact = crate::benchmark::matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &ReadBacklogMetrics {
                pending_read_index_requests: 0,
                pending_bounded_stale_reads: 0,
            },
            &ReadBacklogThresholds {
                pending_read_index_warning: 1_024,
                pending_bounded_stale_read_warning: 1_024,
            },
            &timer_status,
            &NodeRuntimeTimerThresholds::default(),
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect("timer-aware benchmark runtime-pressure artifact builds");

        assert!(!artifact.report.ready);
        assert!(artifact
            .report
            .production_blockers
            .contains(&"runtime_pressure:no_node_runtime_timer_pressure".to_string()));
        assert!(artifact
            .runtime_pressure_prometheus
            .text
            .contains("rustraft_runtime_pressure_node_runtime_timer"));
        let readiness_input = crate::benchmark::matrixraft_production_readiness_input_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts(
            input.clone(),
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &ReadBacklogMetrics {
                pending_read_index_requests: 0,
                pending_bounded_stale_reads: 0,
            },
            &ReadBacklogThresholds {
                pending_read_index_warning: 1_024,
                pending_bounded_stale_read_warning: 1_024,
            },
            &timer_status,
            &NodeRuntimeTimerThresholds::default(),
            &RuntimePressureAdmissionPolicy::fail_closed(),
        )
        .expect("timer-aware benchmark evidence builds readiness input");
        assert!(readiness_input
            .runtime_pressure_admission
            .as_ref()
            .expect("runtime pressure admission is attached")
            .node_runtime_timer_pressure);
        assert!(artifact
            .diagnostic_json_lines
            .contains("runtime_pressure:no_node_runtime_timer_pressure"));
        crate::benchmark::matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &artifact,
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &ReadBacklogMetrics {
                pending_read_index_requests: 0,
                pending_bounded_stale_reads: 0,
            },
            &ReadBacklogThresholds {
                pending_read_index_warning: 1_024,
                pending_bounded_stale_read_warning: 1_024,
            },
            &timer_status,
            &NodeRuntimeTimerThresholds::default(),
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect("fresh timer-aware artifact validates against its source inputs");
    }

    #[test]
    fn benchmark_runtime_pressure_readiness_artifact_exports_blocked_metrics_and_logs() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let mut benchmark = ready_benchmark_report();
        benchmark.pass_tolerance_percent = 5.0;
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let artifact = crate::benchmark::matrixraft_benchmark_runtime_pressure_readiness_artifact(
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect("valid pressured benchmark artifacts should still produce diagnostics");

        assert!(!artifact.report.ready);
        assert!(artifact
            .prometheus
            .text
            .contains("status=\"blocked\""));
        assert!(artifact
            .prometheus
            .text
            .contains("blocker=\"runtime_pressure:admission_accepted\""));
        assert!(artifact
            .diagnostic_json_lines
            .contains("runtime_pressure:admission_accepted"));
    }

    #[test]
    fn benchmark_runtime_pressure_readiness_artifact_validator_rejects_schema_and_payload_drift() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = None;
        input.runtime_pressure_admission = None;
        let benchmark = ready_benchmark_report();
        let summary =
            crate::benchmark::matrixraft_baseline_raft_benchmark_failure_summary(&benchmark);

        let artifact = crate::benchmark::matrixraft_benchmark_runtime_pressure_readiness_artifact(
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect("valid benchmark artifacts should produce a release artifact");

        let mut stale_schema = artifact.clone();
        stale_schema.schema =
            "matrixraft.benchmark_runtime_pressure_readiness_artifact.v0".to_string();
        let error = crate::benchmark::matrixraft_validate_benchmark_runtime_pressure_readiness_artifact(
            &stale_schema,
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect_err("stale schema must be rejected");
        assert!(error.contains(
            "benchmark:runtime_pressure_readiness_artifact_schema_mismatch"
        ));

        let mut drifted = artifact;
        drifted.labels.push(("cluster".to_string(), "tampered".to_string()));
        let error =
            crate::benchmark::matrixraft_validate_benchmark_runtime_pressure_readiness_artifact(
                &drifted,
                &input,
                &benchmark,
                &summary,
                &MemoryMetrics::zero(),
                &MemoryOptimizationThresholds::default(),
                &LatencyMetrics::zero(),
                &LatencyOptimizationThresholds::default(),
                &[],
                &RuntimePressureAdmissionPolicy::fail_closed(),
                &[("service", "rustraft-ci"), ("workload", "release-scale")],
            )
            .expect_err("label payload drift must be rejected");
        assert!(error.contains("benchmark:runtime_pressure_readiness_labels_mismatch"));

        let mut drifted = crate::benchmark::matrixraft_benchmark_runtime_pressure_readiness_artifact(
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect("valid benchmark artifacts should produce a release artifact");
        drifted
            .diagnostic_json_lines
            .push_str("{\"target\":\"tampered\"}\n");
        let error = crate::benchmark::matrixraft_validate_benchmark_runtime_pressure_readiness_artifact(
            &drifted,
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect_err("diagnostic payload drift must be rejected");
        assert!(error.contains(
            "benchmark:runtime_pressure_readiness_diagnostic_json_lines_mismatch"
        ));

        let mut drifted = crate::benchmark::matrixraft_benchmark_runtime_pressure_readiness_artifact(
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect("valid benchmark artifacts should produce a release artifact");
        drifted.runtime_pressure_prometheus.metric_count = 0;
        let error = crate::benchmark::matrixraft_validate_benchmark_runtime_pressure_readiness_artifact(
            &drifted,
            &input,
            &benchmark,
            &summary,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &[],
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        )
        .expect_err("runtime-pressure metric count drift must be rejected");
        assert!(error.contains(
            "benchmark:runtime_pressure_readiness_runtime_prometheus_metric_count_mismatch"
        ));
    }

    #[test]
    fn production_readiness_input_runtime_pressure_helper_preserves_fail_closed_pressure() {
        let mut input = ready_production_input();
        input.runtime_pressure_admission = None;
        let mut peer = PeerProgress::new(2, 10, PipelineLimits::production_default());
        peer.append_queue_limit = 4;
        peer.append_queue_depth = 4;
        let input = crate::benchmark::matrixraft_production_readiness_input_with_runtime_pressure_evidence(
            input,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &ScaleRateMetrics {
                proposal_qps: 700,
                append_entries_qps: 2_100,
                read_index_qps: 950,
                apply_entries_qps: 900,
                replication_mib_per_sec: 60,
                apply_mib_per_sec: 120,
            },
            &ScaleOptimizationTargets {
                min_proposal_qps: 1_000,
                min_append_entries_qps: 2_000,
                min_read_index_qps: 900,
                min_apply_entries_qps: 1_000,
                min_replication_mib_per_sec: 100,
                min_apply_mib_per_sec: 100,
            },
            &[peer],
            &RuntimePressureAdmissionPolicy::fail_closed(),
        );

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:admission_accepted".to_string()));
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:no_scale_pressure".to_string()));
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:no_pipeline_pressure".to_string()));
    }

    #[test]
    fn production_readiness_gate_rejects_read_backlog_pressure() {
        let mut input = ready_production_input();
        input.runtime_pressure_admission = None;
        let input = crate::benchmark::matrixraft_production_readiness_input_with_runtime_pressure_and_read_backlog_evidence(
            input,
            &MemoryMetrics::zero(),
            &MemoryOptimizationThresholds::default(),
            &LatencyMetrics::zero(),
            &LatencyOptimizationThresholds::default(),
            &ScaleRateMetrics {
                proposal_qps: 2_000,
                append_entries_qps: 4_000,
                read_index_qps: 3_000,
                apply_entries_qps: 3_500,
                replication_mib_per_sec: 256,
                apply_mib_per_sec: 256,
            },
            &ScaleOptimizationTargets {
                min_proposal_qps: 1_000,
                min_append_entries_qps: 2_000,
                min_read_index_qps: 1_500,
                min_apply_entries_qps: 1_500,
                min_replication_mib_per_sec: 128,
                min_apply_mib_per_sec: 128,
            },
            &[],
            &ReadBacklogMetrics {
                pending_read_index_requests: 2_048,
                pending_bounded_stale_reads: 0,
            },
            &ReadBacklogThresholds {
                pending_read_index_warning: 1_024,
                pending_bounded_stale_read_warning: 1_024,
            },
            &RuntimePressureAdmissionPolicy::fail_closed(),
        );

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert!(report
            .missing
            .contains(&"runtime_pressure:no_read_backlog_pressure".to_string()));
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:admission_accepted".to_string()));
        assert!(report
            .production_blockers
            .contains(&"runtime_pressure:no_read_backlog_pressure".to_string()));
        assert!(report.recommended_next_actions.iter().any(|action| action
            .contains("clear pending ReadIndex and bounded-stale read backlog")));
    }

    #[test]
    fn production_readiness_gate_rejects_pressured_runtime_admission() {
        let mut input = ready_production_input();
        let admission = input
            .runtime_pressure_admission
            .as_mut()
            .expect("ready input has runtime-pressure evidence");
        admission.accepted = false;
        admission.scale_pressure = true;
        admission.pipeline_pressure = true;
        admission.read_backlog_pressure = true;
        admission.scale_pressure_details = vec![ScalePressureDetail {
            component: "scale.proposal_qps".to_string(),
            observed_value: 75_000,
            target_value: 100_000,
            deficit: 25_000,
            target_percent: 75,
        }];
        admission.pipeline_pressure_details = vec![PipelinePressureDetail {
            peer_id: 2,
            component: "pipeline.append_queue".to_string(),
            observed_value: 150,
            threshold_value: 100,
            excess: 50,
        }];
        admission.read_backlog_pressure_details = vec![ReadBacklogPressureDetail {
            component: "read_backlog.pending_read_index".to_string(),
            observed_value: 60,
            threshold_value: 50,
            excess: 10,
        }];
        admission.reason = "rejected_runtime_pressure".to_string();
        admission.rejected_component = Some("pipeline.append_queue".to_string());
        admission
            .actions
            .push("increase_append_queue_capacity_or_reduce_peer_append_burst".to_string());

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        for missing in [
            "runtime_pressure:admission_accepted",
            "runtime_pressure:no_scale_pressure",
            "runtime_pressure:no_pipeline_pressure",
            "runtime_pressure:no_read_backlog_pressure",
            "runtime_pressure:no_pending_actions",
        ] {
            assert!(
                report.missing.contains(&missing.to_string()),
                "missing runtime-pressure blocker {missing}: {report:#?}"
            );
            assert!(report.production_blockers.contains(&missing.to_string()));
        }
        assert!(report.production_blockers.contains(
            &"runtime_pressure:top_bottleneck:rank1:pipeline:pipeline.append_queue:score_percent:50".to_string()
        ));
        assert!(report.production_blockers.contains(
            &"runtime_pressure:top_bottleneck:rank2:scale:scale.proposal_qps:score_percent:25".to_string()
        ));
        assert!(report.production_blockers.contains(
            &"runtime_pressure:top_bottleneck:rank3:read_backlog:read_backlog.pending_read_index:score_percent:20".to_string()
        ));
        assert!(report.recommended_next_actions.iter().any(|action| action
            .contains("clear runtime-pressure admission rejection")));
        assert!(report
            .recommended_next_actions
            .iter()
            .any(|action| action.contains("meet release QPS and throughput targets")));
        assert!(report.recommended_next_actions.iter().any(|action| action
            .contains("clear per-peer append/apply/reorder pipeline pressure")));
        assert!(report.recommended_next_actions.iter().any(|action| action
            .contains("clear pending ReadIndex and bounded-stale read backlog")));
        assert!(report.recommended_next_actions.iter().any(|action| action
            .contains("resolve the top runtime-pressure bottleneck")));
    }

    #[test]
    fn production_readiness_gate_rejects_model_only_benchmark_evidence() {
        let mut input = ready_production_input();
        input.baseline_raft_benchmark = Some(BaselineRaftBenchmarkEvidence {
            real_baseline_raft: false,
            matrixraft_runtime: false,
            baseline_raft_reference: false,
            matrixraft_rust_candidate: false,
            correctness_passed: true,
            performance_within_threshold: true,
            resource_within_threshold: true,
            workloads: vec!["single_key_writes".to_string()],
            blockers: vec![
                "benchmark:model_baseline_raft:single_key_writes".to_string(),
                "benchmark:model_rustraft:single_key_writes".to_string(),
            ],
            missing_baseline_raft_binaries: Vec::new(),
            unsupported_workloads: Vec::new(),
            correctness_blockers: Vec::new(),
            performance_blockers: Vec::new(),
        });
        let report = matrixraft_production_readiness_report(&input);
        assert!(!report.ready);
        assert!(report
            .production_blockers
            .contains(&"benchmark:real_baseline_raft".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:rustraft_runtime".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:baseline_raft_reference".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:rustraft_rust_candidate".to_string()));
        assert!(report
            .production_blockers
            .contains(&"benchmark:model_baseline_raft:single_key_writes".to_string()));
    }

    #[test]
    fn production_readiness_gate_reports_classified_benchmark_blockers() {
        let mut input = ready_production_input();
        let missing_binary =
            "single_key_writes:benchmark:baseline_raft_kvserver_binary_missing:/tmp/baseline_raft/kvserver"
                .to_string();
        let unsupported_workload =
            "lease_reads:benchmark:baseline_raft_native_kvbench_unsupported:lease_reads"
                .to_string();
        let correctness_blocker =
            "read_index_reads:benchmark:baseline_raft_native_kvbench_zero_operations".to_string();
        let performance_blocker = "batched_writes:benchmark:p99_regression".to_string();
        input.baseline_raft_benchmark = Some(BaselineRaftBenchmarkEvidence {
            real_baseline_raft: true,
            matrixraft_runtime: true,
            baseline_raft_reference: true,
            matrixraft_rust_candidate: true,
            correctness_passed: false,
            performance_within_threshold: false,
            resource_within_threshold: false,
            workloads: vec![
                "single_key_writes".to_string(),
                "lease_reads".to_string(),
                "read_index_reads".to_string(),
                "batched_writes".to_string(),
            ],
            blockers: vec![
                missing_binary.clone(),
                unsupported_workload.clone(),
                correctness_blocker.clone(),
                performance_blocker.clone(),
            ],
            missing_baseline_raft_binaries: vec![missing_binary.clone()],
            unsupported_workloads: vec![unsupported_workload.clone()],
            correctness_blockers: vec![correctness_blocker.clone()],
            performance_blockers: vec![performance_blocker.clone()],
        });

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        for blocker in [
            "benchmark:baseline_raft_binaries_missing",
            "benchmark:unsupported_workloads",
            "benchmark:correctness_blockers",
            "benchmark:performance_blockers",
            missing_binary.as_str(),
            unsupported_workload.as_str(),
            correctness_blocker.as_str(),
            performance_blocker.as_str(),
        ] {
            assert!(
                report.production_blockers.contains(&blocker.to_string()),
                "missing blocker {blocker} in {:#?}",
                report.production_blockers
            );
        }
        for action in [
            "build or configure the real BaselineRaft benchmark binaries before claiming parity",
            "implement full BaselineRaft harness coverage for unsupported workloads",
            "fix BaselineRaft/RustRaft benchmark correctness failures before performance gating",
            "fix RustRaft benchmark p50/p99/throughput regressions against BaselineRaft",
        ] {
            assert!(
                report
                    .recommended_next_actions
                    .contains(&action.to_string()),
                "missing action {action} in {:#?}",
                report.recommended_next_actions
            );
        }
    }

    #[test]
    fn production_readiness_gate_rejects_benchmark_resource_regression() {
        let mut input = ready_production_input();
        let benchmark = input
            .baseline_raft_benchmark
            .as_mut()
            .expect("ready input should include benchmark evidence");
        benchmark.resource_within_threshold = false;
        benchmark.performance_blockers.push(
            "batched_writes:peak_resident_memory_ratio_1.500_exceeds_1.100".to_string(),
        );

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert!(report
            .production_blockers
            .contains(&"benchmark:resource_threshold".to_string()));
        assert!(report.recommended_next_actions.contains(
            &"bring RustRaft CPU and peak resident memory within the configured BaselineRaft threshold"
                .to_string()
        ));
    }

    #[test]
    fn production_readiness_gate_rejects_incomplete_admin_status_surface() {
        let mut input = ready_production_input();
        input.admin_status_surface = Some(matrixraft_admin_status_surface_evidence(
            &AdminStatusSurfaceInput {
                commit_index: 104,
                max_observed_node_commit_index: 106,
                quorum_size: 2,
                quorum_peer_ids: vec![2, 3],
                peer_pipeline: Vec::new(),
                wal_last_log_index: 80,
                wal_segment_lifecycle_present: false,
            },
        ));

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .production_blockers
            .contains(&"status:admin_surface_complete".to_string()));
        assert!(report
            .production_blockers
            .contains(&"status:quorum_peer_progress".to_string()));
        assert!(report
            .production_blockers
            .contains(&"wal_segment_lifecycle_missing".to_string()));
        assert!(report
            .production_blockers
            .contains(&"cluster_commit_index_inconsistent".to_string()));
    }

    #[test]
    fn production_readiness_gate_rejects_missing_fault_harness_evidence() {
        let mut input = ready_production_input();
        input.fault_harness = Some(fault::matrixraft_fault_harness_readiness_report(&[]));

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .production_blockers
            .contains(&"fault:harness_ready".to_string()));
        assert!(report
            .production_blockers
            .contains(&"fault:partition_heal_missing".to_string()));
        assert!(report
            .production_blockers
            .contains(&"fault:packet_loss_majority:evidence_missing".to_string()));
        assert!(report
            .production_blockers
            .contains(&"fault:rolling_restart_joint_consensus:evidence_missing".to_string()));
    }

    #[test]
    fn production_readiness_gate_reports_specific_wal_blocker() {
        let mut input = ready_production_input();
        input.wal_lifecycle = Some(WalLifecycleEvidence {
            segment_lifecycle_present: true,
            retained_range_present: true,
            sequence_range_present: true,
            log_index_range_present: true,
            compaction_observed: false,
            slow_fsync_backpressure_observed: true,
            compaction_after_slow_fsync_observed: true,
            released_segment_count: 0,
            compacted_after_slow_fsync_count: 1,
            slow_fsync_segment_count: 1,
            compacted_slow_fsync_segment_count: 1,
        });
        let report = matrixraft_production_readiness_report(&input);
        assert!(!report.ready);
        assert!(report.missing.contains(&"wal:compaction".to_string()));
        assert!(report
            .recommended_next_actions
            .contains(&"prove WAL compaction/released segment behavior".to_string()));
    }

    #[test]
    fn production_readiness_gate_rejects_wal_without_compaction_after_slow_fsync() {
        let mut input = ready_production_input();
        input
            .wal_lifecycle
            .as_mut()
            .unwrap()
            .compaction_after_slow_fsync_observed = false;

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .missing
            .contains(&"wal:compaction_after_slow_fsync".to_string()));
    }

    #[test]
    fn production_readiness_gate_rejects_pipeline_without_same_peer_packet_loss_reorder_recovery() {
        let mut input = ready_production_input();
        input
            .peer_pipeline
            .as_mut()
            .unwrap()
            .packet_loss_reorder_same_peer_recovered = false;

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report
            .missing
            .contains(&"pipeline:packet_loss_reorder_same_peer_recovered".to_string()));
    }

    #[test]
    fn production_readiness_gate_rejects_pipeline_with_unrecovered_faulted_peer() {
        let mut input = ready_production_input();
        input
            .peer_pipeline
            .as_mut()
            .unwrap()
            .packet_loss_reorder_all_faulted_peers_recovered = false;

        let report = matrixraft_production_readiness_report(&input);

        assert!(!report.ready);
        assert_eq!(report.production_status, ProductionStatus::Blocked);
        assert!(report.missing.contains(
            &"pipeline:packet_loss_reorder_all_faulted_peers_recovered".to_string()
        ));
    }

    #[test]
    fn safety_helpers_accept_healthy_state() {
        let status = StatusSnapshot {
            group_id: 1,
            node_id: 1,
            role: StateRole::Leader,
            term: 2,
            leader_id: Some(1),
            commit_index: 10,
            applied_index: 10,
            last_log_index: 10,
            last_snapshot_index: 4,
            peers: vec![PeerStatus {
                node_id: 2,
                matched: 10,
                next_index: 11,
                learner: true,
                healthy: true,
                lag: 0,
            }],
        };
        assert!(
            matrixraft_read_safety_decision(
                &status,
                &ReadIndexRequest {
                    group_id: 1,
                    requester_id: 1,
                    min_commit_index: 10,
                    allow_lease_read: true,
                },
            )
            .safe
        );
        assert!(matrixraft_learner_promotion_decision(&status, 2, 0).promotable);
    }

    #[test]
    fn leader_only_commits_current_term_entry_by_quorum_counting() {
        let mut cluster = RaftCluster::new(
            1,
            Config::default(),
            vec![
                Peer {
                    node_id: 1,
                    raft_addr: "127.0.0.1:19001".to_string(),
                    snapshot_addr: "127.0.0.1:20001".to_string(),
                    role: ReplicaRole::Voter,
                    auto_promote: false,
                },
                Peer {
                    node_id: 2,
                    raft_addr: "127.0.0.1:19002".to_string(),
                    snapshot_addr: "127.0.0.1:20002".to_string(),
                    role: ReplicaRole::Voter,
                    auto_promote: false,
                },
                Peer {
                    node_id: 3,
                    raft_addr: "127.0.0.1:19003".to_string(),
                    snapshot_addr: "127.0.0.1:20003".to_string(),
                    role: ReplicaRole::Voter,
                    auto_promote: false,
                },
            ],
        )
        .expect("cluster");
        cluster.current_term = 2;
        cluster.leader_id = Some(1);
        for node in cluster.nodes.values_mut() {
            node.hard_state.current_term = 2;
            node.raft_role = if node.id == 1 {
                StateRole::Leader
            } else {
                StateRole::Follower
            };
            node.append_entry(LogEntry {
                log_id: LogId { term: 1, index: 1 },
                payload: b"committed-before-election".to_vec(),
                is_command: true,
            });
            node.advance_commit(1);
        }
        cluster.commit_index = 1;

        for node_id in [1, 2] {
            cluster
                .nodes
                .get_mut(&node_id)
                .expect("node")
                .append_entry(LogEntry {
                    log_id: LogId { term: 1, index: 2 },
                    payload: b"previous-term-entry".to_vec(),
                    is_command: true,
                });
        }
        cluster.refresh_commit_index();
        assert_eq!(cluster.commit_index, 1);

        for node_id in [1, 2] {
            cluster
                .nodes
                .get_mut(&node_id)
                .expect("node")
                .append_entry(LogEntry {
                    log_id: LogId { term: 2, index: 3 },
                    payload: b"current-term-entry".to_vec(),
                    is_command: true,
                });
        }
        cluster.refresh_commit_index();
        assert_eq!(cluster.commit_index, 3);
        assert_eq!(
            cluster.nodes.get(&1).expect("leader").hard_state.committed,
            Some(LogId { term: 2, index: 3 })
        );
    }

    #[test]
    fn runtime_read_safety_rejects_stale_leader_lease() {
        let decision = matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
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
        });
        assert!(!decision.allowed);
        assert!(decision.stale_leader_lease_rejected);
        assert_eq!(decision.reason, "stale_leader_lease");
    }

    #[test]
    fn runtime_read_safety_rejects_lagging_follower() {
        let decision = matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
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
        });
        assert!(!decision.allowed);
        assert!(decision.lagging_follower_read_rejected);
        assert_eq!(decision.reason, "replica_lagging");
    }

    #[test]
    fn runtime_read_safety_allows_bounded_stale_within_lag_budget() {
        let decision = matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
            operation: ReadSafetyOperation::BoundedStaleRead,
            node_id: 2,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: true,
            has_majority: true,
            node_commit_index: 8,
            leader_commit_index: 10,
            max_stale_index_lag: 2,
        });
        assert!(decision.allowed);
        assert_eq!(decision.read_index, 8);
    }

    #[test]
    fn runtime_read_safety_rejects_minority_writes() {
        let decision = matrixraft_read_safety_runtime_decision(ReadSafetyRuntimeInput {
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
        });
        assert!(!decision.allowed);
        assert!(decision.minority_partition_write_rejected);
        assert_eq!(decision.reason, "minority_partition");
    }

    #[test]
    fn membership_readiness_requires_failover_scale_up_and_scale_down_for_meta_and_data_nodes() {
        let report = matrixraft_membership_readiness_report(&ready_membership_transitions());
        assert!(report.ready, "{report:#?}");
        assert!(report
            .satisfied
            .contains(&"metaserver:failover".to_string()));
        assert!(report.satisfied.contains(&"metaserver:scaleup".to_string()));
        assert!(report
            .satisfied
            .contains(&"metaserver:scaledown".to_string()));
        assert!(report.satisfied.contains(&"datanode:failover".to_string()));
        assert!(report.satisfied.contains(&"datanode:scaleup".to_string()));
        assert!(report.satisfied.contains(&"datanode:scaledown".to_string()));
    }

    #[test]
    fn membership_readiness_fails_closed_when_transition_evidence_is_missing() {
        let transitions = ready_membership_transitions()
            .into_iter()
            .filter(|item| {
                !(item.scope == MembershipScope::DataNode
                    && item.transition == MembershipTransitionKind::ScaleDown)
            })
            .collect::<Vec<_>>();
        let report = matrixraft_membership_readiness_report(&transitions);
        assert!(!report.ready);
        assert!(report
            .missing
            .contains(&"datanode:scaledown:evidence_present".to_string()));
    }

    #[test]
    fn membership_readiness_rejects_unsafe_scale_up_without_joint_consensus() {
        let mut transition = membership_transition(
            MembershipScope::Metaserver,
            MembershipTransitionKind::ScaleUp,
        );
        transition.joint_consensus_used = false;
        let missing = matrixraft_membership_transition_missing(&transition);
        assert!(missing.contains(&"joint_consensus_used".to_string()));
    }

    #[test]
    fn membership_readiness_rejects_joint_consensus_without_old_and_new_quorum_ack() {
        let mut transition = membership_transition(
            MembershipScope::DataNode,
            MembershipTransitionKind::ScaleDown,
        );
        transition.joint_acknowledged_voters = vec![1, 2];
        transition.joint_old_majority_acked = false;

        let missing = matrixraft_membership_transition_missing(&transition);

        assert!(missing.contains(&"joint_quorum_commit_proven".to_string()));
    }
}
