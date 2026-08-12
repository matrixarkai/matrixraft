// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// production readiness, status/admin reports, and harness-facing evidence.
// Split from src/lib.rs to keep the crate facade small and focused.

pub fn rustraft_production_readiness_report(
    input: &RustRaftProductionReadinessInput,
) -> RustRaftProductionReadinessReport {
    let parity = rustraft_parity_report(&input.readiness);
    let mut satisfied = parity
        .satisfied
        .iter()
        .map(|id| format!("contract:{id}"))
        .collect::<Vec<_>>();
    let mut missing = parity
        .missing
        .iter()
        .map(|id| format!("contract:{id}"))
        .collect::<Vec<_>>();
    let mut production_blockers = parity.production_blockers.clone();
    let mut recommended_next_actions = Vec::new();

    if parity.ready {
        satisfied.push("contract:all_required_semantics".to_string());
    } else {
        recommended_next_actions.push(
            "fix RustRaft semantic contract/readiness gaps before production rollout".to_string(),
        );
    }

    require_option(
        "pipeline:evidence_present",
        input.peer_pipeline.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
        "attach per-peer pipeline evidence from the running RustRaft group",
    );
    if let Some(pipeline) = &input.peer_pipeline {
        for (present, id, action) in [
            (
                pipeline.per_peer_pipeline_state_present,
                "pipeline:per_peer_state",
                "export per-peer replication/apply pipeline state",
            ),
            (
                pipeline.append_backpressure_enforced,
                "pipeline:append_backpressure",
                "prove append queue backpressure under load",
            ),
            (
                pipeline.apply_backpressure_enforced,
                "pipeline:apply_backpressure",
                "prove apply queue backpressure under load",
            ),
            (
                pipeline.memory_replicate_bytes_enforced,
                "pipeline:memory_replicate_bytes",
                "prove max_memory_replicate_log_bytes enforcement",
            ),
            (
                pipeline.oversized_log_rejection_present,
                "pipeline:oversized_log_rejection",
                "prove oversized log entry rejection",
            ),
            (
                pipeline.out_of_order_append_handling_present,
                "pipeline:out_of_order_append_handling",
                "prove out-of-order append handling/rejection",
            ),
            (
                pipeline.reorder_timeout_drop_present,
                "pipeline:reorder_timeout_drop",
                "prove timed-out reordered entries are dropped safely",
            ),
            (
                pipeline.packet_loss_probe_present,
                "pipeline:packet_loss_probe",
                "prove packet loss transitions peers into probe/catch-up",
            ),
            (
                pipeline.packet_loss_recovery_present,
                "pipeline:packet_loss_recovery",
                "prove peer replication recovers after packet loss",
            ),
            (
                pipeline.reorder_convergence_present,
                "pipeline:reorder_convergence",
                "prove peer replication converges after reordered appends",
            ),
            (
                pipeline.packet_loss_reorder_same_peer_recovered,
                "pipeline:packet_loss_reorder_same_peer_recovered",
                "prove one peer recovers after both packet loss and reordered appends",
            ),
            (
                pipeline.stale_term_rejection_present,
                "pipeline:stale_term_rejection",
                "prove stale-term replication messages are rejected",
            ),
            (
                pipeline.reorder_queue_enabled,
                "pipeline:reorder_queue",
                "enable and prove reorder queue behavior",
            ),
        ] {
            require_bool(
                present,
                id,
                &mut satisfied,
                &mut missing,
                &mut production_blockers,
                &mut recommended_next_actions,
                action,
            );
        }
    }

    require_option(
        "snapshot:evidence_present",
        input.snapshot_lifecycle.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
        "attach snapshot send/install lifecycle evidence",
    );
    if let Some(snapshot) = &input.snapshot_lifecycle {
        for (present, id, action) in [
            (
                snapshot.sender_lifecycle_present,
                "snapshot:sender_lifecycle",
                "prove snapshot sender lifecycle",
            ),
            (
                snapshot.downloader_lifecycle_present,
                "snapshot:downloader_lifecycle",
                "prove snapshot downloader/install lifecycle",
            ),
            (
                snapshot.retry_backpressure_present,
                "snapshot:retry_backpressure",
                "prove snapshot retry/backpressure behavior",
            ),
            (
                snapshot.chunk_retry_present,
                "snapshot:chunk_retry",
                "prove snapshot chunk retry behavior",
            ),
            (
                snapshot.send_timeout_present,
                "snapshot:send_timeout",
                "prove snapshot send timeout behavior",
            ),
            (
                snapshot.rate_limit_present,
                "snapshot:rate_limit",
                "prove snapshot rate limiting",
            ),
            (
                snapshot.sustained_sender_load_present,
                "snapshot:sustained_sender_load",
                "prove snapshot sender behavior under sustained load",
            ),
            (
                snapshot.sustained_downloader_load_present,
                "snapshot:sustained_downloader_load",
                "prove snapshot downloader behavior under sustained load",
            ),
            (
                snapshot.sustained_sender_completion_present,
                "snapshot:sustained_sender_completion",
                "prove snapshot sender ack completion under sustained load",
            ),
            (
                snapshot.sustained_downloader_completion_present,
                "snapshot:sustained_downloader_completion",
                "prove snapshot downloader install completion under sustained load",
            ),
            (
                snapshot.install_progress_present,
                "snapshot:install_progress",
                "export snapshot install progress",
            ),
            (
                snapshot.install_rollback_present,
                "snapshot:install_rollback",
                "prove snapshot install rollback",
            ),
            (
                snapshot.membership_change_present,
                "snapshot:membership_change",
                "prove snapshot behavior during membership change",
            ),
            (
                snapshot.rejoin_after_compacted_log_present,
                "snapshot:rejoin_after_compacted_log",
                "prove rejoin after compacted log",
            ),
        ] {
            require_bool(
                present,
                id,
                &mut satisfied,
                &mut missing,
                &mut production_blockers,
                &mut recommended_next_actions,
                action,
            );
        }
    }

    require_option(
        "wal:evidence_present",
        input.wal_lifecycle.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
        "attach WAL segment/range/backpressure evidence",
    );
    if let Some(wal) = &input.wal_lifecycle {
        for (present, id, action) in [
            (
                wal.segment_lifecycle_present,
                "wal:segment_lifecycle",
                "prove WAL segment lifecycle",
            ),
            (
                wal.retained_range_present,
                "wal:retained_range",
                "prove retained WAL range reporting",
            ),
            (
                wal.sequence_range_present,
                "wal:sequence_range",
                "prove WAL sequence range reporting",
            ),
            (
                wal.log_index_range_present,
                "wal:log_index_range",
                "prove WAL log-index range reporting",
            ),
            (
                wal.compaction_observed,
                "wal:compaction",
                "prove WAL compaction/released segment behavior",
            ),
            (
                wal.slow_fsync_backpressure_observed,
                "wal:slow_fsync_backpressure",
                "prove slow fsync backpressure behavior",
            ),
            (
                wal.compaction_after_slow_fsync_observed,
                "wal:compaction_after_slow_fsync",
                "prove WAL compaction still releases segments after slow fsync pressure",
            ),
        ] {
            require_bool(
                present,
                id,
                &mut satisfied,
                &mut missing,
                &mut production_blockers,
                &mut recommended_next_actions,
                action,
            );
        }
    }

    require_data_node_rollout(
        input.data_node_rollout.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );
    require_meta_rollout(
        input.metaserver_rollout.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );
    require_membership_transitions(
        &input.membership_transitions,
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );
    require_admin_status_surface(
        input.admin_status_surface.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );
    require_fault_harness(
        input.fault_harness.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );
    require_baseline_raft_benchmark(
        input.baseline_raft_benchmark.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );

    let ready = missing.is_empty() && production_blockers.is_empty();
    RustRaftProductionReadinessReport {
        parity,
        public_api: rustraft_public_api_contract(),
        ready,
        production_status: if ready {
            RustRaftProductionStatus::ProductionReady
        } else {
            RustRaftProductionStatus::Blocked
        },
        satisfied,
        missing,
        production_blockers,
        recommended_next_actions,
    }
}

fn require_baseline_raft_benchmark(
    benchmark: Option<&RustRaftBaselineRaftBenchmarkEvidence>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    require_option(
        "benchmark:evidence_present",
        benchmark,
        satisfied,
        missing,
        blockers,
        actions,
        "attach real BaselineRaft-vs-RustRaft benchmark evidence",
    );
    let Some(benchmark) = benchmark else {
        blockers.push("benchmark:real_baseline_raft_missing".to_string());
        return;
    };
    for (present, id, action) in [
        (
            benchmark.real_baseline_raft,
            "benchmark:real_baseline_raft",
            "run the benchmark against a real BaselineRaft binary",
        ),
        (
            benchmark.rustraft_runtime,
            "benchmark:rustraft_runtime",
            "run the benchmark against the RustRaft runtime runner",
        ),
        (
            benchmark.baseline_raft_reference,
            "benchmark:baseline_raft_reference",
            "run the benchmark against the real reference Raft implementation",
        ),
        (
            benchmark.rustraft_rust_candidate,
            "benchmark:rustraft_rust_candidate",
            "run the benchmark against the Rust RustRaft candidate implementation",
        ),
        (
            benchmark.correctness_passed,
            "benchmark:correctness",
            "fix correctness failures before measuring performance",
        ),
        (
            benchmark.performance_within_threshold,
            "benchmark:performance_threshold",
            "bring RustRaft p50/p99 latency and throughput within the configured BaselineRaft threshold",
        ),
    ] {
        require_bool(present, id, satisfied, missing, blockers, actions, action);
    }
    for workload in benchmark::rustraft_baseline_raft_benchmark_workloads() {
        let workload_id = workload.id();
        require_bool(
            benchmark
                .workloads
                .iter()
                .any(|observed| observed == workload_id),
            &format!("benchmark:workload:{workload_id}"),
            satisfied,
            missing,
            blockers,
            actions,
            "run every required BaselineRaft-vs-RustRaft parity workload",
        );
    }
    let required_workloads = benchmark::rustraft_baseline_raft_benchmark_workloads();
    let required_workload_ids: std::collections::BTreeSet<&'static str> = required_workloads
        .iter()
        .map(|workload| workload.id())
        .collect();
    let mut observed_workloads = std::collections::BTreeSet::new();
    for workload_id in &benchmark.workloads {
        require_bool(
            required_workload_ids.contains(workload_id.as_str()),
            &format!("benchmark:workload_unknown:{workload_id}"),
            satisfied,
            missing,
            blockers,
            actions,
            "remove non-canonical benchmark workloads from production parity evidence",
        );
        require_bool(
            observed_workloads.insert(workload_id.as_str()),
            &format!("benchmark:workload_duplicate:{workload_id}"),
            satisfied,
            missing,
            blockers,
            actions,
            "deduplicate benchmark workloads before claiming production parity",
        );
    }
    require_bool(
        benchmark.workloads.len() == required_workloads.len(),
        "benchmark:workload_set_exact",
        satisfied,
        missing,
        blockers,
        actions,
        "provide exactly the canonical BaselineRaft-vs-RustRaft workload set",
    );
    require_benchmark_blocker_category(
        "benchmark:blockers",
        &benchmark.blockers,
        satisfied,
        missing,
        blockers,
        actions,
        "clear all BaselineRaft-vs-RustRaft benchmark blockers before claiming production parity",
    );
    require_benchmark_blocker_category(
        "benchmark:baseline_raft_binaries_missing",
        &benchmark.missing_baseline_raft_binaries,
        satisfied,
        missing,
        blockers,
        actions,
        "build or configure the real BaselineRaft benchmark binaries before claiming parity",
    );
    require_benchmark_blocker_category(
        "benchmark:unsupported_workloads",
        &benchmark.unsupported_workloads,
        satisfied,
        missing,
        blockers,
        actions,
        "implement full BaselineRaft harness coverage for unsupported workloads",
    );
    require_benchmark_blocker_category(
        "benchmark:correctness_blockers",
        &benchmark.correctness_blockers,
        satisfied,
        missing,
        blockers,
        actions,
        "fix BaselineRaft/RustRaft benchmark correctness failures before performance gating",
    );
    require_benchmark_blocker_category(
        "benchmark:performance_blockers",
        &benchmark.performance_blockers,
        satisfied,
        missing,
        blockers,
        actions,
        "fix RustRaft benchmark p50/p99/throughput regressions against BaselineRaft",
    );
    for blocker in &benchmark.blockers {
        if benchmark
            .missing_baseline_raft_binaries
            .iter()
            .chain(benchmark.unsupported_workloads.iter())
            .chain(benchmark.correctness_blockers.iter())
            .chain(benchmark.performance_blockers.iter())
            .any(|classified| classified == blocker)
        {
            continue;
        }
        require_bool(
            false,
            blocker,
            satisfied,
            missing,
            blockers,
            actions,
            "clear uncategorized BaselineRaft benchmark blocker",
        );
    }
}

fn require_benchmark_blocker_category(
    category_id: &str,
    category_blockers: &[String],
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
    action: &str,
) {
    if category_blockers.is_empty() {
        return;
    }
    require_bool(
        false,
        category_id,
        satisfied,
        missing,
        blockers,
        actions,
        action,
    );
    for blocker in category_blockers {
        require_bool(
            false, blocker, satisfied, missing, blockers, actions, action,
        );
    }
}

fn require_fault_harness(
    fault_harness: Option<&fault::RustRaftFaultHarnessReadinessReport>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    require_option(
        "fault:harness_present",
        fault_harness,
        satisfied,
        missing,
        blockers,
        actions,
        "attach BaselineRaft-style fault harness evidence from real RustRaft process runs",
    );
    let Some(fault_harness) = fault_harness else {
        blockers.push("fault:harness_missing".to_string());
        blockers.push("fault:partition_heal_missing".to_string());
        return;
    };

    require_bool(
        fault_harness.ready,
        "fault:harness_ready",
        satisfied,
        missing,
        blockers,
        actions,
        "run all required BaselineRaft fault scenarios against RustRaft",
    );

    for result in &fault_harness.results {
        let scenario_id = result.scenario.id();
        require_bool(
            result.ready,
            &format!("fault:scenario:{scenario_id}"),
            satisfied,
            missing,
            blockers,
            actions,
            "clear BaselineRaft fault scenario blocker",
        );
    }

    for blocker in &fault_harness.missing {
        if blocker.starts_with("packet_loss_majority:") {
            blockers.push("fault:partition_heal_missing".to_string());
        }
        require_bool(
            false,
            &format!("fault:{blocker}"),
            satisfied,
            missing,
            blockers,
            actions,
            "clear BaselineRaft fault harness blocker",
        );
    }
}

fn require_admin_status_surface(
    status: Option<&RustRaftAdminStatusSurfaceEvidence>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    require_option(
        "status:admin_surface_present",
        status,
        satisfied,
        missing,
        blockers,
        actions,
        "attach RustRaft admin/status surface evidence from the serving runtime",
    );
    let Some(status) = status else {
        blockers.push("status:admin_surface_missing".to_string());
        return;
    };

    for (present, id, action) in [
        (
            status.complete,
            "status:admin_surface_complete",
            "fix incomplete RustRaft admin/status surface evidence",
        ),
        (
            status.peer_rows > 0,
            "status:peer_rows",
            "export per-peer pipeline rows",
        ),
        (
            status.quorum_size > 0,
            "status:quorum_size",
            "export configured quorum size",
        ),
        (
            status.quorum_peer_progress_observed,
            "status:quorum_peer_progress",
            "prove quorum peer progress reaches the commit index",
        ),
        (
            status.peer_pipeline_runtime_activity_observed,
            "status:peer_pipeline_runtime_activity",
            "prove peer pipeline counters are populated by runtime activity",
        ),
        (
            status.peer_pipeline_limits_observed,
            "status:peer_pipeline_limits",
            "export per-peer replication/apply limits",
        ),
        (
            status.wal_segment_lifecycle_present,
            "status:wal_segment_lifecycle",
            "export WAL segment lifecycle in admin status",
        ),
        (
            status.wal_log_range_covers_commit,
            "status:wal_log_range_covers_commit",
            "prove WAL retained log range covers the committed index",
        ),
        (
            status.peer_next_index_present,
            "status:peer_next_index",
            "export next-index per peer",
        ),
        (
            status.majority_configured,
            "status:majority_configured",
            "export nonzero majority/quorum configuration",
        ),
        (
            status.cluster_commit_index_consistent,
            "status:cluster_commit_index_consistent",
            "prove cluster/admin commit index is consistent with node reports",
        ),
    ] {
        require_bool(present, id, satisfied, missing, blockers, actions, action);
    }

    for blocker in &status.blockers {
        require_bool(
            false,
            blocker,
            satisfied,
            missing,
            blockers,
            actions,
            "clear RustRaft admin/status blocker",
        );
    }
}

pub fn rustraft_baseline_raft_runtime_capability_report(
    input: &RustRaftProductionReadinessInput,
) -> RustRaftBaselineRaftRuntimeCapabilityReport {
    let data_semantics = input
        .data_node_rollout
        .as_ref()
        .map(|rollout| &rollout.operational_semantics);
    let meta_semantics = input
        .metaserver_rollout
        .as_ref()
        .map(|rollout| &rollout.operational_semantics);
    let membership_report = rustraft_membership_readiness_report(&input.membership_transitions);

    let mut capability_evidence = Vec::new();
    capability_evidence.push(runtime_capability(
        "process_path_rollout_evidence",
        "RustRaftProductionReadinessInput::{data_node_rollout,metaserver_rollout}",
        &[
            (
                input
                    .data_node_rollout
                    .as_ref()
                    .is_some_and(|rollout| rollout.ready),
                "data_node.ready",
            ),
            (
                input
                    .data_node_rollout
                    .as_ref()
                    .is_some_and(|rollout| rollout.observed_process_requests > 0),
                "data_node.observed_process_requests",
            ),
            (
                input.data_node_rollout.as_ref().is_some_and(|rollout| {
                    rollout.independent_wal_dirs && rollout.independent_snapshot_dirs
                }),
                "data_node.independent_wal_and_snapshot_dirs",
            ),
            (
                input.data_node_rollout.as_ref().is_some_and(|rollout| {
                    !rollout.nodes.is_empty()
                        && rollout.per_node_log_store_inspection_count as usize
                            >= rollout.nodes.len()
                }),
                "data_node.per_node_log_store_inspection",
            ),
            (
                input
                    .metaserver_rollout
                    .as_ref()
                    .is_some_and(|rollout| rollout.ready),
                "metaserver.ready",
            ),
            (
                input
                    .metaserver_rollout
                    .as_ref()
                    .is_some_and(|rollout| rollout.observed_process_requests > 0),
                "metaserver.observed_process_requests",
            ),
            (
                input.metaserver_rollout.as_ref().is_some_and(|rollout| {
                    rollout.independent_wal_dirs && rollout.independent_snapshot_dirs
                }),
                "metaserver.independent_wal_and_snapshot_dirs",
            ),
            (
                input.metaserver_rollout.as_ref().is_some_and(|rollout| {
                    !rollout.nodes.is_empty()
                        && rollout.per_node_log_store_inspection_count as usize
                            >= rollout.nodes.len()
                }),
                "metaserver.per_node_log_store_inspection",
            ),
        ],
    ));

    let pipeline = input.peer_pipeline.as_ref();
    capability_evidence.push(runtime_capability(
        "per_peer_replication_pipeline_state",
        "RustRaftPipelineEvidence",
        &[
            (
                pipeline.is_some_and(|evidence| evidence.per_peer_pipeline_state_present),
                "pipeline.per_peer_pipeline_state_present",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.append_backpressure_enforced),
                "pipeline.append_backpressure_enforced",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.apply_backpressure_enforced),
                "pipeline.apply_backpressure_enforced",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.memory_replicate_bytes_enforced),
                "pipeline.memory_replicate_bytes_enforced",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.oversized_log_rejection_present),
                "pipeline.oversized_log_rejection_present",
            ),
        ],
    ));
    capability_evidence.push(runtime_capability(
        "reorder_queue_semantics",
        "RustRaftPipelineEvidence",
        &[
            (
                pipeline.is_some_and(|evidence| evidence.reorder_queue_enabled),
                "pipeline.reorder_queue_enabled",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.out_of_order_append_handling_present),
                "pipeline.out_of_order_append_handling_present",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.reorder_timeout_drop_present),
                "pipeline.reorder_timeout_drop_present",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.packet_loss_probe_present),
                "pipeline.packet_loss_probe_present",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.packet_loss_recovery_present),
                "pipeline.packet_loss_recovery_present",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.reorder_convergence_present),
                "pipeline.reorder_convergence_present",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.packet_loss_reorder_same_peer_recovered),
                "pipeline.packet_loss_reorder_same_peer_recovered",
            ),
            (
                pipeline.is_some_and(|evidence| evidence.stale_term_rejection_present),
                "pipeline.stale_term_rejection_present",
            ),
        ],
    ));

    let snapshot = input.snapshot_lifecycle.as_ref();
    capability_evidence.push(runtime_capability(
        "snapshot_sender_downloader_lifecycle",
        "RustRaftSnapshotLifecycleEvidence",
        &[
            (
                snapshot.is_some_and(|evidence| evidence.sender_lifecycle_present),
                "snapshot.sender_lifecycle_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.downloader_lifecycle_present),
                "snapshot.downloader_lifecycle_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.retry_backpressure_present),
                "snapshot.retry_backpressure_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.chunk_retry_present),
                "snapshot.chunk_retry_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.send_timeout_present),
                "snapshot.send_timeout_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.rate_limit_present),
                "snapshot.rate_limit_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.sustained_sender_load_present),
                "snapshot.sustained_sender_load_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.sustained_downloader_load_present),
                "snapshot.sustained_downloader_load_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.sustained_sender_completion_present),
                "snapshot.sustained_sender_completion_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.sustained_downloader_completion_present),
                "snapshot.sustained_downloader_completion_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.install_progress_present),
                "snapshot.install_progress_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.install_rollback_present),
                "snapshot.install_rollback_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.membership_change_present),
                "snapshot.membership_change_present",
            ),
            (
                snapshot.is_some_and(|evidence| evidence.rejoin_after_compacted_log_present),
                "snapshot.rejoin_after_compacted_log_present",
            ),
        ],
    ));

    let wal = input.wal_lifecycle.as_ref();
    capability_evidence.push(runtime_capability(
        "wal_segment_lifecycle",
        "RustRaftWalLifecycleEvidence",
        &[
            (
                wal.is_some_and(|evidence| evidence.segment_lifecycle_present),
                "wal.segment_lifecycle_present",
            ),
            (
                wal.is_some_and(|evidence| evidence.retained_range_present),
                "wal.retained_range_present",
            ),
            (
                wal.is_some_and(|evidence| evidence.sequence_range_present),
                "wal.sequence_range_present",
            ),
            (
                wal.is_some_and(|evidence| evidence.log_index_range_present),
                "wal.log_index_range_present",
            ),
            (
                wal.is_some_and(|evidence| evidence.compaction_observed),
                "wal.compaction_observed",
            ),
            (
                wal.is_some_and(|evidence| evidence.slow_fsync_backpressure_observed),
                "wal.slow_fsync_backpressure_observed",
            ),
            (
                wal.is_some_and(|evidence| evidence.compaction_after_slow_fsync_observed),
                "wal.compaction_after_slow_fsync_observed",
            ),
        ],
    ));

    capability_evidence.push(runtime_capability(
        "read_index_and_lease_safety",
        "RustRaftProcessOperationalSemanticsEvidence",
        &[
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.read_index_validated
                }),
                "semantics.read_index_validated",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.leader_lease_validated
                }),
                "semantics.leader_lease_validated",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.stale_leader_lease_rejection_observed
                }),
                "semantics.stale_leader_lease_rejection_observed",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.follower_lease_expiration_observed
                }),
                "semantics.follower_lease_expiration_observed",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.lagging_follower_read_rejected
                }),
                "semantics.lagging_follower_read_rejected",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.bounded_stale_read_acceptance_observed
                        && evidence.bounded_stale_read_rejection_observed
                }),
                "semantics.bounded_stale_read_acceptance_and_rejection",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.minority_partition_read_rejection_observed
                }),
                "semantics.minority_partition_read_rejection_observed",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.stale_follower_write_rejected
                        && evidence.healed_follower_catchup_observed
                }),
                "semantics.stale_write_rejection_and_healed_catchup",
            ),
        ],
    ));

    capability_evidence.push(runtime_capability(
        "membership_role_semantics",
        "RustRaftMembershipTransitionEvidence",
        &[
            (
                membership_report.ready,
                "membership.required_transitions_ready",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.membership_rescale_validated
                }),
                "semantics.membership_rescale_validated",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.membership_add_promote_remove_validated
                }),
                "semantics.membership_add_promote_remove_validated",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.leader_transfer_exact_once_validated
                        && evidence.leader_transfer_under_load_validated
                }),
                "semantics.leader_transfer_exact_once_under_load",
            ),
        ],
    ));

    capability_evidence.push(runtime_capability(
        "fsm_apply_atomicity",
        "RustRaftProcessOperationalSemanticsEvidence",
        &[
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.apply_pipeline_converged
                }),
                "semantics.apply_pipeline_converged",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.wal_persistence_observed
                }),
                "semantics.wal_persistence_observed",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.fsm_apply_idempotent_replay_observed
                }),
                "semantics.fsm_apply_idempotent_replay_observed",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.storage_mutation_wal_fence_atomicity_observed
                }),
                "semantics.storage_mutation_wal_fence_atomicity_observed",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.snapshot_install_apply_fence_atomicity_observed
                }),
                "semantics.snapshot_install_apply_fence_atomicity_observed",
            ),
            (
                all_semantics(data_semantics, meta_semantics, |evidence| {
                    evidence.process_restart_after_apply_crash_recovered
                }),
                "semantics.process_restart_after_apply_crash_recovered",
            ),
        ],
    ));

    capability_evidence.push(runtime_capability(
        "admin_metrics_surface",
        "RustRaftReadinessSnapshot",
        &[
            (
                input.readiness.rustraft_operator_observability_present,
                "readiness.rustraft_operator_observability_present",
            ),
            (
                input.peer_pipeline.is_some(),
                "status.peer_pipeline_evidence_attached",
            ),
            (
                input.snapshot_lifecycle.is_some(),
                "status.snapshot_lifecycle_evidence_attached",
            ),
            (
                input.wal_lifecycle.is_some(),
                "status.wal_lifecycle_evidence_attached",
            ),
        ],
    ));

    let satisfied = capability_evidence
        .iter()
        .filter(|item| item.present)
        .map(|item| item.capability.clone())
        .collect::<Vec<_>>();
    let missing = capability_evidence
        .iter()
        .filter(|item| !item.present)
        .map(|item| item.capability.clone())
        .collect::<Vec<_>>();
    let blockers = capability_evidence
        .iter()
        .filter(|item| !item.present)
        .flat_map(|item| {
            item.evidence
                .iter()
                .filter(|field| field.starts_with("missing:"))
                .map(move |field| format!("{}:{}", item.capability, field))
        })
        .collect::<Vec<_>>();
    RustRaftBaselineRaftRuntimeCapabilityReport {
        ready: missing.is_empty() && blockers.is_empty(),
        capability_evidence,
        satisfied,
        missing,
        blockers,
    }
}

pub fn rustraft_baseline_raft_runtime_capability_prometheus(
    report: &RustRaftBaselineRaftRuntimeCapabilityReport,
    labels: &[(&str, &str)],
) -> RustRaftPrometheusMetricSet {
    let mut out = String::new();
    let mut metric_count = 0;
    out.push_str("# HELP rustraft_baseline_raft_ready Whether BaselineRaft-derived runtime capability evidence is complete.\n");
    out.push_str("# TYPE rustraft_baseline_raft_ready gauge\n");
    push_prometheus_metric(
        &mut out,
        "rustraft_baseline_raft_ready",
        labels,
        u64::from(report.ready),
    );
    metric_count += 1;

    out.push_str("# HELP rustraft_baseline_raft_capability_ready BaselineRaft-derived runtime capability readiness by family.\n");
    out.push_str("# TYPE rustraft_baseline_raft_capability_ready gauge\n");
    for capability in &report.capability_evidence {
        let capability_label = capability.capability.as_str();
        let mut metric_labels = labels.to_vec();
        metric_labels.push(("capability", capability_label));
        metric_labels.push(("source", capability.source_reference.as_str()));
        push_prometheus_metric(
            &mut out,
            "rustraft_baseline_raft_capability_ready",
            &metric_labels,
            u64::from(capability.present),
        );
        metric_count += 1;

        for evidence in &capability.evidence {
            let field = evidence
                .strip_prefix("present:")
                .or_else(|| evidence.strip_prefix("missing:"))
                .unwrap_or(evidence.as_str());
            let mut evidence_labels = labels.to_vec();
            evidence_labels.push(("capability", capability_label));
            evidence_labels.push(("field", field));
            push_prometheus_metric(
                &mut out,
                "rustraft_baseline_raft_capability_field_present",
                &evidence_labels,
                u64::from(evidence.starts_with("present:")),
            );
            metric_count += 1;
        }
    }

    out.push_str(
        "# HELP rustraft_baseline_raft_blocker_present BaselineRaft-derived runtime capability blockers.\n",
    );
    out.push_str("# TYPE rustraft_baseline_raft_blocker_present gauge\n");
    for blocker in &report.blockers {
        let mut blocker_labels = labels.to_vec();
        blocker_labels.push(("blocker", blocker.as_str()));
        push_prometheus_metric(
            &mut out,
            "rustraft_baseline_raft_blocker_present",
            &blocker_labels,
            1,
        );
        metric_count += 1;
    }

    push_prometheus_metric(
        &mut out,
        "rustraft_baseline_raft_satisfied_capability_count",
        labels,
        report.satisfied.len() as u64,
    );
    metric_count += 1;
    push_prometheus_metric(
        &mut out,
        "rustraft_baseline_raft_missing_capability_count",
        labels,
        report.missing.len() as u64,
    );
    metric_count += 1;
    push_prometheus_metric(
        &mut out,
        "rustraft_baseline_raft_blocker_count",
        labels,
        report.blockers.len() as u64,
    );
    metric_count += 1;

    RustRaftPrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count,
        text: out,
    }
}

pub fn rustraft_cross_plane_process_evidence_prometheus(
    summary: &RustRaftCrossPlaneProcessEvidenceSummary,
    labels: &[(&str, &str)],
) -> RustRaftPrometheusMetricSet {
    let mut out = String::new();
    let mut metric_count = 0;
    out.push_str("# HELP rustraft_process_evidence_count Cross-plane process-path evidence counters by plane and evidence kind.\n");
    out.push_str("# TYPE rustraft_process_evidence_count gauge\n");
    for (plane, metric, value) in [
        (
            "data_node",
            "spawned_process_count",
            summary.data_node_spawned_process_count,
        ),
        (
            "metaserver",
            "spawned_process_count",
            summary.metaserver_spawned_process_count,
        ),
        (
            "both",
            "spawned_process_count",
            summary.total_spawned_process_count,
        ),
        (
            "data_node",
            "observed_process_requests",
            summary.data_node_observed_process_requests,
        ),
        (
            "metaserver",
            "observed_process_requests",
            summary.metaserver_observed_process_requests,
        ),
        (
            "both",
            "observed_process_requests",
            summary.total_observed_process_requests,
        ),
        (
            "data_node",
            "read_index_responses_observed",
            summary.data_node_read_index_responses_observed,
        ),
        (
            "metaserver",
            "read_index_responses_observed",
            summary.metaserver_read_index_responses_observed,
        ),
        (
            "both",
            "read_index_responses_observed",
            summary.total_read_index_responses_observed,
        ),
        (
            "data_node",
            "restarted_node_count",
            summary.data_node_restarted_node_count,
        ),
        (
            "metaserver",
            "restarted_node_count",
            summary.metaserver_restarted_node_count,
        ),
        (
            "both",
            "restarted_node_count",
            summary.total_restarted_node_count,
        ),
        (
            "data_node",
            "per_node_log_store_inspection_count",
            summary.data_node_per_node_log_store_inspection_count,
        ),
        (
            "metaserver",
            "per_node_log_store_inspection_count",
            summary.metaserver_per_node_log_store_inspection_count,
        ),
        (
            "both",
            "per_node_log_store_inspection_count",
            summary.total_per_node_log_store_inspection_count,
        ),
    ] {
        let mut metric_labels = labels.to_vec();
        metric_labels.push(("plane", plane));
        metric_labels.push(("evidence", metric));
        push_prometheus_metric(
            &mut out,
            "rustraft_process_evidence_count",
            &metric_labels,
            value,
        );
        metric_count += 1;
    }

    out.push_str("# HELP rustraft_process_evidence_ready Cross-plane process-path evidence booleans for both planes.\n");
    out.push_str("# TYPE rustraft_process_evidence_ready gauge\n");
    for (evidence, ready) in [
        (
            "independent_wal_dirs_on_both_planes",
            summary.independent_wal_dirs_on_both_planes,
        ),
        (
            "independent_snapshot_dirs_on_both_planes",
            summary.independent_snapshot_dirs_on_both_planes,
        ),
        (
            "write_or_mutation_proposed_through_process_api_on_both_planes",
            summary.write_or_mutation_proposed_through_process_api_on_both_planes,
        ),
        (
            "multi_process_log_store_validated_on_both_planes",
            summary.multi_process_log_store_validated_on_both_planes,
        ),
        (
            "restart_recovery_validated_on_both_planes",
            summary.restart_recovery_validated_on_both_planes,
        ),
        (
            "read_index_observed_on_both_planes",
            summary.read_index_observed_on_both_planes,
        ),
    ] {
        let mut metric_labels = labels.to_vec();
        metric_labels.push(("evidence", evidence));
        push_prometheus_metric(
            &mut out,
            "rustraft_process_evidence_ready",
            &metric_labels,
            u64::from(ready),
        );
        metric_count += 1;
    }

    RustRaftPrometheusMetricSet {
        format: "prometheus_text_v0.0.4".to_string(),
        metric_count,
        text: out,
    }
}

pub fn rustraft_cross_plane_process_evidence_artifact(
    data_node_report: &RustRaftDataNodeProcessRolloutReport,
    metaserver_report: &RustRaftMetaProcessRolloutReport,
    labels: &[(&str, &str)],
) -> RustRaftCrossPlaneProcessEvidenceArtifact {
    let readiness =
        rustraft_cross_plane_process_readiness_blocker_report(data_node_report, metaserver_report);
    let summary =
        rustraft_cross_plane_process_evidence_summary(data_node_report, metaserver_report);
    let prometheus = rustraft_cross_plane_process_evidence_prometheus(&summary, labels);
    RustRaftCrossPlaneProcessEvidenceArtifact {
        schema: "rustraft.cross_plane_process_evidence.v1".to_string(),
        readiness,
        summary,
        prometheus,
    }
}

pub fn rustraft_validate_cross_plane_process_evidence_artifact(
    artifact: &RustRaftCrossPlaneProcessEvidenceArtifact,
) -> RustRaftCrossPlaneProcessEvidenceArtifactValidationReport {
    let schema_valid = artifact.schema == "rustraft.cross_plane_process_evidence.v1";
    let readiness_ready =
        artifact.readiness.ready && artifact.readiness.remaining_blockers.is_empty();
    let summary = &artifact.summary;
    let summary_ready = summary.total_spawned_process_count >= 6
        && summary.data_node_spawned_process_count >= 3
        && summary.metaserver_spawned_process_count >= 3
        && summary.total_observed_process_requests > 0
        && summary.total_read_index_responses_observed > 0
        && summary.total_restarted_node_count >= 2
        && summary.total_per_node_log_store_inspection_count >= 6
        && summary.independent_wal_dirs_on_both_planes
        && summary.independent_snapshot_dirs_on_both_planes
        && summary.write_or_mutation_proposed_through_process_api_on_both_planes
        && summary.multi_process_log_store_validated_on_both_planes
        && summary.restart_recovery_validated_on_both_planes
        && summary.read_index_observed_on_both_planes;
    let prometheus_complete = artifact.prometheus.format == "prometheus_text_v0.0.4"
        && artifact.prometheus.metric_count >= 21
        && artifact
            .prometheus
            .text
            .contains("rustraft_process_evidence_count")
        && artifact
            .prometheus
            .text
            .contains("rustraft_process_evidence_ready");
    let mut missing = Vec::new();
    if !schema_valid {
        missing.push("schema must be rustraft.cross_plane_process_evidence.v1".to_string());
    }
    if !readiness_ready {
        missing.push("readiness.ready must be true with zero remaining blockers".to_string());
    }
    if summary.total_spawned_process_count < 6 {
        missing
            .push("summary.total_spawned_process_count must cover both 3-node planes".to_string());
    }
    if summary.data_node_spawned_process_count < 3 {
        missing.push("summary.data_node_spawned_process_count must be at least 3".to_string());
    }
    if summary.metaserver_spawned_process_count < 3 {
        missing.push("summary.metaserver_spawned_process_count must be at least 3".to_string());
    }
    if summary.total_observed_process_requests == 0 {
        missing.push("summary.total_observed_process_requests must be non-zero".to_string());
    }
    if summary.total_read_index_responses_observed == 0 {
        missing.push("summary.total_read_index_responses_observed must be non-zero".to_string());
    }
    if summary.total_restarted_node_count < 2 {
        missing.push("summary.total_restarted_node_count must show restart recovery".to_string());
    }
    if summary.total_per_node_log_store_inspection_count < 6 {
        missing.push(
            "summary.total_per_node_log_store_inspection_count must inspect every process"
                .to_string(),
        );
    }
    for (present, field) in [
        (
            summary.independent_wal_dirs_on_both_planes,
            "summary.independent_wal_dirs_on_both_planes",
        ),
        (
            summary.independent_snapshot_dirs_on_both_planes,
            "summary.independent_snapshot_dirs_on_both_planes",
        ),
        (
            summary.write_or_mutation_proposed_through_process_api_on_both_planes,
            "summary.write_or_mutation_proposed_through_process_api_on_both_planes",
        ),
        (
            summary.multi_process_log_store_validated_on_both_planes,
            "summary.multi_process_log_store_validated_on_both_planes",
        ),
        (
            summary.restart_recovery_validated_on_both_planes,
            "summary.restart_recovery_validated_on_both_planes",
        ),
        (
            summary.read_index_observed_on_both_planes,
            "summary.read_index_observed_on_both_planes",
        ),
    ] {
        if !present {
            missing.push(format!("{field} must be true"));
        }
    }
    if !prometheus_complete {
        missing.push(
            "prometheus must include process evidence count and readiness metrics".to_string(),
        );
    }
    RustRaftCrossPlaneProcessEvidenceArtifactValidationReport {
        valid: schema_valid && readiness_ready && summary_ready && prometheus_complete,
        schema_valid,
        readiness_ready,
        summary_ready,
        prometheus_complete,
        missing,
    }
}

pub fn rustraft_data_node_process_rollout_readiness_report(
    rollout: &RustRaftDataNodeProcessRolloutReport,
) -> RustRaftProcessRolloutReadinessReport {
    let mut satisfied = Vec::new();
    let mut missing = Vec::new();
    let mut blockers = Vec::new();
    let mut recommended_next_actions = Vec::new();
    require_data_node_rollout(
        Some(rollout),
        &mut satisfied,
        &mut missing,
        &mut blockers,
        &mut recommended_next_actions,
    );
    let ready = missing.is_empty() && blockers.is_empty();
    RustRaftProcessRolloutReadinessReport {
        scope: "data_node".to_string(),
        ready,
        production_status: if ready {
            RustRaftProductionStatus::ProductionReady
        } else {
            RustRaftProductionStatus::Blocked
        },
        satisfied,
        missing,
        blockers,
        recommended_next_actions,
    }
}

pub fn rustraft_meta_process_rollout_readiness_report(
    rollout: &RustRaftMetaProcessRolloutReport,
) -> RustRaftProcessRolloutReadinessReport {
    let mut satisfied = Vec::new();
    let mut missing = Vec::new();
    let mut blockers = Vec::new();
    let mut recommended_next_actions = Vec::new();
    require_meta_rollout(
        Some(rollout),
        &mut satisfied,
        &mut missing,
        &mut blockers,
        &mut recommended_next_actions,
    );
    let ready = missing.is_empty() && blockers.is_empty();
    RustRaftProcessRolloutReadinessReport {
        scope: "metaserver".to_string(),
        ready,
        production_status: if ready {
            RustRaftProductionStatus::ProductionReady
        } else {
            RustRaftProductionStatus::Blocked
        },
        satisfied,
        missing,
        blockers,
        recommended_next_actions,
    }
}

pub fn rustraft_cross_plane_process_readiness_report(
    data_node_report: &RustRaftDataNodeProcessRolloutReport,
    metaserver_report: &RustRaftMetaProcessRolloutReport,
) -> RustRaftCrossPlaneProcessReadinessReport {
    let data_process_path = data_node_process_path_ready(data_node_report);
    let meta_process_path = meta_process_path_ready(metaserver_report);
    let multi_process_data_node_and_metaserver_raft = data_process_path && meta_process_path;
    let failover_on_both_planes =
        data_node_report.failover_validated && metaserver_report.failover_validated;
    let membership_add_remove_under_load = data_node_report.membership_change_validated
        && metaserver_report.membership_change_validated
        && data_node_report
            .operational_semantics
            .leader_transfer_under_load_validated
        && metaserver_report
            .operational_semantics
            .leader_transfer_under_load_validated;
    let secondary_lag_and_catchup = data_node_report.follower_lag_validated
        && data_node_report.secondary_read_validated
        && metaserver_report.follower_lag_validated
        && metaserver_report.secondary_read_validated
        && data_node_report
            .operational_semantics
            .healed_follower_catchup_observed
        && metaserver_report
            .operational_semantics
            .healed_follower_catchup_observed;
    let snapshot_restart_after_compaction = data_node_report.snapshot_install_validated
        && data_node_report.restart_recovery_validated
        && data_node_report.recovered_after_restart
        && metaserver_report.snapshot_install_validated
        && metaserver_report.recovered_after_restart
        && data_node_report
            .operational_semantics
            .follower_rejoin_after_compaction_validated
        && metaserver_report
            .operational_semantics
            .follower_rejoin_after_compaction_validated;

    let mut remaining_blockers = Vec::new();
    append_data_node_process_blockers(
        &mut remaining_blockers,
        "data_node_report",
        data_node_report,
    );
    append_meta_process_blockers(
        &mut remaining_blockers,
        "metaserver_report",
        metaserver_report,
    );
    push_missing(
        &mut remaining_blockers,
        multi_process_data_node_and_metaserver_raft,
        "final_raft_readiness.multi_process_data_node_and_metaserver_raft",
    );
    push_missing(
        &mut remaining_blockers,
        failover_on_both_planes,
        "final_raft_readiness.failover_on_both_planes",
    );
    push_missing(
        &mut remaining_blockers,
        membership_add_remove_under_load,
        "final_raft_readiness.membership_add_remove_under_load",
    );
    push_missing(
        &mut remaining_blockers,
        secondary_lag_and_catchup,
        "final_raft_readiness.secondary_lag_and_catchup",
    );
    push_missing(
        &mut remaining_blockers,
        snapshot_restart_after_compaction,
        "final_raft_readiness.snapshot_restart_after_compaction",
    );
    remaining_blockers.sort();
    remaining_blockers.dedup();

    let ready = multi_process_data_node_and_metaserver_raft
        && failover_on_both_planes
        && membership_add_remove_under_load
        && secondary_lag_and_catchup
        && snapshot_restart_after_compaction
        && remaining_blockers.is_empty();

    RustRaftCrossPlaneProcessReadinessReport {
        ready,
        multi_process_data_node_and_metaserver_raft,
        failover_on_both_planes,
        membership_add_remove_under_load,
        secondary_lag_and_catchup,
        snapshot_restart_after_compaction,
        remaining_blockers,
    }
}

pub fn rustraft_cross_plane_process_readiness_blocker_report(
    data_node_report: &RustRaftDataNodeProcessRolloutReport,
    metaserver_report: &RustRaftMetaProcessRolloutReport,
) -> RustRaftCrossPlaneProcessReadinessBlockerReport {
    let report = rustraft_cross_plane_process_readiness_report(data_node_report, metaserver_report);
    RustRaftCrossPlaneProcessReadinessBlockerReport {
        ready: report.ready,
        multi_process_data_node_and_metaserver_raft: report
            .multi_process_data_node_and_metaserver_raft,
        failover_on_both_planes: report.failover_on_both_planes,
        membership_add_remove_under_load: report.membership_add_remove_under_load,
        secondary_lag_and_catchup: report.secondary_lag_and_catchup,
        snapshot_restart_after_compaction: report.snapshot_restart_after_compaction,
        remaining_blockers: report
            .remaining_blockers
            .iter()
            .map(|field| rustraft_process_readiness_blocker(field))
            .collect(),
    }
}

pub fn rustraft_cross_plane_process_evidence_summary(
    data_node_report: &RustRaftDataNodeProcessRolloutReport,
    metaserver_report: &RustRaftMetaProcessRolloutReport,
) -> RustRaftCrossPlaneProcessEvidenceSummary {
    RustRaftCrossPlaneProcessEvidenceSummary {
        data_node_spawned_process_count: data_node_report.spawned_process_count,
        metaserver_spawned_process_count: metaserver_report.spawned_process_count,
        total_spawned_process_count: data_node_report.spawned_process_count
            + metaserver_report.spawned_process_count,
        data_node_observed_process_requests: data_node_report.observed_process_requests,
        metaserver_observed_process_requests: metaserver_report.observed_process_requests,
        total_observed_process_requests: data_node_report.observed_process_requests
            + metaserver_report.observed_process_requests,
        data_node_read_index_responses_observed: data_node_report.read_index_responses_observed,
        metaserver_read_index_responses_observed: metaserver_report.read_index_responses_observed,
        total_read_index_responses_observed: data_node_report.read_index_responses_observed
            + metaserver_report.read_index_responses_observed,
        data_node_restarted_node_count: data_node_report.restarted_node_count,
        metaserver_restarted_node_count: metaserver_report.restarted_node_count,
        total_restarted_node_count: data_node_report.restarted_node_count
            + metaserver_report.restarted_node_count,
        data_node_per_node_log_store_inspection_count: data_node_report
            .per_node_log_store_inspection_count,
        metaserver_per_node_log_store_inspection_count: metaserver_report
            .per_node_log_store_inspection_count,
        total_per_node_log_store_inspection_count: data_node_report
            .per_node_log_store_inspection_count
            + metaserver_report.per_node_log_store_inspection_count,
        independent_wal_dirs_on_both_planes: data_node_report.independent_wal_dirs
            && metaserver_report.independent_wal_dirs,
        independent_snapshot_dirs_on_both_planes: data_node_report.independent_snapshot_dirs
            && metaserver_report.independent_snapshot_dirs,
        write_or_mutation_proposed_through_process_api_on_both_planes: data_node_report
            .write_proposed_through_process_api
            && metaserver_report.mutation_proposed_through_process_api,
        multi_process_log_store_validated_on_both_planes: data_node_report
            .multi_process_log_store_validated
            && metaserver_report.multi_process_log_store_validated,
        restart_recovery_validated_on_both_planes: data_node_report.restart_recovery_validated
            && metaserver_report.recovered_after_restart,
        read_index_observed_on_both_planes: data_node_report.read_index_responses_observed > 0
            && metaserver_report.read_index_responses_observed > 0,
    }
}

pub fn rustraft_process_readiness_blocker(evidence_field: &str) -> RustRaftProcessReadinessBlocker {
    RustRaftProcessReadinessBlocker {
        blocker: format!(
            "{}_missing",
            evidence_field.replace(['.', '*', '{', '}', '[', ']', ','], "_")
        ),
        evidence_field: evidence_field.to_string(),
        detail: rustraft_process_readiness_field_detail(evidence_field).to_string(),
    }
}

pub fn rustraft_named_readiness_blockers<I, S>(
    blocker: &str,
    evidence_field: &str,
    details: I,
) -> Vec<RustRaftProcessReadinessBlocker>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    details
        .into_iter()
        .map(|detail| RustRaftProcessReadinessBlocker {
            blocker: blocker.to_string(),
            evidence_field: evidence_field.to_string(),
            detail: detail.as_ref().to_string(),
        })
        .collect()
}

pub fn rustraft_data_node_process_rollout_blockers(
    prefix: &str,
    report: Option<&RustRaftDataNodeProcessRolloutReport>,
) -> Vec<RustRaftProcessReadinessBlocker> {
    let Some(report) = report else {
        return vec![rustraft_missing_process_rollout_report_blocker(prefix)];
    };
    let mut blockers = Vec::new();
    append_data_node_process_blockers(&mut blockers, prefix, report);
    blockers
        .into_iter()
        .map(|field| rustraft_process_readiness_blocker(&field))
        .collect()
}

pub fn rustraft_meta_process_rollout_blockers(
    prefix: &str,
    report: Option<&RustRaftMetaProcessRolloutReport>,
) -> Vec<RustRaftProcessReadinessBlocker> {
    let Some(report) = report else {
        return vec![rustraft_missing_process_rollout_report_blocker(prefix)];
    };
    let mut blockers = Vec::new();
    append_meta_process_blockers(&mut blockers, prefix, report);
    blockers
        .into_iter()
        .map(|field| rustraft_process_readiness_blocker(&field))
        .collect()
}

fn rustraft_missing_process_rollout_report_blocker(
    prefix: &str,
) -> RustRaftProcessReadinessBlocker {
    RustRaftProcessReadinessBlocker {
        blocker: format!("{prefix}_missing"),
        evidence_field: prefix.to_string(),
        detail: "No process-harness report was supplied; local fixtures cannot satisfy production Raft readiness.".to_string(),
    }
}

pub fn rustraft_process_readiness_field_detail(evidence_field: &str) -> &'static str {
    match evidence_field {
        "final_raft_readiness.multi_process_data_node_and_metaserver_raft" => {
            "both data-node and metaserver Raft evidence must come from spawned process paths with independent WAL/snapshot dirs, observed process requests, read-index responses, restart recovery, and per-node log-store inspection"
        }
        "final_raft_readiness.failover_on_both_planes" => {
            "data-node and metaserver failover must both be validated"
        }
        "final_raft_readiness.membership_add_remove_under_load" => {
            "membership add/remove must be observed on both planes while the Raft runtime proves leader transfer under active write load"
        }
        "final_raft_readiness.secondary_lag_and_catchup" => {
            "secondary replica lag, read rejection while lagging, catch-up, and read eligibility must be observed on both planes"
        }
        "final_raft_readiness.snapshot_restart_after_compaction" => {
            "snapshot install, restart recovery, and follower rejoin after compacted logs must be observed on both planes"
        }
        field if field.contains(".operational_semantics.") => {
            "RustRaft/BaselineRaft-derived operational semantics evidence is incomplete"
        }
        field if field.ends_with(".ready") => "process rollout report must be ready",
        field if field.ends_with(".spawned_process_count") => {
            "multi-process data-node/metaserver Raft evidence requires at least three spawned nodes"
        }
        field if field.ends_with(".independent_wal_dirs") => {
            "each process must use an independent WAL directory"
        }
        field if field.ends_with(".independent_snapshot_dirs") => {
            "each process must use an independent snapshot directory"
        }
        field if field.ends_with(".observed_process_requests") => {
            "harness must observe real process API traffic rather than in-memory fixture calls"
        }
        field if field.ends_with(".read_index_responses_observed") => {
            "process harness must observe read-index responses"
        }
        field if field.ends_with(".restart_recovery_validated") => {
            "restart recovery must be validated after persisted WAL/snapshot state"
        }
        field if field.ends_with(".nodes[*].restarted_log_store_applied_index") => {
            "every node must restart, pass log-store inspection, and converge applied index to commit index"
        }
        field if field.ends_with(".process_api_observed") => {
            "writes/mutations must be proposed through process APIs"
        }
        field if field.ends_with(".multi_process_log_store_validated") => {
            "independent process log stores must be inspected and validated"
        }
        field if field.ends_with(".failover_validated") => {
            "failover must be validated on this plane"
        }
        field if field.ends_with(".membership_change_validated") => {
            "membership add/remove under load must be validated"
        }
        field if field.ends_with(".follower_lag_validated") => {
            "secondary lag and catch-up must be observed"
        }
        field if field.ends_with(".secondary_read_validated") => {
            "secondary read eligibility after catch-up must be validated"
        }
        field if field.ends_with(".snapshot_install_validated") => {
            "snapshot install/restart after compaction must be validated"
        }
        _ => "RustRaft process-path readiness evidence is incomplete",
    }
}

pub fn rustraft_data_node_strict_process_rollout_validated(
    report: &RustRaftDataNodeProcessRolloutReport,
) -> bool {
    report.ready
        && data_node_process_path_ready(report)
        && report.recovered_after_restart
        && report.restart_recovery_validated
        && report.snapshot_install_validated
        && report.applied_fence_validated
        && report.crash_after_storage_mutation_recovered
        && report.crash_after_wal_persist_recovered
        && report.crash_during_snapshot_install_recovered
        && report.apply_fence_recovered_after_restart
        && report.leader_transfer_validated
        && report.failover_validated
        && report.membership_change_validated
        && report.follower_lag_validated
        && report.secondary_read_validated
        && report.operational_semantics.proves_runtime_semantics()
}

pub fn rustraft_meta_strict_process_rollout_validated(
    report: &RustRaftMetaProcessRolloutReport,
) -> bool {
    report.ready
        && meta_process_path_ready(report)
        && report.read_index_validated
        && report.snapshot_install_validated
        && report.recovered_after_restart
        && report.scheduler_task_replay_validated
        && report.crash_after_meta_mutation_recovered
        && report.crash_after_meta_wal_persist_recovered
        && report.crash_during_meta_snapshot_install_recovered
        && report.meta_apply_fence_recovered_after_restart
        && report.failover_validated
        && report.membership_change_validated
        && report.follower_lag_validated
        && report.secondary_read_validated
        && report.operational_semantics.proves_runtime_semantics()
}

fn data_node_process_path_ready(report: &RustRaftDataNodeProcessRolloutReport) -> bool {
    process_path_proof_is_complete(
        report.spawned_process_count,
        report.independent_wal_dirs,
        report.independent_snapshot_dirs,
        report.observed_process_requests,
        report.read_index_responses_observed,
        report.restarted_node_count,
        report.per_node_log_store_inspection_count,
        &report.nodes,
    ) && report.write_proposed_through_process_api
        && report.multi_process_log_store_validated
        && nodes_restarted_and_log_checked(&report.nodes)
}

fn meta_process_path_ready(report: &RustRaftMetaProcessRolloutReport) -> bool {
    process_path_proof_is_complete(
        report.spawned_process_count,
        report.independent_wal_dirs,
        report.independent_snapshot_dirs,
        report.observed_process_requests,
        report.read_index_responses_observed,
        report.restarted_node_count,
        report.per_node_log_store_inspection_count,
        &report.nodes,
    ) && report.mutation_proposed_through_process_api
        && report.applied_raft_mutations > 0
        && report.multi_process_log_store_validated
        && nodes_restarted_and_log_checked(&report.nodes)
}

fn process_path_proof_is_complete(
    spawned_process_count: u64,
    independent_wal_dirs: bool,
    independent_snapshot_dirs: bool,
    observed_process_requests: u64,
    read_index_responses_observed: u64,
    restarted_node_count: u64,
    per_node_log_store_inspection_count: u64,
    nodes: &[RustRaftProcessNodeEvidence],
) -> bool {
    let expected = nodes.len() as u64;
    spawned_process_count >= 3
        && expected >= 3
        && spawned_process_count >= expected
        && independent_wal_dirs
        && independent_snapshot_dirs
        && observed_process_requests >= expected
        && read_index_responses_observed > 0
        && restarted_node_count >= expected
        && per_node_log_store_inspection_count >= expected
        && unique_non_empty_values(nodes.iter().map(|node| node.addr.as_str()))
        && unique_non_empty_values(nodes.iter().map(|node| node.wal_dir.as_str()))
        && unique_non_empty_values(nodes.iter().map(|node| node.snapshot_dir.as_str()))
}

fn nodes_restarted_and_log_checked(nodes: &[RustRaftProcessNodeEvidence]) -> bool {
    nodes.iter().all(|node| {
        node.restarted && node.log_store_validated && node.applied_index >= node.commit_index
    })
}

fn unique_non_empty_values<'a>(values: impl Iterator<Item = &'a str>) -> bool {
    let mut seen = HashSet::new();
    let mut count = 0usize;
    for value in values {
        if value.is_empty() || !seen.insert(value) {
            return false;
        }
        count += 1;
    }
    count >= 3
}

fn append_data_node_process_blockers(
    blockers: &mut Vec<String>,
    prefix: &str,
    report: &RustRaftDataNodeProcessRolloutReport,
) {
    append_common_process_blockers(
        blockers,
        prefix,
        report.ready,
        report.spawned_process_count,
        report.independent_wal_dirs,
        report.independent_snapshot_dirs,
        report.observed_process_requests,
        report.read_index_responses_observed,
        report.restarted_node_count,
        report.per_node_log_store_inspection_count,
        &report.nodes,
        report.write_proposed_through_process_api,
        report.snapshot_install_validated,
        report.recovered_after_restart && report.restart_recovery_validated,
        report.multi_process_log_store_validated,
        report.failover_validated,
        report.membership_change_validated,
        report.follower_lag_validated,
        report.secondary_read_validated,
        &report.operational_semantics,
    );
}

fn append_meta_process_blockers(
    blockers: &mut Vec<String>,
    prefix: &str,
    report: &RustRaftMetaProcessRolloutReport,
) {
    append_common_process_blockers(
        blockers,
        prefix,
        report.ready,
        report.spawned_process_count,
        report.independent_wal_dirs,
        report.independent_snapshot_dirs,
        report.observed_process_requests,
        report.read_index_responses_observed,
        report.restarted_node_count,
        report.per_node_log_store_inspection_count,
        &report.nodes,
        report.mutation_proposed_through_process_api && report.applied_raft_mutations > 0,
        report.snapshot_install_validated,
        report.recovered_after_restart,
        report.multi_process_log_store_validated,
        report.failover_validated,
        report.membership_change_validated,
        report.follower_lag_validated,
        report.secondary_read_validated,
        &report.operational_semantics,
    );
}

#[allow(clippy::too_many_arguments)]
fn append_common_process_blockers(
    blockers: &mut Vec<String>,
    prefix: &str,
    ready: bool,
    spawned_process_count: u64,
    independent_wal_dirs: bool,
    independent_snapshot_dirs: bool,
    observed_process_requests: u64,
    read_index_responses_observed: u64,
    restarted_node_count: u64,
    per_node_log_store_inspection_count: u64,
    nodes: &[RustRaftProcessNodeEvidence],
    process_api_observed: bool,
    snapshot_install_validated: bool,
    restart_recovery_validated: bool,
    multi_process_log_store_validated: bool,
    failover_validated: bool,
    membership_change_validated: bool,
    follower_lag_validated: bool,
    secondary_read_validated: bool,
    operational_semantics: &RustRaftProcessOperationalSemanticsEvidence,
) {
    push_missing(blockers, ready, &format!("{prefix}.ready"));
    push_missing(
        blockers,
        spawned_process_count >= 3
            && nodes.len() >= 3
            && spawned_process_count as usize >= nodes.len(),
        &format!("{prefix}.spawned_process_count"),
    );
    push_missing(
        blockers,
        independent_wal_dirs,
        &format!("{prefix}.independent_wal_dirs"),
    );
    push_missing(
        blockers,
        independent_snapshot_dirs,
        &format!("{prefix}.independent_snapshot_dirs"),
    );
    push_missing(
        blockers,
        observed_process_requests >= nodes.len() as u64,
        &format!("{prefix}.observed_process_requests"),
    );
    push_missing(
        blockers,
        read_index_responses_observed > 0,
        &format!("{prefix}.read_index_responses_observed"),
    );
    push_missing(
        blockers,
        restarted_node_count >= nodes.len() as u64,
        &format!("{prefix}.restarted_node_count"),
    );
    push_missing(
        blockers,
        per_node_log_store_inspection_count >= nodes.len() as u64,
        &format!("{prefix}.per_node_log_store_inspection_count"),
    );
    push_missing(
        blockers,
        unique_non_empty_values(nodes.iter().map(|node| node.addr.as_str())),
        &format!("{prefix}.nodes[*].addr"),
    );
    push_missing(
        blockers,
        unique_non_empty_values(nodes.iter().map(|node| node.wal_dir.as_str())),
        &format!("{prefix}.nodes[*].wal_dir"),
    );
    push_missing(
        blockers,
        unique_non_empty_values(nodes.iter().map(|node| node.snapshot_dir.as_str())),
        &format!("{prefix}.nodes[*].snapshot_dir"),
    );
    push_missing(
        blockers,
        nodes_restarted_and_log_checked(nodes),
        &format!("{prefix}.nodes[*].restarted_log_store_applied_index"),
    );
    push_missing(
        blockers,
        process_api_observed,
        &format!("{prefix}.process_api_observed"),
    );
    push_missing(
        blockers,
        snapshot_install_validated,
        &format!("{prefix}.snapshot_install_validated"),
    );
    push_missing(
        blockers,
        restart_recovery_validated,
        &format!("{prefix}.restart_recovery_validated"),
    );
    push_missing(
        blockers,
        multi_process_log_store_validated,
        &format!("{prefix}.multi_process_log_store_validated"),
    );
    push_missing(
        blockers,
        failover_validated,
        &format!("{prefix}.failover_validated"),
    );
    push_missing(
        blockers,
        membership_change_validated,
        &format!("{prefix}.membership_change_validated"),
    );
    push_missing(
        blockers,
        follower_lag_validated,
        &format!("{prefix}.follower_lag_validated"),
    );
    push_missing(
        blockers,
        secondary_read_validated,
        &format!("{prefix}.secondary_read_validated"),
    );
    for missing in operational_semantics.missing_requirements() {
        push_missing(
            blockers,
            false,
            &format!("{prefix}.operational_semantics.{missing}"),
        );
    }
}

fn push_missing(blockers: &mut Vec<String>, ready: bool, evidence_field: &str) {
    if !ready {
        blockers.push(evidence_field.to_string());
    }
}

fn require_option<T>(
    id: &str,
    value: Option<&T>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
    action: &str,
) {
    require_bool(
        value.is_some(),
        id,
        satisfied,
        missing,
        blockers,
        actions,
        action,
    );
}

fn require_bool(
    present: bool,
    id: &str,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
    action: &str,
) {
    if present {
        satisfied.push(id.to_string());
    } else {
        missing.push(id.to_string());
        blockers.push(id.to_string());
        actions.push(action.to_string());
    }
}

fn runtime_capability(
    capability: &str,
    source_reference: &str,
    fields: &[(bool, &str)],
) -> RaftCapabilityEvidence {
    rustraft_capability_evidence_from_fields(
        capability,
        source_reference,
        fields.iter().map(|(present, field)| (*present, *field)),
    )
}

fn all_semantics(
    data_node: Option<&RustRaftProcessOperationalSemanticsEvidence>,
    metaserver: Option<&RustRaftProcessOperationalSemanticsEvidence>,
    predicate: impl Fn(&RustRaftProcessOperationalSemanticsEvidence) -> bool,
) -> bool {
    match (data_node, metaserver) {
        (Some(data_node), Some(metaserver)) => predicate(data_node) && predicate(metaserver),
        _ => false,
    }
}

fn push_prometheus_metric(out: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (idx, (key, label_value)) in labels.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            out.push_str(&escape_prometheus_label_value(label_value));
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn escape_prometheus_label_value(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}

fn require_data_node_rollout(
    rollout: Option<&RustRaftDataNodeProcessRolloutReport>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    require_option(
        "data_node:evidence_present",
        rollout,
        satisfied,
        missing,
        blockers,
        actions,
        "attach data-node process rollout evidence",
    );
    let Some(rollout) = rollout else {
        return;
    };
    for (present, id, action) in [
        (
            rollout.ready,
            "data_node:ready",
            "make data-node rollout ready",
        ),
        (
            rollout.blockers.is_empty(),
            "data_node:no_blockers",
            "clear data-node rollout blockers",
        ),
        (
            !rollout.nodes.is_empty()
                && rollout.spawned_process_count as usize >= rollout.nodes.len(),
            "data_node:processes_spawned",
            "spawn and observe all data-node RustRaft processes",
        ),
        (
            !rollout.voters.is_empty(),
            "data_node:voters_present",
            "run data-node RustRaft with voter membership",
        ),
        (
            rollout.independent_wal_dirs,
            "data_node:independent_wal_dirs",
            "use independent WAL dirs per data-node process",
        ),
        (
            rollout.independent_snapshot_dirs,
            "data_node:independent_snapshot_dirs",
            "use independent snapshot dirs per data-node process",
        ),
        (
            rollout.write_proposed_through_process_api,
            "data_node:process_write_path",
            "prove writes enter through the process API",
        ),
        (
            rollout.read_index_responses_observed > 0,
            "data_node:read_index",
            "observe data-node read-index responses",
        ),
        (
            rollout.leader_transfer_validated,
            "data_node:leader_transfer",
            "validate data-node leader transfer",
        ),
        (
            rollout.failover_validated,
            "data_node:failover",
            "validate data-node failover",
        ),
        (
            rollout.membership_change_validated,
            "data_node:membership_change",
            "validate data-node membership add/promote/remove",
        ),
        (
            rollout.follower_lag_validated,
            "data_node:follower_lag",
            "validate data-node follower lag handling",
        ),
        (
            rollout.secondary_read_validated,
            "data_node:secondary_read",
            "validate data-node secondary read eligibility",
        ),
        (
            rollout.recovered_after_restart && rollout.restart_recovery_validated,
            "data_node:restart_recovery",
            "validate data-node restart recovery",
        ),
        (
            rollout.snapshot_install_validated,
            "data_node:snapshot_install",
            "validate data-node snapshot install",
        ),
        (
            rollout.applied_fence_validated,
            "data_node:apply_fence",
            "validate data-node apply fence",
        ),
        (
            rollout.multi_process_log_store_validated,
            "data_node:multi_process_log_store",
            "validate independent multi-process log stores",
        ),
        (
            rollout.operational_semantics.proves_runtime_semantics(),
            "data_node:operational_semantics",
            "prove data-node runtime semantics, not only API presence",
        ),
    ] {
        require_bool(present, id, satisfied, missing, blockers, actions, action);
    }
    for missing_requirement in rollout.operational_semantics.missing_requirements() {
        require_bool(
            false,
            &format!("data_node:semantics:{missing_requirement}"),
            satisfied,
            missing,
            blockers,
            actions,
            "complete data-node operational semantics evidence",
        );
    }
    for blocker in &rollout.blockers {
        require_bool(
            false,
            &format!("data_node:blocker:{blocker}"),
            satisfied,
            missing,
            blockers,
            actions,
            "clear data-node rollout blocker",
        );
    }
}

fn require_meta_rollout(
    rollout: Option<&RustRaftMetaProcessRolloutReport>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    require_option(
        "metaserver:evidence_present",
        rollout,
        satisfied,
        missing,
        blockers,
        actions,
        "attach metaserver process rollout evidence",
    );
    let Some(rollout) = rollout else {
        return;
    };
    for (present, id, action) in [
        (
            rollout.ready,
            "metaserver:ready",
            "make metaserver rollout ready",
        ),
        (
            rollout.blockers.is_empty(),
            "metaserver:no_blockers",
            "clear metaserver rollout blockers",
        ),
        (
            !rollout.nodes.is_empty()
                && rollout.spawned_process_count as usize >= rollout.nodes.len(),
            "metaserver:processes_spawned",
            "spawn and observe all metaserver RustRaft processes",
        ),
        (
            !rollout.voters.is_empty(),
            "metaserver:voters_present",
            "run metaserver RustRaft with voter membership",
        ),
        (
            rollout.independent_wal_dirs,
            "metaserver:independent_wal_dirs",
            "use independent WAL dirs per metaserver process",
        ),
        (
            rollout.independent_snapshot_dirs,
            "metaserver:independent_snapshot_dirs",
            "use independent snapshot dirs per metaserver process",
        ),
        (
            rollout.mutation_proposed_through_process_api,
            "metaserver:process_mutation_path",
            "prove metaserver mutations enter through the process API",
        ),
        (
            rollout.read_index_responses_observed > 0 && rollout.read_index_validated,
            "metaserver:read_index",
            "validate metaserver read-index responses",
        ),
        (
            rollout.applied_raft_mutations > 0,
            "metaserver:applied_mutations",
            "observe applied metaserver RustRaft mutations",
        ),
        (
            rollout.scheduler_task_replay_validated,
            "metaserver:scheduler_replay",
            "validate scheduler task replay from RustRaft log",
        ),
        (
            rollout.data_node_membership_results_ready
                && rollout.data_node_membership_workflow_report_attached
                && rollout.data_node_raft_group_results_observed,
            "metaserver:data_node_membership_workflow",
            "validate data-node membership workflow through metaserver RustRaft",
        ),
        (
            rollout.failover_validated,
            "metaserver:failover",
            "validate metaserver failover",
        ),
        (
            rollout.membership_change_validated,
            "metaserver:membership_change",
            "validate metaserver membership change",
        ),
        (
            rollout.follower_lag_validated,
            "metaserver:follower_lag",
            "validate metaserver follower lag handling",
        ),
        (
            rollout.secondary_read_validated,
            "metaserver:secondary_read",
            "validate metaserver secondary read eligibility",
        ),
        (
            rollout.recovered_after_restart,
            "metaserver:restart_recovery",
            "validate metaserver restart recovery",
        ),
        (
            rollout.snapshot_install_validated,
            "metaserver:snapshot_install",
            "validate metaserver snapshot install",
        ),
        (
            rollout.multi_process_log_store_validated,
            "metaserver:multi_process_log_store",
            "validate independent metaserver log stores",
        ),
        (
            rollout.operational_semantics.proves_runtime_semantics(),
            "metaserver:operational_semantics",
            "prove metaserver runtime semantics, not only API presence",
        ),
    ] {
        require_bool(present, id, satisfied, missing, blockers, actions, action);
    }
    for missing_requirement in rollout.operational_semantics.missing_requirements() {
        require_bool(
            false,
            &format!("metaserver:semantics:{missing_requirement}"),
            satisfied,
            missing,
            blockers,
            actions,
            "complete metaserver operational semantics evidence",
        );
    }
    for blocker in &rollout.blockers {
        require_bool(
            false,
            &format!("metaserver:blocker:{blocker}"),
            satisfied,
            missing,
            blockers,
            actions,
            "clear metaserver rollout blocker",
        );
    }
}

fn require_membership_transitions(
    transitions: &[RustRaftMembershipTransitionEvidence],
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    let report = rustraft_membership_readiness_report(transitions);
    if report.ready {
        satisfied.push("membership:all_required_transitions".to_string());
    } else {
        missing.extend(report.missing.iter().map(|id| format!("membership:{id}")));
        blockers.extend(report.missing.iter().map(|id| format!("membership:{id}")));
        actions.push(
            "run metaserver and data-node RustRaft failover, scale-up, and scale-down transitions"
                .to_string(),
        );
    }
    for id in report.satisfied {
        satisfied.push(format!("membership:{id}"));
    }
}
