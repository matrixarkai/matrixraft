// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::benchmark::{
    matrixraft_assert_production_baseline_raft_parity,
    matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer,
    matrixraft_baseline_raft_benchmark_failure_summary, matrixraft_find_baseline_raft_harness,
    matrixraft_release_benchmark_runtime_timer_status,
    matrixraft_run_baseline_raft_parity_benchmark,
    matrixraft_scale_optimization_inputs_from_benchmark_report,
    matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer,
    matrixraft_validate_production_baseline_raft_benchmark_options, BenchmarkFailureSummary,
    BenchmarkOptions, BenchmarkScaleOptimizationInputs, ExternalBaselineRaftRunner,
    RuntimeBenchmarkRunner,
};
use matrixraft::{
    matrixraft_read_release_pressure_snapshot, LatencyBucket, LatencyHistogram, LatencyMetrics,
    LatencyOptimizationThresholds, MemoryMetrics, MemoryOptimizationThresholds,
    NodeRuntimeTimerThresholds, ProductionReadinessInput, ReadBacklogMetrics,
    ReadBacklogThresholds, ReleasePressureSnapshot, RuntimePressureAdmissionPolicy,
};
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

fn main() {
    let mut options = BenchmarkOptions::default();
    if let Ok(node_count) = std::env::var("RUSTRAFT_BENCHMARK_NODE_COUNT") {
        options.node_count = node_count.parse().unwrap_or_else(|error| {
            eprintln!("invalid RUSTRAFT_BENCHMARK_NODE_COUNT={node_count}: {error}");
            std::process::exit(2);
        });
    }
    if let Ok(iterations) = std::env::var("RUSTRAFT_BENCHMARK_ITERATIONS") {
        options.iterations_per_workload = iterations.parse().unwrap_or_else(|error| {
            eprintln!("invalid RUSTRAFT_BENCHMARK_ITERATIONS={iterations}: {error}");
            std::process::exit(2);
        });
    }
    if let Ok(batch_size) = std::env::var("RUSTRAFT_BENCHMARK_BATCH_SIZE") {
        options.batch_size = batch_size.parse().unwrap_or_else(|error| {
            eprintln!("invalid RUSTRAFT_BENCHMARK_BATCH_SIZE={batch_size}: {error}");
            std::process::exit(2);
        });
    }
    if let Ok(payload_size_bytes) = std::env::var("RUSTRAFT_BENCHMARK_PAYLOAD_SIZE_BYTES") {
        options.payload_size_bytes = payload_size_bytes.parse().unwrap_or_else(|error| {
            eprintln!(
                "invalid RUSTRAFT_BENCHMARK_PAYLOAD_SIZE_BYTES={payload_size_bytes}: {error}"
            );
            std::process::exit(2);
        });
    }
    if let Ok(pass_tolerance_percent) = std::env::var("RUSTRAFT_BENCHMARK_PASS_TOLERANCE_PERCENT") {
        options.pass_tolerance_percent = pass_tolerance_percent.parse().unwrap_or_else(|error| {
            eprintln!(
                "invalid RUSTRAFT_BENCHMARK_PASS_TOLERANCE_PERCENT={pass_tolerance_percent}: {error}"
            );
            std::process::exit(2);
        });
    }
    if let Err(blockers) = matrixraft_validate_production_baseline_raft_benchmark_options(&options)
    {
        eprintln!("BaselineRaft parity benchmark invalid production options: {blockers}");
        std::process::exit(2);
    }
    let build_profile =
        std::env::var("RUSTRAFT_BENCHMARK_PROFILE").unwrap_or_else(|_| "release".to_string());
    let baseline_raft_root = std::env::var("BASELINE_RAFT_ROOT").ok().map(PathBuf::from);
    let baseline_raft_bin = std::env::var("BASELINE_RAFT_BENCHMARK_BIN")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            baseline_raft_root
                .as_ref()
                .and_then(|root| matrixraft_find_baseline_raft_harness(root).ok())
        });
    let Some(baseline_raft_bin) = baseline_raft_bin else {
        eprintln!(
            "BaselineRaft parity benchmark failed: benchmark:real_baseline_raft_missing; set BASELINE_RAFT_ROOT or BASELINE_RAFT_BENCHMARK_BIN"
        );
        std::process::exit(2);
    };
    let mut baseline_raft = match ExternalBaselineRaftRunner::new(
        baseline_raft_bin,
        baseline_raft_root,
        &build_profile,
    ) {
        Ok(runner) => runner,
        Err(error) => {
            eprintln!("BaselineRaft parity benchmark failed: {error}");
            std::process::exit(2);
        }
    };
    let mut rustraft = RuntimeBenchmarkRunner::new(build_profile);
    let report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let scale_inputs = matrixraft_scale_optimization_inputs_from_benchmark_report(&report);
    if let Some(readiness_artifact_out) = non_empty_env("RUSTRAFT_BENCHMARK_READINESS_ARTIFACT_OUT")
    {
        let readiness_input_path =
            non_empty_env("RUSTRAFT_BENCHMARK_READINESS_INPUT").unwrap_or_else(|| {
                eprintln!(
                    "RUSTRAFT_BENCHMARK_READINESS_ARTIFACT_OUT requires RUSTRAFT_BENCHMARK_READINESS_INPUT"
                );
                std::process::exit(2);
            });
        let readiness_input = read_json_artifact::<ProductionReadinessInput>(
            Path::new(&readiness_input_path),
            "production_readiness_input",
        );
        let readiness_labels = parse_prometheus_labels_from_env(
            "RUSTRAFT_BENCHMARK_READINESS_LABELS",
            &[("service", "rustraft-ci"), ("workload", "release-scale")],
        );
        let readiness_label_refs = readiness_labels
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let pressure_snapshot = release_pressure_snapshot_from_env();
        let memory_metrics = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.memory_metrics.clone())
            .unwrap_or_else(memory_metrics_from_env);
        let memory_thresholds = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.memory_thresholds.clone())
            .unwrap_or_else(memory_thresholds_from_env);
        let latency_metrics = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.latency_metrics.clone())
            .unwrap_or_else(latency_metrics_from_env);
        let latency_thresholds = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.latency_thresholds.clone())
            .unwrap_or_else(latency_thresholds_from_env);
        let read_backlog_metrics = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.read_backlog_metrics.clone())
            .unwrap_or_else(read_backlog_metrics_from_env);
        let read_backlog_thresholds = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.read_backlog_thresholds.clone())
            .unwrap_or_else(read_backlog_thresholds_from_env);
        let timer_status = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.timer_status.clone())
            .unwrap_or_else(runtime_timer_status_from_env);
        let timer_thresholds = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.timer_thresholds.clone())
            .unwrap_or_else(timer_thresholds_from_env);
        let readiness_artifact =
            matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &readiness_input,
            &report,
            &summary,
            &memory_metrics,
            &memory_thresholds,
            &latency_metrics,
            &latency_thresholds,
            &[],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &readiness_label_refs,
        )
        .unwrap_or_else(|error| {
            eprintln!("BaselineRaft parity benchmark failed to build readiness artifact: {error}");
            std::process::exit(1);
        });
        matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
            &readiness_artifact,
            &readiness_input,
            &report,
            &summary,
            &memory_metrics,
            &memory_thresholds,
            &latency_metrics,
            &latency_thresholds,
            &[],
            &read_backlog_metrics,
            &read_backlog_thresholds,
            &timer_status,
            &timer_thresholds,
            &RuntimePressureAdmissionPolicy::fail_closed(),
            &readiness_label_refs,
        )
        .unwrap_or_else(|error| {
            eprintln!(
                "BaselineRaft parity benchmark failed readiness artifact validation: {error}"
            );
            std::process::exit(1);
        });
        if let Err(error) =
            write_json_artifact_atomic(Path::new(&readiness_artifact_out), &readiness_artifact)
        {
            eprintln!(
                "BaselineRaft parity benchmark failed to write readiness artifact {readiness_artifact_out}: {error}"
            );
            std::process::exit(2);
        }
    }

    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if let Ok(summary_out) = std::env::var("RUSTRAFT_BENCHMARK_SUMMARY_OUT") {
        if let Err(error) = write_summary_artifact_atomic(Path::new(&summary_out), &summary) {
            eprintln!(
                "BaselineRaft parity benchmark failed to write summary {summary_out}: {error}"
            );
            std::process::exit(2);
        }
    }
    if let Ok(scale_out) = std::env::var("RUSTRAFT_BENCHMARK_SCALE_OUT") {
        if let Err(error) = write_scale_artifact_atomic(Path::new(&scale_out), &scale_inputs) {
            eprintln!(
                "BaselineRaft parity benchmark failed to write scale inputs {scale_out}: {error}"
            );
            std::process::exit(2);
        }
    }
    if let Err(blockers) = matrixraft_assert_production_baseline_raft_parity(&report) {
        eprintln!("BaselineRaft parity benchmark failed: {blockers}");
        eprintln!(
            "BaselineRaft parity benchmark summary: {}",
            serde_json::to_string(&summary).unwrap()
        );
        std::process::exit(1);
    }
}

fn write_summary_artifact_atomic(
    summary_out: &Path,
    summary: &BenchmarkFailureSummary,
) -> std::io::Result<()> {
    write_json_artifact_atomic(summary_out, summary)
}

fn write_scale_artifact_atomic(
    scale_out: &Path,
    scale_inputs: &BenchmarkScaleOptimizationInputs,
) -> std::io::Result<()> {
    write_json_artifact_atomic(scale_out, scale_inputs)
}

fn write_json_artifact_atomic<T: Serialize>(out: &Path, artifact: &T) -> std::io::Result<()> {
    if let Some(parent) = out.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let parent = out.parent().unwrap_or_else(|| Path::new("."));
    let file_name = out
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact.json");
    let tmp_path = parent.join(format!(".{file_name}.{}.tmp", std::process::id()));
    let mut tmp_file = File::create(&tmp_path)?;
    tmp_file.write_all(serde_json::to_string_pretty(artifact).unwrap().as_bytes())?;
    tmp_file.sync_all()?;
    drop(tmp_file);
    if let Err(error) = fs::rename(&tmp_path, out) {
        let _ = fs::remove_file(&tmp_path);
        return Err(error);
    }
    Ok(())
}

fn read_json_artifact<T: serde::de::DeserializeOwned>(path: &Path, label: &str) -> T {
    let bytes = fs::read(path).unwrap_or_else(|error| {
        eprintln!("failed to read {label} {}: {error}", path.display());
        std::process::exit(2);
    });
    serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        eprintln!("failed to parse {label} {}: {error}", path.display());
        std::process::exit(2);
    })
}

fn parse_prometheus_labels_from_env(
    name: &str,
    default_labels: &[(&str, &str)],
) -> Vec<(String, String)> {
    let Some(raw) = non_empty_env(name) else {
        return default_labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
    };
    raw.split(',')
        .map(|entry| {
            let Some((key, value)) = entry.split_once('=') else {
                eprintln!("invalid {name} entry {entry:?}; expected comma-separated key=value");
                std::process::exit(2);
            };
            let key = key.trim();
            if key.is_empty() {
                eprintln!("invalid {name} entry {entry:?}; label key is empty");
                std::process::exit(2);
            }
            (key.to_string(), value.trim().to_string())
        })
        .collect()
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn release_pressure_snapshot_from_env() -> Option<ReleasePressureSnapshot> {
    non_empty_env("RUSTRAFT_BENCHMARK_PRESSURE_SNAPSHOT").map(|path| {
        matrixraft_read_release_pressure_snapshot(Path::new(&path)).unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(2);
        })
    })
}

fn memory_metrics_from_env() -> MemoryMetrics {
    let mut metrics = MemoryMetrics::zero();
    metrics.process_resident_memory_bytes = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_PROCESS_RESIDENT_MEMORY_BYTES",
        metrics.process_resident_memory_bytes,
    );
    metrics.heap_allocated_bytes = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_HEAP_ALLOCATED_BYTES",
        metrics.heap_allocated_bytes,
    );
    metrics.log_cache_bytes = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_LOG_CACHE_BYTES",
        metrics.log_cache_bytes,
    );
    metrics.snapshot_buffer_bytes = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_SNAPSHOT_BUFFER_BYTES",
        metrics.snapshot_buffer_bytes,
    );
    metrics.replication_buffer_bytes = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_REPLICATION_BUFFER_BYTES",
        metrics.replication_buffer_bytes,
    );
    metrics
}

fn memory_thresholds_from_env() -> MemoryOptimizationThresholds {
    let mut thresholds = MemoryOptimizationThresholds::default();
    thresholds.process_resident_warning_bytes = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_PROCESS_RESIDENT_WARNING_BYTES",
        thresholds.process_resident_warning_bytes,
    );
    thresholds.heap_allocated_warning_bytes = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_HEAP_ALLOCATED_WARNING_BYTES",
        thresholds.heap_allocated_warning_bytes,
    );
    thresholds.log_cache_warning_bytes = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_LOG_CACHE_WARNING_BYTES",
        thresholds.log_cache_warning_bytes,
    );
    thresholds.snapshot_buffer_warning_bytes = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_SNAPSHOT_BUFFER_WARNING_BYTES",
        thresholds.snapshot_buffer_warning_bytes,
    );
    thresholds.replication_buffer_warning_bytes = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_REPLICATION_BUFFER_WARNING_BYTES",
        thresholds.replication_buffer_warning_bytes,
    );
    thresholds
}

fn latency_metrics_from_env() -> LatencyMetrics {
    let mut metrics = LatencyMetrics::zero();
    metrics.append_latency_ms = latency_histogram_from_p99_env(
        "RUSTRAFT_BENCHMARK_APPEND_P99_MS",
        metrics.append_latency_ms,
    );
    metrics.vote_latency_ms =
        latency_histogram_from_p99_env("RUSTRAFT_BENCHMARK_VOTE_P99_MS", metrics.vote_latency_ms);
    metrics.pre_vote_latency_ms = latency_histogram_from_p99_env(
        "RUSTRAFT_BENCHMARK_PRE_VOTE_P99_MS",
        metrics.pre_vote_latency_ms,
    );
    metrics.read_index_latency_ms = latency_histogram_from_p99_env(
        "RUSTRAFT_BENCHMARK_READ_INDEX_P99_MS",
        metrics.read_index_latency_ms,
    );
    metrics.snapshot_install_latency_ms = latency_histogram_from_p99_env(
        "RUSTRAFT_BENCHMARK_SNAPSHOT_INSTALL_P99_MS",
        metrics.snapshot_install_latency_ms,
    );
    metrics
}

fn latency_thresholds_from_env() -> LatencyOptimizationThresholds {
    let mut thresholds = LatencyOptimizationThresholds::default();
    thresholds.append_p99_warning_ms = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_APPEND_P99_WARNING_MS",
        thresholds.append_p99_warning_ms,
    );
    thresholds.vote_p99_warning_ms = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_VOTE_P99_WARNING_MS",
        thresholds.vote_p99_warning_ms,
    );
    thresholds.pre_vote_p99_warning_ms = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_PRE_VOTE_P99_WARNING_MS",
        thresholds.pre_vote_p99_warning_ms,
    );
    thresholds.read_index_p99_warning_ms = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_READ_INDEX_P99_WARNING_MS",
        thresholds.read_index_p99_warning_ms,
    );
    thresholds.snapshot_install_p99_warning_ms = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_SNAPSHOT_INSTALL_P99_WARNING_MS",
        thresholds.snapshot_install_p99_warning_ms,
    );
    thresholds
}

fn latency_histogram_from_p99_env(name: &str, default: LatencyHistogram) -> LatencyHistogram {
    let Some(p99_ms) = non_empty_env(name).map(|value| {
        value.parse::<u64>().unwrap_or_else(|error| {
            eprintln!("invalid {name}={value}: {error}");
            std::process::exit(2);
        })
    }) else {
        return default;
    };
    LatencyHistogram {
        buckets: vec![LatencyBucket {
            le_ms: p99_ms.to_string(),
            count: 100,
        }],
        sum_ms: p99_ms.saturating_mul(100),
        count: 100,
    }
}

fn read_backlog_metrics_from_env() -> ReadBacklogMetrics {
    let mut metrics = ReadBacklogMetrics::zero();
    metrics.pending_read_index_requests = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_PENDING_READ_INDEX_REQUESTS",
        metrics.pending_read_index_requests,
    );
    metrics.pending_bounded_stale_reads = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_PENDING_BOUNDED_STALE_READS",
        metrics.pending_bounded_stale_reads,
    );
    metrics
}

fn read_backlog_thresholds_from_env() -> ReadBacklogThresholds {
    let mut thresholds = ReadBacklogThresholds::default();
    thresholds.pending_read_index_warning = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_PENDING_READ_INDEX_WARNING",
        thresholds.pending_read_index_warning,
    );
    thresholds.pending_bounded_stale_read_warning = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_PENDING_BOUNDED_STALE_READ_WARNING",
        thresholds.pending_bounded_stale_read_warning,
    );
    thresholds
}

fn runtime_timer_status_from_env() -> matrixraft::RuntimeTimerStatus {
    let mut status = matrixraft_release_benchmark_runtime_timer_status();
    status.pending_ticks = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_RUNTIME_TIMER_PENDING_TICKS",
        status.pending_ticks,
    );
    status.max_pending_ticks = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_RUNTIME_TIMER_MAX_PENDING_TICKS",
        status.max_pending_ticks,
    );
    status.accepted_ticks = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_RUNTIME_TIMER_ACCEPTED_TICKS",
        status.accepted_ticks,
    );
    status.rejected_ticks = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_RUNTIME_TIMER_REJECTED_TICKS",
        status.rejected_ticks,
    );
    status.completed_ticks = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_RUNTIME_TIMER_COMPLETED_TICKS",
        status.completed_ticks,
    );
    if let Some(reason) = non_empty_env("RUSTRAFT_BENCHMARK_RUNTIME_TIMER_LAST_ADMISSION_REASON") {
        status.last_tick_admission_reason = reason;
    }
    status
}

fn timer_thresholds_from_env() -> NodeRuntimeTimerThresholds {
    let mut thresholds = NodeRuntimeTimerThresholds::default();
    thresholds.utilization_warning_percent = parse_u64_env_or_default(
        "RUSTRAFT_BENCHMARK_RUNTIME_TIMER_UTILIZATION_WARNING_PERCENT",
        thresholds.utilization_warning_percent,
    );
    thresholds
}

fn parse_u64_env_or_default(name: &str, default: u64) -> u64 {
    non_empty_env(name)
        .map(|value| {
            value.parse::<u64>().unwrap_or_else(|error| {
                eprintln!("invalid {name}={value}: {error}");
                std::process::exit(2);
            })
        })
        .unwrap_or(default)
}
