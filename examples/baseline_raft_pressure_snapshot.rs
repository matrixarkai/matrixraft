// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_release_pressure_snapshot_json, matrixraft_write_release_pressure_snapshot_atomic,
    LatencyBucket, LatencyHistogram, LatencyMetrics, LatencyOptimizationThresholds, MemoryMetrics,
    MemoryOptimizationThresholds, NodeRuntimeTimerThresholds, ReadBacklogMetrics,
    ReadBacklogThresholds, ReleasePressureSnapshot,
};
use std::path::PathBuf;

fn main() {
    let mut out_path = std::env::var("RUSTRAFT_BENCHMARK_PRESSURE_SNAPSHOT_OUT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --out");
                    usage();
                    std::process::exit(2);
                };
                out_path = Some(PathBuf::from(value));
            }
            "-h" | "--help" => {
                usage();
                return;
            }
            other => {
                eprintln!("unknown argument: {other}");
                usage();
                std::process::exit(2);
            }
        }
    }

    let snapshot = ReleasePressureSnapshot::complete(
        memory_metrics_from_env(),
        memory_thresholds_from_env(),
        latency_metrics_from_env(),
        latency_thresholds_from_env(),
        read_backlog_metrics_from_env(),
        read_backlog_thresholds_from_env(),
        runtime_timer_status_from_env(),
        timer_thresholds_from_env(),
    );
    if let Some(out_path) = out_path {
        if let Err(error) = matrixraft_write_release_pressure_snapshot_atomic(&out_path, &snapshot)
        {
            eprintln!(
                "BaselineRaft pressure snapshot failed to write {}: {error}",
                out_path.display()
            );
            std::process::exit(2);
        }
    } else {
        println!("{}", matrixraft_release_pressure_snapshot_json(&snapshot));
    }
}

fn usage() {
    eprintln!(
        "Usage: baseline_raft_pressure_snapshot [--out PATH]\nReads RUSTRAFT_BENCHMARK_* pressure env vars and writes a replayable release pressure snapshot."
    );
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
    let mut status = matrixraft::matrixraft_release_benchmark_runtime_timer_status();
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

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}
