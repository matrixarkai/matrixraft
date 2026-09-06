// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::benchmark::{
    matrixraft_assert_production_baseline_raft_artifacts,
    matrixraft_release_benchmark_runtime_timer_status,
    matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer,
    BenchmarkFailureSummary, BenchmarkReport, BenchmarkRuntimePressureReadinessArtifact,
};
use matrixraft::{
    matrixraft_read_release_pressure_snapshot,
    matrixraft_validate_benchmark_scale_optimization_inputs, BenchmarkScaleOptimizationInputs,
    LatencyBucket, LatencyHistogram, LatencyMetrics, LatencyOptimizationThresholds, MemoryMetrics,
    MemoryOptimizationThresholds, NodeRuntimeTimerThresholds, ProductionReadinessInput,
    ReadBacklogMetrics, ReadBacklogThresholds, ReleasePressureSnapshot,
    RuntimePressureAdmissionPolicy,
};
use std::{
    env, fs,
    path::PathBuf,
    time::{Duration, SystemTime},
};

const DEFAULT_MAX_ARTIFACT_AGE_SECONDS: u64 = 24 * 60 * 60;

fn main() {
    let mut report_path = None;
    let mut summary_path = None;
    let mut scale_path = None;
    let mut readiness_input_path = None;
    let mut readiness_artifact_path = None;
    let mut readiness_labels = None;
    let mut pressure_snapshot_path = None;
    let mut pending_read_index_requests = None;
    let mut pending_bounded_stale_reads = None;
    let mut pending_read_index_warning = None;
    let mut pending_bounded_stale_read_warning = None;
    let mut runtime_timer_pending_ticks = None;
    let mut runtime_timer_max_pending_ticks = None;
    let mut runtime_timer_accepted_ticks = None;
    let mut runtime_timer_rejected_ticks = None;
    let mut runtime_timer_completed_ticks = None;
    let mut runtime_timer_last_admission_reason = None;
    let mut runtime_timer_utilization_warning_percent = None;
    let mut max_age_seconds = env::var("RUSTRAFT_BENCHMARK_MAX_ARTIFACT_AGE_SECONDS")
        .ok()
        .map(|value| parse_max_age_seconds(&value))
        .unwrap_or(DEFAULT_MAX_ARTIFACT_AGE_SECONDS);
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--report" => report_path = args.next().map(PathBuf::from),
            "--summary" => summary_path = args.next().map(PathBuf::from),
            "--scale" => scale_path = args.next().map(PathBuf::from),
            "--readiness-input" => readiness_input_path = args.next().map(PathBuf::from),
            "--readiness-artifact" => readiness_artifact_path = args.next().map(PathBuf::from),
            "--readiness-labels" => readiness_labels = args.next(),
            "--pressure-snapshot" => pressure_snapshot_path = args.next().map(PathBuf::from),
            "--pending-read-index-requests" => {
                pending_read_index_requests =
                    Some(parse_required_u64_arg(&arg, args.next().as_deref()));
            }
            "--pending-bounded-stale-reads" => {
                pending_bounded_stale_reads =
                    Some(parse_required_u64_arg(&arg, args.next().as_deref()));
            }
            "--pending-read-index-warning" => {
                pending_read_index_warning =
                    Some(parse_required_u64_arg(&arg, args.next().as_deref()));
            }
            "--pending-bounded-stale-read-warning" => {
                pending_bounded_stale_read_warning =
                    Some(parse_required_u64_arg(&arg, args.next().as_deref()));
            }
            "--runtime-timer-pending-ticks" => {
                runtime_timer_pending_ticks =
                    Some(parse_required_u64_arg(&arg, args.next().as_deref()));
            }
            "--runtime-timer-max-pending-ticks" => {
                runtime_timer_max_pending_ticks =
                    Some(parse_required_u64_arg(&arg, args.next().as_deref()));
            }
            "--runtime-timer-accepted-ticks" => {
                runtime_timer_accepted_ticks =
                    Some(parse_required_u64_arg(&arg, args.next().as_deref()));
            }
            "--runtime-timer-rejected-ticks" => {
                runtime_timer_rejected_ticks =
                    Some(parse_required_u64_arg(&arg, args.next().as_deref()));
            }
            "--runtime-timer-completed-ticks" => {
                runtime_timer_completed_ticks =
                    Some(parse_required_u64_arg(&arg, args.next().as_deref()));
            }
            "--runtime-timer-last-admission-reason" => {
                runtime_timer_last_admission_reason = Some(args.next().unwrap_or_else(|| {
                    eprintln!("missing value for {arg}");
                    usage();
                    std::process::exit(2);
                }));
            }
            "--runtime-timer-utilization-warning-percent" => {
                runtime_timer_utilization_warning_percent =
                    Some(parse_required_u64_arg(&arg, args.next().as_deref()));
            }
            "--max-age-seconds" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --max-age-seconds");
                    usage();
                    std::process::exit(2);
                };
                max_age_seconds = parse_max_age_seconds(&value);
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

    let Some(report_path) = report_path else {
        eprintln!("missing required --report PATH");
        usage();
        std::process::exit(2);
    };
    let Some(summary_path) = summary_path else {
        eprintln!("missing required --summary PATH");
        usage();
        std::process::exit(2);
    };
    if report_path == summary_path {
        eprintln!(
            "benchmark:artifact_path_collision:report_summary:{}",
            report_path.display()
        );
        std::process::exit(2);
    }
    if let Some(scale_path) = &scale_path {
        if *scale_path == report_path {
            artifact_path_collision("report_scale", scale_path);
        }
        if *scale_path == summary_path {
            artifact_path_collision("summary_scale", scale_path);
        }
    }
    if readiness_input_path.is_some() != readiness_artifact_path.is_some() {
        eprintln!("benchmark:readiness_verification_requires_input_and_artifact");
        std::process::exit(2);
    }
    if readiness_labels.is_some() && readiness_artifact_path.is_none() {
        eprintln!("benchmark:readiness_labels_require_artifact");
        std::process::exit(2);
    }
    if let Some(readiness_input_path) = &readiness_input_path {
        if scale_path.as_ref() == Some(readiness_input_path) {
            artifact_path_collision("scale_readiness_input", readiness_input_path);
        }
        if *readiness_input_path == report_path {
            artifact_path_collision("report_readiness_input", readiness_input_path);
        }
        if *readiness_input_path == summary_path {
            artifact_path_collision("summary_readiness_input", readiness_input_path);
        }
    }
    if let Some(readiness_artifact_path) = &readiness_artifact_path {
        if scale_path.as_ref() == Some(readiness_artifact_path) {
            artifact_path_collision("scale_readiness", readiness_artifact_path);
        }
        if *readiness_artifact_path == report_path {
            artifact_path_collision("report_readiness", readiness_artifact_path);
        }
        if *readiness_artifact_path == summary_path {
            artifact_path_collision("summary_readiness", readiness_artifact_path);
        }
        if readiness_input_path.as_ref() == Some(readiness_artifact_path) {
            artifact_path_collision("readiness_input_artifact", readiness_artifact_path);
        }
    }
    if let Some(pressure_snapshot_path) = &pressure_snapshot_path {
        if *pressure_snapshot_path == report_path {
            artifact_path_collision("pressure_snapshot_report", pressure_snapshot_path);
        }
        if *pressure_snapshot_path == summary_path {
            artifact_path_collision("pressure_snapshot_summary", pressure_snapshot_path);
        }
        if scale_path.as_ref() == Some(pressure_snapshot_path) {
            artifact_path_collision("pressure_snapshot_scale", pressure_snapshot_path);
        }
        if readiness_input_path.as_ref() == Some(pressure_snapshot_path) {
            artifact_path_collision("pressure_snapshot_readiness_input", pressure_snapshot_path);
        }
        if readiness_artifact_path.as_ref() == Some(pressure_snapshot_path) {
            artifact_path_collision(
                "pressure_snapshot_readiness_artifact",
                pressure_snapshot_path,
            );
        }
    }

    assert_fresh_artifact(&report_path, "report", max_age_seconds);
    assert_fresh_artifact(&summary_path, "summary", max_age_seconds);
    if let Some(scale_path) = &scale_path {
        assert_fresh_artifact(scale_path, "scale", max_age_seconds);
    }
    if let Some(readiness_input_path) = &readiness_input_path {
        assert_fresh_artifact(readiness_input_path, "readiness_input", max_age_seconds);
    }
    if let Some(readiness_artifact_path) = &readiness_artifact_path {
        assert_fresh_artifact(readiness_artifact_path, "readiness", max_age_seconds);
    }
    if let Some(pressure_snapshot_path) = &pressure_snapshot_path {
        assert_fresh_artifact(pressure_snapshot_path, "pressure_snapshot", max_age_seconds);
    }
    if pressure_snapshot_path.is_some() && readiness_artifact_path.is_none() {
        eprintln!("benchmark:pressure_snapshot_requires_readiness_artifact");
        std::process::exit(2);
    }
    let report = read_json::<BenchmarkReport>(&report_path, "report");
    let summary = read_json::<BenchmarkFailureSummary>(&summary_path, "summary");
    if let Some(scale_path) = &scale_path {
        let scale_inputs = read_json::<BenchmarkScaleOptimizationInputs>(scale_path, "scale");
        match matrixraft_validate_benchmark_scale_optimization_inputs(&scale_inputs, &report) {
            Ok(()) => {
                println!("BaselineRaft-vs-RustRaft scale artifact verified");
            }
            Err(error) => {
                eprintln!("BaselineRaft-vs-RustRaft scale artifact failed: {error}");
                std::process::exit(1);
            }
        }
    }
    match matrixraft_assert_production_baseline_raft_artifacts(&report, &summary) {
        Ok(()) => {
            println!("BaselineRaft-vs-RustRaft benchmark artifacts verified");
        }
        Err(error) => {
            eprintln!("BaselineRaft-vs-RustRaft benchmark artifacts failed: {error}");
            std::process::exit(1);
        }
    }
    if let (Some(readiness_input_path), Some(readiness_artifact_path)) =
        (&readiness_input_path, &readiness_artifact_path)
    {
        let readiness_input =
            read_json::<ProductionReadinessInput>(readiness_input_path, "readiness_input");
        let readiness_artifact = read_json::<BenchmarkRuntimePressureReadinessArtifact>(
            readiness_artifact_path,
            "readiness",
        );
        let expected_labels = readiness_labels
            .as_deref()
            .map(parse_prometheus_labels)
            .unwrap_or_else(|| readiness_artifact.labels.clone());
        let readiness_label_refs = expected_labels
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let pressure_snapshot =
            release_pressure_snapshot_from_path_or_env(pressure_snapshot_path.as_ref());
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
            .unwrap_or_else(|| {
                read_backlog_metrics_from_inputs(
                    pending_read_index_requests,
                    pending_bounded_stale_reads,
                )
            });
        let read_backlog_thresholds = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.read_backlog_thresholds.clone())
            .unwrap_or_else(|| {
                read_backlog_thresholds_from_inputs(
                    pending_read_index_warning,
                    pending_bounded_stale_read_warning,
                )
            });
        let timer_status = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.timer_status.clone())
            .unwrap_or_else(|| {
                runtime_timer_status_from_inputs(
                    runtime_timer_pending_ticks,
                    runtime_timer_max_pending_ticks,
                    runtime_timer_accepted_ticks,
                    runtime_timer_rejected_ticks,
                    runtime_timer_completed_ticks,
                    runtime_timer_last_admission_reason,
                )
            });
        let timer_thresholds = pressure_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.timer_thresholds.clone())
            .unwrap_or_else(|| {
                timer_thresholds_from_inputs(runtime_timer_utilization_warning_percent)
            });
        match matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer(
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
        ) {
            Ok(()) => {
                println!("BaselineRaft-vs-RustRaft readiness artifact verified");
            }
            Err(error) => {
                eprintln!("BaselineRaft-vs-RustRaft readiness artifact failed: {error}");
                std::process::exit(1);
            }
        }
    }
}

fn parse_max_age_seconds(value: &str) -> u64 {
    match value.parse::<u64>() {
        Ok(seconds) if seconds > 0 => seconds,
        _ => {
            eprintln!("benchmark:invalid_max_artifact_age_seconds:{value}");
            std::process::exit(2);
        }
    }
}

fn parse_required_u64_arg(flag: &str, value: Option<&str>) -> u64 {
    let Some(value) = value else {
        eprintln!("missing value for {flag}");
        usage();
        std::process::exit(2);
    };
    value.parse::<u64>().unwrap_or_else(|error| {
        eprintln!("invalid {flag} {value}: {error}");
        std::process::exit(2);
    })
}

fn usage() {
    eprintln!(
        "Usage: baseline_raft_parity_verify --report PATH --summary PATH [--scale PATH] [--readiness-input PATH --readiness-artifact PATH --readiness-labels key=value,... --pressure-snapshot PATH] [--pending-read-index-requests N --pending-bounded-stale-reads N --runtime-timer-pending-ticks N --runtime-timer-max-pending-ticks N --runtime-timer-rejected-ticks N] [--max-age-seconds N]"
    );
}

fn artifact_path_collision(label: &str, path: &PathBuf) -> ! {
    eprintln!(
        "benchmark:artifact_path_collision:{label}:{}",
        path.display()
    );
    std::process::exit(2);
}

fn assert_fresh_artifact(path: &PathBuf, label: &str, max_age_seconds: u64) {
    let metadata = fs::metadata(path).unwrap_or_else(|error| {
        eprintln!("failed to stat {label} {}: {error}", path.display());
        std::process::exit(2);
    });
    if !metadata.is_file() {
        eprintln!("benchmark:artifact_not_file:{label}:{}", path.display());
        std::process::exit(2);
    }
    if metadata.len() == 0 {
        eprintln!("benchmark:artifact_empty:{label}:{}", path.display());
        std::process::exit(2);
    }
    let modified = metadata.modified().unwrap_or_else(|error| {
        eprintln!("failed to stat {label} {}: {error}", path.display());
        std::process::exit(2);
    });
    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or_else(|_| Duration::from_secs(0));
    if age.as_secs() > max_age_seconds {
        eprintln!(
            "BaselineRaft-vs-RustRaft benchmark artifacts failed: benchmark:artifact_stale:{label}:age_seconds={}:max_age_seconds={}",
            age.as_secs(),
            max_age_seconds
        );
        std::process::exit(1);
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &PathBuf, label: &str) -> T {
    let bytes = fs::read(path).unwrap_or_else(|error| {
        eprintln!("failed to read {label} {}: {error}", path.display());
        std::process::exit(2);
    });
    serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        eprintln!("failed to parse {label} {}: {error}", path.display());
        std::process::exit(2);
    })
}

fn parse_prometheus_labels(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let Some((key, value)) = entry.split_once('=') else {
                eprintln!(
                    "invalid --readiness-labels entry {entry:?}; expected comma-separated key=value"
                );
                std::process::exit(2);
            };
            let key = key.trim();
            if key.is_empty() {
                eprintln!("invalid --readiness-labels entry {entry:?}; label key is empty");
                std::process::exit(2);
            }
            (key.to_string(), value.trim().to_string())
        })
        .collect()
}

fn release_pressure_snapshot_from_path_or_env(
    path: Option<&PathBuf>,
) -> Option<ReleasePressureSnapshot> {
    path.cloned()
        .or_else(|| {
            env::var("RUSTRAFT_BENCHMARK_PRESSURE_SNAPSHOT")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(PathBuf::from)
        })
        .map(|path| {
            matrixraft_read_release_pressure_snapshot(&path).unwrap_or_else(|error| {
                eprintln!("{error}");
                std::process::exit(2);
            })
        })
}

fn memory_metrics_from_env() -> MemoryMetrics {
    let mut metrics = MemoryMetrics::zero();
    metrics.process_resident_memory_bytes =
        parse_u64_env("RUSTRAFT_BENCHMARK_PROCESS_RESIDENT_MEMORY_BYTES")
            .unwrap_or(metrics.process_resident_memory_bytes);
    metrics.heap_allocated_bytes = parse_u64_env("RUSTRAFT_BENCHMARK_HEAP_ALLOCATED_BYTES")
        .unwrap_or(metrics.heap_allocated_bytes);
    metrics.log_cache_bytes =
        parse_u64_env("RUSTRAFT_BENCHMARK_LOG_CACHE_BYTES").unwrap_or(metrics.log_cache_bytes);
    metrics.snapshot_buffer_bytes = parse_u64_env("RUSTRAFT_BENCHMARK_SNAPSHOT_BUFFER_BYTES")
        .unwrap_or(metrics.snapshot_buffer_bytes);
    metrics.replication_buffer_bytes = parse_u64_env("RUSTRAFT_BENCHMARK_REPLICATION_BUFFER_BYTES")
        .unwrap_or(metrics.replication_buffer_bytes);
    metrics
}

fn memory_thresholds_from_env() -> MemoryOptimizationThresholds {
    let mut thresholds = MemoryOptimizationThresholds::default();
    thresholds.process_resident_warning_bytes =
        parse_u64_env("RUSTRAFT_BENCHMARK_PROCESS_RESIDENT_WARNING_BYTES")
            .unwrap_or(thresholds.process_resident_warning_bytes);
    thresholds.heap_allocated_warning_bytes =
        parse_u64_env("RUSTRAFT_BENCHMARK_HEAP_ALLOCATED_WARNING_BYTES")
            .unwrap_or(thresholds.heap_allocated_warning_bytes);
    thresholds.log_cache_warning_bytes =
        parse_u64_env("RUSTRAFT_BENCHMARK_LOG_CACHE_WARNING_BYTES")
            .unwrap_or(thresholds.log_cache_warning_bytes);
    thresholds.snapshot_buffer_warning_bytes =
        parse_u64_env("RUSTRAFT_BENCHMARK_SNAPSHOT_BUFFER_WARNING_BYTES")
            .unwrap_or(thresholds.snapshot_buffer_warning_bytes);
    thresholds.replication_buffer_warning_bytes =
        parse_u64_env("RUSTRAFT_BENCHMARK_REPLICATION_BUFFER_WARNING_BYTES")
            .unwrap_or(thresholds.replication_buffer_warning_bytes);
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
    thresholds.append_p99_warning_ms = parse_u64_env("RUSTRAFT_BENCHMARK_APPEND_P99_WARNING_MS")
        .unwrap_or(thresholds.append_p99_warning_ms);
    thresholds.vote_p99_warning_ms = parse_u64_env("RUSTRAFT_BENCHMARK_VOTE_P99_WARNING_MS")
        .unwrap_or(thresholds.vote_p99_warning_ms);
    thresholds.pre_vote_p99_warning_ms =
        parse_u64_env("RUSTRAFT_BENCHMARK_PRE_VOTE_P99_WARNING_MS")
            .unwrap_or(thresholds.pre_vote_p99_warning_ms);
    thresholds.read_index_p99_warning_ms =
        parse_u64_env("RUSTRAFT_BENCHMARK_READ_INDEX_P99_WARNING_MS")
            .unwrap_or(thresholds.read_index_p99_warning_ms);
    thresholds.snapshot_install_p99_warning_ms =
        parse_u64_env("RUSTRAFT_BENCHMARK_SNAPSHOT_INSTALL_P99_WARNING_MS")
            .unwrap_or(thresholds.snapshot_install_p99_warning_ms);
    thresholds
}

fn latency_histogram_from_p99_env(name: &str, default: LatencyHistogram) -> LatencyHistogram {
    let Some(p99_ms) = parse_u64_env(name) else {
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

fn read_backlog_metrics_from_inputs(
    pending_read_index_requests: Option<u64>,
    pending_bounded_stale_reads: Option<u64>,
) -> ReadBacklogMetrics {
    let mut metrics = ReadBacklogMetrics::zero();
    metrics.pending_read_index_requests = pending_read_index_requests
        .or_else(|| parse_u64_env("RUSTRAFT_BENCHMARK_PENDING_READ_INDEX_REQUESTS"))
        .unwrap_or(metrics.pending_read_index_requests);
    metrics.pending_bounded_stale_reads = pending_bounded_stale_reads
        .or_else(|| parse_u64_env("RUSTRAFT_BENCHMARK_PENDING_BOUNDED_STALE_READS"))
        .unwrap_or(metrics.pending_bounded_stale_reads);
    metrics
}

fn read_backlog_thresholds_from_inputs(
    pending_read_index_warning: Option<u64>,
    pending_bounded_stale_read_warning: Option<u64>,
) -> ReadBacklogThresholds {
    let mut thresholds = ReadBacklogThresholds::default();
    thresholds.pending_read_index_warning = pending_read_index_warning
        .or_else(|| parse_u64_env("RUSTRAFT_BENCHMARK_PENDING_READ_INDEX_WARNING"))
        .unwrap_or(thresholds.pending_read_index_warning);
    thresholds.pending_bounded_stale_read_warning = pending_bounded_stale_read_warning
        .or_else(|| parse_u64_env("RUSTRAFT_BENCHMARK_PENDING_BOUNDED_STALE_READ_WARNING"))
        .unwrap_or(thresholds.pending_bounded_stale_read_warning);
    thresholds
}

fn runtime_timer_status_from_inputs(
    pending_ticks: Option<u64>,
    max_pending_ticks: Option<u64>,
    accepted_ticks: Option<u64>,
    rejected_ticks: Option<u64>,
    completed_ticks: Option<u64>,
    last_admission_reason: Option<String>,
) -> matrixraft::RuntimeTimerStatus {
    let mut status = matrixraft_release_benchmark_runtime_timer_status();
    status.pending_ticks = pending_ticks
        .or_else(|| parse_u64_env("RUSTRAFT_BENCHMARK_RUNTIME_TIMER_PENDING_TICKS"))
        .unwrap_or(status.pending_ticks);
    status.max_pending_ticks = max_pending_ticks
        .or_else(|| parse_u64_env("RUSTRAFT_BENCHMARK_RUNTIME_TIMER_MAX_PENDING_TICKS"))
        .unwrap_or(status.max_pending_ticks);
    status.accepted_ticks = accepted_ticks
        .or_else(|| parse_u64_env("RUSTRAFT_BENCHMARK_RUNTIME_TIMER_ACCEPTED_TICKS"))
        .unwrap_or(status.accepted_ticks);
    status.rejected_ticks = rejected_ticks
        .or_else(|| parse_u64_env("RUSTRAFT_BENCHMARK_RUNTIME_TIMER_REJECTED_TICKS"))
        .unwrap_or(status.rejected_ticks);
    status.completed_ticks = completed_ticks
        .or_else(|| parse_u64_env("RUSTRAFT_BENCHMARK_RUNTIME_TIMER_COMPLETED_TICKS"))
        .unwrap_or(status.completed_ticks);
    status.last_tick_admission_reason = last_admission_reason
        .or_else(|| env::var("RUSTRAFT_BENCHMARK_RUNTIME_TIMER_LAST_ADMISSION_REASON").ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(status.last_tick_admission_reason);
    status
}

fn timer_thresholds_from_inputs(
    utilization_warning_percent: Option<u64>,
) -> NodeRuntimeTimerThresholds {
    let mut thresholds = NodeRuntimeTimerThresholds::default();
    thresholds.utilization_warning_percent = utilization_warning_percent
        .or_else(|| parse_u64_env("RUSTRAFT_BENCHMARK_RUNTIME_TIMER_UTILIZATION_WARNING_PERCENT"))
        .unwrap_or(thresholds.utilization_warning_percent);
    thresholds
}

fn parse_u64_env(name: &str) -> Option<u64> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value.parse::<u64>().unwrap_or_else(|error| {
                eprintln!("invalid {name}={value}: {error}");
                std::process::exit(2);
            })
        })
}
