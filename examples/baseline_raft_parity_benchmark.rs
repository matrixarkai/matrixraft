// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::benchmark::{
    matrixraft_assert_production_baseline_raft_parity,
    matrixraft_baseline_raft_benchmark_failure_summary, matrixraft_find_baseline_raft_harness,
    matrixraft_run_baseline_raft_parity_benchmark,
    matrixraft_validate_production_baseline_raft_benchmark_options,
    RustRaftBenchmarkFailureSummary, RustRaftBenchmarkOptions, RustRaftExternalBaselineRaftRunner,
    RustRaftRuntimeBenchmarkRunner,
};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

fn main() {
    let mut options = RustRaftBenchmarkOptions::default();
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
    let mut baseline_raft = match RustRaftExternalBaselineRaftRunner::new(
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
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new(build_profile);
    let report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);

    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if let Ok(summary_out) = std::env::var("RUSTRAFT_BENCHMARK_SUMMARY_OUT") {
        if let Err(error) = write_summary_artifact_atomic(Path::new(&summary_out), &summary) {
            eprintln!(
                "BaselineRaft parity benchmark failed to write summary {summary_out}: {error}"
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
    summary: &RustRaftBenchmarkFailureSummary,
) -> std::io::Result<()> {
    if let Some(parent) = summary_out
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let parent = summary_out.parent().unwrap_or_else(|| Path::new("."));
    let file_name = summary_out
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("summary.json");
    let tmp_path = parent.join(format!(".{file_name}.{}.tmp", std::process::id()));
    let mut tmp_file = File::create(&tmp_path)?;
    tmp_file.write_all(serde_json::to_string_pretty(summary).unwrap().as_bytes())?;
    tmp_file.sync_all()?;
    drop(tmp_file);
    if let Err(error) = fs::rename(&tmp_path, summary_out) {
        let _ = fs::remove_file(&tmp_path);
        return Err(error);
    }
    Ok(())
}
