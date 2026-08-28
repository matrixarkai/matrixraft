// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::benchmark::{
    matrixraft_assert_production_baseline_raft_artifacts, RustRaftBenchmarkFailureSummary,
    RustRaftBenchmarkReport,
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
    let mut max_age_seconds = env::var("RUSTRAFT_BENCHMARK_MAX_ARTIFACT_AGE_SECONDS")
        .ok()
        .map(|value| parse_max_age_seconds(&value))
        .unwrap_or(DEFAULT_MAX_ARTIFACT_AGE_SECONDS);
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--report" => report_path = args.next().map(PathBuf::from),
            "--summary" => summary_path = args.next().map(PathBuf::from),
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

    assert_fresh_artifact(&report_path, "report", max_age_seconds);
    assert_fresh_artifact(&summary_path, "summary", max_age_seconds);
    let report = read_json::<RustRaftBenchmarkReport>(&report_path, "report");
    let summary = read_json::<RustRaftBenchmarkFailureSummary>(&summary_path, "summary");
    match matrixraft_assert_production_baseline_raft_artifacts(&report, &summary) {
        Ok(()) => {
            println!("BaselineRaft-vs-RustRaft benchmark artifacts verified");
        }
        Err(error) => {
            eprintln!("BaselineRaft-vs-RustRaft benchmark artifacts failed: {error}");
            std::process::exit(1);
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

fn usage() {
    eprintln!(
        "Usage: baseline_raft_parity_verify --report PATH --summary PATH [--max-age-seconds N]"
    );
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
